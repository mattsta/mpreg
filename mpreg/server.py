import asyncio
import pprint as pp
import random
import sys
import time
import traceback
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from graphlib import TopologicalSorter
from typing import Any

import ulid
import websockets.client
import websockets.server
from loguru import logger

from .client_peer import MPREGClient
from .config import MPREGSettings
from .connection import Connection
from .model import (
    CommandNotFoundException,
    GossipMessage,
    PeerInfo,
    RPCCommand,
    RPCError,
    RPCInternalAnswer,
    RPCInternalRequest,
    RPCRequest,
    RPCResponse,
    RPCServerMessage,
    RPCServerRequest,
)
from .registry import Command, CommandRegistry
from .serialization import JsonSerializer

############################################
#
# Fancy Setup
#
############################################

# Use more efficient coroutine logic if available
# https://docs.python.org/3.12/library/asyncio-task.html#asyncio.eager_task_factory
asyncio.get_event_loop().set_task_factory(asyncio.eager_task_factory)

# maximum 4 GB messages should be enough for anybody, right?
MPREG_DATA_MAX = 2**32


def rpc_command(name: str, resources: Iterable[str] | None = None) -> Callable:
    """Decorator to register a function as an RPC command.

    Args:
        name: The name of the RPC command.
        resources: Optional iterable of resource strings associated with the command.

    Returns:
        A decorator that registers the function as an RPC command.
    """

    def decorator(func: Callable) -> Callable:
        # This is a placeholder. The actual registration will happen when the MPREGServer is initialized.
        # We store the metadata on the function itself.
        setattr(func, "_rpc_command_name", name)
        setattr(
            func,
            "_rpc_command_resources",
            frozenset(resources) if resources is not None else frozenset(),
        )
        return func

    return decorator


############################################
#
# Default commands for all servers
#
############################################
@rpc_command(name="echo", resources=[])
def echo(arg):
    """Default echo handler for all servers.

    Single-argument echo demo."""
    return arg


@rpc_command(name="echos", resources=[])
def echos(*args):
    """Default echos handler for all servers.

    Multi-argument echo demo."""
    return args


############################################
#
# RPC Management
#
############################################
@dataclass
class RPC:
    """Representation of a full RPC request hierarchy.

    Upon instantiation, we create the full call graph from our JSON RPC format
    so this entire RPC can be executed in the cluster."""

    req: RPCRequest

    def __post_init__(self) -> None:
        self.funs = {cmd.name: cmd for cmd in self.req.cmds}

        # We also have to resolve the funs to a call graph...
        sorter: TopologicalSorter = TopologicalSorter()

        for name, rpc_command in self.funs.items():
            # only attach child dependencies IF the argument matches a NAME in the entire RPC Request
            deps = filter(lambda x: x in self.funs, rpc_command.args)
            sorter.add(name, *deps)

        levels = []
        sorter.prepare()
        while sorter.is_active():
            level = []
            # iterate ONE CONCURRENT LEVEL of execution
            for idx, task in enumerate(sorter.get_ready()):
                level.append(task)
                sorter.done(task)
            else:
                # append this concurrent execution level to the call order
                levels.append(level)

        # logger.info("Runnable levels are: {}", levels)
        self.levels = levels

    def tasks(self):
        yield from self.levels


############################################
#
# MPREG Server Instance
#
############################################


############################################
#
# Cluster Representation and Servers Management
#
############################################
@dataclass
class Cluster:
    cluster_id: str
    advertised_urls: tuple[str, ...]
    # map of RPC names to resources to hosts
    # e.g. {"rpc-name": {"resource-1": set(servers), "resource-2": set(servers), ...}}
    funtimes: dict[str, dict[frozenset[str], set[str]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(set))
    )

    # collection of all servers for easy reference
    servers: set[str] = field(default_factory=set)

    # Information about known peers in the cluster, for gossip protocol.
    # Key: peer URL, Value: PeerInfo object.
    peers_info: dict[str, PeerInfo] = field(default_factory=dict)
    dead_peer_timeout: float = field(default=30.0)
    registry: CommandRegistry = field(init=False)
    serializer: JsonSerializer = field(init=False)

    def __post_init__(self) -> None:
        self.waitingFor: dict[str, asyncio.Event] = dict()
        self.answer: dict[str, Any] = dict()
        # These will be set by MPREGServer
        # self.registry = CommandRegistry()
        # self.serializer = JsonSerializer()

    def add_fun_ability(self, peer_info: PeerInfo):
        """Add a new function and resource(s) to a server mapping.

        This method updates the cluster's knowledge of which functions are available
        on which servers and with what resources.
        """
        for fun in peer_info.funs:
            self.funtimes[fun][peer_info.locs].add(peer_info.url)
        self.servers.add(peer_info.url)
        self.peers_info[peer_info.url] = peer_info

    def remove_server(self, server: Connection):
        """If server disconnects, remove it from ALL fun mappings.

        Note: this may leave some empty {fun: {resource: set()}} but it's okay"""

        # iterate everything and remove matching server from all sets
        for fun, resources in self.funtimes.items():
            for locations, servers in resources.items():
                servers.discard(server.url)  # Changed to server.url

        self.servers.discard(server.url)

    def server_cmd(
        self, server_connection: Connection, cmd: RPCServerMessage
    ) -> RPCResponse:
        """A remote server is telling us about itself.

        This method processes incoming server-to-server messages, such as HELLO,
        GOODBYE, or STATUS updates, and updates the cluster's state accordingly.
        """
        # logger.info("New command here too: {}", cmd)
        match cmd.what:
            case "HELLO":
                # A new server is announcing its capabilities.
                # Add its functions and locations to the cluster's funtimes mapping.
                peer_info = PeerInfo(
                    url=server_connection.url,
                    funs=cmd.funs,
                    locs=frozenset(cmd.locs),
                    last_seen=time.time(),
                    cluster_id=cmd.cluster_id,
                    advertised_urls=cmd.advertised_urls,
                )
                self.add_fun_ability(peer_info)
                return RPCResponse(r="ADDED", u=str(ulid.new()))
            case "GOODBYE":
                # TODO: also remove on any error/disconnect in other places.......
                self.remove_server(server_connection)
                return RPCResponse(r="GONE", u=str(ulid.new()))
            case "STATUS":
                ...
                return RPCResponse(r="STATUS", u=str(ulid.new()))
            case _:
                assert None

    def process_gossip_message(self, gossip_message: GossipMessage):
        """Processes an incoming gossip message, updating local peer information.

        This method iterates through the peers in the gossip message and updates
        the local peers_info, adding new peers or updating existing ones if
        the received information is more recent.
        """
        for peer_info in gossip_message.peers:
            # Only process if the cluster_id matches
            if peer_info.cluster_id != self.cluster_id:
                logger.warning(
                    "Received gossip from different cluster ID: Expected {}, Got {}",
                    self.cluster_id,
                    peer_info.cluster_id,
                )
                continue

            if (
                peer_info.url not in self.peers_info
                or peer_info.last_seen > self.peers_info[peer_info.url].last_seen
            ):
                logger.info("Updating peer info for {}: {}", peer_info.url, peer_info)
                self.peers_info[peer_info.url] = peer_info

        # Prune dead peers: remove peers not updated recently
        current_time = time.time()
        peers_to_remove = [
            url
            for url, info in self.peers_info.items()
            if current_time - info.last_seen > self.dead_peer_timeout
        ]
        for url in peers_to_remove:
            logger.info("Removing stale peer: {}", url)
            del self.peers_info[url]

    def server_for(
        self, fun: str, locs: frozenset[str]
    ) -> str | None:  # Changed return type
        """Finds a suitable server for a given function and location.

        Args:
            fun: The name of the function to find a server for.
            locs: The frozenset of locations/resources required by the function.

        Returns:
            A Connection object to the suitable server, or None if no server is found.
        """
        # If no servers exist, we can't find anything anywhere.
        # NOTE: This should be impossible, because we register OURSELF as a server on startup.
        if not self.servers:
            return None

        # If no location requested, we can run ANYWHERE.
        if not locs:
            return random.choice(tuple(self.servers))

        # If an EXACT loc request exists in the rpc-capability map, use that single result.
        servers = self.funtimes[fun].get(locs)

        if not servers:
            # Else, no EXACT match, but try to discover a FULL match.
            # TODO: if this is too slow, we could also directly add all matching subset possibilities
            #       with something like:
            #       itertools.chain(*[itertools.combinations(loc, N) for N in range(1, len(loc) + 1)])
            for funlocs, srvs in self.funtimes[fun].items():
                if locs.issubset(funlocs):
                    # We found a match for ALL our requested resources (even if the target has MORE).
                    servers = srvs
                    break
            else:
                # Else, loop failed to break, so we have NO matching servers.
                servers = None

        # Random selection only works if found elements exist.
        if servers:
            return random.choice(tuple(servers))

        # Else, we tried everything and no matches were found.
        return None

    async def answerFor(self, rid: str):
        """Wait for a reply on unique id 'rid' then return the answer."""
        e = asyncio.Event()

        self.waitingFor[rid] = e

        # logger.info("[{}] Sleeping waiting for answer...", rid)
        await e.wait()

        # logger.info("Woke up!")
        del self.waitingFor[rid]

    async def _execute_local_command(
        self,
        rpc_command: RPCCommand,
        results: dict[str, Any],  # Added type hint
    ) -> Any:
        """Executes a command locally using the command registry.

        Args:
            rpc_command: The RPCCommand to execute locally.
            results: The current intermediate results of the RPC execution.

        Returns:
            The result of the local command execution.
        """
        return self.registry.get(rpc_command.fun)(  # Access registry via self.registry
            *rpc_command.args, **rpc_command.kwargs
        )

    async def _execute_remote_command(
        self, rpc_step: RPCCommand, results: dict[str, Any], where: Connection
    ) -> Any:
        """Sends a command to a remote server and waits for the response.

        Args:
            rpc_step: The RPCCommand to execute remotely.
            results: The current intermediate results of the RPC execution.
            where: The Connection object representing the remote server.

        Returns:
            The result of the remote command execution.

        Raises:
            ConnectionError: If the connection to the remote server is not open.
        """
        localrid = str(ulid.new())
        body = self.serializer.serialize(  # Access serializer via self.serializer
            RPCInternalRequest(
                command=rpc_step.fun,
                args=rpc_step.args,
                kwargs=rpc_step.kwargs,
                results=results,
                u=localrid,
            ).model_dump()
        )

        try:
            await where.send(body)
        except ConnectionError:
            logger.error(
                "[{}] Connection to remote server closed. Removing from services for now...",
                where.url,
            )
            self.remove_server(where)
            # Retry if the server was removed, as another might be available
            # This is a form of self-healing for the cluster.
            new_where = self.server_for(rpc_step.fun, rpc_step.locs)
            if new_where:
                return await self._execute_remote_command(
                    rpc_step, results, Connection(url=new_where)
                )  # Changed new_where to where
            else:
                raise ConnectionError(
                    f"No alternative server found for: {rpc_step.fun} at {rpc_step.locs}"
                )

        waiting = await asyncio.create_task(self.answerFor(localrid))
        got = self.answer[localrid]
        del self.answer[localrid]
        return got

    async def run(self, rpc) -> Any:
        """Run the RPC.

        Steps:
          - Look up location for all RPCs in current level.
          - Send RPCs for current level.
            - If only ONE RPC at this level, delegate REMAINDER OF CALLING to the single server.
            - If MORE THAN ONE RPC at this level, coordinate the next level.
          - Reply to client.
        """

        async def runner(
            rpc_command: RPCCommand, results: dict[str, Any]
        ) -> dict[str, Any]:
            """Executes a single RPC command, either locally or remotely.

            Args:
                rpc_command: The RPCCommand to execute.
                results: The current intermediate results of the RPC execution.

            Returns:
                A dictionary containing the result of the executed command.
            """
            where = self.server_for(rpc_command.fun, rpc_command.locs)
            if not where:
                logger.error("Sorry, no server found for: {}", rpc_command)
                raise CommandNotFoundException(command_name=rpc_command.fun)

            if where == "self":
                got = await self._execute_local_command(rpc_command, results)
            else:
                got = await self._execute_remote_command(
                    rpc_command, results, Connection(url=where)
                )  # Wrap string in Connection

            results[rpc_command.name] = got

            # TODO: this should return a STABLE DATACLASS OBJECT and not just a generic dict
            return {rpc_command.name: got}

        results: dict[str, Any] = {}  # Added type hint
        # logger.info("rpc is: {}", rpc)
        for level in rpc.tasks():
            # Note: EVERYTHING in the current level is parallelizable!
            cmds = []
            # logger.info("Tasks for level are: {}", level)
            for name in level:
                rpc_command = rpc.funs[name]
                # here, each 'cmd' is only command NAME we look up in the rpc to run with the resolved local args
                cmds.append(runner(rpc_command, results))

            got = await asyncio.gather(*cmds)

        result = {}
        for g in got:
            result.update(g)

        return result

        # using result.update() in a loop is twice as fast as this:
        # return {k: v for g in got for k, v in g.items()}


############################################
#
# Actual MPREG Cluster Server Entrypoint
#
############################################
@dataclass
class MPREGServer:
    """Main MPREG server application class."""

    settings: MPREGSettings = field(  # Changed from Field to field
        default_factory=MPREGSettings,
        metadata={"description": "Server configuration settings."},
    )

    def __post_init__(self) -> None:
        """Initializes the MPREGServer instance.

        Sets up the cluster, command registry, and client tracking.
        """
        self.registry = CommandRegistry()  # Moved registry initialization here
        self.serializer = JsonSerializer()  # Moved serializer initialization here
        self.cluster = Cluster(
            cluster_id=self.settings.cluster_id,
            advertised_urls=tuple(self.settings.advertised_urls or []),
        )
        self.cluster.registry = self.registry
        self.cluster.serializer = self.serializer
        self.clients: set[Connection] = set()  # Added type hint
        self.peer_clients: dict[str, MPREGClient] = {}

        # Register default commands and any commands decorated with @rpc_command
        self._register_default_commands()
        self._discover_and_register_rpc_commands()

    def report(self):
        """General report of current server state."""

        logger.info("Resources: {}", pp.pformat(self.settings.resources))
        # logger.info("Funs: {}", pp.pformat(self.settings.funs)) # Removed as funs is no longer in settings
        logger.info("Clients: {}", pp.pformat(self.clients))

    def _register_default_commands(self) -> None:
        """Registers the default RPC commands (echo, echos)."""
        self.register_command("echo", echo, [])
        self.register_command("echos", echos, [])

    def _discover_and_register_rpc_commands(self) -> None:
        """Discovers and registers RPC commands defined using the @rpc_command decorator.

        This method inspects the current module's global namespace for functions
        that have been decorated with @rpc_command and registers them with the server.
        """
        for name in dir(sys.modules[__name__]):
            obj = getattr(sys.modules[__name__], name)
            if callable(obj) and hasattr(obj, "_rpc_command_name"):
                rpc_name = getattr(obj, "_rpc_command_name")
                rpc_resources = getattr(obj, "_rpc_command_resources")
                self.register_command(rpc_name, obj, rpc_resources)
                logger.info(
                    "Registered RPC command: {} with resources {}",
                    rpc_name,
                    rpc_resources,
                )

    def run_server(
        self,
        server_connection: Connection,
        req: RPCServerRequest,
    ) -> RPCResponse:
        """Run a server-to-server command in the cluster.

        This method handles incoming server-to-server communication, such as
        new server announcements (HELLO), graceful shutdowns (GOODBYE), or
        status updates (STATUS).
        """
        try:
            # The 'what' field in the server message determines the action.
            # This uses Pydantic's validation to ensure the message structure is correct.
            match req.server.what:
                case "HELLO":
                    # A new server is announcing its capabilities.
                    # Add its functions and locations to the cluster's funtimes mapping.
                    peer_info = PeerInfo(
                        url=server_connection.url,
                        funs=req.server.funs,
                        locs=frozenset(req.server.locs),
                        last_seen=time.time(),
                        cluster_id=req.server.cluster_id,
                        advertised_urls=req.server.advertised_urls,
                    )
                    self.cluster.add_fun_ability(peer_info)
                    return RPCResponse(r="ADDED", u=req.u)
                case "GOODBYE":
                    # A server is gracefully shutting down.
                    # Remove it from all fun mappings in the cluster.
                    self.cluster.remove_server(server_connection)
                    return RPCResponse(r="GONE", u=req.u)
                case "STATUS":
                    # A server is sending a status update (e.g., for gossip protocol).\
                    # TODO: Implement actual status processing.
                    return RPCResponse(r="STATUS", u=req.u)
                case _:
                    # Handle unknown server message types.
                    return RPCResponse(
                        r=None,
                        error=RPCError(
                            code=1000,
                            message=f"Unknown server message type: {req.server.what}",
                        ),
                        u=req.u,
                    )
        except Exception as e:
            # Catch any exceptions during server command processing and return an error response.
            logger.exception("Error processing server command")
            return RPCResponse(
                r=None,
                error=RPCError(
                    code=1002,
                    message="Internal server error",
                    details=traceback.format_exc(),
                ),
                u=req.u,
            )

    async def run_rpc(self, req: RPCRequest) -> RPCResponse:
        """Run a client RPC request command in the cluster.

        This method takes an RPCRequest, constructs an RPC execution graph,
        and then runs the commands across the cluster.
        """
        try:
            # Create an RPC object from the incoming request. This handles
            # the topological sorting of commands.
            rpc = RPC(req)
            # Execute the RPC and return the results.
            return RPCResponse(r=await self.cluster.run(rpc), u=req.u)
        except Exception as e:
            # Catch any exceptions during RPC execution and return an error response.
            logger.exception("Error running RPC")
            return RPCResponse(
                r=None,
                error=RPCError(
                    code=1003,
                    message="RPC execution failed",
                    details=traceback.format_exc(),
                ),
                u=req.u,
            )

    @logger.catch
    async def opened(self, websocket: websockets.server.WebSocketServerProtocol):
        """Handles a new incoming websocket connection.

        This method is the entry point for all incoming messages, routing them
        to the appropriate handler based on their 'role'.
        """
        # Create a Connection object for the incoming websocket.
        connection = Connection(url=str(websocket.remote_address))
        connection.websocket = (
            websocket  # Assign the raw websocket to the connection object
        )

        try:
            self.clients.add(connection)
            async for msg in websocket:
                # Attempt to parse the incoming message into a Pydantic model.
                # This provides automatic validation and type conversion.
                parsed_msg = self.serializer.deserialize(
                    msg.encode("utf-8") if isinstance(msg, str) else msg
                )

                # logger.info("[{}:{}] Received: {}", websocket.host, websocket.port, parsed_msg)

                response_model: RPCResponse | RPCInternalAnswer | None = None
                match parsed_msg.get("role"):
                    case "server":
                        # SERVER-TO-SERVER communications packet
                        # (joining/leaving cluster, gossip updates of servers attached to other servers)
                        server_request = RPCServerRequest.model_validate(parsed_msg)
                        logger.info(
                            "[{}:{}] Server message: {}",
                            *websocket.remote_address,
                            server_request.model_dump_json(),
                        )
                        response_model = self.run_server(connection, server_request)
                        # Enforce cluster_id matching for HELLO messages
                        if (
                            server_request.server.what == "HELLO"
                            and server_request.server.cluster_id
                            != self.settings.cluster_id
                        ):
                            logger.warning(
                                "[{}:{}] Received HELLO from different cluster ID: Expected {}, Got {}",
                                *websocket.remote_address,
                                self.cluster.cluster_id,  # Changed to self.cluster.cluster_id
                                server_request.server.cluster_id,
                            )
                            # Close connection or return an error response
                            response_model = RPCResponse(
                                r=None,
                                error=RPCError(
                                    code=1005, message="Cluster ID mismatch"
                                ),
                                u=server_request.u,
                            )
                            await websocket.send(
                                self.serializer.serialize(response_model.model_dump())
                            )
                            return  # Terminate connection for mismatch

                    case "rpc":
                        # CLIENT request
                        rpc_request = RPCRequest.model_validate(parsed_msg)
                        logger.info(
                            "[{}:{} :: {}] Running request...",
                            *websocket.remote_address,
                            rpc_request.u,
                        )
                        response_model = await self.run_rpc(rpc_request)
                    case "internal-answer":
                        # REPLY from a previous INTERNAL-RPC request
                        internal_answer = RPCInternalAnswer.model_validate(parsed_msg)

                        # add answer globally for the consumer to read again
                        self.cluster.answer[internal_answer.u] = internal_answer.answer

                        # notify the waiting process we have an answer now
                        self.cluster.waitingFor[internal_answer.u].set()

                        # logger.info("[{}] Processed Internal Answer: {}", internal_answer.u, internal_answer.answer)

                        # no result here, this is returned upstream elsewhere
                        continue
                    case "internal-rpc":
                        # FORWARDED REQUEST from ANOTHER SERVER in MID-RPC mode.
                        # We know this request is FOR US since it was sent TO US directly.
                        internal_rpc = RPCInternalRequest.model_validate(parsed_msg)
                        command = internal_rpc.command
                        args = internal_rpc.args
                        kwargs = internal_rpc.kwargs
                        u = internal_rpc.u

                        # Generate RESULT PAYLOAD
                        answer_payload = self.registry.get(command)(*args, **kwargs)
                        response_model = RPCInternalAnswer(answer=answer_payload, u=u)

                        # logger.info("[{}] Generated answer: {}", u, answer_payload)

                    case "gossip":
                        # Incoming gossip message from a peer.
                        gossip_message = GossipMessage.model_validate(parsed_msg)
                        # Enforce cluster_id matching for gossip messages
                        if gossip_message.cluster_id != self.settings.cluster_id:
                            logger.warning(
                                "[{}:{}] Received gossip from different cluster ID: Expected {}, Got {}",
                                *websocket.remote_address,
                                self.cluster.cluster_id,  # Changed to self.cluster.cluster_id
                                gossip_message.cluster_id,
                            )
                            continue  # Ignore gossip from different cluster
                        self.cluster.process_gossip_message(gossip_message)
                        # Gossip messages do not typically require a direct response.
                        continue

                    case _:
                        # Handle unknown message roles.
                        logger.error(
                            "[{}:{}] Invalid RPC request role: {}",
                            *websocket.remote_address,
                            parsed_msg.get("role"),
                        )
                        response_model = RPCResponse(
                            r=None,
                            error=RPCError(
                                code=1004,
                                message=f"Invalid RPC request role: {parsed_msg.get('role')}",
                            ),
                            u=parsed_msg.get("u", "unknown"),
                        )

                # If a response model was generated, send it back to the client.
                if response_model:
                    try:
                        await websocket.send(
                            self.serializer.serialize(response_model.model_dump())
                        )
                    except Exception:
                        logger.error(
                            "[{}:{}] Client connection error! Dropping reply.",
                            *websocket.remote_address,
                        )
        finally:
            # TODO: if this was a SERVER, we need to clean up the server resources.
            # TODO: if this was a CLIENT, we need to cancel any oustanding requests/subscriptions too.
            self.clients.remove(connection)

    async def _establish_peer_connection(self, peer_url: str) -> None:
        """Establishes a connection to a single peer and adds it to peer_clients.

        Args:
            peer_url: The URL of the peer to connect to.
        """
        peer_client = self.peer_clients.get(peer_url)
        if (
            peer_client
            and peer_client.peer_connection
            and peer_client.peer_connection.is_connected
        ):
            logger.info(
                "[{}] Already connected to peer: {}", self.settings.name, peer_url
            )
            return

        logger.info(
            "[{}] Attempting to establish connection to peer: {}",
            self.settings.name,
            peer_url,
        )
        try:
            peer_client = MPREGClient(
                url=peer_url,
                registry=self.registry,
                serializer=self.serializer,
                local_funs=tuple(self.registry._commands.keys()),
                local_resources=frozenset(self.settings.resources or []),
                cluster_id=self.settings.cluster_id,
                local_advertised_urls=tuple(  # Changed to tuple
                    self.settings.advertised_urls
                    or [f"ws://{self.settings.host}:{self.settings.port}"]
                ),
            )
            await peer_client.connect()
            self.peer_clients[peer_url] = peer_client
            logger.info(
                "[{}] Successfully connected to peer: {}", self.settings.name, peer_url
            )
        except Exception as e:
            logger.error(
                "[{}] Failed to connect to peer {}: {}", self.settings.name, peer_url, e
            )

    async def _manage_peer_connections(self) -> None:
        """Periodically checks for new peers from gossip and establishes connections.

        This background task ensures that the server proactively connects to
        other known peers in the cluster, maintaining a robust mesh network.
        """
        while True:
            # Iterate through known peers from gossip and try to connect if not already connected.
            for peer_url, peer_info in list(
                self.cluster.peers_info.items()
            ):  # Use list to avoid RuntimeError during dict modification
                if (
                    peer_url != f"ws://{self.settings.host}:{self.settings.port}"
                    and peer_url not in self.peer_clients
                ):
                    # Only connect if the cluster_id matches
                    if peer_info.cluster_id == self.settings.cluster_id:
                        await self._establish_peer_connection(peer_url)
                    else:
                        logger.warning(
                            "[{}] Discovered peer {} with mismatched cluster ID: {}",
                            self.settings.name,
                            peer_url,
                            peer_info.cluster_id,
                        )

            # Periodically check for new peers.
            await asyncio.sleep(self.settings.gossip_interval)

    def register_command(
        self, name: str, func: Callable[..., Any], resources: Iterable[str]
    ) -> None:
        """Register a command with the server.

        Args:
            name: The name of the command.
            func: The callable function that implements the command.
            resources: An iterable of resource strings associated with the command.
        """
        self.registry.register(Command(name, func))
        peer_info = PeerInfo(
            url=f"ws://{self.settings.host}:{self.settings.port}",
            funs=(name,),
            locs=frozenset(resources),
            last_seen=time.time(),
            cluster_id=self.settings.cluster_id,
            advertised_urls=tuple(
                self.settings.advertised_urls
                or [f"ws://{self.settings.host}:{self.settings.port}"]
            ),
        )
        self.cluster.add_fun_ability(peer_info)

    async def server(self) -> None:
        """Starts the MPREG server and handles incoming and outgoing connections.

        This method sets up the websocket server, registers default commands,
        and manages connections to other peers if specified in the settings.
        """
        logger.info(
            "[{}:{}] [{}] Launching server...",
            self.settings.host,
            self.settings.port,
            self.settings.name,
        )

        # Register OURSELF with the global echo target.
        # The resources for these default commands are empty, meaning they are available globally.
        self._register_default_commands()  # Call _register_default_commands here
        self._discover_and_register_rpc_commands()  # Call _discover_and_register_rpc_commands here

        # Start a background task to manage peer connections based on gossip.
        self._peer_connection_manager_task = asyncio.create_task(
            self._manage_peer_connections()
        )

        async with websockets.server.serve(
            self.opened,
            self.settings.host,
            self.settings.port,
            max_size=None,
            max_queue=None,
            read_limit=MPREG_DATA_MAX,
            write_limit=MPREG_DATA_MAX,
        ):
            # If no external connection requested, run an infinite wait here
            # to keep the server alive.
            if not self.settings.peers and not self.settings.connect:
                await asyncio.Future()
                return

            # If static peers are configured, establish initial connections.
            if self.settings.peers:
                for peer_url in self.settings.peers:
                    await self._establish_peer_connection(peer_url)

            # Keep the server running indefinitely.
            await asyncio.Future()

    def start(self) -> None:
        try:
            asyncio.run(self.server())
        except KeyboardInterrupt:
            logger.warning("Thanks for playing!")


@logger.catch
def cmd():
    """You can run an MPREG Server standalone without embedding into a process.

    Running standalone allows you to run server(s) as pure forwarding agents (except for the global shared default commands).
    """

    import jsonargparse

    # Load settings using Pydantic-settings. This will automatically read from
    # environment variables or a .env file if present.
    settings = MPREGSettings()

    # Create an MPREGServer instance with the loaded settings.
    server_instance = MPREGServer(settings=settings)

    # Use jsonargparse to allow command-line overriding of settings.
    # This integrates with Pydantic-settings to provide a robust configuration system.
    jsonargparse.CLI(server_instance, as_dict=False)
