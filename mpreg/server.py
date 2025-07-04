import asyncio
import pprint as pp

import random
import sys
import traceback
from collections import defaultdict
from dataclasses import dataclass, field

from graphlib import TopologicalSorter
from pathlib import Path

from typing import Any, Callable, Iterable, Mapping

import orjson
import ulid
import websockets.client
import websockets.server

from loguru import logger

from .model import RPCCommand


############################################
#
# Fancy Setup
#
############################################

# Use more efficient coroutine logic if available
# https://docs.python.org/3.12/library/asyncio-task.html#asyncio.eager_task_factory
if sys.version_info >= (3, 12):
    asyncio.get_event_loop().set_task_factory(asyncio.eager_task_factory)

# maximum 4 GB messages should be enough for anybody, right?
MPREG_DATA_MAX = 2**32


############################################
#
# Default commands for all servers
#
############################################
def echo(arg):
    """Default echo handler for all servers.

    Single-argument echo demo."""
    return arg


def echos(*args):
    """Default echos handler for all servers.

    Multi-argument echo demo."""
    return args


############################################
#
# RPC Management
#
############################################
@dataclass(slots=True)
class Command:
    name: str
    fun: list[Callable[Any, Any]]

    def __post_init__(self) -> None:
        assert (
            len(self.fun) == 1
        ), f"Sorry, the 'fun' input must have ONLY length 1 but found length {len(self.fun)=}!"

    def call(self, args, results) -> Any:
        # replace argument in arg map with RESULT (or leave as arg if not a result mapper)
        # this means: args must have the same result name as the arg name everywhere.
        for idx, arg in enumerate(args):
            args[idx] = results.get(arg, args[idx])

        # this is a hack so we don't call self.fun() which would make python call self.fun(self, *args)
        fun = self.fun[0]
        # TODO: implement kwargs with an additional 'kwargs' dict in the rpc schema?
        # logger.info("[{}] Calling: {}({})", self.name, fun, args)

        got = fun(*args)
        # logger.info("[{}] Result: {}", self.name, got)

        return got


@dataclass(slots=True)
class RPCStep:
    """RPC Step is one row of an RPC for the distributed system."""

    name: str
    command: RPCCommand

    me: str = field(default_factory=lambda: str(ulid.new()))


    def call(self, results) -> Any:
        return self.command.call(self.args, results)


@dataclass
class RPC:
    """Representation of a full RPC request hierarchy.

    Upon instantiation, we create the full call graph from our JSON RPC format
    so this entire RPC can be executed in the cluster."""

    req: list[Any]

    def __post_init__(self) -> None:
        self.funs = {
            row["name"]: RPCStep(
                row["name"],
                RPCCommand(
                    row["name"], row["fun"], row["args"], frozenset(row["locs"])
                ),
            )
            for row in self.req["cmds"]
        }

        # We also have to resolve the funs to a call graph...
        sorter = TopologicalSorter()

        for name, rpc in self.funs.items():
            # only attach child dependencies IF the argument matches a NAME in the entire RPC Request
            deps = filter(lambda x: x in self.funs, rpc.command.args)
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
@dataclass
class Server:
    """Representation of one server in the cluster.

    Purpose: maintains contact with remote servers for command sending and result receiving.
    """

    host: str
    port: int

    def __post_init__(self) -> None:
        # TODO: maybe just allow URL to be passed in completely
        self.url = f"ws://{self.host}:{self.port}"
        self.ws = None

    async def connect(self):
        if self.ws:
            await self.ws.close()
            self.ws = None

        # https://websockets.readthedocs.io/en/stable/reference/asyncio/client.html
        self.ws = await websockets.connect(self.url, user_agent_header=None)

    async def call(self, cmd, args):
        """Send this command request to the server and return reply."""

        # if server IS THIS HOST, we run command DIRECTLY (because re-sending it to us won't help!)

        # else, we request from REMOTE SOURCE
        while True:
            try:
                cmdfmt = ...
                return await self.ws.send(cmdfmt)
            except websockets.ConnectionClosedOK:
                # connection gone, reconnect.......
                logger.error(
                    "[{}] websocket connection closed. Reconnecting...", self.url
                )
                self.ws = None
                await self.connect()
            except:
                # TODO maybe re-connect if this is a websocket error.........
                logger.exception("Failed to send? Retrying...")
                await asyncio.sleep(1)


############################################
#
# Cluster Representation and Servers Management
#
############################################
@dataclass
class Cluster:
    # map of RPC names to resources to hosts
    # e.g. {"rpc-name": {"resource-1": set(servers), "resource-2": set(servers), ...}}
    funtimes: dict[dict[frozenset[str], set[str]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(set))
    )

    # collection of all servers for easy reference
    servers: set[str] = field(default_factory=set)

    # for OURSELF, a map of command names to command implementations
    # (base case of the cluster distribution mechanism so we _can_ find targets eventually)
    self: dict[str, Command] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(set))
    )

    def __post_init__(self) -> None:
        self.waitingFor = dict()
        self.answer = dict()
        ...

    def add_self_ability(
        self, fun: str, call: Callable[Any, Any], resources: Iterable[str]
    ):
        """Add an ability for THIS SERVER so we have a final command resolve point."""
        logger.info("Adding local behavior: {} ({}) -> {}", fun, resources, call)
        self.self[fun] = Command(fun, call)

        # also tell the cluster we exist for routing this new local command
        self.add_fun_ability("self", fun, resources)

    def add_fun_ability(self, server, fun: str, resources: Iterable[str]):
        """Add a new function and resource(s) to a server mapping."""
        # TODO: figure out what these server objects are... index into a server connector map? probably on direct clients?

        self.funtimes[fun][frozenset(resources)].add(server)
        self.servers.add(server)

    def remove_server(self, server):
        """If server disconnects, remove it from ALL fun mappings.

        Note: this may leave some empty {fun: {resource: set()}} but it's okay"""

        # iterate everything and remove matching server from all sets
        for fun, resources in self.funtimes.items():
            for locations, servers in resources.items():
                servers.discard(server)

        self.servers.discard(server)

    def server_cmd(self, server_websocket, cmd):
        """A remote server is telling us about itself.

        # TODO: this should be creating a Server() object in the fun mappings instead of
        #       adding WEBSOCKTS directly to the mappings! We can't reconnect to a "websocket" object
        #       because those were established INBOUND ONLY and they aren't feeding us their own listening
        #       details yet.

        Potential actions here are:
          - HELLO: new connection advertising capabilites
          - GOODBYE: clean shutdown
          - STATUS: gossip local status/metrics around
        """

        # logger.info("New command here too: {}", cmd)
        match cmd["what"]:
            case "HELLO":
                # HELLO has a list of LOCATION and a list of FUNS for those locations
                for fun in cmd["funs"]:
                    self.add_fun_ability(server_websocket, fun, cmd["locs"])

                return "ADDED"
            case "GOODBYE":
                # TODO: also remove on any error/disconnect in other places.......
                self.remove_server(server_websocket)
                return "GONE"
            case "STATUS":
                ...
                return "STATUS"
            case _:
                assert None

    def server_for(self, fun, locs):
        server = None

        # if no servers exist, we can't find anything anywhere
        # NOTE: this should be impossible, because we register OURSELF as a server on startup...
        if not self.servers:
            return None

        # if no location requested, we can run ANYWHERE
        if not locs:
            return random.choice(tuple(self.servers))

        # if EXACT loc request exists in the rpc-capability map, use single result
        servers = self.funtimes[fun][locs]

        if not servers:
            # else, no EXACT match, but try to discover a FULL match?
            # TODO: if this is too slow, we could also directly add all matching subset possibilities
            #       with something like:
            #       itertools.chain(*[itertools.combinations(loc, N) for N in range(1, len(loc) + 1)])
            for funlocs, servers in self.funtimes[fun].items():
                if locs & funlocs == locs:
                    # we found a match for ALL our requested resources (even if the target has MORE)
                    break
            else:
                # else, loop failed to break, so we have NO matching servers
                servers = None

        # random selection only works if found elements exist
        if servers:
            return random.choice(tuple(servers))

        # else, we tried everything and no matches were found
        return None

    async def answerFor(self, rid: str):
        """Wait for a reply on unique id 'rid' then return the answer."""
        e = asyncio.Event()

        self.waitingFor[rid] = e

        # logger.info("[{}] Sleeping waiting for answer...", rid)
        await e.wait()

        # logger.info("Woke up!")
        del self.waitingFor[rid]

    async def run(self, rpc) -> Any:
        """Run the RPC.

        Steps:
          - Look up location for all RPCs in current level.
          - Send RPCs for current level.
            - If only ONE RPC at this level, delegate REMAINDER OF CALLING to the single server.
            - If MORE THAN ONE RPC at this level, coordinate the next level.
          - Reply to client.
        """

        async def runner(rpc_step, results):
            where = self.server_for(rpc_step.command.fun, rpc_step.command.locs)
            if not where:
                logger.error("Sorry, no server found for: {}", rpc_step)
                raise Exception(f"No server found for: {rpc_step}")

            # logger.info("[{}] Running {}", where, cmd)
            if where == "self":
                # LOCAL call
                got = self.self[rpc_step.command.fun].call(rpc_step.command.args, results)
            else:
                # REMOTE call (another websocket call, "where" must be a server-websocket thing doing requests)
                localrid = str(ulid.new())
                body = orjson.dumps(
                    dict(
                        role="internal-rpc",
                        internal=dict(
                            command=rpc_step.command.fun,
                            args=rpc_step.command.args,
                            results=results,
                        ),
                        u=localrid,
                    )
                )

                # logger.info("[{}] Sending: {}", where, body)

                try:
                    await where.send(body)
                except websockets.ConnectionClosedOK:
                    # connection gone, but we don't have Server() reconnect logic yet, so just
                    # abandon this for now. TODO: refactor the server mapping to have actual Server()
                    # objects and not just websocket connections from other cluster connections.
                    logger.error(
                        "[{}] websocket connection closed. Removing from services for now...",
                        where,
                    )
                    self.remove_server(where)
                    # actually, RETRY this again because MAYBE we have another server with this
                    # service available?????
                    # (maybe this counts as "self-healing")
                    return await runner(rpc_step, results)

                # now subscribe THIS CLIENT to wait for THIS RESULT ID
                # pause here until we GET AN ASYNC ANSWER BACK FROM THE NETWORK

                waiting = await asyncio.create_task(self.answerFor(localrid))
                got = self.answer[localrid]
                del self.answer[localrid]

            results[rpc_step.name] = got

            # return result ATTACHED TO FUN NAME so the client gets (name -> result) mappings returned
            return {rpc_step.name: got}

        results = {}
        # logger.info("rpc is: {}", rpc)
        for level in rpc.tasks():
            # Note: EVERYTHING in the current level is parallelizable!
            cmds = []
            # logger.info("Tasks for level are: {}", level)
            for name in level:
                rpc_step = rpc.funs[name]
                # here, each 'cmd' is only command NAME we look up in the rpc to run with the resolved local args
                cmds.append(runner(rpc_step, results))

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
    # name of server
    name: str = "NO NAME PROVIDED"

    # where server will listen for connections
    # TODO: could refactor into list[Endpoint] to allow multiple binding
    host: str = "127.0.0.1"
    port: int = 6666

    # a file in RESOURCE SCHEMA FORMAT describing resource keys for this server
    # (e.g. dataset-A, printer-C, NFS-mount-D)
    resources: set[str, ...] | None = None

    # a file in PEER SCHEMA FORMAT describing static peers to initially connect to
    # (additional peers can be discovered via gossip with these local peers)
    peers: list[str] | None = None

    # mappings in the form of {"rpc-name": callable-when-rpc-requested, ...}
    # basically the thing clients call
    # will run for clients: self.funs[rpcName](*args, **kwargs)
    funs: Mapping[str, Callable[..., Any]] | None = None

    # if a string, we use it as the URI of another server to connect to on startup
    connect: str | None = None

    def __post_init__(self) -> None:
        self.cluster = Cluster()
        self.clients = set()

    def report(self):
        """General report of current server state."""

        logger.info("Resources: {}", pp.pformat(self.resources))
        logger.info("Funs: {}", pp.pformat(self.funs))
        logger.info("Clients: {}", pp.pformat(self.clients))

    def run_server(
        self,
        server_websocket: websockets.client.WebSocketClientProtocol,
        req: dict[str, str],
    ):
        """Run a server-to-server command in the cluster."""
        # logger.info("[{}] New server command: {}", server_websocket, req)

        try:
            # Idea:
            #  - Another server connects to us: HELLO I AM <remote server>, I SERVE <locations> WITH <rpc names>
            #     - Optionally: server can include CONTACT ME AT X:Y
            #     - Without a CONTACT ME value, we operate as a PROXY NODE for the NEW SERVER (because without a
            #       CONTACT ME from a server, we can't gossip the new server address to peers, but since WE have
            #       a connection to NEW SERVER, we can accept requests on its behalf and forward along).
            #  - We reply I GOT YOU FAM
            #  - Then, depending on operating mode, we notify our other servers in the cluster:
            #     - NEW NODE: ROUTABLE THROUGH US, CONTACT US TO FORWARD FOR RESULTS
            #     - NEW NODE: CONNECT TO ADDRESS X:Y, CONTACT NODE DIRECTLY
            return dict(
                r=self.cluster.server_cmd(server_websocket, req), u=str(ulid.new())
            )
        except:
            return dict(e=traceback.format_exc(), state="setup", u=str(ulid.new()))

    async def run_rpc(self, req: dict[str, str]):
        """Run a client RPC request command in the cluster."""
        # load current request and remainder of req into the arguments
        # to loop through...

        # style note: we call the ENTIRE REQUEST the 'RPC' full of individual 'Commands'
        # so: RPC -> Commands -> Functions
        try:
            rpc = RPC(req)
            u = req.get("u", None)
        except Exception as e:
            # error return
            logger.exception("rpc setup whoopsie?")
            return dict(e=traceback.format_exc(), state="setup", u=u)

        # Attempt to find a 'fun' with all matching 'loc' requirements
        try:
            # logger.info("Requesting from RPC: {}", rpc)
            return dict(r=await self.cluster.run(rpc), u=u)
        except Exception as e:
            logger.exception("rpc running failed?")
            return dict(e=traceback.format_exc(), state="running", u=u)

    @logger.catch
    async def opened(self, websocket: websockets.client.WebSocketClientProtocol):
        try:
            self.clients.add(websocket)
            async for msg in websocket:
                req = orjson.loads(msg)

                # logger.info("[{}:{}] Received: {}", websocket.host, websocket.port, req)

                match req["role"]:
                    case "server":
                        # SERVER-TO-SERVER communications packet
                        # (joining/leaving cluster, gossip updates of servers attached to other servers)
                        logger.info(
                            "[{}:{}] Server message: {}",
                            *websocket.remote_address,
                            req,
                        )
                        result = orjson.dumps(
                            dict(role="server")
                            | self.run_server(websocket, req["server"])
                        )
                    case "rpc":
                        # CLIENT request
                        logger.info(
                            "[{}:{} :: {}] Running request...",
                            *websocket.remote_address,
                            req["rpc"]["u"],
                        )
                        result = orjson.dumps(await self.run_rpc(req["rpc"]))
                    case "internal-answer":
                        # REPLY from a previous INTERNAL-RPC request
                        answer = req["answer"]
                        u = req["u"]

                        # add answer globally for the consumer to read again
                        self.cluster.answer[u] = answer

                        # notify the waiting process we have an answer now
                        self.cluster.waitingFor[u].set()

                        # logger.info("[{}] Processed Internal Answer: {}", u, answer)

                        # no result here, this is returned upstream elsewhere
                        continue
                    case _:
                        assert None, f"Invalid RPC request? Got: {req}"

                # regular result return
                try:
                    await websocket.send(result)
                except:
                    logger.error(
                        "[{}:{}] Client connection error! Dropping reply.",
                        *websocket.remote_address,
                    )
        finally:
            # TODO: if this was a SERVER, we need to clean up the server resources.
            # TODO: if this was a CLIENT, we need to cancel any oustanding requests/subscriptions too.
            self.clients.remove(websocket)

    async def server(self) -> None:
        logger.info("[{}:{}] [{}] Launching server...", self.host, self.port, self.name)

        # Register OURSELF with the global echo target...
        self.cluster.add_self_ability("echo", [echo], [])
        self.cluster.add_self_ability("echos", [echos], [])
        # TODO: we need to bind the self->echo to echo function mapping in addition to the name...

        async with websockets.server.serve(
            self.opened,
            self.host,
            self.port,
            max_size=None,
            max_queue=None,
            read_limit=MPREG_DATA_MAX,
            write_limit=MPREG_DATA_MAX,
        ):
            # if no external connection requested to another server, run an infinte wait here
            if not self.connect:
                await asyncio.Future()
                return

            # else, we ARE connecting to another server, so this keeps *our* server alive while we are a *client* to another upstream.
            # TODO: we should technically also be a client of MULTIPLE upstreams...
            # connect to another server requested, so do it now and announce our services upstream.
            async for websocket in websockets.connect(
                self.connect,
                user_agent_header=None,
                # 'open_timeout=1' means there's a 1 second delay between reconnect attempts
                open_timeout=1,
                ping_interval=5,
                ping_timeout=30,
                close_timeout=5,
                max_size=None,
                max_queue=None,
                read_limit=MPREG_DATA_MAX,
                write_limit=MPREG_DATA_MAX,
            ):
                await websocket.send(
                    # this is registering CLUSTER NODE FUNCTIONS AND DATASETS into our upstream host
                    orjson.dumps(
                        dict(
                            role="server",
                            server=dict(
                                what="HELLO",
                                funs=[1, 2, 3],
                                locs=["a", "b", "c"],
                            ),
                        )
                    )
                )

                await websocket.send(
                    # this is registering CLUSTER NODE FUNCTIONS AND DATASETS into our upstream host
                    orjson.dumps(
                        dict(
                            role="server",
                            server=dict(
                                what="HELLO",
                                funs=["fun1", "fun2"],
                                locs=["d1", "d2", "d3"],
                            ),
                        )
                    )
                )

                # this is the processing loop for US AS A CLUSTER CLIENT.
                # here we RECEIVE messages from OTHER servers for LOCAL PROCESSING then REPLYING TO THE UPSTREAM
                try:
                    async for msg in websocket:
                        req = orjson.loads(msg)
                        if False:
                            logger.info(
                                "[{} :: {}] Cluster message: {}",
                                self.connect,
                                req.get("u", "[no id]"),
                                req,
                            )
                        match req["role"]:
                            case "server":
                                logger.info(
                                    "[{}:{}] Server status: {}",
                                    *websocket.remote_address,
                                    req,
                                )
                            case "internal-rpc":
                                # FORWARDED REQUEST from ANOTHER SERVER in MID-RPC mode.
                                # We know this request is FOR US since it was sent TO US directly.
                                # TODO: in the case of proxy-forward-internal nodes, we'd need to still lookup the server.
                                # this has role=internal-rpc, internal=(command, args, result), u=uniqueid
                                i = req["internal"]
                                command = i["command"]
                                args = i["args"]
                                results = i["results"]
                                u = req["u"]

                                # Generate RESULT PAYLOAD
                                result = orjson.dumps(
                                    dict(
                                        role="internal-answer",
                                        answer=self.cluster.self[command].call(
                                            args, results
                                        ),
                                        u=u,
                                    )
                                )

                                # logger.info("[{}] Generated answer: {}", u, result)

                                # SEND RESULT PAYLOAD back UPSTREAM
                                await websocket.send(result)
                except (
                    websockets.ConnectionClosedError,
                    websockets.ConnectionClosedOK,
                ):
                    # note: we just need to catch here since the `websockets.connect()` we're inside
                    #       is already an infinte generator for connection retries...
                    logger.info("Server disconnected... reconnecting...")

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

    jsonargparse.CLI(MPREGServer)
