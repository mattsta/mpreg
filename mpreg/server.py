from __future__ import annotations

import asyncio
import pprint as pp
import random
import sys
import time
import traceback
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from graphlib import TopologicalSorter
from typing import Any

import ulid
import websockets.client
import websockets.server
from loguru import logger

from .core.config import MPREGSettings
from .core.connection import Connection

# Federated RPC now uses enhanced _broadcast_new_function with existing MPREG infrastructure
from .core.connection_events import ConnectionEvent, ConnectionEventBus
from .core.model import (
    CommandNotFoundException,
    ConsensusProposalMessage,
    ConsensusVoteMessage,
    GoodbyeReason,
    GossipMessage,
    MPREGException,
    PeerInfo,
    PubSubAck,
    PubSubGossip,
    # Pub/Sub message types
    PubSubNotification,
    PubSubPublish,
    PubSubSubscribe,
    PubSubUnsubscribe,
    RPCCommand,
    RPCError,
    RPCExecutionSummary,
    RPCIntermediateResult,
    RPCInternalAnswer,
    RPCInternalRequest,
    RPCRequest,
    RPCResponse,
    RPCServerGoodbye,
    RPCServerHello,
    RPCServerMessage,
    RPCServerRequest,
)
from .core.registry import Command, CommandRegistry
from .core.serialization import JsonSerializer
from .core.topic_exchange import TopicExchange
from .datastructures.cluster_types import ClusterState
from .datastructures.federated_types import (
    AnnouncementID,
    FederatedRPCAnnouncement,
    FunctionNames,
    ServerCapabilities,
    create_federated_propagation_from_primitives,
)
from .datastructures.type_aliases import NodeURL, Timestamp
from .datastructures.vector_clock import VectorClock

############################################
#
# Fancy Setup
#
############################################

# Use more efficient coroutine logic if available
# https://docs.python.org/3.12/library/asyncio-task.html#asyncio.eager_task_factory
try:
    loop = asyncio.get_running_loop()
    loop.set_task_factory(asyncio.eager_task_factory)
except RuntimeError:
    # No event loop running, this is fine for imports
    pass

# maximum 4 GB messages should be enough for anybody, right?
MPREG_DATA_MAX = 2**32


@dataclass(slots=True)
class MessageStats:
    total_processed: int = 0
    rpc_responses_skipped: int = 0
    server_messages: int = 0
    other_messages: int = 0


def rpc_command(
    name: str, resources: Iterable[str] | None = None
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to register a function as an RPC command.

    Args:
        name: The name of the RPC command.
        resources: Optional iterable of resource strings associated with the command.

    Returns:
        A decorator that registers the function as an RPC command.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
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
def echo(arg: Any) -> Any:
    """Default echo handler for all servers.

    Single-argument echo demo."""
    return arg


@rpc_command(name="echos", resources=[])
def echos(*args: Any) -> tuple[Any, ...]:
    """Default echos handler for all servers.

    Multi-argument echo demo."""
    return args


############################################
#
# RPC Management
#
############################################
@dataclass(slots=True)
class RPC:
    """Representation of a full RPC request hierarchy.

    Upon instantiation, we create the full call graph from our JSON RPC format
    so this entire RPC can be executed in the cluster."""

    req: RPCRequest
    funs: dict[str, RPCCommand] = field(init=False)
    levels: list[list[str]] = field(init=False)

    def __post_init__(self) -> None:
        self.funs = {cmd.name: cmd for cmd in self.req.cmds}

        # We also have to resolve the funs to a call graph...
        sorter: TopologicalSorter[str] = TopologicalSorter()

        def find_dependencies_recursive(obj, deps_list):
            """Recursively search for dependencies in nested data structures."""
            if isinstance(obj, str):
                # Check for exact match first
                if obj in self.funs:
                    deps_list.append(obj)
                # Check for field access pattern (e.g., "step2_result.final")
                elif "." in obj:
                    base_name = obj.split(".", 1)[0]
                    if base_name in self.funs:
                        deps_list.append(base_name)
            elif isinstance(obj, dict):
                # Search dictionary values
                for value in obj.values():
                    find_dependencies_recursive(value, deps_list)
            elif isinstance(obj, list | tuple):
                # Search list/tuple elements
                for item in obj:
                    find_dependencies_recursive(item, deps_list)

        for name, rpc_command in self.funs.items():
            # only attach child dependencies IF the argument matches a NAME in the entire RPC Request
            # Check both direct string arguments AND nested structures
            deps: list[str] = []
            for arg in rpc_command.args:
                find_dependencies_recursive(arg, deps)

            # Remove duplicates while preserving order
            deps = list(dict.fromkeys(deps))
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

        logger.info("Runnable levels are: {}", levels)
        self.levels = levels

    def tasks(self) -> Any:
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
@dataclass(slots=True)
class Cluster:
    """Type-safe cluster management using modern dataclasses and type aliases."""

    # Configuration
    config: ClusterState = field()

    # Server infrastructure
    registry: CommandRegistry = field(init=False)
    serializer: JsonSerializer = field(init=False)

    # Runtime state
    waitingFor: dict[str, asyncio.Event] = field(init=False)
    answer: dict[str, Any] = field(init=False)
    peer_connections: dict[str, Connection] = field(init=False)

    # Connection event system
    connection_event_bus: ConnectionEventBus = field(init=False)

    # Internal storage for peers info
    _peers_info: dict[str, PeerInfo] = field(default_factory=dict, init=False)

    # Track peers that have sent GOODBYE to prevent gossip re-adding them
    # Format: {peer_url: departure_timestamp}
    _departed_peers: dict[str, float] = field(default_factory=dict, init=False)

    @classmethod
    def create(
        cls,
        cluster_id: str,
        advertised_urls: tuple[str, ...],
        local_url: str = "",
        dead_peer_timeout_seconds: float = 30.0,
    ) -> Cluster:
        """Create a Cluster from primitive types."""
        config = ClusterState.create(
            cluster_id=cluster_id,
            advertised_urls=advertised_urls,
            local_url=local_url,
            dead_peer_timeout_seconds=dead_peer_timeout_seconds,
        )
        return cls(config=config)

    def __post_init__(self) -> None:
        print(
            f"🏗️ CLUSTER CREATE: New Cluster instance {id(self)} for {self.config.local_url}"
        )

        self.waitingFor: dict[str, asyncio.Event] = dict()
        self.answer: dict[str, Any] = dict()
        # Persistent connections to peer servers for RPC forwarding
        self.peer_connections: dict[str, Connection] = dict()

        # Initialize connection event system
        self.connection_event_bus = ConnectionEventBus()

        # These will be set by MPREGServer
        # self.registry = CommandRegistry()
        # self.serializer = JsonSerializer()

    # Direct property accessors for type-safe access
    @property
    def cluster_id(self) -> str:
        """Get the cluster ID."""
        return str(self.config.cluster_id)

    @property
    def local_url(self) -> str:
        """Get the local server URL."""
        return str(self.config.local_url)

    @property
    def advertised_urls(self) -> tuple[str, ...]:
        """Get the advertised URLs."""
        return tuple(str(url) for url in self.config.advertised_urls)

    @property
    def funtimes(self) -> dict[str, dict[frozenset[str], set[str]]]:
        """Get funtimes using the function registry."""
        return self.config.function_registry.get_legacy_funtimes()

    @property
    def servers(self) -> set[str]:
        """Get all known servers."""
        return {str(server) for server in self.config.function_registry.known_servers}

    @property
    def peers_info(self) -> dict[str, PeerInfo]:
        """Get peers info."""
        return self._peers_info

    @peers_info.setter
    def peers_info(self, value: dict[str, PeerInfo]) -> None:
        """Set peers info."""
        self._peers_info = value

    @property
    def dead_peer_timeout(self) -> float:
        """Get dead peer timeout."""
        return self.config.dead_peer_timeout_seconds

    @property
    def seen_announcements(self) -> dict[str, float]:
        """Get seen announcements."""
        return {
            str(aid): float(timestamp)
            for aid, timestamp in self.config.announcement_tracker.seen_announcements.items()
        }

    @property
    def announcement_ttl(self) -> float:
        """Get announcement TTL."""
        return float(self.config.announcement_tracker.ttl_seconds)

    def add_fun_ability(self, peer_info: PeerInfo) -> None:
        """Add a new function and resource(s) to a server mapping.

        This method updates the cluster's knowledge of which functions are available
        on which servers and with what resources.
        """
        logger.info(
            "Adding peer {} with functions {} and locs {}",
            peer_info.url,
            peer_info.funs,
            peer_info.locs,
        )

        # Add functions to the type-safe registry
        for fun in peer_info.funs:
            self.config.add_function(fun, tuple(peer_info.locs), peer_info.url)
            logger.info(
                "Registered function '{}' with locs {} on server {}",
                fun,
                peer_info.locs,
                peer_info.url,
            )

        # Update peers_info
        self.peers_info[peer_info.url] = peer_info
        logger.info(
            "Cluster now has {} servers and {} functions",
            self.config.function_registry.server_count,
            self.config.function_registry.function_count,
        )

    def remove_server(self, server: Connection) -> None:
        """If server disconnects, remove it from ALL fun mappings.

        Note: this may leave some empty {fun: {resource: set()}} but it's okay"""

        # Use the type-safe function registry to remove server functions
        server_url: NodeURL = server.url
        self.config.function_registry.remove_server_functions(server_url)

        # Also remove from peers_info
        self.peers_info.pop(server.url, None)

    async def remove_peer(self, peer_url: str) -> bool:
        """Remove peer from cluster with proper cleanup (used by GOODBYE protocol).

        Args:
            peer_url: URL of the peer to remove

        Returns:
            True if peer was removed, False if peer was not in cluster
        """
        if peer_url not in self.peers_info:
            return False

        # Remove from peers_info
        peer_info = self.peers_info.pop(peer_url)
        logger.info(f"Removed peer {peer_url} from cluster")

        # Add to departed peers to prevent gossip re-adding
        self._departed_peers[peer_url] = time.time()
        logger.debug(f"Added {peer_url} to departed peers list")

        # Remove from function registry
        self.config.function_registry.remove_server_functions(peer_url)

        logger.info(
            f"Cleaned up functions for departing peer {peer_url}. "
            f"Cluster now has {self.config.function_registry.server_count} servers "
            f"and {self.config.function_registry.function_count} functions"
        )

        return True

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
                # Note: logical_clock operations are handled by MPREGServer, not Cluster

                peer_info = PeerInfo(
                    url=server_connection.url,
                    funs=cmd.funs,
                    locs=frozenset(cmd.locs),
                    last_seen=time.time(),
                    cluster_id=cmd.cluster_id,
                    advertised_urls=cmd.advertised_urls,
                    logical_clock={},  # Empty clock for Cluster-level operations
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

    def process_gossip_message(self, gossip_message: GossipMessage) -> None:
        """Processes an incoming gossip message using vector clocks for proper ordering.

        ALGORITHMIC FIX: Uses vector clocks instead of timestamps to prevent infinite
        gossip loops caused by clock synchronization issues in distributed systems.
        """
        updates_made = 0

        for peer_info in gossip_message.peers:
            # Only process if the cluster_id matches
            if peer_info.cluster_id != self.cluster_id:
                logger.warning(
                    "Received gossip from different cluster ID: Expected {}, Got {}",
                    self.cluster_id,
                    peer_info.cluster_id,
                )
                continue

            # Handle departed peers: Check if this is a peer re-entry broadcast
            if peer_info.url in self._departed_peers:
                departure_time = self._departed_peers[peer_info.url]
                # If the peer's last_seen is after the departure time, it's a re-entry
                if peer_info.last_seen > departure_time:
                    logger.info(
                        f"Peer {peer_info.url} re-entered after GOODBYE - clearing from departed peers"
                    )
                    del self._departed_peers[peer_info.url]
                    # Continue processing to add the peer back
                else:
                    logger.debug(
                        f"Skipping old gossip about departed peer {peer_info.url}"
                    )
                    continue

            should_update = False
            reason = ""

            if peer_info.url not in self.peers_info:
                # New peer - always add
                should_update = True
                reason = "new peer"
            else:
                existing_peer = self.peers_info[peer_info.url]

                # VECTOR CLOCK COMPARISON: Use logical clocks for proper ordering
                existing_clock = VectorClock.from_dict(existing_peer.logical_clock)
                incoming_clock = VectorClock.from_dict(peer_info.logical_clock)

                comparison = existing_clock.compare(incoming_clock)

                if comparison == "before":
                    # Existing happens before incoming - update
                    should_update = True
                    reason = "vector clock ordering (incoming after existing)"
                elif comparison == "concurrent":
                    # Concurrent updates - use deterministic resolution
                    should_update = self._resolve_concurrent_peer_update(
                        existing_peer, peer_info
                    )
                    reason = f"concurrent update resolution ({should_update})"
                else:
                    # Incoming is equal or before existing - skip
                    should_update = False
                    reason = f"vector clock ordering (incoming {comparison} existing)"

            if should_update:
                logger.debug(
                    "Updating peer info for {}: reason={}", peer_info.url, reason
                )
                self.peers_info[peer_info.url] = peer_info
                updates_made += 1

        # Only log if substantial updates were made
        if updates_made > 0:
            logger.debug("Processed gossip: {} peer updates", updates_made)

        # Prune dead peers - use more conservative approach for large clusters
        self._prune_dead_peers()

    def _resolve_concurrent_peer_update(self, existing: Any, incoming: Any) -> bool:
        """Resolve concurrent peer updates deterministically."""
        # Use deterministic tie-breaking for concurrent updates
        if existing.funs != incoming.funs:
            return tuple(sorted(incoming.funs)) > tuple(sorted(existing.funs))
        elif existing.locs != incoming.locs:
            return tuple(sorted(incoming.locs)) > tuple(sorted(existing.locs))
        elif existing.advertised_urls != incoming.advertised_urls:
            return tuple(sorted(incoming.advertised_urls)) > tuple(
                sorted(existing.advertised_urls)
            )
        else:
            return incoming.url > existing.url

    def _prune_dead_peers(self) -> None:
        """Remove dead peers with cluster-size-aware timeouts."""
        current_time = time.time()
        cluster_size = len(self.peers_info)

        # Scale timeout based on cluster size and gossip characteristics
        if cluster_size >= 30:
            effective_timeout = self.dead_peer_timeout * 4  # 120 seconds
        elif cluster_size >= 10:
            effective_timeout = self.dead_peer_timeout * 3  # 90 seconds
        else:
            effective_timeout = self.dead_peer_timeout * 2  # 60 seconds

        peers_to_remove = [
            url
            for url, info in self.peers_info.items()
            if current_time - info.last_seen > effective_timeout
        ]

        if peers_to_remove:
            logger.debug(
                "Removing {} stale peers (timeout: {}s)",
                len(peers_to_remove),
                effective_timeout,
            )
            for url in peers_to_remove:
                del self.peers_info[url]

        # Clean up departed peers that are old enough (prevent memory leak)
        # Remove from blocklist after 5 minutes - by then all gossip should have stopped
        departed_timeout = 300.0  # 5 minutes
        departed_to_remove = [
            url
            for url, departure_time in self._departed_peers.items()
            if current_time - departure_time > departed_timeout
        ]

        if departed_to_remove:
            logger.debug(
                f"Cleaning up {len(departed_to_remove)} old departed peers from blocklist"
            )
            for url in departed_to_remove:
                del self._departed_peers[url]

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
        from .datastructures.type_aliases import FunctionName

        function_name: FunctionName = fun
        resource_requirements = frozenset(loc for loc in locs) if locs else None

        logger.info("Looking for function '{}' with locs={}", fun, locs)
        logger.info("Available servers: {}", self.config.function_registry.server_count)
        logger.info(
            "Available functions: {}", self.config.function_registry.get_all_functions()
        )

        # Check if the function exists in our registry
        if not self.config.function_registry.has_function(function_name):
            logger.warning("Function '{}' not found in registry", fun)
            return None

        # If no servers exist, we can't find anything anywhere.
        if self.config.function_registry.server_count == 0:
            logger.warning("No servers available!")
            return None

        # Use type-safe function registry to find servers
        available_servers = self.config.function_registry.get_servers_for_function(
            function_name, resource_requirements
        )

        # Convert back to strings for compatibility
        available_server_strings = {str(server) for server in available_servers}

        if available_server_strings:
            selected_server = random.choice(tuple(available_server_strings))
            # Check if the selected server is ourselves
            if selected_server == self.local_url:
                return "self"
            return selected_server

        # If no exact match, try to find servers with compatible resources (supersets)
        if resource_requirements is not None:
            # Get all servers for this function and check for compatible resources
            all_function_servers = (
                self.config.function_registry.get_servers_for_function(function_name)
            )

            if all_function_servers:
                # Fall back to legacy funtimes logic for subset matching
                legacy_funtimes = self.config.function_registry.get_legacy_funtimes()
                for funlocs, srvs in legacy_funtimes[fun].items():
                    if locs.issubset(funlocs):
                        # We found a match for ALL our requested resources (even if the target has MORE).
                        selected_server = random.choice(tuple(srvs))
                        if selected_server == self.local_url:
                            return "self"
                        return selected_server

        # Else, we tried everything and no matches were found.
        return None

    async def answerFor(self, rid: str, timeout: float = 30.0) -> None:
        """Wait for a reply on unique id 'rid' then return the answer."""
        e = asyncio.Event()

        self.waitingFor[rid] = e

        try:
            # logger.info("[{}] Sleeping waiting for answer...", rid)
            await asyncio.wait_for(e.wait(), timeout=timeout)
            # logger.info("Woke up!")
        except TimeoutError:
            logger.error(
                "Timeout waiting for answer for rid={} after {} seconds", rid, timeout
            )
            raise
        finally:
            # Always clean up
            self.waitingFor.pop(rid, None)

    def _resolve_arguments(
        self, args: tuple[Any, ...], kwargs: dict[str, Any], results: dict[str, Any]
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """Resolve arguments by substituting result references with actual values.

        Recursively resolves string references in nested data structures.
        If a string matches a key in results, replace it with the actual result value.

        Args:
            args: Original positional arguments
            kwargs: Original keyword arguments
            results: Dictionary of previous command results

        Returns:
            Tuple of (resolved_args, resolved_kwargs)
        """

        def resolve_value(value: Any) -> Any:
            if isinstance(value, str):
                # Handle dot notation for nested field access (e.g., "edge_data.average")
                if "." in value:
                    parts = value.split(".", 1)
                    base_key = parts[0]
                    field_path = parts[1]
                    logger.debug(
                        "Resolving field access: base_key={}, field_path={}, available_results={}",
                        base_key,
                        field_path,
                        list(results.keys()),
                    )
                    if base_key in results:
                        base_result = results[base_key]
                        logger.debug("Found base result: {}", base_result)
                        # Navigate nested fields
                        current = base_result
                        for field in field_path.split("."):
                            if isinstance(current, dict) and field in current:
                                current = current[field]
                                logger.debug(
                                    "Navigated to field '{}': {}", field, current
                                )
                            else:
                                logger.warning(
                                    "Field '{}' not found in nested result structure: {}",
                                    field,
                                    current,
                                )
                                return value  # Return original if path not found
                        logger.info("Successfully resolved '{}' to: {}", value, current)
                        return current
                    else:
                        logger.warning(
                            "Base key '{}' not found in results: {}",
                            base_key,
                            list(results.keys()),
                        )
                # Simple key lookup
                elif value in results:
                    logger.debug(
                        "Simple key resolution: '{}' -> {}", value, results[value]
                    )
                    return results[value]
            elif isinstance(value, dict):
                return {k: resolve_value(v) for k, v in value.items()}
            elif isinstance(value, list | tuple):
                resolved_list = [resolve_value(item) for item in value]
                return type(value)(resolved_list)
            return value

        resolved_args = tuple(resolve_value(arg) for arg in args)
        resolved_kwargs = {k: resolve_value(v) for k, v in kwargs.items()}

        return resolved_args, resolved_kwargs

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
        # Resolve arguments by substituting previous results
        resolved_args, resolved_kwargs = self._resolve_arguments(
            rpc_command.args, rpc_command.kwargs, results
        )

        command = self.registry.get(rpc_command.fun)
        return await command.call_async(*resolved_args, **resolved_kwargs)

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
        # Resolve arguments by substituting previous results
        resolved_args, resolved_kwargs = self._resolve_arguments(
            rpc_step.args, rpc_step.kwargs, results
        )

        localrid = str(ulid.new())
        body = self.serializer.serialize(  # Access serializer via self.serializer
            RPCInternalRequest(
                command=rpc_step.fun,
                args=resolved_args,
                kwargs=resolved_kwargs,
                results=results,
                u=localrid,
            ).model_dump()
        )

        try:
            logger.info(
                "Sending internal-rpc request: command={}, u={}, to={}",
                rpc_step.fun,
                localrid,
                where.url,
            )
            await where.send(body)
            logger.info(
                "Internal-rpc request sent successfully, waiting for response..."
            )
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

        logger.info("Waiting for answer with rid={}", localrid)
        await asyncio.create_task(self.answerFor(localrid))
        logger.info("Received answer for rid={}", localrid)
        got = self.answer[localrid]
        del self.answer[localrid]
        logger.info("Remote command completed: got={}", got)
        return got

    async def run(self, rpc: RPC, timeout: float = 30.0) -> Any:
        """Run the RPC.

        Steps:
          - Look up location for all RPCs in current level.
          - Send RPCs for current level.
            - If only ONE RPC at this level, delegate REMAINDER OF CALLING to the single server.
            - If MORE THAN ONE RPC at this level, coordinate the next level.
          - Reply to client.
        """
        logger.info(
            "Starting RPC execution with {} levels and timeout={}",
            len(rpc.levels),
            timeout,
        )

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
            logger.info(
                "Executing command '{}' with locs={}", rpc_command.fun, rpc_command.locs
            )

            where = self.server_for(rpc_command.fun, rpc_command.locs)
            logger.info(
                "server_for('{}', {}) returned: {}",
                rpc_command.fun,
                rpc_command.locs,
                where,
            )

            if not where:
                logger.error(
                    "No server found for command '{}' with locs={}",
                    rpc_command.fun,
                    rpc_command.locs,
                )
                logger.info(
                    "Available functions in cluster: {}", list(self.funtimes.keys())
                )
                raise CommandNotFoundException(command_name=rpc_command.fun)

            if where == "self":
                logger.info("Executing '{}' locally", rpc_command.fun)
                got = await self._execute_local_command(rpc_command, results)
            else:
                logger.info("Executing '{}' remotely on {}", rpc_command.fun, where)
                # Use persistent connection if available, else create new one
                logger.debug(
                    "Looking for peer connection: where={}, available_keys={}",
                    where,
                    list(self.peer_connections.keys()),
                )
                connection = self.peer_connections.get(where)
                if not connection or not connection.is_connected:
                    logger.info(
                        "Creating new connection to {} (existing: {}, connected: {})",
                        where,
                        connection is not None,
                        connection.is_connected if connection else False,
                    )
                    # Create new connection if none exists or is dead
                    connection = Connection(url=where)
                    connection.cluster = (
                        self  # Set cluster reference for response handling
                    )
                    await connection.connect()
                    self.peer_connections[where] = connection
                else:
                    logger.info("Using existing connection to {}", where)

                got = await self._execute_remote_command(
                    rpc_command, results, connection
                )

            logger.info(
                "Command '{}' completed with result type: {}",
                rpc_command.fun,
                type(got),
            )
            results[rpc_command.name] = got

            # TODO: this should return a STABLE DATACLASS OBJECT and not just a generic dict
            return {rpc_command.name: got}

        results: dict[str, Any] = {}  # Added type hint
        # Cache the levels to avoid consuming generator multiple times
        levels_list = list(rpc.tasks())
        logger.info("RPC has {} levels to execute", len(levels_list))

        async def execute_with_timeout():
            for level_idx, level in enumerate(levels_list):
                logger.info("Processing level {} with tasks: {}", level_idx, level)
                # Note: EVERYTHING in the current level is parallelizable!
                cmds = []
                for name in level:
                    rpc_command = rpc.funs[name]
                    logger.info("Adding command '{}' to level {}", name, level_idx)
                    # here, each 'cmd' is only command NAME we look up in the rpc to run with the resolved local args
                    cmds.append(runner(rpc_command, results))

                logger.info(
                    "Executing {} commands in parallel for level {}",
                    len(cmds),
                    level_idx,
                )
                got = await asyncio.gather(*cmds)
                logger.info("Level {} completed with {} results", level_idx, len(got))
            return got

        try:
            got = await asyncio.wait_for(execute_with_timeout(), timeout=timeout)
        except TimeoutError:
            logger.error("RPC execution timed out after {} seconds", timeout)
            raise MPREGException(
                rpc_error=RPCError(
                    code=1004,
                    message=f"RPC execution timed out after {timeout} seconds",
                    details="The RPC workflow took too long to complete",
                )
            )

        result = {}
        for g in got:
            result.update(g)
            logger.debug("Merged result: {}", g)

        logger.info(
            "RPC execution completed with final result keys: {}", list(result.keys())
        )
        return result

        # using result.update() in a loop is twice as fast as this:
        # return {k: v for g in got for k, v in g.items()}

    async def run_with_intermediate_results(
        self, rpc: RPC, request: RPCRequest, timeout: float = 30.0
    ) -> tuple[Any, tuple[RPCIntermediateResult, ...], RPCExecutionSummary | None]:
        """Enhanced RPC execution with intermediate results and performance tracking.

        Args:
            rpc: The RPC execution graph
            request: The original RPC request with debugging options
            timeout: Execution timeout in seconds

        Returns:
            Tuple of (final_result, intermediate_results, execution_summary)
        """
        logger.info(
            "Starting enhanced RPC execution with {} levels and timeout={}",
            len(rpc.levels),
            timeout,
        )

        # Track execution state
        execution_start_time = time.time()
        intermediate_results: list[RPCIntermediateResult] = []
        level_execution_times: list[float] = []
        total_commands_executed = 0
        cross_cluster_hops = 0

        async def enhanced_runner(
            rpc_command: RPCCommand, results: dict[str, Any]
        ) -> dict[str, Any]:
            """Enhanced runner that tracks cross-cluster execution."""
            nonlocal cross_cluster_hops

            logger.info(
                "Executing command '{}' with locs={}", rpc_command.fun, rpc_command.locs
            )

            where = self.server_for(rpc_command.fun, rpc_command.locs)
            if not where:
                logger.error(
                    "No server found for command '{}' with locs={}",
                    rpc_command.fun,
                    rpc_command.locs,
                )
                raise CommandNotFoundException(command_name=rpc_command.fun)

            if where != "self":
                cross_cluster_hops += 1

            # Use existing execution logic
            if where == "self":
                got = await self._execute_local_command(rpc_command, results)
            else:
                connection = self.peer_connections.get(where)
                if not connection or not connection.is_connected:
                    connection = Connection(url=where)
                    connection.cluster = self
                    await connection.connect()
                    self.peer_connections[where] = connection
                got = await self._execute_remote_command(
                    rpc_command, results, connection
                )

            # Update results dictionary
            results[rpc_command.name] = got
            return {rpc_command.name: got}

        results: dict[str, Any] = {}
        # Cache the levels to avoid consuming generator multiple times
        levels_list = list(rpc.tasks())
        logger.info("Enhanced RPC has {} levels to execute", len(levels_list))

        async def execute_with_timeout():
            nonlocal total_commands_executed

            for level_idx, level in enumerate(levels_list):
                level_start_time = time.time()
                logger.info("Processing level {} with tasks: {}", level_idx, level)

                # Execute all commands in this level in parallel
                cmds = []
                for name in level:
                    rpc_command = rpc.funs[name]
                    logger.info("Adding command '{}' to level {}", name, level_idx)
                    cmds.append(enhanced_runner(rpc_command, results))

                logger.info(
                    "Executing {} commands in parallel for level {}",
                    len(cmds),
                    level_idx,
                )

                got = await asyncio.gather(*cmds)
                level_execution_time = (time.time() - level_start_time) * 1000.0
                level_execution_times.append(level_execution_time)
                total_commands_executed += len(cmds)

                logger.info("Level {} completed with {} results", level_idx, len(got))

                # Capture intermediate results if requested
                if request.return_intermediate_results:
                    # Build level results from this level only
                    level_results = {}
                    for g in got:
                        level_results.update(g)

                    intermediate_result = RPCIntermediateResult(
                        request_id=request.u,
                        level_index=level_idx,
                        level_results=level_results,
                        accumulated_results=dict(results),  # Copy current state
                        total_levels=len(levels_list),
                        completed_levels=level_idx + 1,
                        timestamp=time.time(),
                        execution_time_ms=level_execution_time,
                    )

                    intermediate_results.append(intermediate_result)

                    # Optional: Stream to topic if configured
                    if request.intermediate_result_callback_topic:
                        # TODO: Implement topic streaming when topic system is integrated
                        logger.debug(
                            "Would publish intermediate result to topic: {}",
                            request.intermediate_result_callback_topic,
                        )

            return got

        try:
            got = await asyncio.wait_for(execute_with_timeout(), timeout=timeout)
        except TimeoutError:
            logger.error("Enhanced RPC execution timed out after {} seconds", timeout)
            raise MPREGException(
                rpc_error=RPCError(
                    code=1004,
                    message=f"RPC execution timed out after {timeout} seconds",
                    details="The enhanced RPC workflow took too long to complete",
                )
            )

        # Build final result
        result = {}
        for g in got:
            result.update(g)

        # Create execution summary if requested
        execution_summary = None
        if request.include_execution_summary and level_execution_times:
            total_execution_time = (time.time() - execution_start_time) * 1000.0
            bottleneck_level_index = level_execution_times.index(
                max(level_execution_times)
            )
            average_level_time = sum(level_execution_times) / len(level_execution_times)

            execution_summary = RPCExecutionSummary(
                request_id=request.u,
                total_execution_time_ms=total_execution_time,
                total_levels=len(level_execution_times),
                level_execution_times=tuple(level_execution_times),
                bottleneck_level_index=bottleneck_level_index,
                average_level_time_ms=average_level_time,
                parallel_commands_executed=total_commands_executed,
                cross_cluster_hops=cross_cluster_hops,
            )

        logger.info(
            "Enhanced RPC execution completed with final result keys: {}",
            list(result.keys()),
        )

        return result, tuple(intermediate_results), execution_summary


############################################
#
# Actual MPREG Cluster Server Entrypoint
#
############################################
@dataclass(slots=True)
class MPREGServer:
    """Main MPREG server application class."""

    settings: MPREGSettings = field(  # Changed from Field to field
        default_factory=lambda: MPREGSettings(),  # type: ignore[call-arg]
        metadata={"description": "Server configuration settings."},
    )

    # Fields assigned in __post_init__
    registry: CommandRegistry = field(init=False)
    serializer: JsonSerializer = field(init=False)
    cluster: Cluster = field(init=False)
    clients: set[Connection] = field(init=False)
    peer_connections: dict[str, Connection] = field(init=False)
    _shutdown_event: asyncio.Event = field(init=False)
    _pending_confirmations: dict[str, dict[str, asyncio.Future[bool]]] = field(
        init=False
    )
    topic_exchange: TopicExchange = field(init=False)
    _logger_handler_id: int = field(init=False)
    # PubSub client tracking
    pubsub_clients: dict[str, websockets.server.WebSocketServerProtocol] = field(
        init=False
    )
    # Function announcement acknowledgment tracking via pub/sub topics
    # Maps announcement_id -> subscription_id for cleanup
    function_confirmation_subscriptions: dict[AnnouncementID, str] = field(
        init=False, default_factory=dict
    )
    subscription_to_client: dict[str, str] = field(init=False)
    # Federation management
    federation_manager: Any = field(init=False)  # FederationConnectionManager
    # Consensus management for intra-cluster consensus
    consensus_manager: Any = field(init=False)  # ConsensusManager
    # Message statistics tracking
    _msg_stats: MessageStats = field(default_factory=MessageStats)

    # Task tracking for proper cleanup
    _background_tasks: set[asyncio.Task[Any]] = field(init=False, default_factory=set)
    _websocket_server: Any = field(
        init=False, default=None
    )  # websockets.server.WebSocketServer
    # Vector clock for logical ordering of peer updates (will be properly initialized in __post_init__)
    logical_clock: VectorClock = field(default_factory=VectorClock)

    def __post_init__(self) -> None:
        """Initializes the MPREGServer instance.

        Sets up the cluster, command registry, and client tracking.
        """
        self.registry = CommandRegistry()  # Moved registry initialization here
        self.serializer = JsonSerializer()  # Moved serializer initialization here
        self.cluster = Cluster.create(
            cluster_id=self.settings.cluster_id,
            advertised_urls=tuple(self.settings.advertised_urls or []),
            local_url=f"ws://{self.settings.host}:{self.settings.port}",
        )
        self.cluster.registry = self.registry
        self.cluster.serializer = self.serializer

        # Update vector clock with proper local URL (VectorClock instance created by default_factory)
        self.logical_clock = VectorClock.single_entry(self.cluster.local_url, 0)
        self.clients: set[Connection] = set()  # Added type hint
        self.peer_connections: dict[
            str, Connection
        ] = {}  # Persistent connections to peers
        self._shutdown_event = asyncio.Event()  # Event to signal server shutdown

        # Initialize function confirmation tracking system
        self._pending_confirmations = {}

        # Initialize federation connection manager
        from mpreg.federation.federation_connection_manager import (
            FederationConnectionManager,
        )

        if self.settings.federation_config is None:
            raise ValueError(
                "Federation config must be initialized before server creation"
            )
        self.federation_manager = FederationConnectionManager(
            self.settings.federation_config
        )

        # Initialize intra-cluster consensus manager
        from mpreg.federation.federation_consensus import ConsensusManager

        self.consensus_manager = ConsensusManager(
            node_id=self.cluster.local_url,  # Use server URL as node ID
            gossip_protocol=None,  # We'll use the server's own gossip system
            message_broadcast_callback=self._broadcast_consensus_message_unified,
            default_consensus_threshold=0.6,
        )

        # Configure logging level - store handler ID for proper cleanup
        logger.remove()  # Remove default handler
        self._logger_handler_id = logger.add(sys.stderr, level=self.settings.log_level)

        # Initialize topic exchange system
        self.topic_exchange = TopicExchange(
            server_url=self.cluster.local_url, cluster_id=self.settings.cluster_id
        )

        # Initialize PubSub client tracking
        self.pubsub_clients: dict[str, websockets.server.WebSocketServerProtocol] = {}
        self.subscription_to_client: dict[str, str] = {}

        # TopicExchange will create notifications, we'll send them in the message handler

        # Register default commands and any commands decorated with @rpc_command
        self._register_default_commands()
        self._discover_and_register_rpc_commands()

    async def stop(self) -> None:
        """Stop the server gracefully."""
        await self.shutdown_async()

    def shutdown(self) -> None:
        """Synchronous shutdown - runs async shutdown in event loop.

        For backwards compatibility. Prefer shutdown_async() for better control.
        """
        try:
            # Try to run in existing event loop
            loop = asyncio.get_running_loop()
            # We're in an async context, can't use asyncio.run()
            # Just set the shutdown event as a signal
            self._shutdown_event.set()
        except RuntimeError:
            # No event loop running, we can use asyncio.run()
            asyncio.run(self.shutdown_async())

    async def shutdown_async(self) -> None:
        """Complete async shutdown - handles everything needed for clean server shutdown.

        This is the primary shutdown method. It's fully self-contained and handles
        both signaling shutdown and performing all cleanup operations.
        """
        logger.info(f"[{self.settings.name}] Starting complete async shutdown...")

        # Send GOODBYE to all peers FIRST, before shutdown signal
        try:
            from .core.model import GoodbyeReason

            if self.peer_connections:  # Only send if we have peers
                await self.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)
                # Brief pause to let GOODBYE messages propagate
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.warning(
                f"[{self.settings.name}] Error sending GOODBYE during shutdown: {e}"
            )

        # Signal shutdown - this allows background tasks to exit gracefully
        self._shutdown_event.set()

        # CRITICAL: Close connections FIRST to allow tasks to exit gracefully
        logger.info(f"[{self.settings.name}] Closing peer connections...")
        for peer_url, connection in self.peer_connections.items():
            try:
                await connection.disconnect()
                logger.debug(f"[{self.settings.name}] Closed connection to {peer_url}")
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error closing connection to {peer_url}: {e}"
                )

        # Close client connections
        logger.info(f"[{self.settings.name}] Closing client connections...")
        for client in self.clients.copy():
            try:
                await client.disconnect()
                logger.debug(f"[{self.settings.name}] Closed client connection")
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error closing client connection: {e}"
                )

        # Stop consensus manager
        try:
            await self.consensus_manager.stop()
            logger.debug(f"[{self.settings.name}] Consensus manager stopped")
        except Exception as e:
            logger.warning(
                f"[{self.settings.name}] Error stopping consensus manager: {e}"
            )

        # BULLETPROOF task cleanup with shutdown event grace period
        if self._background_tasks:
            logger.info(
                f"[{self.settings.name}] Performing cleanup of {len(self._background_tasks)} background tasks..."
            )

            # Create a copy of the task set to avoid set size changes during iteration
            tasks_to_cleanup = list(self._background_tasks)

            # Clear the set immediately to prevent any new additions
            self._background_tasks.clear()

            # Give tasks a brief moment to respond to shutdown event (already set at line 1205)
            if tasks_to_cleanup:
                try:
                    # Short grace period for tasks to respond to shutdown event
                    done, pending = await asyncio.wait(
                        tasks_to_cleanup,
                        timeout=0.1,  # Very brief - just one event loop cycle
                        return_when=asyncio.ALL_COMPLETED,
                    )

                    if pending:
                        # Cancel any tasks that didn't respond to shutdown event
                        for task in pending:
                            if not task.done():
                                task.cancel()

                        # Wait for cancelled tasks to complete
                        await asyncio.gather(*pending, return_exceptions=True)

                    logger.debug(
                        f"[{self.settings.name}] All {len(tasks_to_cleanup)} background tasks cleaned up"
                    )

                except Exception as e:
                    logger.warning(
                        f"[{self.settings.name}] Error during task cleanup: {e}"
                    )
                    # Force cancel all tasks as fallback
                    for task in tasks_to_cleanup:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks_to_cleanup, return_exceptions=True)

        # Close WebSocket server
        if self._websocket_server:
            try:
                self._websocket_server.close()
                await self._websocket_server.wait_closed()
                logger.debug(f"[{self.settings.name}] Closed WebSocket server")
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error closing WebSocket server: {e}"
                )

        logger.info(f"[{self.settings.name}] Async shutdown completed")

        # Clean up logger handler to prevent event loop errors
        try:
            logger.remove(self._logger_handler_id)
        except ValueError:
            # Handler already removed
            pass

    def report(self) -> None:
        """General report of current server state."""

        logger.info("Resources: {}", pp.pformat(self.settings.resources))
        # logger.info("Funs: {}", pp.pformat(self.settings.funs)) # Removed as funs is no longer in settings
        logger.info("Clients: {}", pp.pformat(self.clients))

    def _register_default_commands(self) -> None:
        """Registers the default RPC commands (echo, echos).

        NOTE: These functions are also decorated with @rpc_command, so we skip
        manual registration to avoid duplicates. They will be auto-discovered.
        """
        logger.info(
            f"🚀 {self.settings.name}: _register_default_commands() called - skipping echo/echos (auto-discovered)"
        )
        # Skip manual registration - echo and echos are @rpc_command decorated
        # and will be automatically discovered by _discover_and_register_rpc_commands()

    def _discover_and_register_rpc_commands(self) -> None:
        """Discovers and registers RPC commands defined using the @rpc_command decorator.

        This method inspects the current module's global namespace for functions
        that have been decorated with @rpc_command and registers them with the server.
        """
        logger.info(
            f"🔍 {self.settings.name}: _discover_and_register_rpc_commands() called"
        )
        for name in dir(sys.modules[__name__]):
            obj = sys.modules[__name__].__dict__[name]
            if callable(obj) and "_rpc_command_name" in obj.__dict__:
                rpc_name = obj._rpc_command_name
                rpc_resources = obj._rpc_command_resources
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
                    # Use the server's advertised URL instead of the connection's remote address
                    # For federated announcements, use original_source as the server URL
                    if req.server.hop_count > 0 and req.server.original_source:
                        server_url = req.server.original_source
                    else:
                        server_url = (
                            req.server.advertised_urls[0]
                            if req.server.advertised_urls
                            else server_connection.url
                        )
                    # Increment our logical clock for this peer update
                    self.logical_clock = self.logical_clock.increment(
                        self.cluster.local_url
                    )

                    peer_info = PeerInfo(
                        url=server_url,
                        funs=req.server.funs,
                        locs=frozenset(req.server.locs),
                        last_seen=time.time(),
                        cluster_id=req.server.cluster_id,
                        advertised_urls=req.server.advertised_urls,
                        logical_clock=self.logical_clock.to_dict(),
                    )

                    # GOODBYE Protocol: Allow immediate re-entry with new HELLO
                    # Remove from departed peers list if this peer is reconnecting
                    was_departed = server_url in self.cluster._departed_peers
                    if was_departed:
                        del self.cluster._departed_peers[server_url]
                        logger.info(
                            f"[{self.settings.name}] Allowing re-entry for previously departed peer {server_url} via new HELLO"
                        )
                        # Broadcast this re-entry to all other nodes via gossip
                        asyncio.create_task(self._broadcast_peer_reentry(peer_info))

                    self.cluster.add_fun_ability(peer_info)

                    # Schedule function confirmation publishing as background task
                    asyncio.create_task(
                        self._publish_function_confirmation(
                            req.server.funs, req.server.announcement_id
                        )
                    )

                    # Handle federated forwarding for each function if this has hop information
                    if req.server.hop_count is not None and req.server.announcement_id:
                        for fun_name in req.server.funs:
                            # Forward to other peers if within hop limit and not looping back
                            # Only forward across federation boundaries (different cluster_id)
                            should_forward = (
                                req.server.hop_count < req.server.max_hops
                                and req.server.original_source != self.cluster.local_url
                                and req.server.cluster_id != self.settings.cluster_id
                            )

                            # Debug logging removed - hop count limiting now working correctly

                            if should_forward:
                                try:
                                    # Create forwarding task for each function
                                    task = asyncio.create_task(
                                        self._broadcast_new_function(
                                            fun_name,
                                            req.server.locs,
                                            hop_count=req.server.hop_count + 1,
                                            max_hops=req.server.max_hops,
                                            announcement_id=req.server.announcement_id,
                                            original_source=req.server.original_source,
                                        )
                                    )
                                    self._background_tasks.add(task)
                                    # Remove task from tracking when it completes
                                    task.add_done_callback(
                                        self._background_tasks.discard
                                    )
                                    logger.info(
                                        f"Forwarding federated announcement: {fun_name} (hop {req.server.hop_count + 1})"
                                    )
                                except RuntimeError:
                                    # No event loop - this is normal during initialization
                                    pass

                    # 🔥 BIDIRECTIONAL HELLO: Send our own HELLO back to the connecting server
                    # This ensures PRE-CONNECTION functions are shared when cluster forms
                    try:
                        if (
                            req.server.hop_count == 0
                        ):  # Only respond to direct connections, not forwarded announcements
                            logger.info(
                                f"📡 Sending bidirectional HELLO back to {server_url}"
                            )
                            asyncio.create_task(
                                self._send_hello_response(server_connection)
                            )
                    except RuntimeError:
                        # No event loop during initialization - skip bidirectional response
                        pass

                    return RPCResponse(r="ADDED", u=req.u)
                case "GOODBYE":
                    # A server is gracefully shutting down.
                    # Handle the GOODBYE message properly with cleanup
                    sender_url = req.server.departing_node_url

                    # Process GOODBYE message asynchronously
                    asyncio.create_task(
                        self._handle_goodbye_message(req.server, sender_url)
                    )

                    return RPCResponse(r="GOODBYE_PROCESSED", u=req.u)
                case "STATUS":
                    # A server is sending a status update (e.g., for gossip protocol).\
                    # TODO: Implement actual status processing.
                    return RPCResponse(r="STATUS", u=req.u)
                case "HELLO_ACK":
                    # A server is acknowledging receipt and processing of a HELLO message
                    ack = req.server  # This is an RPCServerHelloAck
                    asyncio.create_task(self._handle_function_confirmation(ack))
                    return RPCResponse(r="ACK_RECEIVED", u=req.u)
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

        Supports enhanced debugging features including intermediate results
        and execution summaries based on request parameters.
        """
        try:
            # Create an RPC object from the incoming request. This handles
            # the topological sorting of commands.
            rpc = RPC(req)

            # Check if enhanced execution is requested
            if (
                req.return_intermediate_results
                or req.include_execution_summary
                or req.debug_mode
            ):
                logger.info("Using enhanced RPC execution with debugging features")
                # Use enhanced execution with intermediate results tracking
                (
                    result,
                    intermediate_results,
                    execution_summary,
                ) = await self.cluster.run_with_intermediate_results(rpc, req)

                return RPCResponse(
                    r=result,
                    u=req.u,
                    intermediate_results=intermediate_results,
                    execution_summary=execution_summary,
                )
            else:
                # Use standard execution for backward compatibility
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
    async def opened(
        self, websocket: websockets.server.WebSocketServerProtocol
    ) -> None:
        """Handles a new incoming websocket connection.

        This method is the entry point for all incoming messages, routing them
        to the appropriate handler based on their 'role'.
        """
        # Create a Connection object for the incoming websocket.
        # Construct proper WebSocket URL from remote address
        remote_host, remote_port = websocket.remote_address
        connection = Connection(url=f"ws://{remote_host}:{remote_port}")
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

                response_model: RPCResponse | RPCInternalAnswer | PubSubAck | None = (
                    None
                )
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
                        # Use federation manager for HELLO message authorization
                        if server_request.server.what == "HELLO":
                            remote_cluster_id = server_request.server.cluster_id
                            remote_node_id = (
                                websocket.remote_address[0]
                                if websocket.remote_address
                                else "unknown"
                            )
                            remote_url = (
                                f"ws://{remote_node_id}:{websocket.remote_address[1]}"
                                if websocket.remote_address
                                else "unknown"
                            )

                            decision = self.federation_manager.handle_connection_hello(
                                remote_cluster_id=remote_cluster_id,
                                remote_node_id=remote_node_id,
                                remote_url=remote_url,
                                local_cluster_id=self.settings.cluster_id,
                                local_node_id=self.settings.name,
                                hello_data={
                                    "functions": list(server_request.server.funs),
                                    "locations": list(server_request.server.locs),
                                    "advertised_urls": list(
                                        server_request.server.advertised_urls
                                    ),
                                },
                            )

                            if not decision.allowed:
                                logger.info(
                                    "[{}:{}] Rejected HELLO from cluster '{}': {}",
                                    *websocket.remote_address,
                                    remote_cluster_id,
                                    decision.error_message,
                                )
                                response_model = RPCResponse(
                                    r=None,
                                    error=RPCError(
                                        code=1003,
                                        message=decision.error_message,
                                    ),
                                    u=server_request.u,
                                )
                            else:
                                # Process the HELLO normally
                                response_model = self.run_server(
                                    connection, server_request
                                )

                                # Notify federation manager of successful connection
                                if remote_cluster_id != self.settings.cluster_id:
                                    self.federation_manager.notify_connection_established(
                                        remote_cluster_id, remote_node_id, remote_url
                                    )
                        else:
                            # Non-HELLO server messages processed normally
                            response_model = self.run_server(connection, server_request)

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
                        results = internal_rpc.results
                        u = internal_rpc.u

                        logger.info(
                            "Received internal-rpc request: command={}, u={}, args={}",
                            command,
                            u,
                            args,
                        )

                        # Resolve arguments using available results (though they should already be resolved)
                        resolved_args, resolved_kwargs = (
                            self.cluster._resolve_arguments(args, kwargs, results)
                        )

                        # Validate argument types for common problematic patterns
                        for i, arg in enumerate(resolved_args):
                            if isinstance(arg, dict | list) and len(str(arg)) > 200:
                                logger.warning(
                                    "Large complex object passed to function: command={}, arg[{}]={}, type={}. "
                                    "Consider if function expects simple values instead of complex objects.",
                                    command,
                                    i,
                                    type(arg).__name__,
                                    type(arg).__name__,
                                )

                        # Generate RESULT PAYLOAD
                        try:
                            command_obj = self.registry.get(command)
                            answer_payload = await command_obj.call_async(
                                *resolved_args, **resolved_kwargs
                            )
                        except Exception as e:
                            logger.error(
                                "Function execution failed: command={}, u={}, error={}, args_types={}",
                                command,
                                u,
                                str(e),
                                [type(arg).__name__ for arg in resolved_args],
                            )
                            # Send a detailed error response instead of hanging
                            response_model = RPCInternalAnswer(
                                answer={
                                    "error": str(e),
                                    "command": command,
                                    "arg_types": [
                                        type(arg).__name__ for arg in resolved_args
                                    ],
                                    "error_type": type(e).__name__,
                                },
                                u=u,
                            )
                            continue
                        response_model = RPCInternalAnswer(answer=answer_payload, u=u)

                        logger.info(
                            "Generated internal answer: u={}, answer={}",
                            u,
                            answer_payload,
                        )

                    case "gossip":
                        # Incoming gossip message from a peer.
                        gossip_message = GossipMessage.model_validate(parsed_msg)
                        # Use federation manager to determine if gossip should be processed
                        if gossip_message.cluster_id != self.settings.cluster_id:
                            # Check if cross-federation gossip is allowed
                            auth_result = self.federation_manager.federation_manager.config.is_cross_federation_connection_allowed(
                                gossip_message.cluster_id
                            )
                            if auth_result.name.startswith("REJECTED"):
                                logger.debug(
                                    "[{}:{}] Ignoring gossip from different cluster ID '{}' (federation policy: {})",
                                    *websocket.remote_address,
                                    gossip_message.cluster_id,
                                    self.federation_manager.federation_manager.config.federation_mode.value,
                                )
                                continue  # Ignore gossip from different cluster per federation policy
                            else:
                                logger.debug(
                                    "[{}:{}] Processing cross-federation gossip from cluster '{}'",
                                    *websocket.remote_address,
                                    gossip_message.cluster_id,
                                )
                        self.cluster.process_gossip_message(gossip_message)
                        # Update consensus manager with new peer information
                        self._update_consensus_known_nodes()
                        # Gossip messages do not typically require a direct response.
                        continue

                    case "pubsub-gossip":
                        # Handle pub/sub gossip messages with topic advertisements
                        pubsub_gossip = PubSubGossip.model_validate(parsed_msg)
                        # Enforce cluster_id matching for pub/sub gossip messages
                        if pubsub_gossip.cluster_id != self.settings.cluster_id:
                            logger.warning(
                                "[{}:{}] Received pub/sub gossip from different cluster ID: Expected {}, Got {}",
                                *websocket.remote_address,
                                self.cluster.cluster_id,
                                pubsub_gossip.cluster_id,
                            )
                            continue  # Ignore gossip from different cluster

                        # Process regular gossip information
                        self.cluster.process_gossip_message(
                            GossipMessage(
                                peers=pubsub_gossip.peers,
                                u=pubsub_gossip.u,
                                cluster_id=pubsub_gossip.cluster_id,
                            )
                        )
                        # Update topic advertisements
                        self.topic_exchange.update_remote_topics(
                            list(pubsub_gossip.topics)
                        )
                        continue

                    case "consensus-proposal":
                        # Handle consensus proposal messages within cluster
                        proposal_msg = ConsensusProposalMessage.model_validate(
                            parsed_msg
                        )
                        # Enforce cluster_id matching for consensus messages
                        if proposal_msg.cluster_id != self.settings.cluster_id:
                            logger.warning(
                                "[{}:{}] Received consensus proposal from different cluster ID: Expected {}, Got {}",
                                *websocket.remote_address,
                                self.settings.cluster_id,
                                proposal_msg.cluster_id,
                            )
                            continue  # Ignore consensus from different cluster

                        # Process consensus proposal via consensus manager
                        logger.info(
                            f"🏛️ [{self.settings.name}] RECEIVED consensus proposal {proposal_msg.proposal_id} from {websocket.remote_address}"
                        )
                        await self.consensus_manager.handle_proposal_message(
                            proposal_msg.model_dump()
                        )
                        logger.info(
                            f"✅ [{self.settings.name}] PROCESSED consensus proposal {proposal_msg.proposal_id}"
                        )
                        continue

                    case "consensus-vote":
                        # Handle consensus vote messages within cluster
                        vote_msg = ConsensusVoteMessage.model_validate(parsed_msg)
                        # Enforce cluster_id matching for consensus messages
                        if vote_msg.cluster_id != self.settings.cluster_id:
                            logger.warning(
                                "[{}:{}] Received consensus vote from different cluster ID: Expected {}, Got {}",
                                *websocket.remote_address,
                                self.settings.cluster_id,
                                vote_msg.cluster_id,
                            )
                            continue  # Ignore consensus from different cluster

                        # Process consensus vote via consensus manager
                        logger.info(
                            f"🗳️ [{self.settings.name}] RECEIVED consensus vote for {vote_msg.proposal_id} from {websocket.remote_address}"
                        )
                        await self.consensus_manager.handle_vote_message(
                            vote_msg.model_dump()
                        )
                        logger.info(
                            f"✅ [{self.settings.name}] PROCESSED consensus vote for {vote_msg.proposal_id}"
                        )
                        continue

                    case "consensus-vote":
                        # Handle consensus vote messages within cluster
                        vote_msg = ConsensusVoteMessage.model_validate(parsed_msg)
                        # Enforce cluster_id matching for consensus messages
                        if vote_msg.cluster_id != self.settings.cluster_id:
                            logger.warning(
                                "[{}:{}] Received consensus vote from different cluster ID: Expected {}, Got {}",
                                *websocket.remote_address,
                                self.settings.cluster_id,
                                vote_msg.cluster_id,
                            )
                            continue  # Ignore consensus from different cluster

                        # Process consensus vote via consensus manager
                        await self.consensus_manager.vote_on_proposal(
                            vote_msg.proposal_id, vote_msg.vote, vote_msg.voter_id
                        )
                        logger.debug(
                            f"Processed consensus vote from {vote_msg.voter_id} on proposal {vote_msg.proposal_id}"
                        )
                        continue

                    case "pubsub-publish":
                        # Handle message publication
                        publish_req = PubSubPublish.model_validate(parsed_msg)
                        notifications = self.topic_exchange.publish_message(
                            publish_req.message
                        )

                        # Send notifications to local subscribers
                        for notification in notifications:
                            await self._send_notification_to_client(notification)

                        # Send acknowledgment
                        response_model = PubSubAck(
                            operation_id=publish_req.u,
                            success=True,
                            u=f"ack_{publish_req.u}",
                        )

                    case "pubsub-subscribe":
                        # Handle subscription request
                        subscribe_req = PubSubSubscribe.model_validate(parsed_msg)
                        self.topic_exchange.add_subscription(subscribe_req.subscription)

                        # Track which client made this subscription
                        client_id = f"client_{id(websocket)}"
                        self.pubsub_clients[client_id] = websocket
                        self.subscription_to_client[
                            subscribe_req.subscription.subscription_id
                        ] = client_id

                        # Send acknowledgment
                        response_model = PubSubAck(
                            operation_id=subscribe_req.u,
                            success=True,
                            u=f"ack_{subscribe_req.u}",
                        )

                    case "pubsub-unsubscribe":
                        # Handle unsubscription request
                        unsubscribe_req = PubSubUnsubscribe.model_validate(parsed_msg)
                        success = self.topic_exchange.remove_subscription(
                            unsubscribe_req.subscription_id
                        )

                        # Clean up client tracking
                        if (
                            unsubscribe_req.subscription_id
                            in self.subscription_to_client
                        ):
                            client_id = self.subscription_to_client.pop(
                                unsubscribe_req.subscription_id
                            )
                            # Check if client has any remaining subscriptions
                            remaining_subs = [
                                sid
                                for sid, cid in self.subscription_to_client.items()
                                if cid == client_id
                            ]
                            if not remaining_subs:
                                self.pubsub_clients.pop(client_id, None)

                        # Send acknowledgment
                        response_model = PubSubAck(
                            operation_id=unsubscribe_req.u,
                            success=success,
                            u=f"ack_{unsubscribe_req.u}",
                        )

                    # Removed "rpc-announcement" case - federated RPC now uses enhanced HELLO messages

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
                    logger.info(
                        "Sending response: type={}, u={}",
                        type(response_model).__name__,
                        response_model.u,
                    )
                    try:
                        await websocket.send(
                            self.serializer.serialize(response_model.model_dump())
                        )
                        logger.info("Response sent successfully")
                    except Exception:
                        logger.error(
                            "[{}:{}] Client connection error! Dropping reply.",
                            *websocket.remote_address,
                        )
        finally:
            # TODO: if this was a SERVER, we need to clean up the server resources.
            # TODO: if this was a CLIENT, we need to cancel any oustanding requests/subscriptions too.
            try:
                self.clients.remove(connection)
            except KeyError:
                # Connection might have already been removed
                pass

            # Clean up PubSub client tracking when client disconnects
            client_id = f"client_{id(websocket)}"
            if client_id in self.pubsub_clients:
                del self.pubsub_clients[client_id]
                # Remove all subscriptions for this client
                subs_to_remove = [
                    sid
                    for sid, cid in self.subscription_to_client.items()
                    if cid == client_id
                ]
                for sub_id in subs_to_remove:
                    del self.subscription_to_client[sub_id]

    async def _send_notification_to_client(
        self, notification: PubSubNotification
    ) -> None:
        """Send a PubSub notification to the appropriate client."""
        subscription_id = notification.subscription_id

        # Find which client this subscription belongs to
        if subscription_id not in self.subscription_to_client:
            logger.warning(f"No client found for subscription {subscription_id}")
            return

        client_id = self.subscription_to_client[subscription_id]
        if client_id not in self.pubsub_clients:
            logger.warning(
                f"Client {client_id} not found for subscription {subscription_id}"
            )
            return

        websocket = self.pubsub_clients[client_id]

        try:
            # Send the notification as a WebSocket message
            await websocket.send(self.serializer.serialize(notification.model_dump()))
            logger.debug(
                f"Sent notification to client {client_id} for subscription {subscription_id}"
            )
        except Exception as e:
            logger.error(f"Failed to send notification to client {client_id}: {e}")
            # Clean up dead client connection
            if client_id in self.pubsub_clients:
                del self.pubsub_clients[client_id]
                subs_to_remove = [
                    sid
                    for sid, cid in self.subscription_to_client.items()
                    if cid == client_id
                ]
                for sub_id in subs_to_remove:
                    del self.subscription_to_client[sub_id]

    async def _establish_peer_connection(self, peer_url: str) -> None:
        """Establishes a persistent connection to a peer for RPC forwarding.

        Args:
            peer_url: The URL of the peer to connect to.
        """
        # Do not establish new connections during shutdown
        if self._shutdown_event.is_set():
            logger.info(
                f"[{self.settings.name}] Skipping peer connection to {peer_url} - shutdown in progress"
            )
            return
        if peer_url in self.peer_connections:
            # Check if existing connection is still valid
            if self.peer_connections[peer_url].is_connected:
                logger.debug(
                    "[{}] Already connected to peer: {}", self.settings.name, peer_url
                )
                return
            else:
                # Clean up dead connection
                # Publish connection lost event
                event = ConnectionEvent.lost(peer_url, self.cluster.local_url)
                self.cluster.connection_event_bus.publish(event)

                del self.peer_connections[peer_url]

        logger.info(
            "[{}] Establishing persistent connection to peer: {}",
            self.settings.name,
            peer_url,
        )
        try:
            # Create persistent connection for RPC forwarding
            # Adjust connection parameters based on cluster size for better scalability
            cluster_size = len(self.cluster.peers_info) + 1  # +1 for self
            if cluster_size >= 50:
                # For very large clusters: fewer retries, longer delays to reduce connection storms
                connection = Connection(url=peer_url, max_retries=3, base_delay=2.0)
            elif cluster_size >= 20:
                # For medium clusters: moderate parameters
                connection = Connection(url=peer_url, max_retries=4, base_delay=1.5)
            else:
                # For small clusters: default aggressive parameters
                connection = Connection(url=peer_url)

            await connection.connect()
            self.peer_connections[peer_url] = connection

            # Publish connection established event
            logger.info(f"Publishing connection established event for {peer_url}")
            event = ConnectionEvent.established(peer_url, self.cluster.local_url)
            self.cluster.connection_event_bus.publish(event)
            logger.info(f"Connection event published for {peer_url}")

            # Send HELLO message to announce ourselves
            # Get all registered functions for HELLO message
            registered_functions = tuple(self.registry._commands.keys())
            logger.info(f"📤 Sending HELLO with functions: {registered_functions}")

            capabilities = ServerCapabilities.create(
                functions=registered_functions,
                resources=tuple(self.settings.resources or []),
                cluster_id=self.settings.cluster_id,
                advertised_urls=tuple(
                    self.settings.advertised_urls
                    or [f"ws://{self.settings.host}:{self.settings.port}"]
                ),
            )

            hello_message = RPCServerRequest(
                server=RPCServerHello.create_from_capabilities(capabilities),
                u=str(ulid.new()),
            )

            await connection.send(self.serializer.serialize(hello_message.model_dump()))

            # 🔥 CRITICAL FIX: Start message reading task for peer connection
            # This ensures we can receive bidirectional HELLO messages and other responses
            task = asyncio.create_task(
                self._handle_peer_connection_messages(connection, peer_url)
            )
            self._background_tasks.add(task)
            # Remove task from tracking when it completes
            task.add_done_callback(self._background_tasks.discard)

            logger.info(
                "[{}] Successfully connected to peer: {}", self.settings.name, peer_url
            )
        except Exception as e:
            logger.error(
                "[{}] Failed to connect to peer {}: {}", self.settings.name, peer_url, e
            )

    async def _handle_peer_connection_messages(
        self, connection: Connection, peer_url: str
    ) -> None:
        """Handle incoming messages from a peer connection.

        This is CRITICAL for bidirectional communication - it ensures that when we
        connect TO a peer, we also READ responses and bidirectional messages FROM that peer.

        Args:
            connection: The outgoing connection to the peer
            peer_url: The URL of the peer we're connected to
        """
        try:
            logger.info(
                f"🔄 Starting message reading loop for peer connection: {peer_url}"
            )

            while connection.is_connected and not self._shutdown_event.is_set():
                try:
                    # Read message from peer connection
                    msg = await connection.receive()
                    if msg is None:
                        break

                    # Parse the message
                    parsed_msg = self.serializer.deserialize(
                        msg.encode("utf-8") if isinstance(msg, str) else msg
                    )

                    # Track message processing statistics for verification

                    self._msg_stats.total_processed += 1

                    # Skip logging and processing for RPC responses (these are just acknowledgments)
                    if "r" in parsed_msg and parsed_msg.get("role") != "server":
                        self._msg_stats.rpc_responses_skipped += 1
                        # Log every 10th skipped RPC response to prove we're actually skipping them
                        if self._msg_stats.rpc_responses_skipped % 10 == 1:
                            logger.info(
                                f"🔄 SKIP: RPC response #{self._msg_stats.rpc_responses_skipped} - Type: {parsed_msg.get('r')} - PREVENTING INFINITE LOOP"
                            )
                        # Log summary every 50 messages
                        if self._msg_stats.total_processed % 50 == 0:
                            logger.info(
                                f"📊 MESSAGE STATS: Total={self._msg_stats.total_processed}, RPC_Skipped={self._msg_stats.rpc_responses_skipped}, Servers={self._msg_stats.server_messages}, Others={self._msg_stats.other_messages}"
                            )
                        continue

                    logger.info(f"📥 Received message from peer {peer_url}: {msg!r}")

                    # Process the message using the same logic as the opened() method
                    if parsed_msg.get("role") == "server":
                        self._msg_stats.server_messages += 1
                        # Convert to proper Pydantic model
                        server_request = RPCServerRequest.model_validate(parsed_msg)
                        logger.info(
                            f"🔥 Processing server message from peer {peer_url}: {server_request.server.what}"
                        )

                        if server_request.server.what == "HELLO":
                            # Process bidirectional HELLO - add functions to our cluster
                            logger.info(
                                f"📨 Processing bidirectional HELLO from {peer_url} with functions: {server_request.server.funs}"
                            )

                            # Register the functions in our cluster - create proper PeerInfo object
                            import time

                            # Increment our logical clock for this peer update
                            self.logical_clock = self.logical_clock.increment(
                                self.cluster.local_url
                            )

                            peer_info = PeerInfo(
                                url=peer_url,
                                funs=tuple(server_request.server.funs),
                                locs=frozenset(server_request.server.locs),
                                advertised_urls=tuple(
                                    server_request.server.advertised_urls
                                ),
                                last_seen=time.time(),
                                cluster_id=server_request.server.cluster_id,
                                logical_clock=self.logical_clock.to_dict(),
                            )
                            self.cluster.add_fun_ability(peer_info)

                            logger.info(
                                f"✅ Successfully processed bidirectional HELLO from {peer_url}"
                            )

                        elif server_request.server.what == "GOODBYE":
                            # Process GOODBYE message - remove peer from cluster
                            logger.info(
                                f"👋 Processing GOODBYE from {peer_url} (reason: {server_request.server.reason.value})"
                            )

                            await self._handle_goodbye_message(
                                server_request.server, peer_url
                            )

                            logger.info(
                                f"✅ Successfully processed GOODBYE from {peer_url}"
                            )

                except TimeoutError:
                    # Timeout is normal - just continue
                    continue
                except Exception as e:
                    logger.warning(
                        f"❌ Error processing message from peer {peer_url}: {e}"
                    )
                    break

        except asyncio.CancelledError:
            logger.debug(
                f"🛑 Peer connection message handler cancelled for {peer_url} (clean shutdown)"
            )
            # Re-raise to properly handle cancellation
            raise
        except Exception as e:
            logger.error(
                f"💥 Fatal error in peer connection message handler for {peer_url}: {e}"
            )
        finally:
            logger.info(
                f"🔚 Message reading loop ended for peer connection: {peer_url}"
            )

    async def _broadcast_peer_reentry(self, peer_info: PeerInfo) -> None:
        """Broadcast peer re-entry to all connected peers via gossip.

        This ensures all nodes clear the departed peer from their blocklists
        when a peer sends a new HELLO after GOODBYE.
        """
        # Create a special gossip message containing just the re-entering peer
        reentry_gossip = GossipMessage(
            peers=(peer_info,),  # Use tuple not list
            u=str(ulid.new()),
            cluster_id=self.settings.cluster_id,
        )

        # Send to all connected peers
        for peer_url, connection in list(self.peer_connections.items()):
            try:
                # Gossip messages are sent as regular RPC messages, not server messages
                message_bytes = self.serializer.serialize(reentry_gossip.model_dump())
                await connection.send(message_bytes)
                logger.info(
                    f"[{self.settings.name}] Broadcast peer re-entry for {peer_info.url} to {peer_url}"
                )
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Failed to broadcast peer re-entry to {peer_url}: {e}"
                )

    async def _handle_goodbye_message(
        self, goodbye: RPCServerGoodbye, sender_url: str
    ) -> None:
        """Handle GOODBYE message from departing peer.

        Args:
            goodbye: The GOODBYE message received
            sender_url: URL of the peer sending the GOODBYE
        """

        logger.info(
            f"[{self.settings.name}] Received GOODBYE from {goodbye.departing_node_url} "
            f"(reason: {goodbye.reason.value}, cluster: {goodbye.cluster_id})"
        )

        # Remove from cluster immediately
        removed = await self.cluster.remove_peer(goodbye.departing_node_url)
        if removed:
            logger.info(
                f"[{self.settings.name}] Removed peer {goodbye.departing_node_url} from cluster"
            )
        else:
            logger.warning(
                f"[{self.settings.name}] Peer {goodbye.departing_node_url} was not in cluster"
            )

        # Close connection to departing node if we have one
        if goodbye.departing_node_url in self.peer_connections:
            try:
                await self.peer_connections[goodbye.departing_node_url].disconnect()
                del self.peer_connections[goodbye.departing_node_url]
                logger.info(
                    f"[{self.settings.name}] Closed connection to departing peer {goodbye.departing_node_url}"
                )
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error closing connection to {goodbye.departing_node_url}: {e}"
                )

        # TODO: Propagate GOODBYE via gossip (Phase 2)
        # await self._propagate_goodbye_via_gossip(goodbye)

    async def send_goodbye(self, reason: GoodbyeReason) -> None:
        """Send GOODBYE to all peers before departure.

        Args:
            reason: Reason for leaving the cluster
        """
        import ulid

        from .core.model import RPCServerGoodbye, RPCServerRequest

        departing_url = f"ws://{self.settings.host}:{self.settings.port}"

        goodbye = RPCServerGoodbye(
            departing_node_url=departing_url,
            cluster_id=self.settings.cluster_id,
            reason=reason,
        )

        goodbye_request = RPCServerRequest(server=goodbye, u=str(ulid.new()))

        logger.info(
            f"[{self.settings.name}] Sending GOODBYE to {len(self.peer_connections)} peers "
            f"(reason: {reason.value})"
        )

        # Broadcast to all connected peers
        message_bytes = self.serializer.serialize(goodbye_request.model_dump())
        successful_sends = 0

        for peer_url, connection in list(self.peer_connections.items()):
            try:
                await connection.send(message_bytes)
                successful_sends += 1
                logger.debug(f"[{self.settings.name}] Sent GOODBYE to {peer_url}")
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Failed to send GOODBYE to {peer_url}: {e}"
                )

        logger.info(
            f"[{self.settings.name}] Successfully sent GOODBYE to {successful_sends}/{len(self.peer_connections)} peers"
        )

    async def _send_hello_response(self, server_connection: Connection) -> None:
        """Send our own HELLO message back to a server that just connected to us.

        This implements bidirectional HELLO exchange, ensuring that when Server2 connects
        to Server1, Server1 also sends its functions (including PRE-CONNECTION functions)
        back to Server2.

        Args:
            server_connection: The connection to the server that sent us a HELLO
        """
        try:
            # Get all our registered functions (including PRE-CONNECTION functions)
            registered_functions = tuple(self.registry._commands.keys())
            logger.info(
                f"📤 Sending bidirectional HELLO with functions: {registered_functions}"
            )

            # Create our server capabilities
            capabilities = ServerCapabilities.create(
                functions=registered_functions,
                resources=tuple(self.settings.resources or []),
                cluster_id=self.settings.cluster_id,
                advertised_urls=tuple(
                    self.settings.advertised_urls
                    or [f"ws://{self.settings.host}:{self.settings.port}"]
                ),
            )

            # Create HELLO message
            hello_message = RPCServerRequest(
                server=RPCServerHello.create_from_capabilities(capabilities),
                u=str(ulid.new()),
            )

            # Send our HELLO back via the existing connection
            if server_connection.is_connected:
                await server_connection.send(
                    self.serializer.serialize(hello_message.model_dump())
                )
                logger.info(f"✅ Sent bidirectional HELLO to {server_connection.url}")
            else:
                logger.warning(
                    f"❌ Cannot send bidirectional HELLO - connection to {server_connection.url} is not connected"
                )

        except Exception as e:
            logger.error(
                f"❌ Error sending bidirectional HELLO to {server_connection.url}: {e}"
            )

    async def _manage_peer_connections(self) -> None:
        """Periodically checks for new peers and maintains connections.

        This background task ensures that the server maintains persistent
        connections to all known peers in the cluster.
        """
        while not self._shutdown_event.is_set():
            # Iterate through known peers and establish connections if needed
            for peer_url, peer_info in list(
                self.cluster.peers_info.items()
            ):  # Use list to avoid RuntimeError during dict modification
                # Check for shutdown before each connection attempt
                if self._shutdown_event.is_set():
                    logger.info(
                        f"[{self.settings.name}] Stopping peer connection loop - shutdown in progress"
                    )
                    return

                if peer_url != f"ws://{self.settings.host}:{self.settings.port}":
                    # Use federation manager to determine if peer connection should be established
                    auth_result = self.federation_manager.federation_manager.config.is_cross_federation_connection_allowed(
                        peer_info.cluster_id
                    )

                    if (
                        peer_info.cluster_id == self.settings.cluster_id
                        or not auth_result.name.startswith("REJECTED")
                    ):
                        await self._establish_peer_connection(peer_url)
                        if peer_info.cluster_id != self.settings.cluster_id:
                            logger.debug(
                                "[{}] Establishing cross-federation connection to peer {} (cluster: {})",
                                self.settings.name,
                                peer_url,
                                peer_info.cluster_id,
                            )
                    else:
                        logger.debug(
                            "[{}] Skipping peer {} due to federation policy (cluster: {}, mode: {})",
                            self.settings.name,
                            peer_url,
                            peer_info.cluster_id,
                            self.federation_manager.federation_manager.config.federation_mode.value,
                        )

            # Clean up dead connections (but not during shutdown to avoid triggering reconnects)
            if not self._shutdown_event.is_set():
                dead_connections = [
                    url
                    for url, conn in self.peer_connections.items()
                    if not conn.is_connected
                ]
                for url in dead_connections:
                    logger.info(
                        "[{}] Removing dead connection to {}", self.settings.name, url
                    )
                    # Publish connection lost event
                    event = ConnectionEvent.lost(url, self.cluster.local_url)
                    self.cluster.connection_event_bus.publish(event)

                    del self.peer_connections[url]

            # 🔥 GOSSIP PROTOCOL: Send peer metadata to all connected peers
            await self._send_gossip_messages()

            # Periodically check for new peers.
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=self.settings.gossip_interval
                )
                # If we get here, shutdown was signaled
                break
            except TimeoutError:
                # Timeout is normal, continue the loop
                continue
            except asyncio.CancelledError:
                # Task was cancelled - exit immediately
                break

    async def _send_gossip_messages(self) -> None:
        """Send gossip messages with peer metadata to all connected peers.

        This implements a scalable gossip protocol with exponential backoff for large clusters
        to prevent gossip storms while maintaining eventual consistency.
        """
        # Get all known peers (including self)
        all_known_peers = list(self.cluster.peers_info.values())

        if not all_known_peers:
            # No peers to gossip about
            return

        # Get active connections
        active_connections = [
            (url, conn)
            for url, conn in self.peer_connections.items()
            if conn.is_connected
        ]

        if not active_connections:
            logger.debug(
                f"[{self.settings.name}] No active peer connections for gossip"
            )
            return

        # SCALABILITY FIX: Implement gossip rate limiting for large clusters
        cluster_size = len(all_known_peers)
        if cluster_size >= 30:
            # Large clusters: Use probabilistic gossip to reduce message volume
            # Only gossip to a random subset of peers to prevent gossip storms
            import random

            max_gossip_targets = min(10, len(active_connections))  # Cap at 10 peers
            active_connections = random.sample(active_connections, max_gossip_targets)

            # Also implement gossip dampening - skip gossip cycles randomly
            gossip_probability = max(
                0.3, 1.0 / (cluster_size / 20)
            )  # Reduce frequency for large clusters
            if random.random() > gossip_probability:
                logger.debug(
                    f"[{self.settings.name}] Skipping gossip cycle (dampening for large cluster)"
                )
                return

        # Create gossip message with all known peer metadata
        # CRITICAL FIX: Update vector clocks before gossiping to reflect our logical time advancement
        # Increment our logical clock once for this gossip event
        self.logical_clock = self.logical_clock.increment(self.cluster.local_url)
        current_logical_clock = self.logical_clock.to_dict()

        # Update logical clocks in place for all peers
        for peer_info in all_known_peers:
            peer_info.logical_clock = current_logical_clock

        gossip_message = GossipMessage(
            peers=tuple(all_known_peers),
            u=str(ulid.new()),
            cluster_id=self.settings.cluster_id,
        )

        logger.debug(
            f"🗣️ [{self.settings.name}] Sending gossip with {len(all_known_peers)} peers to {len(active_connections)} connections"
        )

        # Send gossip message to selected peer connections
        gossip_data = self.serializer.serialize(gossip_message.model_dump())

        for peer_url, connection in active_connections:
            try:
                await connection.send(gossip_data)
                logger.debug(f"📤 Sent gossip to {peer_url}")
            except Exception as e:
                logger.warning(f"❌ Failed to send gossip to {peer_url}: {e}")

    async def _broadcast_consensus_message_unified(
        self, message_data: dict[str, Any]
    ) -> None:
        """
        Unified callback for broadcasting consensus messages (proposals and votes).

        Routes to appropriate method based on message role.

        Args:
            message_data: Consensus message data from ConsensusManager
        """
        message_role = message_data.get("role", "")

        if message_role == "consensus-vote":
            # This is a vote message
            await self._broadcast_consensus_vote(
                proposal_id=message_data["proposal_id"],
                vote=message_data["vote"],
                voter_id=message_data["voter_id"],
            )
        else:
            # This is a proposal message (default)
            await self._broadcast_consensus_message(message_data)

    async def _broadcast_consensus_message(self, proposal_data: dict[str, Any]) -> None:
        """Broadcast consensus proposal via server's gossip infrastructure.

        This method integrates the ConsensusManager with the server's existing
        peer connection infrastructure for intra-cluster consensus.

        Args:
            proposal_data: Consensus proposal data from ConsensusManager
        """
        logger.info(
            f"🎯 [{self.settings.name}] _broadcast_consensus_message CALLED with proposal: {proposal_data.get('proposal_id', 'UNKNOWN')}"
        )
        # Get active connections for intra-cluster broadcast
        active_connections = [
            (url, conn)
            for url, conn in self.peer_connections.items()
            if conn.is_connected
        ]

        if not active_connections:
            logger.debug(
                f"[{self.settings.name}] No active peer connections for consensus broadcast"
            )
            return

        # Create consensus proposal message using server's message format
        from mpreg.core.model import ConsensusProposalMessage

        proposal_message = ConsensusProposalMessage(
            proposal_id=proposal_data["proposal_id"],
            proposer_node_id=proposal_data["proposer_node_id"],
            state_key=proposal_data["state_key"],
            proposed_value=proposal_data["proposed_value"],
            required_votes=proposal_data["required_votes"],
            consensus_deadline=proposal_data["consensus_deadline"],
            u=str(ulid.new()),  # Generate unique message ID
            cluster_id=self.settings.cluster_id,
        )

        logger.info(
            f"🏛️ [{self.settings.name}] Broadcasting consensus proposal {proposal_data['proposal_id']} to {len(active_connections)} peers"
        )

        # Serialize and send to all cluster peers concurrently
        proposal_data_bytes = self.serializer.serialize(proposal_message.model_dump())

        async def send_to_peer(peer_url: str, connection) -> None:
            try:
                await connection.send(proposal_data_bytes)
                logger.info(f"📤 Sent consensus proposal to {peer_url}")
            except Exception as e:
                logger.warning(
                    f"❌ Failed to send consensus proposal to {peer_url}: {e}"
                )

        # Send to all peers concurrently
        await asyncio.gather(
            *[
                send_to_peer(peer_url, connection)
                for peer_url, connection in active_connections
            ],
            return_exceptions=True,
        )

    async def _broadcast_consensus_vote(
        self, proposal_id: str, vote: bool, voter_id: str
    ) -> None:
        """Broadcast consensus vote via server's gossip infrastructure.

        This method broadcasts votes to all cluster peers for vote propagation.

        Args:
            proposal_id: ID of the proposal being voted on
            vote: The vote (True for accept, False for reject)
            voter_id: ID of the node casting the vote
        """
        logger.info(
            f"🗳️ [{self.settings.name}] _broadcast_consensus_vote CALLED for proposal {proposal_id}, vote={vote}, voter={voter_id}"
        )

        # Get active connections for intra-cluster broadcast
        active_connections = [
            (url, conn)
            for url, conn in self.peer_connections.items()
            if conn.is_connected
        ]

        if not active_connections:
            logger.debug(
                f"[{self.settings.name}] No active peer connections for vote broadcast"
            )
            return

        # Create consensus vote message using server's message format
        from mpreg.core.model import ConsensusVoteMessage

        vote_message = ConsensusVoteMessage(
            proposal_id=proposal_id,
            vote=vote,
            voter_id=voter_id,
            u=str(ulid.new()),  # Generate unique message ID
            cluster_id=self.settings.cluster_id,
        )

        logger.info(
            f"🗳️ [{self.settings.name}] Broadcasting consensus vote for {proposal_id} to {len(active_connections)} peers"
        )

        # Serialize and send to all cluster peers concurrently
        vote_data_bytes = self.serializer.serialize(vote_message.model_dump())

        async def send_vote_to_peer(peer_url: str, connection) -> None:
            try:
                await connection.send(vote_data_bytes)
                logger.info(f"📤 Sent consensus vote to {peer_url}")
            except Exception as e:
                logger.warning(f"❌ Failed to send consensus vote to {peer_url}: {e}")

        # Send to all peers concurrently
        await asyncio.gather(
            *[
                send_vote_to_peer(peer_url, connection)
                for peer_url, connection in active_connections
            ],
            return_exceptions=True,
        )

    def _update_consensus_known_nodes(self) -> None:
        """Update consensus manager with current cluster peer information."""
        all_known_peers = list(self.cluster.peers_info.values())

        # Clear existing known nodes
        self.consensus_manager.known_nodes.clear()

        # Add all known peers (including self)
        for peer_info in all_known_peers:
            self.consensus_manager.known_nodes.add(peer_info.url)

        logger.debug(
            f"Updated consensus known_nodes with {len(self.consensus_manager.known_nodes)} peers"
        )

    def register_command(
        self, name: str, func: Callable[..., Any], resources: Iterable[str]
    ) -> None:
        """Register a command with the server.

        Args:
            name: The name of the command.
            func: The callable function that implements the command.
            resources: An iterable of resource strings associated with the command.

        Raises:
            ValueError: If the command name is already registered on this server.
        """
        # Check for duplicate registration on this server
        if name in self.registry:
            import traceback

            stack_trace = traceback.format_stack()
            caller = stack_trace[-2].strip()

            logger.error(
                f"🚨 DUPLICATE FUNCTION REGISTRATION DETECTED: {self.settings.name}"
            )
            logger.error(f"   Function '{name}' is already registered on this server")
            logger.error(f"   Registration attempted from: {caller}")
            logger.error(
                "   This indicates a configuration error - each server must have unique function names"
            )

            raise ValueError(
                f"Function '{name}' is already registered on server '{self.settings.name}'. "
                f"Each server must have unique RPC function names. "
                f"Check for duplicate @rpc_command decorations or manual registrations."
            )

        logger.info(
            f"✅ {self.settings.name}: Registering function '{name}' with resources {list(resources)}"
        )
        self.registry.register(Command(name, func))
        # Update local vector clock for this peer info change
        self.logical_clock = self.logical_clock.increment(self.cluster.local_url)

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
            logical_clock=self.logical_clock.to_dict(),
        )
        self.cluster.add_fun_ability(peer_info)

        # Immediately broadcast the new function to all connected peers (if event loop is running)
        try:
            # Use enhanced federated broadcast instead of separate system
            task = asyncio.create_task(self._broadcast_new_function(name, resources))
            self._background_tasks.add(task)
            # Remove task from tracking when it completes
            task.add_done_callback(self._background_tasks.discard)
            logger.info(f"Federated function announcement initiated for: {name}")
        except RuntimeError:
            # No event loop running - this is normal during server initialization
            pass

    async def _broadcast_new_function(
        self,
        name: str,
        resources: Iterable[str],
        hop_count: int = 0,
        max_hops: int = 3,
        announcement_id: str = "",
        original_source: str = "",
    ) -> None:
        """Broadcast a newly registered function to all connected peers with federated propagation.

        Args:
            name: The name of the newly registered function.
            resources: The resources associated with the function.
            hop_count: Number of hops from original source (for federated propagation).
            max_hops: Maximum hops before dropping message.
            announcement_id: Unique ID for deduplication.
            original_source: Original server that announced the function.
        """
        import time

        logger.info(
            f"🔥 _broadcast_new_function CALLED for {name} (hop={hop_count}) - peer_connections: {len(self.peer_connections)} = {list(self.peer_connections.keys())}"
        )

        # Create type-safe federated announcement
        try:
            effective_original_source = original_source or self.cluster.local_url
            announcement = FederatedRPCAnnouncement.create_initial(
                functions=(name,),
                resources=tuple(resources),
                cluster_id=self.settings.cluster_id,
                original_source=effective_original_source,
                advertised_urls=self.settings.advertised_urls
                or (f"ws://{self.settings.host}:{self.settings.port}",),
                max_hops=max_hops,
            )

            # If this is a forwarded announcement, update hop count
            if hop_count > 0:
                # Create forwarded version with proper hop count
                forwarded_propagation = create_federated_propagation_from_primitives(
                    hop_count=hop_count,
                    max_hops=max_hops,
                    announcement_id=announcement_id,
                    original_source=effective_original_source,  # FIX: Use effective_original_source, not original_source
                )
                announcement = FederatedRPCAnnouncement(
                    capabilities=announcement.capabilities,
                    propagation=forwarded_propagation,
                )
        except Exception as e:
            logger.error(f"Failed to create federated announcement: {e}")
            return

        # Check if we should process this announcement
        current_time: Timestamp = time.time()
        local_node: NodeURL = self.cluster.local_url

        if not announcement.should_process(
            local_node, self.cluster.config.announcement_tracker
        ):
            logger.warning(
                f"⏭️  SKIPPING announcement processing for {name}: {announcement.propagation.announcement_id} (already seen or loop detected)"
            )
            return

        # Mark announcement as seen
        self.cluster.config.mark_announcement_seen(
            str(announcement.propagation.announcement_id)
        )

        # Clean up expired announcements
        self.cluster.config.cleanup_expired_announcements()

        logger.info(
            f"Processing federated announcement: {name} from {str(announcement.propagation.original_source)} (hop {announcement.propagation.hop_count}/{announcement.propagation.max_hops})"
        )

        # Create RPCServerHello using the structured announcement
        hello_message = RPCServerRequest(
            server=RPCServerHello.create_from_capabilities(
                announcement.capabilities, announcement.propagation
            ),
            u=str(ulid.new()),
        )

        # Send to all connected peers concurrently using existing pub/sub system for acknowledgments
        async def send_to_peer(connection: Connection) -> None:
            try:
                if connection.is_connected:
                    await connection.send(
                        self.serializer.serialize(hello_message.model_dump())
                    )
                    logger.debug(
                        "[{}] Broadcasted new function '{}' to peer {}",
                        self.settings.name,
                        name,
                        connection.url,
                    )
            except Exception as e:
                logger.warning(
                    "[{}] Failed to broadcast new function to peer {}: {}",
                    self.settings.name,
                    connection.url,
                    e,
                )

        # Get all known peers from both peer_connections and cluster.peers_info
        all_peer_urls = set(self.peer_connections.keys())
        for peer_url in self.cluster.peers_info.keys():
            if (
                peer_url != f"ws://{self.settings.host}:{self.settings.port}"
            ):  # Don't send to ourselves
                all_peer_urls.add(peer_url)

        # Gather all send operations for concurrent execution
        if all_peer_urls:
            logger.info(
                f"🚀 BROADCASTING {name} to {len(all_peer_urls)} peers: {list(all_peer_urls)}"
            )

            async def send_to_peer_url(peer_url: str) -> None:
                """Send to a peer, creating connection if needed."""
                try:
                    # Use existing connection if available
                    if (
                        peer_url in self.peer_connections
                        and self.peer_connections[peer_url].is_connected
                    ):
                        connection = self.peer_connections[peer_url]
                    else:
                        # Create on-demand connection for broadcasting
                        logger.debug(
                            f"Creating on-demand connection to {peer_url} for broadcast"
                        )
                        connection = Connection(url=peer_url)
                        await connection.connect()
                        # Don't store in peer_connections - this is just for broadcasting

                    if connection.is_connected:
                        await connection.send(
                            self.serializer.serialize(hello_message.model_dump())
                        )
                        logger.debug(f"📤 Sent HELLO to {peer_url}")
                    else:
                        logger.warning(
                            f"❌ Failed to connect to {peer_url} for broadcast"
                        )
                except Exception as e:
                    logger.warning(f"❌ Error sending to {peer_url}: {e}")

            send_tasks = [send_to_peer_url(peer_url) for peer_url in all_peer_urls]
            await asyncio.gather(*send_tasks, return_exceptions=True)

            # Use existing pub/sub system to wait for function registration confirmations
            await self._wait_for_function_confirmation(
                name, announcement.propagation.announcement_id, list(all_peer_urls)
            )

            logger.info(f"✅ BROADCAST COMPLETE for {name}")
        else:
            logger.warning(f"❌ NO KNOWN PEERS - Cannot broadcast {name}")

    async def _wait_for_function_confirmation(
        self,
        function_name: str,
        announcement_id: AnnouncementID,
        peer_urls: list[NodeURL],
    ) -> None:
        """
        Wait for function registration confirmation from peers using existing server-to-server messaging.

        This leverages the existing peer connection infrastructure to wait for HELLO_ACK
        messages that confirm peers have successfully registered the function.
        """
        if not peer_urls:
            return

        expected_confirmations = set(peer_urls)
        received_confirmations: set[str] = set()

        logger.info(
            f"🔔 Waiting for function confirmation from {len(expected_confirmations)} peers: {list(expected_confirmations)}"
        )

        # Store confirmation tracking for this announcement
        if announcement_id not in self._pending_confirmations:
            self._pending_confirmations[announcement_id] = {}

        # Create futures for each expected peer
        for peer_url in expected_confirmations:
            self._pending_confirmations[announcement_id][peer_url] = asyncio.Future[
                bool
            ]()

        try:
            # Wait for confirmations with timeout
            confirmation_futures = list(
                self._pending_confirmations[announcement_id].values()
            )
            await asyncio.wait_for(
                asyncio.gather(*confirmation_futures, return_exceptions=True),
                timeout=2.0,
            )

            # Count successful confirmations
            successful_confirmations = 0
            for peer_url, future in self._pending_confirmations[
                announcement_id
            ].items():
                if future.done() and not future.exception():
                    try:
                        if future.result():
                            successful_confirmations += 1
                            logger.info(
                                f"✅ Function {function_name} confirmed by {peer_url}"
                            )
                    except Exception:
                        pass

            if successful_confirmations == len(expected_confirmations):
                logger.info(
                    f"🎉 All {successful_confirmations} peers confirmed function {function_name}"
                )
            else:
                logger.warning(
                    f"⚠️ Only {successful_confirmations}/{len(expected_confirmations)} peers confirmed function {function_name}"
                )

        except TimeoutError:
            confirmed_peers = []
            missing_peers = []
            for peer_url, future in self._pending_confirmations[
                announcement_id
            ].items():
                if future.done() and not future.exception():
                    try:
                        if future.result():
                            confirmed_peers.append(peer_url)
                        else:
                            missing_peers.append(peer_url)
                    except Exception:
                        missing_peers.append(peer_url)
                else:
                    missing_peers.append(peer_url)

            logger.warning(
                f"⏰ Timeout waiting for function confirmation: confirmed={confirmed_peers}, missing={missing_peers}"
            )

        except Exception as e:
            logger.error(f"Error in function confirmation system: {e}")
        finally:
            # Clean up confirmation tracking
            if announcement_id in self._pending_confirmations:
                del self._pending_confirmations[announcement_id]

    async def _publish_function_confirmation(
        self, functions: FunctionNames, announcement_id: AnnouncementID
    ) -> None:
        """
        Send function registration confirmation using existing server-to-server messaging.

        This sends HELLO_ACK messages back to the announcing server to confirm
        that functions have been successfully added to this server's function map.
        """
        if not announcement_id:
            return

        # Find the original announcing server from the announcement_id or peer connections
        # For now, we'll send confirmations to ALL connected peers who might be waiting
        try:
            import time

            from mpreg.core.model import RPCServerHelloAck, RPCServerRequest

            # Create HELLO_ACK message
            hello_ack = RPCServerHelloAck(
                original_announcement_id=announcement_id,
                acknowledged_functions=functions,
                acknowledging_server=self.cluster.local_url,
                timestamp=time.time(),
            )

            # Wrap in server request
            ack_message = RPCServerRequest(server=hello_ack, u=str(ulid.new()))

            # Send to all connected peers (the announcing server will filter)
            for peer_url, connection in self.peer_connections.items():
                try:
                    if connection.is_connected:
                        await connection.send(
                            self.serializer.serialize(ack_message.model_dump())
                        )
                        logger.debug(
                            f"📤 Sent HELLO_ACK for {list(functions)} to {peer_url}"
                        )
                except Exception as e:
                    logger.warning(f"Failed to send HELLO_ACK to {peer_url}: {e}")

            logger.info(
                f"📡 Sent function confirmation for {list(functions)} (announcement {announcement_id})"
            )

        except Exception as e:
            logger.error(f"Failed to send function confirmation: {e}")

    async def _handle_function_confirmation(self, ack: Any) -> None:
        """
        Handle incoming HELLO_ACK messages to complete function confirmation cycle.

        This processes acknowledgments from peers confirming they have successfully
        registered functions, allowing the broadcast system to verify success.
        """
        try:
            announcement_id = ack.original_announcement_id
            acknowledging_server = ack.acknowledging_server
            acknowledged_functions = ack.acknowledged_functions

            logger.info(
                f"📨 Received HELLO_ACK from {acknowledging_server} for {acknowledged_functions} (announcement {announcement_id})"
            )

            # Complete the confirmation future if we're waiting for it
            if announcement_id in self._pending_confirmations:
                if acknowledging_server in self._pending_confirmations[announcement_id]:
                    future = self._pending_confirmations[announcement_id][
                        acknowledging_server
                    ]
                    if not future.done():
                        future.set_result(True)
                        logger.debug(
                            f"✅ Marked confirmation complete for {acknowledging_server}"
                        )

        except Exception as e:
            logger.error(f"Error handling function confirmation: {e}")

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

        # Default commands and RPC commands are already registered in __init__
        # No need to register them again here

        # Start a background task to manage peer connections based on gossip.
        # Use the proper task management system for consistent cleanup
        peer_task = asyncio.create_task(self._manage_peer_connections())
        self._background_tasks.add(peer_task)
        # Remove task from tracking when it completes
        peer_task.add_done_callback(self._background_tasks.discard)

        # Start consensus manager for intra-cluster consensus
        await self.consensus_manager.start()

        # Populate consensus manager with known cluster peers
        self._update_consensus_known_nodes()

        # Initialize federated RPC system
        # Federated RPC now uses enhanced MPREG infrastructure - no separate initialization needed
        logger.info("Enhanced federated RPC ready via MPREG message bus")

        self._websocket_server = await websockets.server.serve(
            self.opened,
            self.settings.host,
            self.settings.port,
            max_size=None,
            max_queue=None,
            read_limit=MPREG_DATA_MAX,
            write_limit=MPREG_DATA_MAX,
        )

        try:
            # If no external connection requested, wait for shutdown signal
            # to keep the server alive.
            if not self.settings.peers and not self.settings.connect:
                await self._shutdown_event.wait()
                return

            # If static peers are configured, establish initial connections.
            if self.settings.peers:
                for peer_url in self.settings.peers:
                    await self._establish_peer_connection(peer_url)

            # If a specific connect URL is configured, connect to it.
            if self.settings.connect:
                await self._establish_peer_connection(self.settings.connect)

            # Keep the server running until shutdown signal.
            await self._shutdown_event.wait()
        finally:
            # Clean up WebSocket server
            if self._websocket_server:
                self._websocket_server.close()
                await self._websocket_server.wait_closed()

    def start(self) -> None:
        try:
            asyncio.run(self.server())
        except KeyboardInterrupt:
            logger.warning("Thanks for playing!")


@logger.catch
def cmd() -> None:
    """You can run an MPREG Server standalone without embedding into a process.

    Running standalone allows you to run server(s) as pure forwarding agents (except for the global shared default commands).
    """

    import jsonargparse

    # Load settings using Pydantic-settings. This will automatically read from
    # environment variables or a .env file if present.
    settings = MPREGSettings()  # type: ignore[call-arg]

    # Create an MPREGServer instance with the loaded settings.
    server_instance = MPREGServer(settings=settings)

    # Use jsonargparse to allow command-line overriding of settings.
    # This integrates with Pydantic-settings to provide a robust configuration system.
    jsonargparse.CLI(server_instance, as_dict=False)  # type: ignore[no-untyped-call]
