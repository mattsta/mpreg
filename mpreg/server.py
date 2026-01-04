from __future__ import annotations

import asyncio
import contextlib
import inspect
import pprint as pp
import sys
import time
import traceback
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from graphlib import TopologicalSorter
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import ulid
from loguru import logger

from .core.config import MPREGSettings
from .core.connection import Connection
from .core.connection_events import ConnectionEvent, ConnectionEventBus
from .core.logging import configure_logging
from .core.model import (
    CacheStatusMetrics,
    CommandNotFoundException,
    ConsensusProposalMessage,
    ConsensusVoteMessage,
    FabricGossipEnvelope,
    GoodbyeReason,
    MPREGException,
    PubSubAck,
    PubSubMessage,
    # Pub/Sub message types
    PubSubNotification,
    PubSubPublish,
    PubSubSubscribe,
    PubSubUnsubscribe,
    QueueStatusMetrics,
    RPCCommand,
    RPCError,
    RPCExecutionSummary,
    RPCFunctionDescriptor,
    RPCIntermediateResult,
    RPCRequest,
    RPCResponse,
    RPCServerGoodbye,
    RPCServerRequest,
    RPCServerStatus,
    ServerStatusMetrics,
)
from .core.monitoring import create_unified_system_monitor
from .core.monitoring.server_monitoring import (
    ServerMetricsTracker,
    ServerSystemMonitor,
)
from .core.monitoring.system_adapters import FederationSystemMonitor
from .core.monitoring.unified_monitoring import SystemType
from .core.persistence.config import PersistenceMode
from .core.registry import Command, CommandRegistry
from .core.serialization import JsonSerializer
from .core.topic_exchange import TopicExchange
from .core.transport.factory import TransportFactory
from .core.transport.interfaces import (
    TransportConfig,
    TransportConnectionError,
    TransportInterface,
    TransportListener,
)
from .datastructures.cluster_types import ClusterState
from .datastructures.function_identity import (
    FunctionIdentity,
    FunctionSelector,
    SemanticVersion,
    VersionConstraint,
)
from .fabric.federation_graph import (
    FederationGraphEdge,
    FederationGraphNode,
    GeographicCoordinate,
    GraphBasedFederationRouter,
    NodeType,
)
from .fabric.federation_planner import FabricFederationPlanner

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mpreg.core.model import FabricMessageEnvelope, PubSubSubscription
    from mpreg.datastructures.type_aliases import PortAssignmentCallback
    from mpreg.fabric.catalog import CacheNodeProfile, NodeDescriptor, TransportEndpoint
    from mpreg.fabric.function_registry import LocalFunctionRegistration
    from mpreg.fabric.message import MessageHeaders, UnifiedMessage
    from mpreg.fabric.message_codec import unified_message_to_dict
    from mpreg.fabric.route_control import RoutePolicy
    from mpreg.fabric.route_keys import RouteKeyRegistry
    from mpreg.fabric.route_policy_directory import RoutePolicyDirectory
    from mpreg.fabric.route_security import (
        RouteAnnouncementSigner,
        RouteSecurityConfig,
    )

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


@dataclass(frozen=True, slots=True)
class DepartedPeer:
    node_url: str
    instance_id: str
    cluster_id: str
    reason: GoodbyeReason
    departed_at: float
    ttl_seconds: float

    def is_expired(self, now: float | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.departed_at + self.ttl_seconds)


@dataclass(frozen=True, slots=True)
class CommandExecutionResult:
    name: str
    value: Any


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
    settings: MPREGSettings | None = field(init=False, default=None)

    # Server infrastructure
    registry: CommandRegistry = field(init=False)
    serializer: JsonSerializer = field(init=False)

    # Runtime state
    waitingFor: dict[str, asyncio.Event] = field(init=False)
    answer: dict[str, Any] = field(init=False)
    peer_connections: dict[str, Connection] = field(init=False)

    # Connection event system
    connection_event_bus: ConnectionEventBus = field(init=False)

    peer_directory: PeerDirectory | None = field(init=False, default=None)
    fabric_engine: RoutingEngine | None = field(init=False, default=None)
    fabric_graph_router: GraphBasedFederationRouter | None = field(
        init=False, default=None
    )
    fabric_link_state_router: GraphBasedFederationRouter | None = field(
        init=False, default=None
    )
    fabric_router: Any = field(init=False, default=None)
    _server: MPREGServer | None = field(init=False, default=None, repr=False)

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
        logger.trace(
            "Cluster created: id={} local_url={}",
            id(self),
            self.config.local_url,
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
        """Get funtimes using the fabric catalog."""
        engine = self.fabric_engine
        if not engine:
            return {}
        catalog = engine.routing_index.catalog
        funtimes: dict[str, dict[frozenset[str], set[str]]] = {}
        for endpoint in catalog.functions.entries():
            fun_name = endpoint.identity.name
            funtimes.setdefault(fun_name, {}).setdefault(endpoint.resources, set()).add(
                endpoint.node_id
            )
        return funtimes

    @property
    def servers(self) -> set[str]:
        """Get all known servers with advertised functions."""
        engine = self.fabric_engine
        if not engine:
            return set()
        entries = engine.routing_index.catalog.functions.entries()
        return {endpoint.node_id for endpoint in entries}

    @property
    def dead_peer_timeout(self) -> float:
        """Get dead peer timeout."""
        return self.config.dead_peer_timeout_seconds

    def _cluster_id_for_server(self, server_url: str) -> str | None:
        """Resolve cluster_id for a server URL, considering advertised URLs."""
        if server_url == self.local_url or server_url in self.advertised_urls:
            return self.cluster_id
        directory = self.peer_directory
        if directory:
            return directory.cluster_id_for_node(server_url)
        return None

    def cluster_id_for_node_url(self, node_url: str) -> str | None:
        """Public resolver for cluster IDs by node URL."""
        return self._cluster_id_for_server(node_url)

    def peer_urls_for_cluster(self, cluster_id: str) -> list[str]:
        """Return known peer URLs for a given cluster ID."""
        directory = self.peer_directory
        if not directory:
            return []
        return directory.peers_for_cluster(cluster_id, connected_only=True)

    def peer_neighbors(self) -> list[PeerNeighbor]:
        """Return known peer neighbor references for reachable peers."""
        directory = self.peer_directory
        if not directory:
            return []
        return directory.neighbors(connected_only=True)

    def _fabric_allowed_clusters(self) -> frozenset[str] | None:
        settings = self.settings
        if not settings or settings.federation_config is None:
            return frozenset({self.cluster_id})
        from mpreg.fabric.federation_config import FederationMode

        config = settings.federation_config
        if config.federation_mode is FederationMode.STRICT_ISOLATION:
            return frozenset({self.cluster_id})
        return None

    def server_for(
        self,
        fun: str,
        locs: frozenset[str],
        *,
        function_id: str | None = None,
        version_constraint: str | None = None,
        target_cluster: str | None = None,
    ) -> str | None:  # Changed return type
        """Finds a suitable server for a given function and location.

        Args:
            fun: The name of the function to find a server for.
            locs: The frozenset of locations/resources required by the function.
            target_cluster: Optional cluster ID constraint for routing.

        Returns:
            A Connection object to the suitable server, or None if no server is found.
        """
        from .datastructures.type_aliases import FunctionName

        function_name: FunctionName = fun
        resource_requirements = frozenset(loc for loc in locs) if locs else None
        cluster_constraint = target_cluster

        logger.info("Looking for function '{}' with locs={}", fun, locs)
        selector = FunctionSelector(
            name=function_name,
            function_id=function_id,
            version_constraint=VersionConstraint.parse(version_constraint)
            if version_constraint
            else None,
        )

        if not self.fabric_engine:
            return None
        from .fabric.index import FunctionQuery

        query = FunctionQuery(
            selector=selector,
            resources=resource_requirements or frozenset(),
            cluster_id=cluster_constraint,
        )
        plan = self.fabric_engine.plan_function_route(
            query,
            routing_path=tuple(),
            hop_budget=self.settings.fabric_routing_max_hops if self.settings else None,
        )
        if plan.selected_target:
            allowed_clusters = self._fabric_allowed_clusters()
            if allowed_clusters is not None:
                next_cluster = (
                    plan.forwarding.next_cluster
                    if plan.forwarding and plan.forwarding.next_cluster
                    else plan.selected_target.cluster_id
                )
                if next_cluster not in allowed_clusters:
                    return None
            target_node = plan.selected_target.node_id
            if target_node == self.local_url:
                return "self"
            if plan.forwarding and plan.forwarding.can_forward:
                return plan.forwarding.next_peer_url
            return target_node
        return None

    async def _plan_rpc_via_fabric_router(
        self, rpc_command: RPCCommand
    ) -> tuple[
        FabricRouteResult | None,
        FabricRouteTarget | None,
        ClusterRoutePlan | None,
        str | None,
    ]:
        if not self.fabric_router:
            return None, None, None, None
        from mpreg.fabric.message import (
            DeliveryGuarantee,
            MessageHeaders,
            MessageType,
            UnifiedMessage,
        )
        from mpreg.fabric.rpc_messages import FabricRPCRequest

        correlation_id = str(ulid.new())
        headers = MessageHeaders(
            correlation_id=correlation_id,
            source_cluster=self.settings.cluster_id if self.settings else None,
            target_cluster=rpc_command.target_cluster,
            routing_path=tuple(),
            federation_path=tuple(),
            hop_budget=(
                self.settings.fabric_routing_max_hops if self.settings else None
            ),
        )
        payload = FabricRPCRequest(
            request_id=correlation_id,
            command=rpc_command.fun,
            args=rpc_command.args,
            kwargs=rpc_command.kwargs,
            resources=tuple(sorted(rpc_command.locs)),
            function_id=rpc_command.function_id,
            version_constraint=rpc_command.version_constraint,
            target_cluster=rpc_command.target_cluster,
            reply_to=self.local_url,
        ).to_dict()
        message = UnifiedMessage(
            message_id=correlation_id,
            topic=f"mpreg.rpc.execute.{rpc_command.fun}",
            message_type=MessageType.RPC,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload=payload,
            headers=headers,
            timestamp=time.time(),
        )
        route = await self.fabric_router.route_message(message)
        if not route.targets:
            return route, None, None, None
        target = route.targets[0]
        allowed_clusters = self._fabric_allowed_clusters()
        if (
            allowed_clusters is not None
            and target.cluster_id
            and target.cluster_id not in allowed_clusters
        ):
            return route, None, None, None
        cluster_plan = (
            route.cluster_routes.get(target.cluster_id) if target.cluster_id else None
        )
        forwarding = cluster_plan.forwarding if cluster_plan else None
        if forwarding and not forwarding.can_forward:
            return route, target, cluster_plan, None
        connections = self.peer_connections
        next_hop: str | None = None
        if target.node_id and target.node_id in connections:
            connection = connections[target.node_id]
            if connection and connection.is_connected:
                next_hop = target.node_id
        if next_hop is None and forwarding and forwarding.next_peer_url:
            next_hop = forwarding.next_peer_url
        if next_hop is None:
            next_hop = target.node_id
        return route, target, cluster_plan, next_hop

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

    async def _get_or_open_peer_connection(
        self,
        peer_url: str,
        *,
        fast_connect: bool = True,
    ) -> Connection:
        connection = self.peer_connections.get(peer_url)
        if connection and connection.is_connected:
            return connection

        server = self._server
        if server is not None:
            await server._establish_peer_connection(peer_url, fast_connect=fast_connect)
            connection = self.peer_connections.get(peer_url)
            if connection and connection.is_connected:
                return connection

        connection = Connection(url=peer_url)
        connection.cluster = self
        await connection.connect()
        self.peer_connections[peer_url] = connection
        return connection

    async def _execute_remote_command(
        self,
        rpc_step: RPCCommand,
        results: dict[str, Any],
        where: Connection,
        *,
        routing_path: tuple[str, ...] | None = None,
        remaining_hops: int | None = None,
        target_cluster: str | None = None,
        target_node: str | None = None,
        federation_path: tuple[str, ...] | None = None,
        federation_remaining_hops: int | None = None,
        retry_remaining: int = 1,
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
        wait_event = asyncio.Event()
        self.waitingFor[localrid] = wait_event
        from mpreg.core.model import FabricMessageEnvelope
        from mpreg.fabric.message import (
            DeliveryGuarantee,
            MessageHeaders,
            MessageType,
            UnifiedMessage,
        )
        from mpreg.fabric.message_codec import unified_message_to_dict
        from mpreg.fabric.rpc_messages import FabricRPCRequest

        headers = MessageHeaders(
            correlation_id=localrid,
            source_cluster=self.settings.cluster_id if self.settings else None,
            target_cluster=target_cluster,
            routing_path=routing_path or (self.local_url,),
            federation_path=federation_path or tuple(),
            hop_budget=remaining_hops,
        )
        rpc_payload = FabricRPCRequest(
            request_id=localrid,
            command=rpc_step.fun,
            args=resolved_args,
            kwargs=resolved_kwargs,
            resources=tuple(sorted(rpc_step.locs)),
            function_id=rpc_step.function_id,
            version_constraint=rpc_step.version_constraint,
            target_cluster=target_cluster,
            target_node=target_node,
            reply_to=self.local_url,
            federation_path=federation_path or tuple(),
            federation_remaining_hops=federation_remaining_hops,
        )
        message = UnifiedMessage(
            message_id=localrid,
            topic=f"mpreg.rpc.execute.{rpc_step.fun}",
            message_type=MessageType.RPC,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload=rpc_payload.to_dict(),
            headers=headers,
            timestamp=time.time(),
        )
        envelope = FabricMessageEnvelope(payload=unified_message_to_dict(message))
        body = self.serializer.serialize(envelope.model_dump())

        connection = where
        if not connection.is_connected:
            connection = self.peer_connections.get(where.url, connection)
        if not connection.is_connected:
            server = self._server
            if server is not None:
                await server._establish_peer_connection(where.url, fast_connect=True)
                connection = self.peer_connections.get(where.url, connection)
        if not connection.is_connected:
            sent_via_fabric = False
            server = self._server
            if server is not None and server._fabric_transport:
                if server._get_all_peer_connections():
                    await server._send_fabric_message(
                        message,
                        target_nodes=(where.url,),
                    )
                    sent_via_fabric = True
            if not sent_via_fabric:
                await connection.connect()
                self.peer_connections[where.url] = connection
                if server is not None:
                    server._track_background_task(
                        asyncio.create_task(
                            server._handle_peer_connection_messages(
                                connection, where.url
                            )
                        )
                    )
        try:
            logger.info(
                "Sending fabric rpc request: command={}, u={}, to={}",
                rpc_step.fun,
                localrid,
                connection.url,
            )
            if connection.is_connected:
                await connection.send(body)
            logger.info("Fabric rpc request sent successfully, waiting for response...")
        except ConnectionError:
            self.waitingFor.pop(localrid, None)
            logger.error(
                "[{}] Connection to remote server closed. Removing from services for now...",
                connection.url,
            )
            self.remove_server(connection)
            # Retry if the server was removed, as another might be available
            # This is a form of self-healing for the cluster.
            new_where = self.server_for(
                rpc_step.fun,
                rpc_step.locs,
                function_id=rpc_step.function_id,
                version_constraint=rpc_step.version_constraint,
                target_cluster=target_cluster,
            )
            if new_where:
                return await self._execute_remote_command(
                    rpc_step,
                    results,
                    Connection(url=new_where),
                    routing_path=routing_path,
                    remaining_hops=remaining_hops,
                    target_cluster=target_cluster,
                    target_node=target_node,
                    federation_path=federation_path,
                    federation_remaining_hops=federation_remaining_hops,
                    retry_remaining=retry_remaining,
                )  # Changed new_where to where
            else:
                raise ConnectionError(
                    f"No alternative server found for: {rpc_step.fun} at {rpc_step.locs}"
                )

        try:
            logger.info("Waiting for answer with rid={}", localrid)
            await asyncio.wait_for(wait_event.wait(), timeout=30.0)
            logger.info("Received answer for rid={}", localrid)
        except TimeoutError:
            logger.debug(
                "Timeout waiting for fabric rpc response (rid={}, retries_left={})",
                localrid,
                retry_remaining,
            )
            if retry_remaining > 0:
                new_where = self.server_for(
                    rpc_step.fun,
                    rpc_step.locs,
                    function_id=rpc_step.function_id,
                    version_constraint=rpc_step.version_constraint,
                    target_cluster=target_cluster,
                )
                if new_where:
                    return await self._execute_remote_command(
                        rpc_step,
                        results,
                        Connection(url=new_where),
                        routing_path=routing_path,
                        remaining_hops=remaining_hops,
                        target_cluster=target_cluster,
                        target_node=target_node,
                        federation_path=federation_path,
                        federation_remaining_hops=federation_remaining_hops,
                        retry_remaining=retry_remaining - 1,
                    )
            raise
        finally:
            self.waitingFor.pop(localrid, None)
        got = self.answer.get(localrid)
        if localrid in self.answer:
            del self.answer[localrid]
        logger.info("Remote command completed: got={}", got)
        try:
            approx_size = sys.getsizeof(got)
            if approx_size >= 100 * 1024 * 1024:
                logger.warning(
                    "[{}] Large RPC payload buffered (~{} bytes) for {}",
                    self.settings.name,
                    approx_size,
                    rpc_step.fun,
                )
        except Exception:
            pass
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
        ) -> CommandExecutionResult:
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
            route, target, cluster_plan, where = await self._plan_rpc_via_fabric_router(
                rpc_command
            )
            forwarding = cluster_plan.forwarding if cluster_plan else None
            if forwarding and not forwarding.can_forward:
                return CommandExecutionResult(
                    name=rpc_command.name,
                    value={
                        "error": "fabric_forward_failed",
                        "command": rpc_command.fun,
                        "target_cluster": target.cluster_id if target else None,
                        "reason": forwarding.reason.value,
                    },
                )
            if not route or not target:
                logger.error(
                    "No fabric route for command '{}' with locs={}",
                    rpc_command.fun,
                    rpc_command.locs,
                )
                raise CommandNotFoundException(command_name=rpc_command.fun)

            if target.node_id == self.local_url:
                logger.info("Executing '{}' locally", rpc_command.fun)
                got = await self._execute_local_command(rpc_command, results)
            else:
                if not where:
                    logger.error(
                        "No fabric next hop for command '{}' with locs={}",
                        rpc_command.fun,
                        rpc_command.locs,
                    )
                    raise CommandNotFoundException(command_name=rpc_command.fun)
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
                        "Establishing peer connection to {} (existing: {}, connected: {})",
                        where,
                        connection is not None,
                        connection.is_connected if connection else False,
                    )
                    connection = await self._get_or_open_peer_connection(
                        where, fast_connect=True
                    )
                else:
                    logger.info("Using existing connection to {}", where)
                routing_path = (self.local_url,)
                remaining_hops = (
                    self.settings.fabric_routing_max_hops if self.settings else None
                )
                federation_path = forwarding.federation_path if forwarding else None
                federation_remaining_hops = (
                    forwarding.remaining_hops if forwarding else None
                )
                target_cluster = target.cluster_id if target else None
                target_node = target.node_id if target else None
                got = await self._execute_remote_command(
                    rpc_command,
                    results,
                    connection,
                    routing_path=routing_path,
                    remaining_hops=remaining_hops,
                    target_cluster=target_cluster,
                    target_node=target_node,
                    federation_path=federation_path,
                    federation_remaining_hops=federation_remaining_hops,
                )

            logger.info(
                "Command '{}' completed with result type: {}",
                rpc_command.fun,
                type(got),
            )
            results[rpc_command.name] = got

            return CommandExecutionResult(name=rpc_command.name, value=got)

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
            result[g.name] = g.value
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
        ) -> CommandExecutionResult:
            """Enhanced runner that tracks cross-cluster execution."""
            nonlocal cross_cluster_hops

            logger.info(
                "Executing command '{}' with locs={}", rpc_command.fun, rpc_command.locs
            )
            route, target, cluster_plan, where = await self._plan_rpc_via_fabric_router(
                rpc_command
            )
            forwarding = cluster_plan.forwarding if cluster_plan else None
            if forwarding and not forwarding.can_forward:
                got = {
                    "error": "fabric_forward_failed",
                    "command": rpc_command.fun,
                    "target_cluster": target.cluster_id if target else None,
                    "reason": forwarding.reason.value,
                }
                results[rpc_command.name] = got
                return CommandExecutionResult(name=rpc_command.name, value=got)
            if not route or not target:
                logger.error(
                    "No fabric route for command '{}' with locs={}",
                    rpc_command.fun,
                    rpc_command.locs,
                )
                raise CommandNotFoundException(command_name=rpc_command.fun)

            if target.node_id != self.local_url:
                target_cluster = target.cluster_id
                if target_cluster and target_cluster != self.settings.cluster_id:
                    cross_cluster_hops += 1

            # Use existing execution logic
            if target.node_id == self.local_url:
                got = await self._execute_local_command(rpc_command, results)
            else:
                if not where:
                    logger.error(
                        "No fabric next hop for command '{}' with locs={}",
                        rpc_command.fun,
                        rpc_command.locs,
                    )
                    raise CommandNotFoundException(command_name=rpc_command.fun)
                connection = self.peer_connections.get(where)
                if not connection or not connection.is_connected:
                    connection = await self._get_or_open_peer_connection(
                        where, fast_connect=True
                    )
                routing_path = (self.local_url,)
                remaining_hops = (
                    self.settings.fabric_routing_max_hops if self.settings else None
                )
                federation_path = forwarding.federation_path if forwarding else None
                federation_remaining_hops = (
                    forwarding.remaining_hops if forwarding else None
                )
                target_node = target.node_id if target else None
                target_cluster = target.cluster_id if target else None
                got = await self._execute_remote_command(
                    rpc_command,
                    results,
                    connection,
                    routing_path=routing_path,
                    remaining_hops=remaining_hops,
                    target_cluster=target_cluster,
                    target_node=target_node,
                    federation_path=federation_path,
                    federation_remaining_hops=federation_remaining_hops,
                )

            # Update results dictionary
            results[rpc_command.name] = got
            return CommandExecutionResult(name=rpc_command.name, value=got)

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
                    level_results = {g.name: g.value for g in got}

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
                        pubsub_message = PubSubMessage(
                            message_id=f"rpc_intermediate_{request.u}_{level_idx}",
                            topic=request.intermediate_result_callback_topic,
                            payload=asdict(intermediate_result),
                            publisher=self.settings.name,
                            headers={"event": "rpc.intermediate_result"},
                            timestamp=time.time(),
                        )
                        notifications = self.topic_exchange.publish_message(
                            pubsub_message
                        )
                        for notification in notifications:
                            await self._send_notification_to_client(notification)

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
            result[g.name] = g.value

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
    _auto_allocated_port: int | None = field(init=False, default=None)
    _auto_allocated_monitoring_port: int | None = field(init=False, default=None)
    registry: CommandRegistry = field(init=False)
    serializer: JsonSerializer = field(init=False)
    cluster: Cluster = field(init=False)
    clients: set[Connection] = field(init=False)
    peer_connections: dict[str, Connection] = field(init=False)
    _inbound_peer_connections: dict[str, Connection] = field(init=False)
    _shutdown_event: asyncio.Event = field(init=False)
    topic_exchange: TopicExchange = field(init=False)
    _logger_handler_ids: tuple[int, ...] = field(init=False)
    # PubSub client tracking
    pubsub_clients: dict[str, TransportInterface] = field(init=False)
    peer_status: dict[str, RPCServerStatus] = field(default_factory=dict)
    subscription_to_client: dict[str, str] = field(init=False)
    # Federation management
    federation_manager: Any = field(init=False)  # FederationConnectionManager
    # Consensus management for intra-cluster consensus
    consensus_manager: Any = field(init=False)  # ConsensusManager
    # Message statistics tracking
    _msg_stats: MessageStats = field(default_factory=MessageStats)
    _metrics_tracker: ServerMetricsTracker = field(init=False)
    _unified_monitor: Any = field(init=False, default=None)
    _monitoring_system: Any = field(init=False, default=None)
    _cache_manager: Any = field(init=False, default=None)
    _queue_manager: Any = field(init=False, default=None)
    _cache_fabric_protocol: Any = field(init=False, default=None)
    _cache_fabric_transport: Any = field(init=False, default=None)
    _persistence_registry: Any = field(init=False, default=None)
    _fabric_control_plane: Any = field(init=False, default=None)
    _peer_directory: Any = field(init=False, default=None)
    _fabric_gossip_transport: Any = field(init=False, default=None)
    _fabric_transport: Any = field(init=False, default=None)
    _fabric_topic_announcer: Any = field(init=False, default=None)
    _fabric_router: Any = field(init=False, default=None)
    _fabric_federation_planner: Any = field(init=False, default=None)
    _fabric_queue_federation: Any = field(init=False, default=None)
    _fabric_queue_delivery: Any = field(init=False, default=None)
    _fabric_raft_transport: Any = field(init=False, default=None)
    _fabric_catalog_refresh_task: asyncio.Task[None] | None = field(
        init=False, default=None
    )
    _fabric_route_key_refresh_task: asyncio.Task[None] | None = field(
        init=False, default=None
    )
    _local_function_registry: Any = field(init=False, default=None)
    _fabric_snapshot_last_saved_at: float | None = field(init=False, default=None)
    _fabric_snapshot_last_restored_at: float | None = field(init=False, default=None)
    _fabric_snapshot_last_saved_counts: dict[str, int] = field(
        init=False, default_factory=dict
    )
    _fabric_snapshot_last_restored_counts: dict[str, int] = field(
        init=False, default_factory=dict
    )
    _fabric_snapshot_last_route_keys_saved: int | None = field(init=False, default=None)
    _fabric_snapshot_last_route_keys_restored: int | None = field(
        init=False, default=None
    )
    # Task tracking for proper cleanup
    _background_tasks: set[asyncio.Task[Any]] = field(init=False, default_factory=set)
    _transport_listener: TransportListener | None = field(init=False, default=None)
    _instance_id: str = field(init=False)
    _peer_instance_ids: dict[str, str] = field(init=False)
    _departed_peers: dict[str, DepartedPeer] = field(init=False, default_factory=dict)
    _peer_connection_locks: dict[str, asyncio.Lock] = field(init=False)

    def __post_init__(self) -> None:
        """Initializes the MPREGServer instance.

        Sets up the cluster, command registry, and client tracking.
        """
        if self.settings.port in (None, 0):
            from .core.port_allocator import allocate_port

            selected_port = allocate_port("servers")
            self.settings.port = selected_port
            self._auto_allocated_port = selected_port
            self._invoke_port_callback(
                self.settings.on_port_assigned, selected_port, label="server"
            )
        self.registry = CommandRegistry()  # Moved registry initialization here
        self.serializer = JsonSerializer()  # Moved serializer initialization here
        self.cluster = Cluster.create(
            cluster_id=self.settings.cluster_id,
            advertised_urls=tuple(self.settings.advertised_urls or []),
            local_url=f"ws://{self.settings.host}:{self.settings.port}",
        )
        self.cluster.settings = self.settings
        self.cluster.registry = self.registry
        self.cluster.serializer = self.serializer

        self._instance_id = str(ulid.new())
        self._peer_instance_ids = {self.cluster.local_url: self._instance_id}
        self._peer_connection_locks = {}
        self.clients: set[Connection] = set()  # Added type hint
        self.peer_connections: dict[
            str, Connection
        ] = {}  # Persistent connections to peers
        self.cluster.peer_connections = self.peer_connections
        self.cluster._server = self
        self._inbound_peer_connections = {}
        self._shutdown_event = asyncio.Event()  # Event to signal server shutdown

        # Initialize federation connection manager
        from mpreg.fabric.connection_manager import (
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
        from mpreg.fabric.consensus import ConsensusManager

        self.consensus_manager = ConsensusManager(
            node_id=self.cluster.local_url,  # Use server URL as node ID
            gossip_protocol=None,  # We'll use the server's own gossip system
            message_broadcast_callback=self._broadcast_consensus_message_unified,
            node_count_provider=self._consensus_node_count,
            default_consensus_threshold=0.6,
        )

        # Configure logging - store handler IDs for proper cleanup
        self._logger_handler_ids = configure_logging(
            self.settings.log_level,
            debug_scopes=self.settings.log_debug_scopes,
            colorize=False,
        )

        if self.settings.persistence_config is not None:
            from mpreg.core.persistence.registry import PersistenceRegistry

            self._persistence_registry = PersistenceRegistry(
                self.settings.persistence_config
            )

        # Initialize topic exchange system
        self.topic_exchange = TopicExchange(
            server_url=self.cluster.local_url, cluster_id=self.settings.cluster_id
        )

        # Initialize PubSub client tracking
        self.pubsub_clients = {}
        self.subscription_to_client: dict[str, str] = {}

        # Metrics tracking for monitoring endpoints
        self._metrics_tracker = ServerMetricsTracker()
        from mpreg.fabric.function_registry import LocalFunctionRegistry

        self._local_function_registry = LocalFunctionRegistry(
            node_id=self.cluster.local_url,
            cluster_id=self.settings.cluster_id,
            ttl_seconds=self.settings.fabric_catalog_ttl_seconds,
        )
        self._initialize_fabric_control_plane()

        # TopicExchange will create notifications, we'll send them in the message handler

        # Register default commands and any commands decorated with @rpc_command
        self._register_default_commands()
        self._discover_and_register_rpc_commands()

    def _invoke_port_callback(
        self,
        callback: PortAssignmentCallback | None,
        port: int,
        *,
        label: str,
    ) -> None:
        if callback is None:
            return
        try:
            result = callback(port)
        except Exception as exc:
            logger.warning("Port callback {} failed for {}: {}", label, port, exc)
            return
        if inspect.isawaitable(result):
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                try:
                    asyncio.run(result)
                except Exception as exc:
                    logger.warning(
                        "Async port callback {} failed for {}: {}", label, port, exc
                    )
            else:

                async def _runner() -> None:
                    try:
                        await result
                    except Exception as exc:
                        logger.warning(
                            "Async port callback {} failed for {}: {}",
                            label,
                            port,
                            exc,
                        )

                loop.create_task(_runner())

    def __del__(self) -> None:
        if self._auto_allocated_monitoring_port is not None:
            try:
                from .core.port_allocator import release_port

                release_port(self._auto_allocated_monitoring_port)
            except Exception:
                pass
            finally:
                self._auto_allocated_monitoring_port = None

        if self._auto_allocated_port is not None:
            try:
                from .core.port_allocator import release_port

                release_port(self._auto_allocated_port)
            except Exception:
                pass
            finally:
                self._auto_allocated_port = None

    def _initialize_fabric_control_plane(self) -> None:
        """Initialize the unified fabric control plane."""
        from mpreg.fabric.adapters.topic_exchange import TopicExchangeCatalogAdapter
        from mpreg.fabric.announcers import FabricTopicAnnouncer
        from mpreg.fabric.control_plane import FabricControlPlane
        from mpreg.fabric.gossip import GossipProtocol
        from mpreg.fabric.peer_directory import PeerDirectory
        from mpreg.fabric.server_gossip_transport import ServerGossipTransport
        from mpreg.fabric.server_transport import ServerFabricTransport

        self._peer_directory = PeerDirectory(
            local_node_id=self.cluster.local_url,
            local_cluster_id=self.settings.cluster_id,
        )
        self.cluster.peer_directory = self._peer_directory
        self.cluster.connection_event_bus.subscribe(self._peer_directory)

        transport = ServerGossipTransport(server=self, serializer=self.serializer)
        gossip = GossipProtocol(
            node_id=self.cluster.local_url,
            transport=transport,
            gossip_interval=self.settings.gossip_interval,
        )
        link_state_router = None
        if self.settings.fabric_link_state_mode is not None:
            from mpreg.fabric.link_state import LinkStateMode

            if self.settings.fabric_link_state_mode is not LinkStateMode.DISABLED:
                link_state_router = self._ensure_fabric_link_state_router()
        self._fabric_control_plane = FabricControlPlane.create(
            local_cluster=self.settings.cluster_id,
            gossip=gossip,
            catalog_policy=self._fabric_catalog_policy(),
            catalog_observers=(self._peer_directory,),
            catalog_max_hops=self.settings.fabric_routing_max_hops,
            sender_cluster_resolver=self.cluster.cluster_id_for_node_url,
            route_policy=self.settings.fabric_route_policy,
            route_export_policy=self.settings.fabric_route_export_policy,
            route_neighbor_policy_resolver=(
                self.settings.fabric_route_neighbor_policies.resolver()
                if self.settings.fabric_route_neighbor_policies
                else None
            ),
            route_export_neighbor_policy_resolver=(
                self.settings.fabric_route_export_neighbor_policies.resolver()
                if self.settings.fabric_route_export_neighbor_policies
                else None
            ),
            route_security_config=self.settings.fabric_route_security_config,
            route_signer=self.settings.fabric_route_signer,
            route_public_key_resolver=(
                self.settings.fabric_route_key_registry.resolver()
                if self.settings.fabric_route_key_registry
                else None
            ),
            route_key_registry=self.settings.fabric_route_key_registry,
            route_key_ttl_seconds=self.settings.fabric_route_key_ttl_seconds,
            route_key_announce_interval=self.settings.fabric_route_key_announce_interval_seconds,
            route_ttl_seconds=self.settings.fabric_route_ttl_seconds,
            route_announce_interval=self.settings.fabric_route_announce_interval_seconds,
            link_state_mode=self.settings.fabric_link_state_mode,
            link_state_neighbor_locator=self.cluster.peer_neighbors,
            link_state_router=link_state_router,
            link_state_ttl_seconds=self.settings.fabric_link_state_ttl_seconds,
            link_state_announce_interval=self.settings.fabric_link_state_announce_interval_seconds,
            link_state_area=self.settings.fabric_link_state_area,
            link_state_area_policy=self.settings.fabric_link_state_area_policy,
        )
        if self._fabric_control_plane.route_withdrawal_coordinator:
            self.cluster.connection_event_bus.subscribe(
                self._fabric_control_plane.route_withdrawal_coordinator
            )
        self._fabric_gossip_transport = transport
        self._fabric_transport = ServerFabricTransport(
            server=self, serializer=self.serializer
        )
        self.cluster.fabric_engine = self._fabric_control_plane.engine
        topic_adapter = TopicExchangeCatalogAdapter.for_exchange(self.topic_exchange)
        self._fabric_topic_announcer = FabricTopicAnnouncer(
            adapter=topic_adapter,
            broadcaster=self._fabric_control_plane.broadcaster,
        )
        self._initialize_fabric_federation_planner()
        self._initialize_fabric_router()
        self._initialize_fabric_queue_federation()
        self._initialize_fabric_raft_transport()

    def _initialize_fabric_router(self) -> None:
        if not self._fabric_control_plane:
            return
        from mpreg.fabric.pubsub_router import PubSubRoutingPlanner
        from mpreg.fabric.router import FabricRouter, FabricRoutingConfig

        pubsub_planner = PubSubRoutingPlanner(
            routing_index=self._fabric_control_plane.index,
            local_node_id=self.cluster.local_url,
            allowed_clusters=self._fabric_allowed_clusters(),
        )
        config = FabricRoutingConfig(
            local_cluster_id=self.settings.cluster_id,
            local_node_id=self.cluster.local_url,
        )
        self._fabric_router = FabricRouter(
            config=config,
            routing_index=self._fabric_control_plane.index,
            routing_engine=self._fabric_control_plane.engine,
            pubsub_planner=pubsub_planner,
            message_queue=self._queue_manager,
        )
        self.cluster.fabric_router = self._fabric_router

    def _initialize_fabric_queue_federation(self) -> None:
        if (
            self._queue_manager is None
            or self._fabric_control_plane is None
            or self._fabric_transport is None
        ):
            return
        from mpreg.fabric.cluster_messenger import ClusterMessenger
        from mpreg.fabric.queue_delivery import FabricQueueDeliveryCoordinator
        from mpreg.fabric.queue_federation import FabricQueueFederationManager

        queue_announcer = self._fabric_control_plane.queue_announcer()
        messenger = ClusterMessenger(
            cluster_id=self.settings.cluster_id,
            node_id=self.cluster.local_url,
            transport=self._fabric_transport,
            peer_locator=self.cluster.peer_urls_for_cluster,
            hop_planner=self._fabric_federation_planner,
            max_hops=self.settings.fabric_routing_max_hops,
        )
        self._fabric_queue_federation = FabricQueueFederationManager(
            cluster_id=self.settings.cluster_id,
            node_id=self.cluster.local_url,
            routing_index=self._fabric_control_plane.index,
            queue_manager=self._queue_manager,
            queue_announcer=queue_announcer,
            messenger=messenger,
            allowed_clusters=self._fabric_allowed_clusters(),
        )
        self._fabric_queue_delivery = FabricQueueDeliveryCoordinator(
            cluster_id=self.settings.cluster_id,
            queue_federation=self._fabric_queue_federation,
            allowed_clusters=self._fabric_allowed_clusters(),
        )

    def _initialize_fabric_raft_transport(self) -> None:
        if not self._fabric_transport or not self.settings.fabric_routing_enabled:
            return
        from mpreg.fabric.cluster_messenger import ClusterMessenger
        from mpreg.fabric.raft_transport import (
            FabricRaftTransport,
            FabricRaftTransportConfig,
            FabricRaftTransportHooks,
        )

        messenger = ClusterMessenger(
            cluster_id=self.settings.cluster_id,
            node_id=self.cluster.local_url,
            transport=self._fabric_transport,
            peer_locator=self.cluster.peer_urls_for_cluster,
            hop_planner=self._fabric_federation_planner,
            max_hops=self.settings.fabric_routing_max_hops,
        )
        hooks = FabricRaftTransportHooks(
            send_direct=self._fabric_transport.send_message,
            send_to_cluster=lambda cluster_id,
            message,
            source_peer_url: messenger.send_to_cluster(
                message, cluster_id, source_peer_url
            ),
            resolve_cluster=self.cluster.cluster_id_for_node_url,
        )
        self._fabric_raft_transport = FabricRaftTransport(
            config=FabricRaftTransportConfig(
                node_id=self.cluster.local_url,
                cluster_id=self.settings.cluster_id,
                request_timeout_seconds=self.settings.fabric_raft_request_timeout_seconds,
                max_hops=self.settings.fabric_routing_max_hops,
            ),
            hooks=hooks,
        )

    def register_raft_node(self, node: Any) -> None:
        if not self._fabric_raft_transport:
            raise RuntimeError("Fabric raft transport is not initialized")
        self._fabric_raft_transport.register_node(node)

    def fabric_raft_transport(self) -> Any | None:
        return self._fabric_raft_transport

    def _initialize_fabric_federation_planner(self) -> None:
        """Initialize the fabric federation planner for multi-hop routing."""
        if not self.settings.fabric_routing_enabled:
            return
        graph_router = self._ensure_fabric_graph_router()
        self._fabric_federation_planner = FabricFederationPlanner(
            local_cluster=self.settings.cluster_id,
            graph_router=graph_router,
            link_state_router=self.cluster.fabric_link_state_router,
            link_state_mode=self.settings.fabric_link_state_mode,
            link_state_ecmp_paths=self.settings.fabric_link_state_ecmp_paths,
            peer_locator=self.cluster.peer_urls_for_cluster,
            neighbor_locator=self.cluster.peer_neighbors,
            default_max_hops=self.settings.fabric_routing_max_hops,
            route_table=(
                self._fabric_control_plane.route_table
                if self._fabric_control_plane
                else None
            ),
        )
        if self._fabric_control_plane:
            self._fabric_control_plane.engine.federation_planner = (
                self._fabric_federation_planner
            )

    def _fabric_node_capabilities(self) -> frozenset[str]:
        capabilities = {"rpc"}
        if self._queue_manager is not None:
            capabilities.add("queue")
        if self._cache_manager is not None:
            capabilities.add("cache")
        return frozenset(capabilities)

    def _link_state_neighbor_clusters(self) -> frozenset[str]:
        try:
            from mpreg.fabric.link_state import LinkStateMode

            if self.settings.fabric_link_state_mode is LinkStateMode.DISABLED:
                return frozenset()
        except Exception:
            return frozenset()
        if not self._fabric_control_plane:
            return frozenset()
        table = self._fabric_control_plane.link_state_table
        if table is None:
            return frozenset()
        return table.neighbor_clusters(self.settings.cluster_id)

    def _iter_peer_nodes_for_connection(self) -> list[NodeDescriptor]:
        if not self._peer_directory:
            return []
        nodes = [
            node
            for node in self._peer_directory.nodes()
            if node.node_id != self.cluster.local_url
        ]
        neighbor_clusters = self._link_state_neighbor_clusters()
        if neighbor_clusters:
            return sorted(
                nodes,
                key=lambda node: (
                    0 if node.cluster_id in neighbor_clusters else 1,
                    node.cluster_id,
                    node.node_id,
                ),
            )
        return sorted(nodes, key=lambda node: (node.cluster_id, node.node_id))

    def _select_peer_dial_url(self, node: NodeDescriptor) -> str:
        endpoints = list(node.transport_endpoints)
        if not endpoints:
            return node.node_id
        try:
            from mpreg.core.transport.defaults import ConnectionType

            internal_type = ConnectionType.INTERNAL.value
        except Exception:
            internal_type = "internal"
        candidates = [
            endpoint
            for endpoint in endpoints
            if endpoint.connection_type == internal_type
        ]
        if not candidates:
            candidates = endpoints
        protocol_preference = ("wss", "ws", "tcps", "tcp")
        for protocol in protocol_preference:
            for endpoint in candidates:
                if endpoint.protocol == protocol and endpoint.host and endpoint.port:
                    return endpoint.endpoint
        for endpoint in candidates:
            if endpoint.host and endpoint.port:
                return endpoint.endpoint
        return node.node_id

    def _fabric_transport_endpoints(self) -> tuple[TransportEndpoint, ...]:
        from mpreg.core.transport.defaults import ConnectionType
        from mpreg.fabric.catalog import TransportEndpoint

        endpoints: set[TransportEndpoint] = set()
        urls = (self.cluster.local_url, *self.cluster.advertised_urls)
        for url in urls:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.hostname or not parsed.port:
                continue
            endpoints.add(
                TransportEndpoint(
                    connection_type=ConnectionType.INTERNAL.value,
                    protocol=parsed.scheme,
                    host=parsed.hostname,
                    port=parsed.port,
                )
            )
        if not endpoints:
            return ()
        return tuple(
            sorted(
                endpoints,
                key=lambda endpoint: (
                    endpoint.connection_type,
                    endpoint.protocol,
                    endpoint.host,
                    endpoint.port,
                ),
            )
        )

    def _fabric_allowed_clusters(self) -> frozenset[str] | None:
        config = self.settings.federation_config
        if config is None:
            return frozenset({self.settings.cluster_id})
        from mpreg.fabric.federation_config import FederationMode

        if config.federation_mode is FederationMode.STRICT_ISOLATION:
            return frozenset({self.settings.cluster_id})
        return None

    def _fabric_catalog_policy(self):
        from mpreg.fabric.catalog_policy import CatalogFilterPolicy

        config = self.settings.federation_config
        allowed_clusters = self._fabric_allowed_clusters()
        if config is None:
            return CatalogFilterPolicy(
                local_cluster=self.settings.cluster_id,
                allowed_clusters=allowed_clusters,
                node_filter=self._fabric_allows_node,
            )
        security_policy = config.security_policy
        return CatalogFilterPolicy(
            local_cluster=self.settings.cluster_id,
            allowed_clusters=allowed_clusters,
            allowed_functions=security_policy.allowed_functions_cross_federation,
            blocked_functions=security_policy.blocked_functions_cross_federation,
            node_filter=self._fabric_allows_node,
        )

    def _fabric_allows_node(self, node_id: str, cluster_id: str) -> bool:
        if self._is_peer_departed(node_id):
            logger.debug(
                "[{}] Catalog filter rejected departed node: node_id={} cluster_id={}",
                self.settings.name,
                node_id,
                cluster_id,
            )
            return False
        return True

    async def _announce_fabric_functions(self) -> None:
        if not self._fabric_control_plane or not self._local_function_registry:
            return
        from mpreg.fabric.adapters.function_registry import LocalFunctionCatalogAdapter
        from mpreg.fabric.announcers import FabricFunctionAnnouncer

        adapter = LocalFunctionCatalogAdapter(
            registry=self._local_function_registry,
            node_resources=frozenset(self.settings.resources or set()),
            node_capabilities=self._fabric_node_capabilities(),
            transport_endpoints=self._fabric_transport_endpoints(),
        )
        announcer = FabricFunctionAnnouncer(
            adapter=adapter, broadcaster=self._fabric_control_plane.broadcaster
        )
        await announcer.announce()

    def _publish_fabric_function_update(
        self, registration: LocalFunctionRegistration
    ) -> None:
        if not self._fabric_control_plane or not self._local_function_registry:
            return
        import uuid

        from mpreg.fabric.catalog import NodeDescriptor
        from mpreg.fabric.catalog_delta import RoutingCatalogDelta

        now = time.time()
        endpoint = registration.to_endpoint(
            node_id=self.cluster.local_url,
            cluster_id=self.settings.cluster_id,
            ttl_seconds=self._local_function_registry.ttl_seconds,
            advertised_at=now,
        )
        node = NodeDescriptor(
            node_id=self.cluster.local_url,
            cluster_id=self.settings.cluster_id,
            resources=frozenset(self.settings.resources or set()),
            capabilities=self._fabric_node_capabilities(),
            transport_endpoints=self._fabric_transport_endpoints(),
            advertised_at=now,
            ttl_seconds=self._local_function_registry.ttl_seconds,
        )
        delta = RoutingCatalogDelta(
            update_id=str(uuid.uuid4()),
            cluster_id=self.settings.cluster_id,
            sent_at=now,
            functions=(endpoint,),
            nodes=(node,),
        )
        self._fabric_control_plane.applier.apply(delta, now=now)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._track_background_task(
            loop.create_task(
                self._fabric_control_plane.broadcaster.publisher.publish(delta)
            )
        )

    async def _announce_fabric_cache_role(self) -> None:
        if not self._fabric_control_plane or not self._cache_fabric_protocol:
            return
        from mpreg.fabric.adapters.cache_federation import CacheFederationCatalogAdapter
        from mpreg.fabric.announcers import FabricCacheRoleAnnouncer

        adapter = CacheFederationCatalogAdapter(
            node_id=self.cluster.local_url,
            cluster_id=self.settings.cluster_id,
            ttl_seconds=self.settings.fabric_catalog_ttl_seconds,
        )
        announcer = FabricCacheRoleAnnouncer(
            adapter=adapter, broadcaster=self._fabric_control_plane.broadcaster
        )
        await announcer.announce()

    async def _announce_fabric_topics(self) -> None:
        if not self._fabric_topic_announcer:
            return
        await self._fabric_topic_announcer.announce_all(self.topic_exchange)

    async def _announce_fabric_subscription(
        self, subscription: PubSubSubscription
    ) -> None:
        if not self._fabric_topic_announcer:
            return
        await self._fabric_topic_announcer.announce_subscription(subscription)

    async def _remove_fabric_subscription(self, subscription_id: str) -> None:
        if not self._fabric_topic_announcer:
            return
        await self._fabric_topic_announcer.remove_subscription(subscription_id)

    async def _forward_pubsub_publish(
        self,
        message: PubSubMessage,
        *,
        source_peer_url: str | None = None,
    ) -> None:
        await self._forward_fabric_pubsub_message(
            message, headers=None, source_peer_url=source_peer_url
        )

    def _next_fabric_headers(
        self,
        correlation_id: str,
        headers: MessageHeaders | None,
        *,
        max_hops: int | None,
    ) -> MessageHeaders | None:
        from mpreg.fabric.message import MessageHeaders

        if headers is None:
            return MessageHeaders(
                correlation_id=correlation_id,
                source_cluster=self.settings.cluster_id,
                routing_path=(self.cluster.local_url,),
                federation_path=(self.settings.cluster_id,),
                hop_budget=max_hops,
            )

        if self.cluster.local_url in headers.routing_path:
            return None

        hop_budget = headers.hop_budget
        if hop_budget is None:
            hop_budget = max_hops
        elif max_hops is not None:
            hop_budget = min(hop_budget, max_hops)

        routing_path = headers.routing_path
        if not routing_path or routing_path[-1] != self.cluster.local_url:
            routing_path = (*routing_path, self.cluster.local_url)

        federation_path = headers.federation_path
        if not federation_path or federation_path[-1] != self.settings.cluster_id:
            federation_path = (*federation_path, self.settings.cluster_id)

        hop_count = max(0, len(routing_path) - 1)
        if hop_budget is not None and hop_count > hop_budget:
            return None

        return MessageHeaders(
            correlation_id=headers.correlation_id or correlation_id,
            source_cluster=headers.source_cluster or self.settings.cluster_id,
            target_cluster=headers.target_cluster,
            routing_path=routing_path,
            federation_path=federation_path,
            hop_budget=hop_budget,
            priority=headers.priority,
            metadata=dict(headers.metadata),
        )

    def _fabric_next_hop_for_cluster(
        self, cluster_id: str, *, headers: MessageHeaders
    ) -> str | None:
        if not self._fabric_federation_planner:
            return None
        plan = self._fabric_federation_planner.plan_next_hop(
            target_cluster=cluster_id,
            visited_clusters=headers.federation_path,
            remaining_hops=headers.hop_budget,
        )
        if plan.can_forward and plan.next_peer_url:
            return plan.next_peer_url
        return None

    def _fabric_next_hop_for_node(
        self, node_id: str, *, headers: MessageHeaders
    ) -> str | None:
        cluster_id = self.cluster.cluster_id_for_node_url(node_id)
        if not cluster_id:
            return None
        return self._fabric_next_hop_for_cluster(cluster_id, headers=headers)

    def _next_pubsub_headers(
        self, correlation_id: str, headers: MessageHeaders | None
    ) -> MessageHeaders | None:
        return self._next_fabric_headers(
            correlation_id,
            headers,
            max_hops=self.settings.fabric_routing_max_hops,
        )

    async def _forward_fabric_pubsub_message(
        self,
        message: PubSubMessage,
        *,
        headers: MessageHeaders | None,
        source_peer_url: str | None = None,
    ) -> None:
        if (
            not self._fabric_router
            or not self._fabric_transport
            or not self.settings.fabric_routing_enabled
        ):
            return
        from mpreg.fabric.message import (
            DeliveryGuarantee,
            MessageType,
            UnifiedMessage,
        )

        next_headers = self._next_pubsub_headers(message.message_id, headers)
        if next_headers is None:
            return

        unified_message = UnifiedMessage(
            message_id=message.message_id,
            topic=message.topic,
            message_type=MessageType.PUBSUB,
            delivery=DeliveryGuarantee.BROADCAST,
            payload=message.model_dump(),
            headers=next_headers,
            timestamp=message.timestamp,
        )
        route = await self._fabric_router.route_message(unified_message)
        if not route.targets:
            return

        connections = {
            url: connection
            for url, connection in self._get_all_peer_connections().items()
            if connection.is_connected
        }
        target_nodes: set[str] = set()
        for target in route.targets:
            node_id = target.node_id
            if not node_id or node_id == self.cluster.local_url:
                continue
            if node_id in connections:
                target_nodes.add(node_id)
                continue
            if not target.cluster_id:
                continue
            plan = route.cluster_routes.get(target.cluster_id)
            if plan and plan.forwarding and plan.forwarding.can_forward:
                if plan.forwarding.next_peer_url:
                    target_nodes.add(plan.forwarding.next_peer_url)

        if not target_nodes:
            return

        await self._send_fabric_message(
            unified_message,
            target_nodes=tuple(sorted(target_nodes)),
            source_peer_url=source_peer_url,
        )

    async def _send_fabric_message(
        self,
        message: UnifiedMessage,
        *,
        target_nodes: tuple[str, ...],
        source_peer_url: str | None = None,
        allow_routing_path_targets: bool = False,
    ) -> None:
        if not self._fabric_transport:
            return
        connections = {
            url: connection
            for url, connection in self._get_all_peer_connections().items()
            if connection.is_connected
        }
        if not connections:
            return

        target_nodes_set = set(target_nodes)
        direct_targets = {
            node_id for node_id in target_nodes_set if node_id in connections
        }
        send_to: set[str] = set()
        if direct_targets:
            send_to.update(direct_targets)
            if target_nodes_set - direct_targets:
                send_to.update(connections.keys())
        else:
            send_to.update(connections.keys())

        if source_peer_url:
            send_to.discard(source_peer_url)
        if not allow_routing_path_targets:
            for visited in message.headers.routing_path:
                send_to.discard(visited)

        if not send_to:
            return

        tasks = [
            self._fabric_transport.send_message(peer_id, message)
            for peer_id in sorted(send_to)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for peer_id, result in zip(sorted(send_to), results):
            if isinstance(result, Exception):
                logger.warning(
                    "[{}] Fabric message forward to {} failed: {}",
                    self.settings.name,
                    peer_id,
                    result,
                )

    async def _send_fabric_rpc_response(
        self,
        *,
        request_id: str,
        reply_to: str,
        reply_cluster: str | None = None,
        success: bool,
        result: Any | None = None,
        error: dict[str, Any] | None = None,
        routing_path: tuple[str, ...] = (),
    ) -> None:
        if not reply_to:
            return
        from mpreg.core.model import FabricMessageEnvelope
        from mpreg.fabric.message import (
            DeliveryGuarantee,
            MessageHeaders,
            MessageType,
            UnifiedMessage,
        )
        from mpreg.fabric.message_codec import unified_message_to_dict
        from mpreg.fabric.rpc_messages import FabricRPCResponse

        response_path = tuple(routing_path)
        if not response_path:
            response_path = (self.cluster.local_url,)
        elif response_path[-1] != self.cluster.local_url:
            response_path = (*response_path, self.cluster.local_url)

        response_route = (self.cluster.local_url,)

        next_hop = reply_to
        if len(response_path) > 1:
            next_hop = response_path[-2]

        headers = MessageHeaders(
            correlation_id=request_id,
            source_cluster=self.settings.cluster_id,
            target_cluster=reply_cluster,
            routing_path=response_route,
            federation_path=(self.settings.cluster_id,),
            hop_budget=self.settings.fabric_routing_max_hops,
        )
        response = FabricRPCResponse(
            request_id=request_id,
            success=success,
            result=result,
            error=error,
            responder=self.cluster.local_url,
            reply_to=reply_to,
            reply_cluster=reply_cluster,
            routing_path=response_path,
        )
        message = UnifiedMessage(
            message_id=request_id,
            topic="mpreg.rpc.response",
            message_type=MessageType.RPC,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload=response.to_dict(),
            headers=headers,
        )
        if next_hop and self._is_peer_departed(next_hop, None):
            next_hop = None

        if next_hop:
            connections = self._get_all_peer_connections()
            connection = connections.get(next_hop)
            if not connection or not connection.is_connected:
                next_hop = None

        if next_hop and self._fabric_transport:
            if await self._fabric_transport.send_message(next_hop, message):
                return
            connection = self._get_all_peer_connections().get(next_hop)
            if connection and connection.is_connected:
                envelope = FabricMessageEnvelope(
                    payload=unified_message_to_dict(message)
                )
                data = self.serializer.serialize(envelope.model_dump())
                try:
                    await connection.send(data)
                    return
                except Exception:
                    pass
            next_hop = None

        if not next_hop and reply_cluster and self._fabric_federation_planner:
            visited_clusters = tuple(
                cluster_id
                for cluster_id in (
                    self.cluster._cluster_id_for_server(node) for node in response_path
                )
                if cluster_id and cluster_id != self.settings.cluster_id
            )
            plan = self._fabric_federation_planner.plan_next_hop(
                target_cluster=reply_cluster,
                visited_clusters=visited_clusters,
                remaining_hops=self.settings.fabric_routing_max_hops,
            )
            if plan.can_forward and plan.next_peer_url:
                await self._send_fabric_message(
                    message,
                    target_nodes=(plan.next_peer_url,),
                    allow_routing_path_targets=True,
                )
                return

        if not next_hop and reply_to:
            await self._send_fabric_message(
                message,
                target_nodes=(reply_to,),
                allow_routing_path_targets=True,
            )
            await self._establish_peer_connection(reply_to, fast_connect=True)
            connection = self._get_all_peer_connections().get(reply_to)
            if connection and connection.is_connected:
                envelope = FabricMessageEnvelope(
                    payload=unified_message_to_dict(message)
                )
                data = self.serializer.serialize(envelope.model_dump())
                try:
                    await connection.send(data)
                    return
                except Exception:
                    pass

        if not next_hop:
            return

    async def _handle_fabric_rpc_request(
        self,
        message: UnifiedMessage,
        *,
        source_peer_url: str | None = None,
    ) -> None:
        from mpreg.fabric.index import FunctionQuery
        from mpreg.fabric.message import (
            DeliveryGuarantee,
            MessageType,
            UnifiedMessage,
        )
        from mpreg.fabric.rpc_messages import FabricRPCRequest

        payload = FabricRPCRequest.from_dict(message.payload)
        if message.message_type is not MessageType.RPC:
            return

        selector = FunctionSelector(
            name=payload.command,
            function_id=payload.function_id,
            version_constraint=VersionConstraint.parse(payload.version_constraint)
            if payload.version_constraint
            else None,
        )
        requested_resources = frozenset(payload.resources)
        if (
            payload.target_cluster
            and payload.target_cluster != self.settings.cluster_id
        ):
            local_matches: list[Any] = []
        elif payload.target_node and payload.target_node != self.cluster.local_url:
            local_matches = []
        elif self._fabric_control_plane:
            local_query = FunctionQuery(
                selector=selector,
                resources=requested_resources,
                cluster_id=self.settings.cluster_id,
                node_id=self.cluster.local_url,
            )
            local_matches = self._fabric_control_plane.index.find_functions(local_query)
        else:
            local_matches = []

        if local_matches:
            try:
                command_obj = self.registry.get(payload.command)
                answer_payload = await command_obj.call_async(
                    *payload.args, **payload.kwargs
                )
                await self._send_fabric_rpc_response(
                    request_id=payload.request_id,
                    reply_to=payload.reply_to,
                    reply_cluster=message.headers.source_cluster,
                    success=True,
                    result=answer_payload,
                    routing_path=message.headers.routing_path,
                )
            except Exception as e:
                await self._send_fabric_rpc_response(
                    request_id=payload.request_id,
                    reply_to=payload.reply_to,
                    reply_cluster=message.headers.source_cluster,
                    success=False,
                    error={
                        "error": str(e),
                        "command": payload.command,
                        "error_type": type(e).__name__,
                    },
                    routing_path=message.headers.routing_path,
                )
            return

        if not self._fabric_control_plane:
            await self._send_fabric_rpc_response(
                request_id=payload.request_id,
                reply_to=payload.reply_to,
                reply_cluster=message.headers.source_cluster,
                success=False,
                error={
                    "error": "fabric_routing_disabled",
                    "command": payload.command,
                },
                routing_path=message.headers.routing_path,
            )
            return

        if not self._fabric_router:
            await self._send_fabric_rpc_response(
                request_id=payload.request_id,
                reply_to=payload.reply_to,
                reply_cluster=message.headers.source_cluster,
                success=False,
                error={
                    "error": "fabric_routing_disabled",
                    "command": payload.command,
                },
                routing_path=message.headers.routing_path,
            )
            return

        route = await self._fabric_router.route_message(message)
        if not route.targets:
            await self._send_fabric_rpc_response(
                request_id=payload.request_id,
                reply_to=payload.reply_to,
                reply_cluster=message.headers.source_cluster,
                success=False,
                error={
                    "error": "fabric_route_not_found",
                    "command": payload.command,
                },
                routing_path=message.headers.routing_path,
            )
            return

        target = route.targets[0]
        allowed_clusters = self._fabric_allowed_clusters()
        if (
            allowed_clusters is not None
            and target.cluster_id
            and target.cluster_id not in allowed_clusters
        ):
            await self._send_fabric_rpc_response(
                request_id=payload.request_id,
                reply_to=payload.reply_to,
                reply_cluster=message.headers.source_cluster,
                success=False,
                error={
                    "error": "fabric_route_not_found",
                    "command": payload.command,
                },
                routing_path=message.headers.routing_path,
            )
            return

        forward_plan = (
            route.cluster_routes.get(target.cluster_id) if target.cluster_id else None
        )
        forwarding = forward_plan.forwarding if forward_plan else None
        if forwarding and not forwarding.can_forward:
            await self._send_fabric_rpc_response(
                request_id=payload.request_id,
                reply_to=payload.reply_to,
                reply_cluster=message.headers.source_cluster,
                success=False,
                error={
                    "error": "fabric_forward_failed",
                    "command": payload.command,
                    "target_cluster": target.cluster_id,
                    "reason": forwarding.reason.value,
                },
                routing_path=message.headers.routing_path,
            )
            return

        connections = self._get_all_peer_connections()
        next_hop: str | None = None
        if target.node_id and target.node_id in connections:
            connection = connections[target.node_id]
            if connection and connection.is_connected:
                next_hop = target.node_id
        if next_hop is None and forwarding and forwarding.next_peer_url:
            next_hop = forwarding.next_peer_url
        if next_hop is None:
            next_hop = target.node_id
        if not next_hop:
            await self._send_fabric_rpc_response(
                request_id=payload.request_id,
                reply_to=payload.reply_to,
                reply_cluster=message.headers.source_cluster,
                success=False,
                error={
                    "error": "fabric_route_not_found",
                    "command": payload.command,
                },
                routing_path=message.headers.routing_path,
            )
            return
        if next_hop in message.headers.routing_path:
            await self._send_fabric_rpc_response(
                request_id=payload.request_id,
                reply_to=payload.reply_to,
                reply_cluster=message.headers.source_cluster,
                success=False,
                error={
                    "error": "fabric_route_loop",
                    "command": payload.command,
                },
                routing_path=message.headers.routing_path,
            )
            return

        next_headers = self._next_fabric_headers(
            payload.request_id,
            message.headers,
            max_hops=self.settings.fabric_routing_max_hops,
        )
        if next_headers is None:
            await self._send_fabric_rpc_response(
                request_id=payload.request_id,
                reply_to=payload.reply_to,
                reply_cluster=message.headers.source_cluster,
                success=False,
                error={
                    "error": "fabric_hop_budget_exhausted",
                    "command": payload.command,
                },
                routing_path=message.headers.routing_path,
            )
            return

        updated_payload = FabricRPCRequest(
            request_id=payload.request_id,
            command=payload.command,
            args=payload.args,
            kwargs=payload.kwargs,
            resources=payload.resources,
            function_id=payload.function_id,
            version_constraint=payload.version_constraint,
            target_cluster=target.cluster_id,
            target_node=target.node_id,
            reply_to=payload.reply_to,
            federation_path=(
                forwarding.federation_path if forwarding else payload.federation_path
            ),
            federation_remaining_hops=(
                forwarding.remaining_hops
                if forwarding
                else payload.federation_remaining_hops
            ),
        )
        forward_message = UnifiedMessage(
            message_id=payload.request_id,
            topic=f"mpreg.rpc.execute.{payload.command}",
            message_type=MessageType.RPC,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload=updated_payload.to_dict(),
            headers=next_headers,
            timestamp=message.timestamp,
        )
        await self._send_fabric_message(
            forward_message,
            target_nodes=(next_hop,),
            source_peer_url=source_peer_url,
        )

    async def _handle_fabric_rpc_response(self, message: UnifiedMessage) -> None:
        from mpreg.fabric.message import UnifiedMessage
        from mpreg.fabric.rpc_messages import FabricRPCResponse

        response = FabricRPCResponse.from_dict(message.payload)
        if response.request_id in self.cluster.waitingFor:
            if response.success:
                self.cluster.answer[response.request_id] = response.result
            else:
                self.cluster.answer[response.request_id] = response.error or {
                    "error": "fabric_rpc_failed",
                    "request_id": response.request_id,
                }
            self.cluster.waitingFor[response.request_id].set()
            return

        connections = self._get_all_peer_connections()
        previous_hop: str | None = None
        if response.routing_path:
            try:
                index = response.routing_path.index(self.cluster.local_url)
            except ValueError:
                index = -1
            if index > 0:
                candidate = response.routing_path[index - 1]
                connection = connections.get(candidate)
                if connection and connection.is_connected:
                    previous_hop = candidate

        next_headers = self._next_fabric_headers(
            response.request_id,
            message.headers,
            max_hops=self.settings.fabric_routing_max_hops,
        )
        if next_headers is None:
            return

        forward_message = UnifiedMessage(
            message_id=message.message_id,
            topic=message.topic,
            message_type=message.message_type,
            delivery=message.delivery,
            payload=message.payload,
            headers=next_headers,
            timestamp=message.timestamp,
        )

        if previous_hop:
            await self._send_fabric_message(
                forward_message,
                target_nodes=(previous_hop,),
                allow_routing_path_targets=True,
            )
            return

        reply_cluster = response.reply_cluster or message.headers.target_cluster
        if reply_cluster and self._fabric_federation_planner:
            plan = self._fabric_federation_planner.plan_next_hop(
                target_cluster=reply_cluster,
                visited_clusters=message.headers.federation_path,
                remaining_hops=message.headers.hop_budget,
            )
            if plan.can_forward and plan.next_peer_url:
                await self._send_fabric_message(
                    forward_message,
                    target_nodes=(plan.next_peer_url,),
                )
                return

        if response.reply_to:
            await self._establish_peer_connection(response.reply_to, fast_connect=True)
            connection = self._get_all_peer_connections().get(response.reply_to)
            if connection and connection.is_connected:
                envelope = FabricMessageEnvelope(
                    payload=unified_message_to_dict(forward_message)
                )
                data = self.serializer.serialize(envelope.model_dump())
                with contextlib.suppress(Exception):
                    await connection.send(data)
            return

        reply_cluster = response.reply_cluster
        if not reply_cluster and response.reply_to:
            reply_cluster = self.cluster._cluster_id_for_server(response.reply_to)
        if not reply_cluster or not self._fabric_federation_planner:
            return

        visited_clusters = tuple(
            cluster_id
            for cluster_id in (
                self.cluster._cluster_id_for_server(node)
                for node in next_headers.routing_path
            )
            if cluster_id and cluster_id != self.settings.cluster_id
        )
        plan = self._fabric_federation_planner.plan_next_hop(
            target_cluster=reply_cluster,
            visited_clusters=visited_clusters,
            remaining_hops=next_headers.hop_budget,
        )
        if not plan.can_forward or not plan.next_peer_url:
            return
        await self._send_fabric_message(
            forward_message,
            target_nodes=(plan.next_peer_url,),
        )

    async def _handle_fabric_message(
        self,
        message: UnifiedMessage,
        *,
        source_peer_url: str | None = None,
    ) -> None:
        from mpreg.fabric.message import MessageType

        if message.message_type is MessageType.PUBSUB:
            pubsub_message = PubSubMessage.model_validate(message.payload)
            notifications = self.topic_exchange.publish_message(pubsub_message)
            for notification in notifications:
                await self._send_notification_to_client(notification)
            await self._forward_fabric_pubsub_message(
                pubsub_message,
                headers=message.headers,
                source_peer_url=source_peer_url,
            )
            return
        if message.message_type is MessageType.QUEUE:
            if self._fabric_queue_federation:
                await self._fabric_queue_federation.handle_fabric_message(
                    message, source_peer_url=source_peer_url
                )
            else:
                logger.debug(
                    "[{}] Ignoring fabric queue message; no queue federation manager",
                    self.settings.name,
                )
            return
        if message.message_type is MessageType.CACHE:
            if self._cache_fabric_transport:
                await self._cache_fabric_transport.handle_message(
                    message, source_peer_url=source_peer_url
                )
            else:
                logger.debug(
                    "[{}] Ignoring fabric cache message; cache federation disabled",
                    self.settings.name,
                )
            return
        if message.message_type is MessageType.RPC:
            kind = (
                message.payload.get("kind")
                if isinstance(message.payload, dict)
                else None
            )
            from mpreg.fabric.rpc_messages import (
                FABRIC_RPC_REQUEST_KIND,
                FABRIC_RPC_RESPONSE_KIND,
            )

            if kind == FABRIC_RPC_REQUEST_KIND:
                await self._handle_fabric_rpc_request(
                    message, source_peer_url=source_peer_url
                )
                return
            if kind == FABRIC_RPC_RESPONSE_KIND:
                await self._handle_fabric_rpc_response(message)
                return
        if message.message_type is MessageType.CONTROL:
            if self._fabric_raft_transport:
                handled = await self._fabric_raft_transport.handle_message(
                    message, source_peer_url=source_peer_url
                )
                if handled:
                    return
            if self._fabric_queue_delivery:
                from mpreg.fabric.queue_delivery import is_queue_consensus_message

                if is_queue_consensus_message(message):
                    await self._fabric_queue_delivery.handle_control_message(
                        message, source_peer_url=source_peer_url
                    )
                    return

        logger.debug(
            "[{}] Unhandled fabric message type: {}",
            self.settings.name,
            message.message_type.value,
        )

    async def _start_fabric_control_plane(self) -> None:
        if not self._fabric_control_plane:
            return
        await self._restore_fabric_snapshots()
        await self._fabric_control_plane.gossip.start()
        if self._fabric_control_plane.route_announcer:
            await self._fabric_control_plane.route_announcer.start()
        if self._fabric_control_plane.route_key_announcer:
            await self._fabric_control_plane.route_key_announcer.start()
        if self._fabric_control_plane.link_state_announcer:
            await self._fabric_control_plane.link_state_announcer.start()
        await self._announce_fabric_functions()
        await self._announce_fabric_cache_role()
        await self._announce_fabric_topics()
        if self._fabric_queue_federation:
            await self._fabric_queue_federation.advertise_existing_queues()
        await self._start_fabric_catalog_refresh()
        await self._start_fabric_route_key_refresh()

    async def _restore_fabric_snapshots(self) -> None:
        if not self._persistence_registry or not self._fabric_control_plane:
            return
        from mpreg.fabric.persistence import FabricSnapshotStore

        store = FabricSnapshotStore(
            self._persistence_registry.key_value_store("fabric")
        )
        snapshot_time = time.time()
        try:
            counts = await store.load_catalog(
                self._fabric_control_plane.catalog, now=snapshot_time
            )
            self._fabric_snapshot_last_restored_counts = dict(counts)
            self._fabric_snapshot_last_restored_at = snapshot_time
            if counts:
                logger.debug(
                    "[{}] Restored fabric catalog snapshot: {}",
                    self.settings.name,
                    counts,
                )
        except Exception as exc:
            logger.warning(
                "[{}] Fabric catalog snapshot restore failed: {}",
                self.settings.name,
                exc,
            )
        if self._fabric_control_plane.route_key_registry:
            try:
                restored = await store.load_route_keys(
                    self._fabric_control_plane.route_key_registry, now=snapshot_time
                )
                self._fabric_snapshot_last_route_keys_restored = restored
                if self._fabric_snapshot_last_restored_at is None:
                    self._fabric_snapshot_last_restored_at = snapshot_time
                if restored:
                    logger.debug(
                        "[{}] Restored {} fabric route keys from snapshot",
                        self.settings.name,
                        restored,
                    )
            except Exception as exc:
                logger.warning(
                    "[{}] Fabric route key snapshot restore failed: {}",
                    self.settings.name,
                    exc,
                )

    async def _save_fabric_snapshots(self) -> None:
        if not self._persistence_registry or not self._fabric_control_plane:
            return
        from mpreg.fabric.persistence import FabricSnapshotStore

        store = FabricSnapshotStore(
            self._persistence_registry.key_value_store("fabric")
        )
        snapshot_time = time.time()
        try:
            catalog = self._fabric_control_plane.catalog
            self._fabric_snapshot_last_saved_counts = {
                "functions": catalog.functions.entry_count(),
                "topics": catalog.topics.entry_count(),
                "queues": catalog.queues.entry_count(),
                "caches": catalog.caches.entry_count(),
                "cache_profiles": catalog.cache_profiles.entry_count(),
                "nodes": catalog.nodes.entry_count(),
            }
            await store.save_catalog(
                self._fabric_control_plane.catalog, now=snapshot_time
            )
            self._fabric_snapshot_last_saved_at = snapshot_time
        except Exception as exc:
            logger.warning(
                "[{}] Fabric catalog snapshot save failed: {}",
                self.settings.name,
                exc,
            )
        if self._fabric_control_plane.route_key_registry:
            try:
                route_registry = self._fabric_control_plane.route_key_registry
                route_registry.purge_expired(now=snapshot_time)
                self._fabric_snapshot_last_route_keys_saved = sum(
                    len(key_set.keys) for key_set in route_registry.key_sets.values()
                )
                await store.save_route_keys(route_registry, now=snapshot_time)
                if self._fabric_snapshot_last_saved_at is None:
                    self._fabric_snapshot_last_saved_at = snapshot_time
            except Exception as exc:
                logger.warning(
                    "[{}] Fabric route key snapshot save failed: {}",
                    self.settings.name,
                    exc,
                )

    async def _start_fabric_catalog_refresh(self) -> None:
        if (
            not self._fabric_control_plane
            or not self._local_function_registry
            or self._shutdown_event.is_set()
        ):
            return
        if (
            self._fabric_catalog_refresh_task
            and not self._fabric_catalog_refresh_task.done()
        ):
            return
        refresh_interval = max(1.0, self.settings.fabric_catalog_ttl_seconds * 0.5)

        async def _refresh_loop() -> None:
            while not self._shutdown_event.is_set():
                try:
                    await self._announce_fabric_functions()
                except Exception as exc:
                    logger.warning(
                        "[{}] Fabric catalog refresh failed: {}",
                        self.settings.name,
                        exc,
                    )
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=refresh_interval
                    )
                except TimeoutError:
                    continue

        self._fabric_catalog_refresh_task = asyncio.create_task(_refresh_loop())
        self._track_background_task(self._fabric_catalog_refresh_task)

    async def _start_fabric_route_key_refresh(self) -> None:
        if (
            not self.settings.fabric_route_key_provider
            or not self.settings.fabric_route_key_registry
            or self._shutdown_event.is_set()
        ):
            return
        if (
            self._fabric_route_key_refresh_task
            and not self._fabric_route_key_refresh_task.done()
        ):
            return
        refresh_interval = max(
            1.0, float(self.settings.fabric_route_key_refresh_interval_seconds)
        )

        async def _refresh_loop() -> None:
            while not self._shutdown_event.is_set():
                try:
                    from mpreg.fabric.route_keys import refresh_route_keys

                    await refresh_route_keys(
                        self.settings.fabric_route_key_provider,
                        self.settings.fabric_route_key_registry,
                    )
                except Exception as exc:
                    logger.warning(
                        "[{}] Fabric route key refresh failed: {}",
                        self.settings.name,
                        exc,
                    )
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=refresh_interval
                    )
                except TimeoutError:
                    continue

        self._fabric_route_key_refresh_task = asyncio.create_task(_refresh_loop())
        self._track_background_task(self._fabric_route_key_refresh_task)

    def _ensure_fabric_graph_router(self) -> GraphBasedFederationRouter:
        """Ensure a fabric graph router is available for routing."""
        graph_router = self.cluster.fabric_graph_router
        if graph_router is None:
            graph_router = GraphBasedFederationRouter(
                cache_ttl_seconds=30.0,
                max_cache_size=10000,
            )
            self.cluster.fabric_graph_router = graph_router
        self._ensure_fabric_graph_node(
            cluster_id=self.settings.cluster_id,
            region=self.settings.cache_region,
            latitude=self.settings.cache_latitude,
            longitude=self.settings.cache_longitude,
        )
        return graph_router

    def _ensure_fabric_link_state_router(self) -> GraphBasedFederationRouter:
        """Ensure a link-state graph router is available for optional routing."""
        router = self.cluster.fabric_link_state_router
        if router is None:
            router = GraphBasedFederationRouter(
                cache_ttl_seconds=30.0,
                max_cache_size=10000,
            )
            self.cluster.fabric_link_state_router = router
        return router

    def reload_fabric_route_config(
        self,
        *,
        route_policy: RoutePolicy | None = None,
        export_policy: RoutePolicy | None = None,
        neighbor_policies: RoutePolicyDirectory | None = None,
        export_neighbor_policies: RoutePolicyDirectory | None = None,
        security_config: RouteSecurityConfig | None = None,
        key_registry: RouteKeyRegistry | None = None,
        signer: RouteAnnouncementSigner | None = None,
    ) -> None:
        """Reload fabric route configuration without dropping existing routes."""
        if not self._fabric_control_plane:
            return

        if route_policy is not None:
            self.settings.fabric_route_policy = route_policy
        if export_policy is not None:
            self.settings.fabric_route_export_policy = export_policy
        if neighbor_policies is not None:
            self.settings.fabric_route_neighbor_policies = neighbor_policies
        if export_neighbor_policies is not None:
            self.settings.fabric_route_export_neighbor_policies = (
                export_neighbor_policies
            )
        if security_config is not None:
            self.settings.fabric_route_security_config = security_config
        if key_registry is not None:
            self.settings.fabric_route_key_registry = key_registry
        if signer is not None:
            self.settings.fabric_route_signer = signer

        neighbor_resolver = (
            self.settings.fabric_route_neighbor_policies.resolver()
            if self.settings.fabric_route_neighbor_policies
            else None
        )
        export_neighbor_resolver = (
            self.settings.fabric_route_export_neighbor_policies.resolver()
            if self.settings.fabric_route_export_neighbor_policies
            else None
        )
        public_key_resolver = (
            self.settings.fabric_route_key_registry.resolver()
            if self.settings.fabric_route_key_registry
            else None
        )
        self._fabric_control_plane.update_route_configuration(
            route_policy=self.settings.fabric_route_policy,
            export_policy=self.settings.fabric_route_export_policy,
            neighbor_policy_resolver=neighbor_resolver,
            export_neighbor_policy_resolver=export_neighbor_resolver,
            security_config=self.settings.fabric_route_security_config,
            public_key_resolver=public_key_resolver,
            signer=self.settings.fabric_route_signer,
            force=True,
        )

    def _ensure_fabric_graph_node(
        self,
        *,
        cluster_id: str,
        region: str,
        latitude: float,
        longitude: float,
    ) -> None:
        """Ensure a fabric graph node exists for a cluster."""
        router = self.cluster.fabric_graph_router
        if router is None:
            return
        if cluster_id in router.graph.nodes:
            return
        node = FederationGraphNode(
            node_id=cluster_id,
            node_type=NodeType.CLUSTER,
            region=region,
            coordinates=GeographicCoordinate(latitude, longitude),
            max_capacity=1000,
            current_load=0.0,
            health_score=1.0,
            processing_latency_ms=1.0,
            bandwidth_mbps=1000,
            reliability_score=1.0,
        )
        router.add_node(node)

    def _register_fabric_graph_peer(self, peer_cluster_id: str) -> None:
        """Register a peer cluster in the fabric graph."""
        router = self.cluster.fabric_graph_router
        if router is None:
            return
        if peer_cluster_id == self.settings.cluster_id:
            return
        self._ensure_fabric_graph_node(
            cluster_id=self.settings.cluster_id,
            region=self.settings.cache_region,
            latitude=self.settings.cache_latitude,
            longitude=self.settings.cache_longitude,
        )
        self._ensure_fabric_graph_node(
            cluster_id=peer_cluster_id,
            region="unknown",
            latitude=0.0,
            longitude=0.0,
        )
        router.add_edge(
            FederationGraphEdge(
                source_id=self.settings.cluster_id,
                target_id=peer_cluster_id,
                latency_ms=5.0,
                bandwidth_mbps=1000,
                reliability_score=0.99,
            )
        )

    def _remove_fabric_graph_peer(self, peer_cluster_id: str) -> None:
        """Remove fabric graph entry when a peer cluster disconnects."""
        router = self.cluster.fabric_graph_router
        if router is None:
            return
        if peer_cluster_id == self.settings.cluster_id:
            return
        still_present = False
        if self._peer_directory:
            still_present = any(
                node.cluster_id == peer_cluster_id
                for node in self._peer_directory.nodes()
            )
        if still_present:
            return
        router.remove_edge(self.settings.cluster_id, peer_cluster_id)
        router.remove_node(peer_cluster_id)

    async def _initialize_default_systems(self) -> None:
        """Initialize optional cache/queue systems for unified deployments."""
        if self._persistence_registry is not None:
            await self._persistence_registry.open()

        if self.settings.enable_default_cache:
            from mpreg.fabric.cache_federation import FabricCacheProtocol
            from mpreg.fabric.cache_transport import ServerCacheTransport

            from .core.global_cache import GlobalCacheConfiguration, GlobalCacheManager

            cache_config = GlobalCacheConfiguration(
                enable_l4_federation=self.settings.enable_cache_federation,
                local_cluster_id=self.settings.cluster_id,
                local_region=self.settings.cache_region,
            )

            cache_protocol = None
            cache_transport = None
            if cache_config.enable_l3_distributed or cache_config.enable_l4_federation:
                allowed_clusters = (
                    self._fabric_allowed_clusters()
                    if cache_config.enable_l4_federation
                    else frozenset({self.settings.cluster_id})
                )
                from mpreg.fabric.cache_selection import CachePeerSelector
                from mpreg.fabric.federation_graph import GeographicCoordinate

                peer_selector = CachePeerSelector(
                    local_region=self.settings.cache_region,
                    local_coordinates=GeographicCoordinate(
                        latitude=self.settings.cache_latitude,
                        longitude=self.settings.cache_longitude,
                    ),
                )
                cache_transport = ServerCacheTransport(
                    server=self,
                    serializer=self.serializer,
                    routing_index=(
                        self._fabric_control_plane.index
                        if self._fabric_control_plane
                        else None
                    ),
                    allowed_clusters=allowed_clusters,
                    peer_selector=peer_selector,
                )
                cache_protocol = FabricCacheProtocol(
                    node_id=self.cluster.local_url,
                    transport=cache_transport,
                    gossip_interval=cache_config.gossip_interval_seconds,
                )

            cache_manager = GlobalCacheManager(
                cache_config,
                cache_protocol=cache_protocol,
                persistence_registry=self._persistence_registry,
            )

            self._cache_manager = cache_manager
            self._cache_fabric_protocol = cache_protocol
            self._cache_fabric_transport = cache_transport
            self.attach_cache_manager(cache_manager)

        if self.settings.enable_default_queue:
            from .core.message_queue_manager import create_reliable_queue_manager

            queue_manager = create_reliable_queue_manager(
                persistence_registry=self._persistence_registry
            )
            self._queue_manager = queue_manager
            self.attach_queue_manager(queue_manager)
            if self._persistence_registry is not None:
                await queue_manager.restore_persisted_queues()
            if self._fabric_router:
                self._fabric_router.message_queue = queue_manager
            self._initialize_fabric_queue_federation()

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
        if not self._shutdown_event.is_set():
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

        if self._fabric_control_plane:
            try:
                if self._fabric_control_plane.route_announcer:
                    await self._fabric_control_plane.route_announcer.stop()
                if self._fabric_control_plane.route_key_announcer:
                    await self._fabric_control_plane.route_key_announcer.stop()
                if self._fabric_control_plane.link_state_announcer:
                    await self._fabric_control_plane.link_state_announcer.stop()
                await self._fabric_control_plane.gossip.stop()
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error stopping fabric gossip: {e}"
                )

        try:
            await self._save_fabric_snapshots()
        except Exception as e:
            logger.warning(f"[{self.settings.name}] Error saving fabric snapshots: {e}")

        # CRITICAL: Close connections FIRST to allow tasks to exit gracefully
        logger.info(f"[{self.settings.name}] Closing peer connections...")
        connections_to_close = set(self.peer_connections.values())
        connections_to_close.update(self._inbound_peer_connections.values())
        connections_to_close.update(self.clients)
        for connection in connections_to_close:
            try:
                await connection.disconnect()
            except Exception as e:
                logger.warning(f"[{self.settings.name}] Error closing connection: {e}")
        self.peer_connections.clear()
        self._inbound_peer_connections.clear()
        self.clients.clear()

        # Close any tracked PubSub clients
        if self.pubsub_clients:
            for client_id, transport in list(self.pubsub_clients.items()):
                try:
                    await transport.disconnect()
                except Exception as e:
                    logger.debug(
                        f"[{self.settings.name}] Error closing pubsub client {client_id}: {e}"
                    )
            self.pubsub_clients.clear()
            self.subscription_to_client.clear()

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
            cleanup_rounds = 0
            while self._background_tasks and cleanup_rounds < 3:
                cleanup_rounds += 1
                # Create a copy of the task set to avoid set size changes during iteration
                tasks_to_cleanup = list(self._background_tasks)

                # Clear the set immediately to capture any new tasks in the next round
                self._background_tasks.clear()

                try:
                    grace_period = max(0.2, min(1.0, len(tasks_to_cleanup) * 0.05))
                    done, pending = await asyncio.wait(
                        tasks_to_cleanup,
                        timeout=grace_period,
                        return_when=asyncio.ALL_COMPLETED,
                    )

                    if pending:
                        # Cancel any tasks that didn't respond to shutdown event
                        for task in pending:
                            if not task.done():
                                task.cancel()

                        # Wait for cancelled tasks to complete
                        await asyncio.gather(*pending, return_exceptions=True)

                except Exception as e:
                    logger.warning(
                        f"[{self.settings.name}] Error during task cleanup: {e}"
                    )
                    # Force cancel all tasks as fallback
                    for task in tasks_to_cleanup:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks_to_cleanup, return_exceptions=True)

            # Final pass for any tasks added during shutdown
            if self._background_tasks:
                leftover_tasks = list(self._background_tasks)
                self._background_tasks.clear()
                for task in leftover_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*leftover_tasks, return_exceptions=True)

        if self._cache_manager is not None:
            try:
                await self._cache_manager.shutdown()
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error shutting down cache manager: {e}"
                )

        if self._fabric_queue_delivery is not None:
            try:
                await self._fabric_queue_delivery.shutdown()
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error shutting down fabric queue delivery: {e}"
                )

        if self._queue_manager is not None:
            try:
                await self._queue_manager.shutdown()
            except AttributeError:
                pass
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error shutting down queue manager: {e}"
                )

        if self._persistence_registry is not None:
            try:
                await self._persistence_registry.close()
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error shutting down persistence registry: {e}"
                )

        # Close transport listener
        if self._transport_listener:
            try:
                await self._transport_listener.stop()
                logger.debug(f"[{self.settings.name}] Closed transport listener")
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error closing transport listener: {e}"
                )

        await self._stop_monitoring_services()

        if self._auto_allocated_monitoring_port is not None:
            try:
                from .core.port_allocator import release_port

                release_port(self._auto_allocated_monitoring_port)
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error releasing monitoring port: {e}"
                )
            finally:
                self._auto_allocated_monitoring_port = None

        if self._auto_allocated_port is not None:
            try:
                from .core.port_allocator import release_port

                release_port(self._auto_allocated_port)
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error releasing server port: {e}"
                )
            finally:
                self._auto_allocated_port = None

        logger.info(f"[{self.settings.name}] Async shutdown completed")

        # Clean up logger handler to prevent event loop errors
        try:
            for handler_id in self._logger_handler_ids:
                logger.remove(handler_id)
        except ValueError:
            # Handler already removed
            pass

    def report(self) -> None:
        """General report of current server state."""

        logger.info("Resources: {}", pp.pformat(self.settings.resources))
        # logger.info("Funs: {}", pp.pformat(self.settings.funs)) # Removed as funs is no longer in settings
        logger.info("Clients: {}", pp.pformat(self.clients))

    def _get_peers_snapshot(self) -> list[dict[str, Any]]:
        """Return a JSON-safe snapshot of current peer info."""
        peers = []
        if not self._peer_directory:
            return peers
        for node in self._peer_directory.nodes():
            if node.node_id == self.cluster.local_url:
                continue
            status = self.peer_status.get(node.node_id)
            funs, locs = self._peer_function_snapshot(node.node_id)
            peers.append(
                {
                    "url": node.node_id,
                    "funs": list(funs),
                    "locs": sorted(locs),
                    "last_seen": node.advertised_at,
                    "cluster_id": node.cluster_id,
                    "advertised_urls": [],
                    "status": status.status if status else "unknown",
                    "status_timestamp": status.timestamp if status else None,
                }
            )
        return sorted(peers, key=lambda peer: peer["url"])

    def _current_function_catalog(self) -> tuple[RPCFunctionDescriptor, ...]:
        if not self._local_function_registry:
            return tuple()
        catalog: list[RPCFunctionDescriptor] = []
        for registration in self._local_function_registry.registrations():
            catalog.append(
                RPCFunctionDescriptor(
                    name=registration.identity.name,
                    function_id=registration.identity.function_id,
                    version=str(registration.identity.version),
                    resources=tuple(sorted(registration.resources)),
                )
            )
        return tuple(catalog)

    def _register_cache_fabric_peer(self, peer_url: str) -> None:
        # Cache federation peer coordination is handled by the transport layer.
        return

    def _remove_cache_fabric_peer(self, peer_url: str) -> None:
        # Cache federation peer coordination is handled by the transport layer.
        return

    def _track_inbound_peer_connection(
        self, peer_url: str, connection: Connection
    ) -> None:
        if peer_url == self.cluster.local_url:
            return
        existing = self._inbound_peer_connections.get(peer_url)
        if existing is connection:
            return
        self._inbound_peer_connections[peer_url] = connection
        event = ConnectionEvent.established(peer_url, self.cluster.local_url)
        self.cluster.connection_event_bus.publish(event)
        self._schedule_catalog_snapshot(peer_url)

    def _drop_inbound_peer_connection(
        self, peer_url: str, connection: Connection
    ) -> None:
        existing = self._inbound_peer_connections.get(peer_url)
        if existing is connection:
            del self._inbound_peer_connections[peer_url]
            event = ConnectionEvent.lost(peer_url, self.cluster.local_url)
            self.cluster.connection_event_bus.publish(event)

    def _schedule_catalog_snapshot(self, peer_url: str) -> None:
        if not self._fabric_control_plane or not self._fabric_gossip_transport:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._track_background_task(
            loop.create_task(self._send_catalog_snapshot_to_peer(peer_url))
        )

    async def _send_catalog_snapshot_to_peer(self, peer_url: str) -> None:
        if (
            not self._fabric_control_plane
            or not self._fabric_gossip_transport
            or peer_url == self.cluster.local_url
        ):
            return
        import uuid

        from mpreg.fabric.catalog_delta import RoutingCatalogDelta
        from mpreg.fabric.gossip import GossipMessage, GossipMessageType

        now = time.time()
        catalog = self._fabric_control_plane.catalog
        delta = RoutingCatalogDelta(
            update_id=f"catalog-snapshot:{uuid.uuid4()}",
            cluster_id=self.settings.cluster_id,
            sent_at=now,
            functions=catalog.functions.entries(now=now),
            topics=tuple(catalog.topics.entries(now=now)),
            queues=tuple(catalog.queues.entries(now=now)),
            caches=tuple(catalog.caches.entries(now=now)),
            cache_profiles=tuple(catalog.cache_profiles.entries(now=now)),
            nodes=tuple(catalog.nodes.entries(now=now)),
        )
        if not (
            delta.functions
            or delta.topics
            or delta.queues
            or delta.caches
            or delta.cache_profiles
            or delta.nodes
        ):
            return
        gossip = self._fabric_control_plane.gossip
        message = GossipMessage(
            message_id=f"{gossip.node_id}:snapshot:{uuid.uuid4()}",
            message_type=GossipMessageType.CATALOG_UPDATE,
            sender_id=gossip.node_id,
            payload=delta.to_dict(),
            vector_clock=gossip.vector_clock.copy(),
            sequence_number=gossip.protocol_stats.messages_created,
            ttl=0,
            max_hops=0,
        )
        gossip.protocol_stats.messages_created += 1
        await self._fabric_gossip_transport.send_message(peer_url, message)

    def _get_all_peer_connections(self) -> dict[str, Connection]:
        connections = dict(self.peer_connections)
        for peer_url, connection in self._inbound_peer_connections.items():
            if peer_url not in connections or not connections[peer_url].is_connected:
                connections[peer_url] = connection
        return connections

    async def _close_peer_connection(self, peer_url: str) -> None:
        for connection_map in (self.peer_connections, self._inbound_peer_connections):
            connection = connection_map.get(peer_url)
            if not connection:
                continue
            try:
                await connection.disconnect()
                logger.info(
                    f"[{self.settings.name}] Closed connection to peer {peer_url}"
                )
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error closing connection to {peer_url}: {e}"
                )
            finally:
                if peer_url in connection_map:
                    del connection_map[peer_url]

    def _peer_function_snapshot(
        self, peer_url: str
    ) -> tuple[tuple[str, ...], frozenset[str]]:
        functions: set[str] = set()
        resources: set[str] = set()
        if self._fabric_control_plane:
            catalog = self._fabric_control_plane.index.catalog
            for endpoint in catalog.functions.entries():
                if endpoint.node_id == peer_url:
                    functions.add(endpoint.identity.name)
                    resources.update(endpoint.resources)
        if self._peer_directory:
            node = self._peer_directory.node_for_id(peer_url)
            if node:
                resources.update(node.resources)
        return tuple(sorted(functions)), frozenset(resources)

    def _record_departed_peer(self, goodbye: RPCServerGoodbye) -> None:
        ttl = self.settings.goodbye_reconnect_grace_seconds
        if ttl <= 0:
            return
        self._departed_peers[goodbye.departing_node_url] = DepartedPeer(
            node_url=goodbye.departing_node_url,
            instance_id=goodbye.instance_id or "",
            cluster_id=goodbye.cluster_id,
            reason=goodbye.reason,
            departed_at=goodbye.timestamp,
            ttl_seconds=ttl,
        )
        logger.debug(
            "[{}] Recorded departed peer: node_url={} cluster_id={} reason={} ttl={}",
            self.settings.name,
            goodbye.departing_node_url,
            goodbye.cluster_id,
            goodbye.reason.value,
            ttl,
        )

    def _record_and_apply_departure(self, goodbye: RPCServerGoodbye) -> None:
        goodbye_log = logger
        before_nodes = ()
        if self._peer_directory:
            before_nodes = tuple(node.node_id for node in self._peer_directory.nodes())
        self._record_departed_peer(goodbye)
        self._apply_remote_departure(
            server_url=goodbye.departing_node_url,
            cluster_id=goodbye.cluster_id,
        )
        if self._peer_directory:
            self._peer_directory.remove_node_by_id(goodbye.departing_node_url)
            after_nodes = tuple(node.node_id for node in self._peer_directory.nodes())
            goodbye_log.debug(
                "[{}] Applied departure cleanup: node_url={} before={} after={} nodes={}",
                self.settings.name,
                goodbye.departing_node_url,
                len(before_nodes),
                len(after_nodes),
                after_nodes,
            )

    def _prune_departed_peers(self, now: float | None = None) -> None:
        timestamp = now if now is not None else time.time()
        expired = [
            node_url
            for node_url, record in self._departed_peers.items()
            if record.is_expired(timestamp)
        ]
        for node_url in expired:
            self._departed_peers.pop(node_url, None)

    def _is_peer_departed(self, peer_url: str, instance_id: str | None = None) -> bool:
        record = self._departed_peers.get(peer_url)
        if record is None:
            return False
        if record.is_expired():
            self._departed_peers.pop(peer_url, None)
            return False
        if instance_id and record.instance_id and instance_id != record.instance_id:
            self._departed_peers.pop(peer_url, None)
            return False
        return True

    def _handle_remote_status(
        self, status: RPCServerStatus, *, connection: Connection | None = None
    ) -> bool:
        if self._is_peer_departed(status.server_url, status.instance_id or None):
            if connection is not None:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
                if loop is not None:
                    self._track_background_task(
                        asyncio.create_task(
                            self._close_peer_connection(status.server_url)
                        )
                    )
            return False
        self.peer_status[status.server_url] = status
        if status.instance_id:
            self._peer_instance_ids[status.server_url] = status.instance_id
        return True

    def _apply_remote_departure(self, *, server_url: str, cluster_id: str) -> None:
        if not self._fabric_control_plane:
            return
        from mpreg.fabric.catalog import NodeKey
        from mpreg.fabric.catalog_delta import RoutingCatalogDelta

        timestamp = time.time()
        catalog = self._fabric_control_plane.index.catalog
        cluster_ids = {cluster_id}
        if self._peer_directory:
            node = self._peer_directory.node_for_id(server_url)
            if node:
                cluster_ids.add(node.cluster_id)
        for node in catalog.nodes.entries():
            if node.node_id == server_url:
                cluster_ids.add(node.cluster_id)

        function_removals = []
        topic_removals = []
        queue_removals = []
        cache_removals = []
        cache_profile_removals = []

        for endpoint in catalog.functions.entries():
            if endpoint.node_id == server_url:
                cluster_ids.add(endpoint.cluster_id)
                function_removals.append(endpoint.key())
        for subscription in catalog.topics.entries():
            if subscription.node_id == server_url:
                cluster_ids.add(subscription.cluster_id)
                topic_removals.append(subscription.subscription_id)
        for endpoint in catalog.queues.entries():
            if endpoint.node_id == server_url:
                cluster_ids.add(endpoint.cluster_id)
                queue_removals.append(endpoint.key())
        for entry in catalog.caches.entries():
            if entry.node_id == server_url:
                cluster_ids.add(entry.cluster_id)
                cache_removals.append(entry.key())
        for profile in catalog.cache_profiles.entries():
            if profile.node_id == server_url:
                cluster_ids.add(profile.cluster_id)
                cache_profile_removals.append(profile.key())

        delta = RoutingCatalogDelta(
            update_id=str(ulid.new()),
            cluster_id=cluster_id,
            sent_at=timestamp,
            function_removals=tuple(function_removals),
            topic_removals=tuple(topic_removals),
            queue_removals=tuple(queue_removals),
            cache_removals=tuple(cache_removals),
            cache_profile_removals=tuple(cache_profile_removals),
            node_removals=(
                tuple(
                    NodeKey(cluster_id=cid, node_id=server_url)
                    for cid in sorted(cluster_ids)
                )
                or (NodeKey(cluster_id=cluster_id, node_id=server_url),)
            ),
        )
        counts = self._fabric_control_plane.applier.apply(delta, now=timestamp)
        logger.debug(
            "[{}] Applied catalog departure delta: node_url={} counts={}",
            self.settings.name,
            server_url,
            counts,
        )
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._track_background_task(
            loop.create_task(
                self._fabric_control_plane.broadcaster.publisher.publish(delta)
            )
        )

    def _mark_peer_disconnected(self, peer_url: str) -> None:
        funs, locs = self._peer_function_snapshot(peer_url)
        cluster_id = (
            self._peer_directory.cluster_id_for_node(peer_url)
            if self._peer_directory
            else None
        )
        self.peer_status[peer_url] = RPCServerStatus(
            server_url=peer_url,
            cluster_id=cluster_id or self.settings.cluster_id,
            status="disconnected",
            active_clients=0,
            peer_count=len(self._peer_directory.nodes()) if self._peer_directory else 0,
            funs=funs,
            locs=locs,
            advertised_urls=tuple(),
            metrics={"disconnected": True},
        )

    def _track_background_task(self, task: asyncio.Task[Any]) -> None:
        """Track background tasks for shutdown cleanup."""
        if self._shutdown_event.is_set():
            task.cancel()
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _register_default_commands(self) -> None:
        """Registers the default RPC commands (echo, echos).

        NOTE: These functions are also decorated with @rpc_command, so we skip
        manual registration to avoid duplicates. They will be auto-discovered.
        """
        logger.debug(
            f"🚀 {self.settings.name}: _register_default_commands() called - skipping echo/echos (auto-discovered)"
        )
        # Skip manual registration - echo and echos are @rpc_command decorated
        # and will be automatically discovered by _discover_and_register_rpc_commands()
        self.register_command("list_peers", self._get_peers_snapshot, [])

    def _discover_and_register_rpc_commands(self) -> None:
        """Discovers and registers RPC commands defined using the @rpc_command decorator.

        This method inspects the current module's global namespace for functions
        that have been decorated with @rpc_command and registers them with the server.
        """
        logger.debug(
            f"🔍 {self.settings.name}: _discover_and_register_rpc_commands() called"
        )
        for name in dir(sys.modules[__name__]):
            obj = sys.modules[__name__].__dict__[name]
            if callable(obj) and "_rpc_command_name" in obj.__dict__:
                rpc_name = obj._rpc_command_name
                rpc_resources = obj._rpc_command_resources
                self.register_command(rpc_name, obj, rpc_resources)
                logger.debug(
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
        graceful shutdowns (GOODBYE) or status updates (STATUS).
        """
        try:
            # The 'what' field in the server message determines the action.
            # This uses Pydantic's validation to ensure the message structure is correct.
            match req.server.what:
                case "GOODBYE":
                    # A server is gracefully shutting down.
                    # Handle the GOODBYE message properly with cleanup
                    sender_url = req.server.departing_node_url

                    # Process GOODBYE message asynchronously
                    self._record_and_apply_departure(req.server)
                    self._track_background_task(
                        asyncio.create_task(
                            self._handle_goodbye_message(
                                req.server, sender_url, record_departure=False
                            )
                        )
                    )

                    return RPCResponse(r="GOODBYE_PROCESSED", u=req.u)
                case "STATUS":
                    # A server is sending a status update (e.g., for gossip protocol).\
                    status = req.server
                    self._handle_remote_status(status)
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

        Supports enhanced debugging features including intermediate results
        and execution summaries based on request parameters.
        """
        start_time = time.time()
        success = False
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

                success = True
                return RPCResponse(
                    r=result,
                    u=req.u,
                    intermediate_results=intermediate_results,
                    execution_summary=execution_summary,
                )
            else:
                # Use standard execution for backward compatibility
                response = RPCResponse(r=await self.cluster.run(rpc), u=req.u)
                success = True
                return response

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
        finally:
            duration_ms = (time.time() - start_time) * 1000.0
            self._metrics_tracker.record_rpc(duration_ms, success)

    @logger.catch
    async def opened(self, transport: TransportInterface) -> None:
        """Handles a new incoming transport connection.

        This method is the entry point for all incoming messages, routing them
        to the appropriate handler based on their 'role'.
        """
        peer_label = transport.url
        connection = Connection.from_transport(transport, url=peer_label)
        peer_url: str | None = None
        peer_cluster_id: str | None = None
        peer_node_id: str | None = None
        is_server_connection = False

        try:
            self.clients.add(connection)
            while not self._shutdown_event.is_set():
                try:
                    msg = await transport.receive()
                except TransportConnectionError:
                    break
                except Exception as exc:
                    logger.warning(
                        "[{}] Transport receive failed: {}",
                        peer_label,
                        exc,
                    )
                    break
                if msg is None:
                    break
                # Attempt to parse the incoming message into a Pydantic model.
                # This provides automatic validation and type conversion.
                parsed_msg = self.serializer.deserialize(
                    msg.encode("utf-8") if isinstance(msg, str) else msg
                )

                # logger.info("[{}] Received: {}", peer_label, parsed_msg)

                response_model: RPCResponse | PubSubAck | None = None
                close_connection = False
                match parsed_msg.get("role"):
                    case "server":
                        # SERVER-TO-SERVER communications packet
                        # (joining/leaving cluster, gossip updates of servers attached to other servers)
                        server_request = RPCServerRequest.model_validate(parsed_msg)
                        logger.debug(
                            "[{}] Server message: {}",
                            peer_label,
                            server_request.model_dump_json(),
                        )
                        remote_cluster_id = None
                        remote_node_id = None
                        peer_url = None
                        status_data: dict[str, Any] = {}

                        if isinstance(server_request.server, RPCServerStatus):
                            remote_cluster_id = server_request.server.cluster_id
                            peer_url = server_request.server.server_url
                            remote_node_id = peer_url
                            status_data = {
                                "functions": list(server_request.server.funs),
                                "locations": list(server_request.server.locs),
                            }
                        elif isinstance(server_request.server, RPCServerGoodbye):
                            remote_cluster_id = server_request.server.cluster_id
                            peer_url = server_request.server.departing_node_url
                            remote_node_id = peer_url

                        if peer_url and not is_server_connection:
                            if isinstance(
                                server_request.server, RPCServerStatus
                            ) and self._is_peer_departed(
                                peer_url,
                                server_request.server.instance_id or None,
                            ):
                                response_model = RPCResponse(
                                    r=None,
                                    error=RPCError(
                                        code=1003,
                                        message="Peer is marked departed",
                                    ),
                                    u=server_request.u,
                                )
                                close_connection = True
                            else:
                                decision = (
                                    self.federation_manager.handle_connection_status(
                                        remote_cluster_id=remote_cluster_id or "",
                                        remote_node_id=remote_node_id or peer_url,
                                        remote_url=peer_url,
                                        local_cluster_id=self.settings.cluster_id,
                                        local_node_id=self.settings.name,
                                        status_data=status_data,
                                    )
                                )
                                if not decision.allowed:
                                    logger.info(
                                        "[{}] Rejected server message from cluster '{}': {}",
                                        peer_label,
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
                                    close_connection = True
                                else:
                                    peer_cluster_id = remote_cluster_id
                                    peer_node_id = remote_node_id
                                    connection.url = peer_url
                                    is_server_connection = True
                                    self._track_inbound_peer_connection(
                                        peer_url, connection
                                    )
                                    self._register_cache_fabric_peer(peer_url)

                                    if (
                                        remote_cluster_id
                                        and remote_cluster_id
                                        != self.settings.cluster_id
                                    ):
                                        self.federation_manager.notify_connection_established(
                                            remote_cluster_id,
                                            remote_node_id or peer_url,
                                            peer_url,
                                        )
                                        self._register_fabric_graph_peer(
                                            remote_cluster_id
                                        )
                                    if isinstance(
                                        server_request.server, RPCServerStatus
                                    ):
                                        await self._send_status_to_connection(
                                            connection
                                        )

                        if response_model is None:
                            response_model = self.run_server(connection, server_request)

                    case "rpc":
                        # CLIENT request
                        rpc_request = RPCRequest.model_validate(parsed_msg)
                        logger.debug(
                            "[{} :: {}] Running request...",
                            peer_label,
                            rpc_request.u,
                        )
                        response_model = await self.run_rpc(rpc_request)
                    case "fabric-gossip":
                        envelope = FabricGossipEnvelope.model_validate(parsed_msg)
                        if self._fabric_control_plane:
                            from mpreg.fabric.gossip import (
                                GossipMessage as FabricGossipMessage,
                            )

                            message = FabricGossipMessage.from_dict(envelope.payload)
                            if not is_server_connection and message.sender_id:
                                peer_url = message.sender_id
                                peer_node_id = message.sender_id
                                connection.url = peer_url
                                is_server_connection = True
                                self._track_inbound_peer_connection(
                                    peer_url, connection
                                )
                                peer_cluster_id = self.cluster.cluster_id_for_node_url(
                                    peer_url
                                )
                                if (
                                    peer_cluster_id
                                    and peer_cluster_id != self.settings.cluster_id
                                ):
                                    self._register_fabric_graph_peer(peer_cluster_id)
                            await self._fabric_control_plane.gossip.handle_received_message(
                                message
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
                                "[{}] Received consensus proposal from different cluster ID: Expected {}, Got {}",
                                peer_label,
                                self.settings.cluster_id,
                                proposal_msg.cluster_id,
                            )
                            continue  # Ignore consensus from different cluster

                        # Process consensus proposal via consensus manager
                        logger.info(
                            "[{}] Received consensus proposal {} from {}",
                            self.settings.name,
                            proposal_msg.proposal_id,
                            peer_label,
                        )
                        await self.consensus_manager.handle_proposal_message(
                            proposal_msg.model_dump()
                        )
                        logger.info(
                            "[{}] Processed consensus proposal {}",
                            self.settings.name,
                            proposal_msg.proposal_id,
                        )
                        continue

                    case "consensus-vote":
                        # Handle consensus vote messages within cluster
                        vote_msg = ConsensusVoteMessage.model_validate(parsed_msg)
                        # Enforce cluster_id matching for consensus messages
                        if vote_msg.cluster_id != self.settings.cluster_id:
                            logger.warning(
                                "[{}] Received consensus vote from different cluster ID: Expected {}, Got {}",
                                peer_label,
                                self.settings.cluster_id,
                                vote_msg.cluster_id,
                            )
                            continue  # Ignore consensus from different cluster

                        # Process consensus vote via consensus manager
                        logger.info(
                            "[{}] Received consensus vote for {} from {}",
                            self.settings.name,
                            vote_msg.proposal_id,
                            peer_label,
                        )
                        await self.consensus_manager.handle_vote_message(
                            vote_msg.model_dump()
                        )
                        logger.info(
                            "[{}] Processed consensus vote for {}",
                            self.settings.name,
                            vote_msg.proposal_id,
                        )
                        continue

                    case "consensus-vote":
                        # Handle consensus vote messages within cluster
                        vote_msg = ConsensusVoteMessage.model_validate(parsed_msg)
                        # Enforce cluster_id matching for consensus messages
                        if vote_msg.cluster_id != self.settings.cluster_id:
                            logger.warning(
                                "[{}] Received consensus vote from different cluster ID: Expected {}, Got {}",
                                peer_label,
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

                    case "fabric-message":
                        from mpreg.core.model import FabricMessageEnvelope
                        from mpreg.fabric.message_codec import unified_message_from_dict

                        envelope = FabricMessageEnvelope.model_validate(parsed_msg)
                        try:
                            fabric_message = unified_message_from_dict(envelope.payload)
                        except Exception as e:
                            logger.warning(
                                "[{}] Invalid fabric message payload: {}",
                                peer_label,
                                e,
                            )
                            continue
                        if not is_server_connection:
                            sender_id = (
                                fabric_message.headers.routing_path[-1]
                                if fabric_message.headers.routing_path
                                else None
                            )
                            if sender_id:
                                peer_url = sender_id
                                peer_node_id = sender_id
                                connection.url = peer_url
                                is_server_connection = True
                                self._track_inbound_peer_connection(
                                    peer_url, connection
                                )
                                peer_cluster_id = fabric_message.headers.source_cluster
                                if (
                                    peer_cluster_id
                                    and peer_cluster_id != self.settings.cluster_id
                                ):
                                    self._register_fabric_graph_peer(peer_cluster_id)
                        await self._handle_fabric_message(
                            fabric_message,
                            source_peer_url=peer_url if is_server_connection else None,
                        )
                        continue

                    case "pubsub-publish":
                        # Handle message publication
                        publish_start = time.time()
                        publish_success = False
                        try:
                            publish_req = PubSubPublish.model_validate(parsed_msg)
                            notifications = self.topic_exchange.publish_message(
                                publish_req.message
                            )

                            # Send notifications to local subscribers
                            for notification in notifications:
                                await self._send_notification_to_client(notification)
                            try:
                                self._track_background_task(
                                    asyncio.create_task(
                                        self._forward_pubsub_publish(
                                            publish_req.message,
                                            source_peer_url=(
                                                peer_url
                                                if is_server_connection
                                                else None
                                            ),
                                        )
                                    )
                                )
                            except RuntimeError:
                                await self._forward_pubsub_publish(
                                    publish_req.message,
                                    source_peer_url=(
                                        peer_url if is_server_connection else None
                                    ),
                                )

                            # Send acknowledgment
                            response_model = PubSubAck(
                                operation_id=publish_req.u,
                                success=True,
                                u=f"ack_{publish_req.u}",
                            )
                            publish_success = True
                        finally:
                            publish_duration_ms = (time.time() - publish_start) * 1000.0
                            self._metrics_tracker.record_pubsub(
                                publish_duration_ms, publish_success
                            )

                    case "pubsub-subscribe":
                        # Handle subscription request
                        subscribe_start = time.time()
                        subscribe_success = False
                        try:
                            subscribe_req = PubSubSubscribe.model_validate(parsed_msg)
                            self.topic_exchange.add_subscription(
                                subscribe_req.subscription
                            )
                            await self._announce_fabric_subscription(
                                subscribe_req.subscription
                            )

                            # Track which client made this subscription
                            client_id = f"client_{id(transport)}"
                            self.pubsub_clients[client_id] = transport
                            self.subscription_to_client[
                                subscribe_req.subscription.subscription_id
                            ] = client_id

                            # Send acknowledgment
                            response_model = PubSubAck(
                                operation_id=subscribe_req.u,
                                success=True,
                                u=f"ack_{subscribe_req.u}",
                            )
                            subscribe_success = True
                            self._metrics_tracker.record_pubsub_subscription()
                        finally:
                            subscribe_duration_ms = (
                                time.time() - subscribe_start
                            ) * 1000.0
                            self._metrics_tracker.record_pubsub(
                                subscribe_duration_ms, subscribe_success
                            )

                    case "pubsub-unsubscribe":
                        # Handle unsubscription request
                        unsubscribe_start = time.time()
                        unsubscribe_success = False
                        try:
                            unsubscribe_req = PubSubUnsubscribe.model_validate(
                                parsed_msg
                            )
                            success = self.topic_exchange.remove_subscription(
                                unsubscribe_req.subscription_id
                            )
                            if success:
                                await self._remove_fabric_subscription(
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
                            unsubscribe_success = success
                            self._metrics_tracker.record_pubsub_unsubscription()
                        finally:
                            unsubscribe_duration_ms = (
                                time.time() - unsubscribe_start
                            ) * 1000.0
                            self._metrics_tracker.record_pubsub(
                                unsubscribe_duration_ms, unsubscribe_success
                            )

                    # Removed "rpc-announcement" case - federated RPC now uses fabric catalog gossip

                    case _:
                        # Handle unknown message roles.
                        logger.error(
                            "[{}] Invalid RPC request role: {}",
                            peer_label,
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
                    logger.debug(
                        "Sending response: type={}, u={}",
                        type(response_model).__name__,
                        response_model.u,
                    )
                    try:
                        await transport.send(
                            self.serializer.serialize(response_model.model_dump())
                        )
                        logger.debug("Response sent successfully")
                    except Exception:
                        logger.error(
                            "[{}] Client connection error! Dropping reply.",
                            peer_label,
                        )
                if close_connection:
                    await connection.disconnect()
                    break
        finally:
            try:
                await connection.disconnect()
            except Exception as e:
                logger.debug(
                    f"[{self.settings.name}] Error closing inbound connection: {e}"
                )
            try:
                self.clients.remove(connection)
            except KeyError:
                # Connection might have already been removed
                pass

            if is_server_connection and peer_url:
                self._drop_inbound_peer_connection(peer_url, connection)
                self._mark_peer_disconnected(peer_url)
                self._remove_cache_fabric_peer(peer_url)
                if peer_cluster_id and peer_node_id:
                    self.federation_manager.notify_connection_closed(
                        peer_cluster_id, peer_node_id
                    )

            # Clean up PubSub client tracking when client disconnects
            client_id = f"client_{id(transport)}"
            if client_id in self.pubsub_clients:
                del self.pubsub_clients[client_id]
                # Remove all subscriptions for this client
                subs_to_remove = [
                    sid
                    for sid, cid in self.subscription_to_client.items()
                    if cid == client_id
                ]
                for sub_id in subs_to_remove:
                    self.topic_exchange.remove_subscription(sub_id)
                    await self._remove_fabric_subscription(sub_id)
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

        transport = self.pubsub_clients[client_id]

        notification_start = time.time()
        notification_success = False
        try:
            await transport.send(self.serializer.serialize(notification.model_dump()))
            notification_success = True
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
                    self.topic_exchange.remove_subscription(sub_id)
                    await self._remove_fabric_subscription(sub_id)
                    del self.subscription_to_client[sub_id]
        finally:
            self._metrics_tracker.record_pubsub_notification()
            notification_duration_ms = (time.time() - notification_start) * 1000.0
            self._metrics_tracker.record_pubsub(
                notification_duration_ms, notification_success
            )

    async def _establish_peer_connection(
        self,
        peer_url: str,
        *,
        fast_connect: bool = False,
        dial_url: str | None = None,
    ) -> None:
        """Establishes a persistent connection to a peer for RPC forwarding.

        Args:
            peer_url: The URL of the peer to connect to.
        """
        # Do not establish new connections during shutdown
        if self._shutdown_event.is_set():
            logger.debug(
                f"[{self.settings.name}] Skipping peer connection to {peer_url} - shutdown in progress"
            )
            return
        lock = self._peer_connection_locks.get(peer_url)
        if lock is None:
            lock = asyncio.Lock()
            self._peer_connection_locks[peer_url] = lock
        async with lock:
            if self._shutdown_event.is_set():
                return
            if peer_url in self.peer_connections:
                # Check if existing connection is still valid
                if self.peer_connections[peer_url].is_connected:
                    logger.debug(
                        "[{}] Already connected to peer: {}",
                        self.settings.name,
                        peer_url,
                    )
                    return
                else:
                    # Clean up dead connection
                    # Publish connection lost event
                    event = ConnectionEvent.lost(peer_url, self.cluster.local_url)
                    self.cluster.connection_event_bus.publish(event)

                    self._remove_cache_fabric_peer(peer_url)

                    del self.peer_connections[peer_url]

            target_url = dial_url or peer_url
            if target_url != peer_url:
                logger.debug(
                    "[{}] Establishing persistent connection to peer: {} (dial={})",
                    self.settings.name,
                    peer_url,
                    target_url,
                )
            else:
                logger.debug(
                    "[{}] Establishing persistent connection to peer: {}",
                    self.settings.name,
                    peer_url,
                )
            try:
                # Create persistent connection for RPC forwarding
                # Adjust connection parameters based on cluster size for better scalability
                cluster_size = (
                    len(self._peer_directory.nodes()) + 1 if self._peer_directory else 1
                )
                if fast_connect:
                    connection = Connection(
                        url=target_url,
                        max_retries=1,
                        base_delay=0.1,
                        open_timeout=0.5,
                        connect_timeout=0.8,
                    )
                elif cluster_size >= 50:
                    # For very large clusters: fewer retries, longer delays to reduce connection storms
                    connection = Connection(
                        url=target_url, max_retries=3, base_delay=2.0
                    )
                elif cluster_size >= 20:
                    # For medium clusters: moderate parameters
                    connection = Connection(
                        url=target_url, max_retries=4, base_delay=1.5
                    )
                else:
                    # For small clusters: default aggressive parameters
                    connection = Connection(url=target_url)

                await connection.connect()
                self.peer_connections[peer_url] = connection

                # Publish connection established event
                logger.debug(
                    "Publishing connection established event for {}",
                    peer_url,
                )
                event = ConnectionEvent.established(peer_url, self.cluster.local_url)
                self.cluster.connection_event_bus.publish(event)
                logger.debug("Connection event published for {}", peer_url)

                self._register_cache_fabric_peer(peer_url)

                await self._send_status_to_connection(connection)
                self._schedule_catalog_snapshot(peer_url)

                # Start message reading task for peer connection.
                # This ensures we can receive STATUS/GOODBYE and fabric messages.
                self._track_background_task(
                    asyncio.create_task(
                        self._handle_peer_connection_messages(connection, peer_url)
                    )
                )

                logger.debug(
                    "[{}] Successfully connected to peer: {}",
                    self.settings.name,
                    peer_url,
                )
            except Exception as e:
                logger.error(
                    "[{}] Failed to connect to peer {}: {}",
                    self.settings.name,
                    peer_url,
                    e,
                )

    async def _bootstrap_seed_peers(self) -> None:
        """Quickly retry seed peers during startup to avoid missing early status."""
        seed_peers = set(self.settings.peers or [])
        if self.settings.connect:
            seed_peers.add(self.settings.connect)
        if not seed_peers:
            return

        deadline = time.time() + max(2.5, self.settings.gossip_interval)
        while time.time() < deadline and not self._shutdown_event.is_set():
            pending = [
                peer_url
                for peer_url in seed_peers
                if peer_url not in self.peer_connections
                or not self.peer_connections[peer_url].is_connected
            ]
            if not pending:
                return
            for peer_url in pending:
                await self._establish_peer_connection(peer_url, fast_connect=True)
            await asyncio.sleep(0.2)

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
            logger.debug(
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
                            logger.debug(
                                f"🔄 SKIP: RPC response #{self._msg_stats.rpc_responses_skipped} - Type: {parsed_msg.get('r')} - PREVENTING INFINITE LOOP"
                            )
                        # Log summary every 50 messages
                        if self._msg_stats.total_processed % 50 == 0:
                            logger.debug(
                                f"📊 MESSAGE STATS: Total={self._msg_stats.total_processed}, RPC_Skipped={self._msg_stats.rpc_responses_skipped}, Servers={self._msg_stats.server_messages}, Others={self._msg_stats.other_messages}"
                            )
                        continue

                    logger.debug(f"📥 Received message from peer {peer_url}: {msg!r}")

                    # Process the message using the same logic as the opened() method
                    if parsed_msg.get("role") == "fabric-message":
                        from mpreg.core.model import FabricMessageEnvelope
                        from mpreg.fabric.message_codec import unified_message_from_dict

                        envelope = FabricMessageEnvelope.model_validate(parsed_msg)
                        try:
                            fabric_message = unified_message_from_dict(envelope.payload)
                        except Exception as e:
                            logger.warning(
                                "Invalid fabric message from {}: {}",
                                peer_url,
                                e,
                            )
                            continue
                        await self._handle_fabric_message(
                            fabric_message, source_peer_url=peer_url
                        )
                        continue
                    if parsed_msg.get("role") == "fabric-gossip":
                        envelope = FabricGossipEnvelope.model_validate(parsed_msg)
                        if self._fabric_control_plane:
                            from mpreg.fabric.gossip import (
                                GossipMessage as FabricGossipMessage,
                            )

                            message = FabricGossipMessage.from_dict(envelope.payload)
                            await self._fabric_control_plane.gossip.handle_received_message(
                                message
                            )
                        continue

                    if parsed_msg.get("role") == "server":
                        self._msg_stats.server_messages += 1
                        # Convert to proper Pydantic model
                        server_request = RPCServerRequest.model_validate(parsed_msg)
                        logger.debug(
                            "Processing server message from peer {}: {}",
                            peer_url,
                            server_request.server.what,
                        )

                        if server_request.server.what == "GOODBYE":
                            # Process GOODBYE message - remove peer from cluster
                            logger.info(
                                "Processing GOODBYE from {} (reason: {})",
                                peer_url,
                                server_request.server.reason.value,
                            )

                            await self._handle_goodbye_message(
                                server_request.server, peer_url
                            )

                            logger.info(
                                "Successfully processed GOODBYE from {}",
                                peer_url,
                            )
                        elif server_request.server.what == "STATUS":
                            status = server_request.server
                            accepted = self._handle_remote_status(
                                status, connection=connection
                            )
                            if accepted:
                                logger.debug(
                                    "Successfully processed STATUS from {}",
                                    peer_url,
                                )
                            else:
                                logger.debug(
                                    "Ignoring STATUS from departed peer {}",
                                    peer_url,
                                )
                                break

                except TimeoutError:
                    # Timeout is normal - just continue
                    continue
                except Exception as e:
                    logger.warning(
                        "Error processing message from peer {}: {}",
                        peer_url,
                        e,
                    )
                    break

        except asyncio.CancelledError:
            logger.debug(
                "Peer connection message handler cancelled for {} (clean shutdown)",
                peer_url,
            )
            # Re-raise to properly handle cancellation
            raise
        except Exception as e:
            logger.error(
                "Fatal error in peer connection message handler for {}: {}",
                peer_url,
                e,
            )
        finally:
            logger.debug(
                "Message reading loop ended for peer connection: {}",
                peer_url,
            )
            try:
                if connection.is_connected:
                    await connection.disconnect()
            except Exception as e:
                logger.debug(
                    f"[{self.settings.name}] Error closing peer connection {peer_url}: {e}"
                )
            if self.peer_connections.get(peer_url) is connection:
                del self.peer_connections[peer_url]
                self._remove_cache_fabric_peer(peer_url)
                self._mark_peer_disconnected(peer_url)

    async def _handle_goodbye_message(
        self,
        goodbye: RPCServerGoodbye,
        sender_url: str,
        *,
        record_departure: bool = True,
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
        if record_departure:
            self._record_and_apply_departure(goodbye)
        self._remove_fabric_graph_peer(goodbye.cluster_id)
        self._remove_cache_fabric_peer(goodbye.departing_node_url)
        await self._close_peer_connection(goodbye.departing_node_url)
        if sender_url != goodbye.departing_node_url:
            await self._close_peer_connection(sender_url)

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
            instance_id=self._instance_id,
        )

        goodbye_request = RPCServerRequest(server=goodbye, u=str(ulid.new()))

        all_connections = self._get_all_peer_connections()
        peer_urls = set(all_connections.keys())
        if self._peer_directory:
            for node in self._peer_directory.nodes():
                peer_urls.add(node.node_id)
        peer_urls.discard(self.cluster.local_url)
        logger.info(
            f"[{self.settings.name}] Sending GOODBYE to {len(peer_urls)} peers "
            f"(reason: {reason.value})"
        )

        # Broadcast to all connected peers
        message_bytes = self.serializer.serialize(goodbye_request.model_dump())
        successful_sends = 0

        for peer_url in sorted(peer_urls):
            connection = all_connections.get(peer_url)
            if connection is None or not connection.is_connected:
                try:
                    temp_connection = Connection(
                        url=peer_url,
                        max_retries=1,
                        base_delay=0.1,
                        open_timeout=0.5,
                        connect_timeout=0.8,
                    )
                    await temp_connection.connect()
                    await temp_connection.send(message_bytes)
                    await temp_connection.disconnect()
                    successful_sends += 1
                    logger.debug(
                        f"[{self.settings.name}] Sent GOODBYE to {peer_url} (temp)"
                    )
                except Exception as e:
                    logger.warning(
                        f"[{self.settings.name}] Failed to send GOODBYE to {peer_url}: {e}"
                    )
                continue
            try:
                await connection.send(message_bytes)
                successful_sends += 1
                logger.debug(f"[{self.settings.name}] Sent GOODBYE to {peer_url}")
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Failed to send GOODBYE to {peer_url}: {e}"
                )

        logger.info(
            f"[{self.settings.name}] Successfully sent GOODBYE to {successful_sends}/{len(peer_urls)} peers"
        )

        if self._fabric_control_plane:
            from mpreg.fabric.catalog import NodeKey
            from mpreg.fabric.catalog_delta import RoutingCatalogDelta

            timestamp = time.time()
            removals = []
            catalog = self._fabric_control_plane.index.catalog
            for endpoint in catalog.functions.entries():
                if endpoint.node_id == self.cluster.local_url:
                    removals.append(endpoint.key())
            delta = RoutingCatalogDelta(
                update_id=str(ulid.new()),
                cluster_id=self.settings.cluster_id,
                sent_at=timestamp,
                function_removals=tuple(removals),
                node_removals=(
                    NodeKey(
                        cluster_id=self.settings.cluster_id,
                        node_id=self.cluster.local_url,
                    ),
                ),
            )
            await self._fabric_control_plane.broadcaster.broadcast(delta, now=timestamp)

    async def _manage_peer_connections(self) -> None:
        """Periodically checks for new peers and maintains connections.

        This background task ensures that the server maintains persistent
        connections to all known peers in the cluster.
        """
        while not self._shutdown_event.is_set():
            self._prune_departed_peers()
            seed_peers = set(self.settings.peers or [])
            if self.settings.connect:
                seed_peers.add(self.settings.connect)
            for peer_url in seed_peers:
                if self._shutdown_event.is_set():
                    return
                if self._is_peer_departed(peer_url, None):
                    continue
                await self._establish_peer_connection(peer_url, fast_connect=True)

            if self._peer_directory:
                for node in self._iter_peer_nodes_for_connection():
                    if self._shutdown_event.is_set():
                        logger.info(
                            f"[{self.settings.name}] Stopping peer connection loop - shutdown in progress"
                        )
                        return
                    peer_url = node.node_id
                    if peer_url == self.cluster.local_url:
                        continue
                    if self._is_peer_departed(
                        peer_url, self._peer_instance_ids.get(peer_url)
                    ):
                        continue
                    auth_result = self.federation_manager.federation_manager.config.is_cross_federation_connection_allowed(
                        node.cluster_id
                    )
                    if (
                        node.cluster_id == self.settings.cluster_id
                        or not auth_result.name.startswith("REJECTED")
                    ):
                        dial_url = self._select_peer_dial_url(node)
                        await self._establish_peer_connection(
                            peer_url, dial_url=dial_url
                        )
                        if node.cluster_id != self.settings.cluster_id:
                            logger.debug(
                                "[{}] Establishing cross-federation connection to peer {} (cluster: {})",
                                self.settings.name,
                                peer_url,
                                node.cluster_id,
                            )
                    else:
                        logger.debug(
                            "[{}] Skipping peer {} due to federation policy (cluster: {}, mode: {})",
                            self.settings.name,
                            peer_url,
                            node.cluster_id,
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

                    self._mark_peer_disconnected(url)
                    self._remove_cache_fabric_peer(url)

                    del self.peer_connections[url]

            await self._broadcast_status()

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

    def _build_cache_metrics_and_profile(
        self, *, now: float | None = None
    ) -> tuple[CacheStatusMetrics | None, CacheNodeProfile | None]:
        if self._cache_manager is None:
            return None, None
        stats = self._cache_manager.get_statistics()
        l1_stats = stats.get("l1_statistics", {})
        memory_bytes = float(l1_stats.get("memory_bytes", 0.0))
        capacity_bytes = max(self.settings.cache_capacity_mb, 1) * 1024 * 1024
        utilization = min((memory_bytes / capacity_bytes) * 100.0, 100.0)
        perf_stats = stats.get("performance_metrics", {})
        latencies = [
            level_stats.get("avg_time_ms", 0.0)
            for level_stats in perf_stats.values()
            if isinstance(level_stats, dict)
        ]
        average_latency_ms = sum(latencies) / len(latencies) if latencies else 0.0
        cache_metrics = CacheStatusMetrics(
            cache_region=self.settings.cache_region,
            cache_latitude=self.settings.cache_latitude,
            cache_longitude=self.settings.cache_longitude,
            cache_capacity_mb=self.settings.cache_capacity_mb,
            cache_utilization_percent=utilization,
            cache_avg_latency_ms=average_latency_ms,
            cache_reliability_score=1.0,
        )
        from mpreg.fabric.catalog import CacheNodeProfile

        profile = CacheNodeProfile(
            node_id=self.cluster.local_url,
            cluster_id=self.settings.cluster_id,
            region=self.settings.cache_region,
            coordinates=GeographicCoordinate(
                latitude=self.settings.cache_latitude,
                longitude=self.settings.cache_longitude,
            ),
            capacity_mb=self.settings.cache_capacity_mb,
            utilization_percent=utilization,
            avg_latency_ms=average_latency_ms,
            reliability_score=1.0,
            advertised_at=now if now is not None else time.time(),
            ttl_seconds=self.settings.fabric_catalog_ttl_seconds,
        )
        return cache_metrics, profile

    def _build_status_message(
        self,
        *,
        cache_metrics: CacheStatusMetrics | None = None,
        queue_metrics: QueueStatusMetrics | None = None,
    ) -> RPCServerStatus:
        """Build a status message with current server state."""
        functions = tuple(sorted(self.registry.keys()))
        resources = frozenset(self.settings.resources or set())
        if cache_metrics is None:
            cache_metrics, _ = self._build_cache_metrics_and_profile()
        if queue_metrics is None and self._queue_manager is not None:
            queue_stats = self._queue_manager.get_global_statistics()
            queue_metrics = QueueStatusMetrics(
                total_queues=queue_stats.total_queues,
                total_messages_sent=queue_stats.total_messages_sent,
                total_messages_received=queue_stats.total_messages_received,
                total_messages_acknowledged=queue_stats.total_messages_acknowledged,
                total_messages_failed=queue_stats.total_messages_failed,
                active_subscriptions=queue_stats.active_subscriptions,
                success_rate=queue_stats.overall_success_rate(),
            )
        status_metrics = ServerStatusMetrics(
            messages_processed=self._msg_stats.total_processed,
            rpc_responses_skipped=self._msg_stats.rpc_responses_skipped,
            server_messages=self._msg_stats.server_messages,
            other_messages=self._msg_stats.other_messages,
            cache_metrics=cache_metrics,
            queue_metrics=queue_metrics,
            route_metrics=(
                self._fabric_control_plane.route_table.metrics_snapshot()
                if self._fabric_control_plane
                else None
            ),
        )
        return RPCServerStatus(
            server_url=self.cluster.local_url,
            cluster_id=self.settings.cluster_id,
            instance_id=self._instance_id,
            status="ok",
            active_clients=len(self.clients),
            peer_count=len(self._peer_directory.nodes()) if self._peer_directory else 0,
            funs=functions,
            locs=resources,
            function_catalog=self._current_function_catalog(),
            advertised_urls=tuple(
                self.settings.advertised_urls
                or [f"ws://{self.settings.host}:{self.settings.port}"]
            ),
            metrics=status_metrics.to_dict(),
        )

    async def _broadcast_status(self) -> None:
        """Broadcast a status update to connected peers."""
        active_connections = [
            (url, conn)
            for url, conn in self.peer_connections.items()
            if conn.is_connected
        ]
        cache_metrics, cache_profile = self._build_cache_metrics_and_profile(
            now=time.time()
        )
        if cache_profile is not None and self._fabric_control_plane is not None:
            from mpreg.fabric.adapters.cache_profile import (
                CacheProfileCatalogAdapter,
            )

            adapter = CacheProfileCatalogAdapter(
                node_id=cache_profile.node_id,
                cluster_id=cache_profile.cluster_id,
                region=cache_profile.region,
                coordinates=cache_profile.coordinates,
                capacity_mb=cache_profile.capacity_mb,
                utilization_percent=cache_profile.utilization_percent,
                avg_latency_ms=cache_profile.avg_latency_ms,
                reliability_score=cache_profile.reliability_score,
                ttl_seconds=cache_profile.ttl_seconds,
            )
            announcer = self._fabric_control_plane.cache_profile_announcer(
                cache_profile_adapter=adapter
            )
            await announcer.announce(now=cache_profile.advertised_at)

        if not active_connections:
            return

        status_message = RPCServerRequest(
            server=self._build_status_message(cache_metrics=cache_metrics),
            u=str(ulid.new()),
        )
        status_data = self.serializer.serialize(status_message.model_dump())

        for peer_url, connection in active_connections:
            try:
                await connection.send(status_data)
                logger.debug(
                    "Sent STATUS to {}",
                    peer_url,
                )
            except Exception as e:
                logger.warning(
                    "Failed to send STATUS to {}: {}",
                    peer_url,
                    e,
                )

    async def _send_status_to_connection(self, connection: Connection) -> None:
        """Send an immediate STATUS update to a newly connected peer."""
        status_message = RPCServerRequest(
            server=self._build_status_message(),
            u=str(ulid.new()),
        )
        status_data = self.serializer.serialize(status_message.model_dump())
        try:
            await connection.send(status_data)
        except Exception as e:
            logger.warning(
                "Failed to send STATUS to {}: {}",
                connection.url,
                e,
            )

    async def _start_monitoring_services(self) -> None:
        """Start unified monitoring and HTTP endpoints if enabled."""
        if not self.settings.monitoring_enabled:
            return

        if self.settings.monitoring_port in (None, 0):
            from .core.port_allocator import allocate_port

            monitoring_port = allocate_port("monitoring")
            self.settings.monitoring_port = monitoring_port
            self._auto_allocated_monitoring_port = monitoring_port
            self._invoke_port_callback(
                self.settings.on_monitoring_port_assigned,
                monitoring_port,
                label="monitoring",
            )
        else:
            monitoring_port = self.settings.monitoring_port
        monitoring_host = self.settings.monitoring_host or self.settings.host

        self._unified_monitor = create_unified_system_monitor()
        self._unified_monitor.rpc_monitor = ServerSystemMonitor(
            system_type=SystemType.RPC,
            system_name=self.settings.name,
            tracker=self._metrics_tracker,
            active_connections_provider=lambda: len(self.clients),
        )
        self._unified_monitor.pubsub_monitor = ServerSystemMonitor(
            system_type=SystemType.PUBSUB,
            system_name=self.settings.name,
            tracker=self._metrics_tracker,
            active_connections_provider=lambda: len(self.pubsub_clients),
        )
        self._unified_monitor.federation_monitor = FederationSystemMonitor(
            federation_manager=self.federation_manager,
            system_name=self.settings.name,
        )

        if self._cache_manager is not None:
            from .core.monitoring.system_adapters import CacheSystemMonitor

            self._unified_monitor.cache_monitor = CacheSystemMonitor(
                cache_manager=self._cache_manager,
                system_name=self.settings.name,
            )
        if self._queue_manager is not None:
            from .core.monitoring.system_adapters import QueueSystemMonitor

            self._unified_monitor.queue_monitor = QueueSystemMonitor(
                queue_manager=self._queue_manager,
                system_name=self.settings.name,
            )

        await self._unified_monitor.start()

        from .core.transport.adapter_registry import get_adapter_endpoint_registry
        from .fabric.monitoring_endpoints import create_federation_monitoring_system

        route_trace_provider = None
        link_state_status_provider = None
        if self._fabric_control_plane is not None:
            from mpreg.fabric.route_control import RouteDestination

            route_table = self._fabric_control_plane.route_table
            link_state_table = self._fabric_control_plane.link_state_table
            link_state_processor = self._fabric_control_plane.link_state_processor

            def _route_trace(
                destination: str, avoid: tuple[str, ...]
            ) -> dict[str, Any]:
                trace = route_table.explain_selection(
                    RouteDestination(cluster_id=destination),
                    avoid_clusters=avoid,
                )
                payload = trace.to_dict()
                if link_state_table is not None:
                    stats = link_state_table.stats.to_dict()
                    payload["link_state_hint"] = {
                        "mode": self.settings.fabric_link_state_mode.value,
                        "area": self.settings.fabric_link_state_area,
                        "allowed_areas": (
                            sorted(link_state_processor.allowed_areas)
                            if link_state_processor
                            and link_state_processor.allowed_areas
                            else None
                        ),
                        "area_mismatch_rejects": stats.get("updates_filtered", 0),
                    }
                return payload

            route_trace_provider = _route_trace
            if link_state_table is not None:

                def _link_state_status() -> dict[str, Any]:
                    stats = link_state_table.stats.to_dict()
                    area_policy = self.settings.fabric_link_state_area_policy
                    summary_filters: list[dict[str, Any]] = []
                    area_hierarchy: dict[str, str] = {}
                    if area_policy:
                        area_hierarchy = {
                            key: value
                            for key, value in area_policy.area_hierarchy.items()
                        }
                        for pair, summary in area_policy.summary_filters.items():
                            summary_filters.append(
                                {
                                    "source_area": pair.source_area,
                                    "target_area": pair.target_area,
                                    "allowed_neighbors": (
                                        sorted(summary.allowed_neighbors)
                                        if summary.allowed_neighbors is not None
                                        else None
                                    ),
                                    "denied_neighbors": (
                                        sorted(summary.denied_neighbors)
                                        if summary.denied_neighbors is not None
                                        else None
                                    ),
                                }
                            )
                    entry_areas = sorted(
                        {
                            entry.area
                            for entry in link_state_table.entries.values()
                            if entry.area is not None
                        }
                    )
                    return {
                        "mode": self.settings.fabric_link_state_mode.value,
                        "area": self.settings.fabric_link_state_area,
                        "area_hierarchy": area_hierarchy,
                        "summary_filters": summary_filters,
                        "allowed_areas": (
                            sorted(link_state_processor.allowed_areas)
                            if link_state_processor
                            and link_state_processor.allowed_areas
                            else None
                        ),
                        "entries": len(link_state_table.entries),
                        "entry_areas": entry_areas,
                        "area_mismatch_rejects": stats.get("updates_filtered", 0),
                        "stats": stats,
                    }

                link_state_status_provider = _link_state_status

        self._monitoring_system = create_federation_monitoring_system(
            settings=self.settings,
            federation_config=self.settings.federation_config,
            federation_manager=self.federation_manager,
            unified_monitor=self._unified_monitor,
            monitoring_port=monitoring_port,
            monitoring_host=monitoring_host,
            enable_cors=self.settings.monitoring_enable_cors,
            route_trace_provider=route_trace_provider,
            link_state_status_provider=link_state_status_provider,
            adapter_endpoint_registry=get_adapter_endpoint_registry(),
            persistence_snapshot_provider=self._persistence_snapshot_metrics,
        )
        try:
            await self._monitoring_system.start()
        except OSError as e:
            logger.warning(
                f"[{self.settings.name}] Monitoring port {monitoring_port} unavailable: {e}. Falling back to an ephemeral port."
            )
            self._monitoring_system.monitoring_port = 0
            await self._monitoring_system.start()

        effective_port = self._monitoring_system.monitoring_port
        logger.debug(
            f"[{self.settings.name}] Monitoring endpoints available at http://{monitoring_host}:{effective_port}"
        )

    async def _persistence_snapshot_metrics(self) -> dict[str, Any]:
        config = self.settings.persistence_config
        if config is None:
            return {"enabled": False}

        payload: dict[str, Any] = {
            "enabled": True,
            "mode": config.mode.value,
            "data_dir": str(config.data_dir),
        }
        if config.mode is PersistenceMode.SQLITE:
            payload["sqlite_path"] = str(config.sqlite_path())

        if self._persistence_registry is not None:
            payload["registry_open"] = getattr(
                self._persistence_registry, "_opened", False
            )

        catalog_counts: dict[str, int] = {}
        route_key_info: dict[str, Any] = {}
        if self._fabric_control_plane is not None:
            catalog = self._fabric_control_plane.catalog
            catalog_counts = {
                "functions": catalog.functions.entry_count(),
                "topics": catalog.topics.entry_count(),
                "queues": catalog.queues.entry_count(),
                "caches": catalog.caches.entry_count(),
                "cache_profiles": catalog.cache_profiles.entry_count(),
                "nodes": catalog.nodes.entry_count(),
            }
            if self._fabric_control_plane.route_key_registry is not None:
                registry = self._fabric_control_plane.route_key_registry
                registry.purge_expired()
                route_key_info = {
                    "clusters": len(registry.key_sets),
                    "active_keys": sum(
                        len(key_set.keys) for key_set in registry.key_sets.values()
                    ),
                }

        payload["fabric"] = {
            "catalog_entries": catalog_counts,
            "route_keys": route_key_info,
            "snapshot_last_saved_at": self._fabric_snapshot_last_saved_at,
            "snapshot_last_restored_at": self._fabric_snapshot_last_restored_at,
            "snapshot_saved_counts": self._fabric_snapshot_last_saved_counts,
            "snapshot_restored_counts": self._fabric_snapshot_last_restored_counts,
            "snapshot_saved_route_keys": self._fabric_snapshot_last_route_keys_saved,
            "snapshot_restored_route_keys": self._fabric_snapshot_last_route_keys_restored,
        }
        return payload

    def attach_cache_manager(self, cache_manager: Any) -> None:
        """Attach a cache manager to the monitoring system."""
        self._cache_manager = cache_manager
        if self._unified_monitor is not None:
            from .core.monitoring.system_adapters import CacheSystemMonitor

            self._unified_monitor.cache_monitor = CacheSystemMonitor(
                cache_manager=cache_manager,
                system_name=self.settings.name,
            )

    def attach_queue_manager(self, queue_manager: Any) -> None:
        """Attach a queue manager to the monitoring system."""
        self._queue_manager = queue_manager
        self._initialize_fabric_queue_federation()
        if self._unified_monitor is not None:
            from .core.monitoring.system_adapters import QueueSystemMonitor

            self._unified_monitor.queue_monitor = QueueSystemMonitor(
                queue_manager=queue_manager,
                system_name=self.settings.name,
            )

    async def _stop_monitoring_services(self) -> None:
        """Stop monitoring services cleanly."""
        if self._monitoring_system is not None:
            await self._monitoring_system.stop()
            self._monitoring_system = None

        if self._unified_monitor is not None:
            await self._unified_monitor.stop()
            self._unified_monitor = None

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
        logger.debug(
            "[{}] Broadcasting consensus proposal {}",
            self.settings.name,
            proposal_data.get("proposal_id", "UNKNOWN"),
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
            "[{}] Broadcasting consensus proposal {} to {} peers",
            self.settings.name,
            proposal_data["proposal_id"],
            len(active_connections),
        )

        # Serialize and send to all cluster peers concurrently
        proposal_data_bytes = self.serializer.serialize(proposal_message.model_dump())

        async def send_to_peer(peer_url: str, connection) -> None:
            try:
                await connection.send(proposal_data_bytes)
                logger.info(
                    "Sent consensus proposal to {}",
                    peer_url,
                )
            except Exception as e:
                logger.warning(
                    "Failed to send consensus proposal to {}: {}",
                    peer_url,
                    e,
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
        logger.debug(
            "[{}] Broadcasting consensus vote for proposal {} (vote={}, voter={})",
            self.settings.name,
            proposal_id,
            vote,
            voter_id,
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
            "[{}] Broadcasting consensus vote for {} to {} peers",
            self.settings.name,
            proposal_id,
            len(active_connections),
        )

        # Serialize and send to all cluster peers concurrently
        vote_data_bytes = self.serializer.serialize(vote_message.model_dump())

        async def send_vote_to_peer(peer_url: str, connection) -> None:
            try:
                await connection.send(vote_data_bytes)
                logger.info(
                    "Sent consensus vote to {}",
                    peer_url,
                )
            except Exception as e:
                logger.warning(
                    "Failed to send consensus vote to {}: {}",
                    peer_url,
                    e,
                )

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
        # Clear existing known nodes
        self.consensus_manager.known_nodes.clear()

        # Add all known peers (including self)
        if self._peer_directory:
            for node in self._peer_directory.nodes():
                self.consensus_manager.known_nodes.add(node.node_id)
        self.consensus_manager.known_nodes.add(self.cluster.local_url)

        logger.debug(
            f"Updated consensus known_nodes with {len(self.consensus_manager.known_nodes)} peers"
        )

    def _consensus_node_count(self) -> int:
        """Estimate consensus node count from catalog or active connections."""
        directory = self._peer_directory
        if directory:
            node_ids = {node.node_id for node in directory.nodes()}
            node_ids.add(self.cluster.local_url)
            return max(1, len(node_ids))

        connected_peers = [
            peer_id
            for peer_id, conn in self._get_all_peer_connections().items()
            if conn.is_connected
        ]
        return max(1, 1 + len(connected_peers))

    def register_command(
        self,
        name: str,
        func: Callable[..., Any],
        resources: Iterable[str],
        *,
        function_id: str | None = None,
        version: str = "1.0.0",
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
        resolved_function_id = function_id or name
        parsed_version = SemanticVersion.parse(version)
        if name in self.registry:
            import traceback

            stack_trace = traceback.format_stack()
            caller = stack_trace[-2].strip()

            logger.error(
                "Duplicate function registration detected on {}",
                self.settings.name,
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

        logger.debug(
            "[{}] Registering function '{}' with resources {}",
            self.settings.name,
            name,
            list(resources),
        )
        identity = FunctionIdentity(
            name=name,
            function_id=resolved_function_id,
            version=parsed_version,
        )
        if self._fabric_control_plane:
            self._fabric_control_plane.catalog.functions.validate_identity(identity)
        registration = None
        if self._local_function_registry:
            self._local_function_registry.validate_identity(identity)
            registration = self._local_function_registry.register(
                identity, frozenset(resources)
            )
        self.registry.register(Command(name, func))

        if registration is not None:
            self._publish_fabric_function_update(registration)
        # Catalog delta already published for this registration.

    async def server(self) -> None:
        """Starts the MPREG server and handles incoming and outgoing connections.

        This method sets up the transport listener, registers default commands,
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

        transport_config = TransportConfig(
            protocol_options={"max_message_size": MPREG_DATA_MAX}
        )
        self._transport_listener = TransportFactory.create_listener(
            "ws",
            self.settings.host,
            self.settings.port,
            transport_config,
        )
        await self._transport_listener.start()
        self._track_background_task(asyncio.create_task(self._accept_connections()))
        # Trigger initial connections to configured peers immediately.
        if self.settings.peers:
            for peer_url in self.settings.peers:
                await self._establish_peer_connection(peer_url, fast_connect=True)
        if self.settings.connect:
            await self._establish_peer_connection(
                self.settings.connect, fast_connect=True
            )

        # Start a background task to manage peer connections based on the catalog.
        # Use the proper task management system for consistent cleanup
        self._track_background_task(
            asyncio.create_task(self._manage_peer_connections())
        )

        self._track_background_task(asyncio.create_task(self._bootstrap_seed_peers()))

        await self._start_monitoring_services()

        # Start consensus manager for intra-cluster consensus
        await self.consensus_manager.start()

        # Populate consensus manager with known cluster peers
        self._update_consensus_known_nodes()

        # Initialize optional cache/queue systems with live event loop
        await self._initialize_default_systems()
        await self._start_fabric_control_plane()

        # Initialize federated RPC system
        # Federated RPC now uses enhanced MPREG infrastructure - no separate initialization needed
        logger.info("Enhanced federated RPC ready via MPREG message bus")

        try:
            # If no external connection requested, wait for shutdown signal
            # to keep the server alive.
            if not self.settings.peers and not self.settings.connect:
                await self._shutdown_event.wait()
                return

            # Keep the server running until shutdown signal.
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            # Ensure background tasks are cancelled even if the server task is cancelled.
            try:
                await asyncio.shield(self.shutdown_async())
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error during cancelled server shutdown: {e}"
                )
            raise
        finally:
            # Clean up WebSocket server
            if self._transport_listener:
                await self._transport_listener.stop()
            await self._stop_monitoring_services()
            try:
                await self.consensus_manager.stop()
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Error stopping consensus manager in server shutdown: {e}"
                )

    def start(self) -> None:
        try:
            asyncio.run(self.server())
        except KeyboardInterrupt:
            logger.warning("Thanks for playing!")

    async def _accept_connections(self) -> None:
        listener = self._transport_listener
        if not listener:
            return
        while not self._shutdown_event.is_set():
            try:
                transport = await listener.accept()
            except Exception as exc:
                if self._shutdown_event.is_set():
                    return
                logger.warning(
                    "[{}] Transport accept failed: {}",
                    self.settings.name,
                    exc,
                )
                await asyncio.sleep(0.05)
                continue
            self._track_background_task(asyncio.create_task(self.opened(transport)))


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
