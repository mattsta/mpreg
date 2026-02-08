from __future__ import annotations

import asyncio
import contextlib
import hashlib
import inspect
import os
import pprint as pp
import re
import sys
import time
import traceback
from collections.abc import Callable, Coroutine, Iterable
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from graphlib import TopologicalSorter
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import urlparse

import ulid
from loguru import logger

LOCAL_ONLY_RPC_COMMANDS: frozenset[str] = frozenset(
    {
        "catalog_query",
        "catalog_watch",
        "cluster_map",
        "cluster_map_v2",
        "dns_describe",
        "dns_list",
        "dns_register",
        "dns_unregister",
        "discovery_access_audit",
        "list_peers",
        "namespace_policy_apply",
        "namespace_policy_audit",
        "namespace_policy_export",
        "namespace_policy_validate",
        "namespace_status",
        "resolver_cache_stats",
        "resolver_resync",
        "rpc_describe",
        "rpc_describe_local",
        "rpc_list",
        "rpc_report",
        "summary_query",
        "summary_watch",
    }
)

_DIAG_TRUE_VALUES = frozenset({"1", "true", "yes", "on", "enabled", "debug"})
CATALOG_SNAPSHOT_DIAG_ENABLED = (
    os.environ.get("MPREG_DEBUG_CATALOG_SNAPSHOT", "").strip().lower()
    in _DIAG_TRUE_VALUES
)
DISCOVERY_SUMMARY_DIAG_ENABLED = (
    os.environ.get("MPREG_DEBUG_DISCOVERY_SUMMARY", "").strip().lower()
    in _DIAG_TRUE_VALUES
)
PEER_SNAPSHOT_DIAG_ENABLED = (
    os.environ.get("MPREG_DEBUG_PEER_SNAPSHOT", "").strip().lower() in _DIAG_TRUE_VALUES
)


@dataclass(frozen=True, slots=True)
class InternalDiscoverySubscriptionAnnouncer:
    schedule: Callable[[Coroutine[Any, Any, None]], None]
    announce: Callable[[], Coroutine[Any, Any, None]]

    def on_connection_established(self, event: ConnectionEvent) -> None:
        self.schedule(self.announce())

    def on_connection_lost(self, event: ConnectionEvent) -> None:
        return


from .core.cluster_map import (
    CatalogQueryRequest,
    CatalogQueryResponse,
    CatalogWatchRequest,
    CatalogWatchResponse,
    ClusterMapRequest,
    ClusterMapResponse,
    ClusterMapSnapshot,
    ClusterNodeSnapshot,
    ListPeersRequest,
    NodeLoadMetrics,
    PeerSnapshot,
    paginate_items,
)
from .core.config import MPREGSettings
from .core.connection import Connection
from .core.connection_events import ConnectionEvent, ConnectionEventBus
from .core.discovery_events import (
    DISCOVERY_DELTA_TOPIC,
    CatalogDeltaCounts,
    DiscoveryDeltaMessage,
)
from .core.discovery_monitoring import (
    DiscoveryAccessAuditEntry,
    DiscoveryAccessAuditLog,
    DiscoveryAccessAuditRequest,
    DiscoveryAccessAuditResponse,
    DiscoveryLagStatus,
    DiscoveryPolicyStatus,
)
from .core.discovery_rate_limit import (
    DiscoveryRateLimitConfig,
    DiscoveryRateLimiter,
    DiscoveryRateLimitKey,
)
from .core.discovery_resolver import (
    CatalogEntryCounts,
    CatalogQueryCacheEntry,
    CatalogQueryCacheKey,
    ClusterMapQueryCacheEntry,
    ClusterMapQueryCacheKey,
    DiscoveryResolverCache,
    DiscoveryResolverCacheStatsResponse,
    DiscoveryResolverQueryCacheConfig,
    DiscoveryResolverResyncResponse,
    QueryCacheState,
)
from .core.discovery_summary import (
    DISCOVERY_SUMMARY_TOPIC,
    DiscoverySummaryMessage,
    ServiceSummary,
    SummaryExportState,
    SummaryQueryRequest,
    SummaryQueryResponse,
    SummaryWatchRequest,
    SummaryWatchResponse,
    summarize_functions,
)
from .core.discovery_summary_resolver import (
    DiscoverySummaryCache,
    DiscoverySummaryCacheStatsResponse,
    SummaryCacheEntryCounts,
)
from .core.discovery_tenant import DiscoveryTenantCredential
from .core.dns_registry import (
    DnsDescribeRequest,
    DnsDescribeResponse,
    DnsListRequest,
    DnsListResponse,
    DnsRegisterRequest,
    DnsRegisterResponse,
    DnsUnregisterRequest,
    DnsUnregisterResponse,
)
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
    PubSubSubscription,
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
    TopicPattern,
)
from .core.monitoring import create_unified_system_monitor
from .core.monitoring.server_monitoring import (
    ServerMetricsTracker,
    ServerSystemMonitor,
)
from .core.monitoring.system_adapters import FederationSystemMonitor
from .core.monitoring.unified_monitoring import SystemType
from .core.namespace_policy import (
    NamespaceCatalogFilterPolicy,
    NamespacePolicyApplyRequest,
    NamespacePolicyApplyResponse,
    NamespacePolicyAuditEntry,
    NamespacePolicyAuditLog,
    NamespacePolicyAuditRequest,
    NamespacePolicyAuditResponse,
    NamespacePolicyDecision,
    NamespacePolicyEngine,
    NamespacePolicyExportResponse,
    NamespacePolicyValidationResponse,
    NamespaceStatusRequest,
    NamespaceStatusResponse,
    validate_namespace_policy_rules,
)
from .core.payloads import Payload, PayloadMapping, apply_overrides, parse_request
from .core.persistence.config import PersistenceMode
from .core.rpc_discovery import (
    RpcDescribeError,
    RpcDescribeItem,
    RpcDescribeRequest,
    RpcDescribeResponse,
    RpcListItem,
    RpcListRequest,
    RpcListResponse,
    RpcReportCount,
    RpcReportRequest,
    RpcReportResponse,
)
from .core.rpc_registry import RpcRegistry
from .core.rpc_spec_sharing import RpcSpecSharePolicy
from .core.serialization import JsonSerializer
from .core.service_registry import ServiceRegistration, ServiceRegistry
from .core.topic_exchange import TopicExchange
from .core.transport.factory import TransportFactory
from .core.transport.interfaces import (
    TransportConfig,
    TransportConnectionError,
    TransportInterface,
    TransportListener,
)
from .datastructures.cluster_types import ClusterState
from .datastructures.function_identity import FunctionSelector, VersionConstraint
from .datastructures.rpc_spec import RpcDocSpec, RpcExampleSpec, RpcRegistration
from .datastructures.type_aliases import EndpointScope, JsonDict, TenantId, ViewerId
from .fabric.catalog_delta import RoutingCatalogDelta
from .fabric.federation_graph import (
    FederationGraphEdge,
    FederationGraphNode,
    GeographicCoordinate,
    GraphBasedFederationRouter,
    NodeType,
)
from .fabric.federation_planner import FabricFederationPlanner

RequestT = TypeVar("RequestT")

_current_viewer_cluster_id: ContextVar[str | None] = ContextVar(
    "mpreg_viewer_cluster_id", default=None
)
_current_viewer_tenant_id: ContextVar[TenantId | None] = ContextVar(
    "mpreg_viewer_tenant_id", default=None
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mpreg.core.model import PubSubSubscription
    from mpreg.datastructures.type_aliases import PortAssignmentCallback
    from mpreg.fabric.catalog import (
        CacheNodeProfile,
        NodeDescriptor,
        RoutingCatalog,
        TransportEndpoint,
    )
    from mpreg.fabric.index import RoutingIndex
    from mpreg.fabric.message import MessageHeaders, UnifiedMessage
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


@dataclass(slots=True)
class RemoteCommandStats:
    total: int = 0
    last_command: str | None = None

    def record(self, command: str) -> None:
        self.total += 1
        self.last_command = command


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


@dataclass(slots=True)
class PeerDialState:
    """Adaptive dial scheduling state for a peer URL."""

    consecutive_failures: int = 0
    last_attempt_at: float = 0.0
    last_success_at: float | None = None
    next_attempt_at: float = 0.0

    def can_attempt(self, now: float) -> bool:
        return now >= self.next_attempt_at

    def record_attempt(self, now: float) -> None:
        self.last_attempt_at = now

    def record_success(self, now: float) -> None:
        self.consecutive_failures = 0
        self.last_success_at = now
        self.next_attempt_at = now

    def record_failure(
        self,
        *,
        now: float,
        base_delay_seconds: float,
        max_delay_seconds: float,
        spread_fraction: float,
    ) -> None:
        self.consecutive_failures += 1
        exponent = max(self.consecutive_failures - 1, 0)
        if base_delay_seconds <= 0:
            delay = max_delay_seconds
        else:
            delay = base_delay_seconds
            max_exponent = 0
            while delay < max_delay_seconds:
                delay *= 2
                max_exponent += 1
            exponent = min(exponent, max_exponent)
            delay = min(max_delay_seconds, base_delay_seconds * (2**exponent))
        jitter_seconds = delay * max(min(spread_fraction, 0.25), 0.0)
        self.next_attempt_at = now + delay + jitter_seconds


@dataclass(frozen=True, slots=True)
class PeerDialConnectionPolicy:
    """Connection attempt policy chosen for a peer dial."""

    max_retries: int
    base_delay_seconds: float
    connect_timeout_seconds: float
    open_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class PeerDialDiagnosticSnapshot:
    """Structured dial attempt snapshot for diagnostics."""

    peer_url: str
    dial_url: str
    context: str
    fast_connect: bool
    peer_target_count: int
    connected_ratio: float
    consecutive_failures: int
    policy: PeerDialConnectionPolicy
    attempt_epoch_seconds: float


@dataclass(frozen=True, slots=True)
class PeerDialLoopSnapshot:
    """Structured scheduler loop snapshot for diagnostics."""

    peer_target_count: int
    connected_ratio: float
    discovery_ratio: float
    pressure: float
    desired_connected: int
    exploration_slots: int
    connected_candidates: int
    due_candidates: int
    selected_candidates: int
    parallelism: int
    dial_budget: int
    not_due_candidates: int
    reconcile_interval_seconds: float


@dataclass(slots=True)
class CatalogSnapshotDispatchState:
    """Track coalesced catalog snapshot dispatch across rapid update bursts."""

    pending_peers: set[str] = field(default_factory=set)
    flush_task: asyncio.Task[None] | None = None
    enqueued_events: int = 0
    flush_batches: int = 0
    peers_flushed: int = 0


@dataclass(frozen=True, slots=True)
class CommandExecutionResult:
    name: str
    value: Any


@dataclass(slots=True)
class CatalogDeltaObserverAdapter:
    publish: Callable[[RoutingCatalogDelta, dict[str, int]], None]

    def on_catalog_delta(
        self, delta: RoutingCatalogDelta, counts: dict[str, int]
    ) -> None:
        self.publish(delta, counts)


############################################
#
# Default commands for all servers
#
############################################
def echo(arg: Any) -> Any:
    """Default echo handler for all servers.

    Single-argument echo demo."""
    return arg


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
    registry: RpcRegistry = field(init=False)
    _service_registry: ServiceRegistry = field(init=False)
    serializer: JsonSerializer = field(init=False)

    # Runtime state
    waitingFor: dict[str, asyncio.Event] = field(init=False)
    answer: dict[str, Any] = field(init=False)
    peer_connections: dict[str, Connection] = field(init=False)

    # Connection event system
    connection_event_bus: ConnectionEventBus = field(init=False)
    _remote_command_stats: RemoteCommandStats = field(
        init=False, default_factory=RemoteCommandStats
    )

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

        # Initialize core services eagerly so cluster repr/introspection stays safe.
        self.registry = RpcRegistry()
        self._service_registry = ServiceRegistry()
        self.serializer = JsonSerializer()

        self.waitingFor: dict[str, asyncio.Event] = dict()
        self.answer: dict[str, Any] = dict()
        # Persistent connections to peer servers for RPC forwarding
        self.peer_connections: dict[str, Connection] = dict()

        # Initialize connection event system
        self.connection_event_bus = ConnectionEventBus()

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

    def _force_local_execution(self, rpc_command: RPCCommand) -> bool:
        if rpc_command.fun not in LOCAL_ONLY_RPC_COMMANDS:
            return False
        settings = self.settings
        if settings is None:
            return False
        target_cluster = rpc_command.target_cluster
        return not (target_cluster and target_cluster != settings.cluster_id)

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
                        return value
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
        """Executes a command locally using the RPC registry.

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

        selector = FunctionSelector(
            name=rpc_command.fun,
            function_id=rpc_command.function_id,
            version_constraint=VersionConstraint.parse(rpc_command.version_constraint)
            if rpc_command.version_constraint
            else None,
        )
        implementation = self.registry.resolve(selector)
        if not implementation:
            raise CommandNotFoundException(command_name=rpc_command.fun)
        return await implementation.call_async(*resolved_args, **resolved_kwargs)

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
            await server._establish_peer_connection(
                peer_url,
                fast_connect=fast_connect,
                dial_context="cluster_get_or_open",
                peer_target_count=1,
            )
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
        allow_fallback: bool = True,
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
        self._remote_command_stats.record(rpc_step.fun)
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
                await server._establish_peer_connection(
                    where.url,
                    fast_connect=True,
                    dial_context="rpc_remote_execute",
                    peer_target_count=1,
                )
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
            if allow_fallback:
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
                        allow_fallback=allow_fallback,
                    )  # Changed new_where to where
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
            if allow_fallback and retry_remaining > 0:
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
                        allow_fallback=allow_fallback,
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
            if self._force_local_execution(rpc_command):
                logger.info("Executing '{}' locally (forced)", rpc_command.fun)
                got = await self._execute_local_command(rpc_command, results)
                results[rpc_command.name] = got
                return CommandExecutionResult(name=rpc_command.name, value=got)
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
            if self._force_local_execution(rpc_command):
                got = await self._execute_local_command(rpc_command, results)
                results[rpc_command.name] = got
                return CommandExecutionResult(name=rpc_command.name, value=got)
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
    _auto_allocated_dns_udp_port: int | None = field(init=False, default=None)
    _auto_allocated_dns_tcp_port: int | None = field(init=False, default=None)
    registry: RpcRegistry = field(init=False)
    _service_registry: ServiceRegistry = field(init=False)
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
    _dns_gateway: Any = field(init=False, default=None)
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
    _internal_discovery_subscription_announcer: (
        InternalDiscoverySubscriptionAnnouncer | None
    ) = field(init=False, default=None)
    _fabric_router: Any = field(init=False, default=None)
    _fabric_federation_planner: Any = field(init=False, default=None)
    _fabric_queue_federation: Any = field(init=False, default=None)
    _fabric_queue_delivery: Any = field(init=False, default=None)
    _fabric_raft_transport: Any = field(init=False, default=None)
    _fabric_catalog_refresh_task: asyncio.Task[None] | None = field(
        init=False, default=None
    )
    _fabric_topic_refresh_task: asyncio.Task[None] | None = field(
        init=False, default=None
    )
    _fabric_route_key_refresh_task: asyncio.Task[None] | None = field(
        init=False, default=None
    )
    _fabric_node_refresh_task: asyncio.Task[None] | None = field(
        init=False, default=None
    )
    _fabric_node_snapshot_cursor: int = field(init=False, default=0)
    _fabric_function_catalog_signature: tuple[str, ...] | None = field(
        init=False, default=None
    )
    _fabric_function_catalog_last_announced_at: float = field(init=False, default=0.0)
    _fabric_service_catalog_signature: tuple[str, ...] | None = field(
        init=False, default=None
    )
    _fabric_service_catalog_last_announced_at: float = field(init=False, default=0.0)
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
    _catalog_snapshot_dispatch: CatalogSnapshotDispatchState = field(
        init=False, default_factory=CatalogSnapshotDispatchState
    )
    _namespace_policy_engine: NamespacePolicyEngine | None = field(
        init=False, default=None
    )
    _namespace_policy_audit_log: NamespacePolicyAuditLog | None = field(
        init=False, default=None
    )
    _discovery_access_audit_log: DiscoveryAccessAuditLog | None = field(
        init=False, default=None
    )
    _discovery_rate_limiter: DiscoveryRateLimiter | None = field(
        init=False, default=None
    )
    _discovery_resolver: DiscoveryResolverCache | None = field(init=False, default=None)
    _discovery_resolver_subscription_id: str | None = field(init=False, default=None)
    _discovery_resolver_prune_task: asyncio.Task[None] | None = field(
        init=False, default=None
    )
    _discovery_resolver_resync_task: asyncio.Task[None] | None = field(
        init=False, default=None
    )
    _discovery_summary_resolver: DiscoverySummaryCache | None = field(
        init=False, default=None
    )
    _discovery_summary_resolver_subscription_id: str | None = field(
        init=False, default=None
    )
    _discovery_summary_resolver_prune_task: asyncio.Task[None] | None = field(
        init=False, default=None
    )
    _discovery_summary_export_task: asyncio.Task[None] | None = field(
        init=False, default=None
    )
    _summary_export_state: SummaryExportState = field(
        init=False, default_factory=SummaryExportState
    )
    # Task tracking for proper cleanup
    _background_tasks: set[asyncio.Task[Any]] = field(init=False, default_factory=set)
    _transport_listener: TransportListener | None = field(init=False, default=None)
    _instance_id: str = field(init=False)
    _peer_instance_ids: dict[str, str] = field(init=False)
    _departed_peers: dict[str, DepartedPeer] = field(init=False, default_factory=dict)
    _peer_connection_locks: dict[str, asyncio.Lock] = field(init=False)
    _peer_dial_state: dict[str, PeerDialState] = field(init=False, default_factory=dict)
    _peer_dial_diagnostics_enabled: bool = field(init=False, default=False)
    _peer_dial_diag_last_summary_at: float = field(init=False, default=0.0)
    _discovery_loop_diagnostics_enabled: bool = field(init=False, default=False)
    _discovery_diag_last_summary_at: float = field(init=False, default=0.0)
    _rpc_spec_share_policy: RpcSpecSharePolicy = field(init=False)

    def __post_init__(self) -> None:
        """Initializes the MPREGServer instance.

        Sets up the cluster, RPC registry, and client tracking.
        """
        if self.settings.port in (None, 0):
            from .core.port_allocator import allocate_port

            selected_port = allocate_port("servers")
            self.settings.port = selected_port
            self._auto_allocated_port = selected_port
            self._invoke_port_callback(
                self.settings.on_port_assigned, selected_port, label="server"
            )
        self.registry = RpcRegistry()  # Moved registry initialization here
        self._service_registry = ServiceRegistry()
        self.serializer = JsonSerializer()  # Moved serializer initialization here
        self.cluster = Cluster.create(
            cluster_id=self.settings.cluster_id,
            advertised_urls=tuple(self.settings.advertised_urls or []),
            local_url=f"ws://{self.settings.host}:{self.settings.port}",
        )
        self.cluster.settings = self.settings
        self.cluster.registry = self.registry
        self.cluster._service_registry = self._service_registry
        self.cluster.serializer = self.serializer

        mode = str(self.settings.rpc_spec_gossip_mode or "summary").strip().lower()
        if mode not in {"summary", "full"}:
            mode = "summary"
        self._rpc_spec_share_policy = RpcSpecSharePolicy(
            mode=mode,
            namespaces=tuple(self.settings.rpc_spec_gossip_namespaces or ()),
            max_bytes=self.settings.rpc_spec_gossip_max_bytes,
        )

        self._instance_id = str(ulid.new())
        self._peer_instance_ids = {self.cluster.local_url: self._instance_id}
        self._peer_connection_locks = {}
        self._peer_dial_state = {}
        self._peer_dial_diagnostics_enabled = (
            self._resolve_peer_dial_diagnostics_enabled()
        )
        self._peer_dial_diag_last_summary_at = 0.0
        self._discovery_loop_diagnostics_enabled = (
            self._resolve_discovery_loop_diagnostics_enabled()
        )
        self._discovery_diag_last_summary_at = 0.0
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
        if self.settings.discovery_summary_export_store_forward_seconds > 0:
            self.topic_exchange.set_backlog_prefix_enabled(
                f"{DISCOVERY_SUMMARY_TOPIC}.", enabled=False
            )
            self.topic_exchange.set_backlog_enabled(
                DISCOVERY_SUMMARY_TOPIC, enabled=False
            )

        # Initialize PubSub client tracking
        self.pubsub_clients = {}
        self.subscription_to_client: dict[str, str] = {}

        # Metrics tracking for monitoring endpoints
        self._metrics_tracker = ServerMetricsTracker()
        self._namespace_policy_audit_log = NamespacePolicyAuditLog()
        self._discovery_access_audit_log = DiscoveryAccessAuditLog(
            max_entries=self.settings.discovery_access_audit_max_entries
        )
        self._initialize_namespace_policy_engine()
        self._initialize_discovery_rate_limiter()
        self._initialize_discovery_resolver()
        self._initialize_discovery_summary_resolver()
        self._initialize_fabric_control_plane()

        # TopicExchange will create notifications, we'll send them in the message handler

        # Register default commands
        self._register_default_commands()

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

    def _initialize_discovery_resolver(self) -> None:
        if not self.settings.discovery_resolver_mode:
            return
        self._discovery_resolver = DiscoveryResolverCache(
            query_cache_config=DiscoveryResolverQueryCacheConfig(
                enabled=self.settings.discovery_resolver_query_cache_enabled,
                ttl_seconds=self.settings.discovery_resolver_query_ttl_seconds,
                stale_while_revalidate_seconds=(
                    self.settings.discovery_resolver_query_stale_seconds
                ),
                negative_ttl_seconds=(
                    self.settings.discovery_resolver_query_negative_ttl_seconds
                ),
                max_entries=self.settings.discovery_resolver_query_cache_max_entries,
            )
        )
        self._register_discovery_resolver_subscription()

    def _initialize_discovery_summary_resolver(self) -> None:
        if not self.settings.discovery_summary_resolver_mode:
            return
        self._discovery_summary_resolver = DiscoverySummaryCache()
        self._register_discovery_summary_resolver_subscription()

    def _initialize_namespace_policy_engine(self) -> None:
        if not self.settings.discovery_policy_enabled:
            return
        self._namespace_policy_engine = NamespacePolicyEngine(
            enabled=self.settings.discovery_policy_enabled,
            default_allow=self.settings.discovery_policy_default_allow,
            rules=self.settings.discovery_policy_rules,
        )

    def _initialize_discovery_rate_limiter(self) -> None:
        max_requests = int(self.settings.discovery_rate_limit_requests_per_minute)
        if max_requests <= 0:
            return
        window_seconds = float(self.settings.discovery_rate_limit_window_seconds)
        max_keys = int(self.settings.discovery_rate_limit_max_keys)
        config = DiscoveryRateLimitConfig(
            enabled=True,
            max_requests=max_requests,
            window_seconds=window_seconds,
            max_keys=max_keys,
        )
        self._discovery_rate_limiter = DiscoveryRateLimiter(config=config)

    def _register_discovery_resolver_subscription(self) -> None:
        if self._discovery_resolver is None:
            return
        if self._discovery_resolver_subscription_id is not None:
            return
        subscription_id = f"resolver_{self.cluster.local_url}_{ulid.new()}"
        subscription = PubSubSubscription(
            subscription_id=subscription_id,
            patterns=(
                TopicPattern(
                    pattern=DISCOVERY_DELTA_TOPIC,
                    exact_match=True,
                ),
            ),
            subscriber=self.cluster.local_url,
            created_at=time.time(),
            get_backlog=False,
            backlog_seconds=0,
        )
        self.topic_exchange.add_internal_subscription(
            subscription, self._handle_discovery_delta_notification
        )
        self._discovery_resolver_subscription_id = subscription_id
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._track_background_task(
            loop.create_task(self._announce_fabric_subscription(subscription))
        )

    def _register_discovery_summary_resolver_subscription(self) -> None:
        if self._discovery_summary_resolver is None:
            return
        if self._discovery_summary_resolver_subscription_id is not None:
            return
        subscription_id = f"summary_resolver_{self.cluster.local_url}_{ulid.new()}"
        pattern_entries: list[tuple[str, bool]] = []
        scopes = tuple(self.settings.discovery_summary_resolver_scopes or ())
        if scopes:
            for scope in scopes:
                clean_scope = scope.strip().lower()
                if not clean_scope:
                    continue
                scoped_topic = f"{DISCOVERY_SUMMARY_TOPIC}.{clean_scope}"
                pattern_entries.append((scoped_topic, True))
                pattern_entries.append((f"{scoped_topic}.#", False))
        else:
            pattern_entries.append((DISCOVERY_SUMMARY_TOPIC, True))
            pattern_entries.append((f"{DISCOVERY_SUMMARY_TOPIC}.#", False))

        seen_patterns: set[tuple[str, bool]] = set()
        patterns: list[TopicPattern] = []
        for pattern, exact_match in pattern_entries:
            key = (pattern, exact_match)
            if key in seen_patterns:
                continue
            seen_patterns.add(key)
            patterns.append(
                TopicPattern(
                    pattern=pattern,
                    exact_match=exact_match,
                )
            )
        subscription = PubSubSubscription(
            subscription_id=subscription_id,
            patterns=tuple(patterns),
            subscriber=self.cluster.local_url,
            created_at=time.time(),
            get_backlog=False,
            backlog_seconds=0,
        )
        if DISCOVERY_SUMMARY_DIAG_ENABLED:
            logger.warning(
                "[DIAG_DISCOVERY_SUMMARY] node={} action=resolver_subscribe subscription_id={} patterns={}",
                self.cluster.local_url,
                subscription_id,
                [
                    (pattern.pattern, pattern.exact_match)
                    for pattern in subscription.patterns
                ],
            )
        self.topic_exchange.add_internal_subscription(
            subscription, self._handle_discovery_summary_notification
        )
        self._discovery_summary_resolver_subscription_id = subscription_id
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._track_background_task(
            loop.create_task(self._announce_fabric_subscription(subscription))
        )

    async def _handle_discovery_delta_notification(
        self, notification: PubSubNotification
    ) -> None:
        resolver = self._discovery_resolver
        if resolver is None or not self.settings.discovery_resolver_mode:
            return
        payload = notification.message.payload
        if not isinstance(payload, dict):
            resolver.record_invalid()
            return
        raw_namespaces = payload.get("namespaces", [])
        namespaces: list[str] = []
        if isinstance(raw_namespaces, (list, tuple)):
            namespaces = [str(item) for item in raw_namespaces if item is not None]
        elif isinstance(raw_namespaces, str):
            namespaces = [raw_namespaces]
        if not self._resolver_allows_namespaces(namespaces):
            resolver.record_skipped()
            return
        delta_payload = payload.get("delta")
        if not isinstance(delta_payload, dict):
            resolver.record_invalid()
            return
        try:
            delta = RoutingCatalogDelta.from_dict(delta_payload)
        except Exception:
            resolver.record_invalid()
            return
        resolver.apply_delta(delta, now=time.time())

    async def _handle_discovery_summary_notification(
        self, notification: PubSubNotification
    ) -> None:
        resolver = self._discovery_summary_resolver
        if resolver is None or not self.settings.discovery_summary_resolver_mode:
            return
        if DISCOVERY_SUMMARY_DIAG_ENABLED:
            logger.warning(
                "[DIAG_DISCOVERY_SUMMARY] node={} action=resolver_notification_received topic={} subscription_id={}",
                self.cluster.local_url,
                notification.message.topic,
                notification.subscription_id,
            )
        payload = notification.message.payload
        if not isinstance(payload, dict):
            resolver.record_invalid()
            return
        raw_namespaces = payload.get("namespaces", [])
        namespaces: list[str] = []
        if isinstance(raw_namespaces, (list, tuple)):
            namespaces = [str(item) for item in raw_namespaces if item is not None]
        elif isinstance(raw_namespaces, str):
            namespaces = [raw_namespaces]
        if not self._summary_resolver_allows_namespaces(namespaces):
            resolver.record_skipped()
            return
        try:
            message = DiscoverySummaryMessage.from_dict(payload)
        except Exception:
            resolver.record_invalid()
            return
        if not self._summary_resolver_allows_scope(message.scope):
            resolver.record_skipped()
            return
        applied = resolver.apply_message(message, now=time.time())
        if DISCOVERY_SUMMARY_DIAG_ENABLED:
            logger.warning(
                "[DIAG_DISCOVERY_SUMMARY] node={} action=resolver_notification_applied topic={} scope={} source_cluster={} summaries={} applied={} entry_count={}",
                self.cluster.local_url,
                notification.message.topic,
                message.scope,
                message.source_cluster,
                len(message.summaries),
                applied,
                resolver.entry_counts().summaries,
            )

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
        discovery_observer = CatalogDeltaObserverAdapter(
            publish=self._publish_discovery_delta
        )
        self._fabric_control_plane = FabricControlPlane.create(
            local_cluster=self.settings.cluster_id,
            gossip=gossip,
            catalog_policy=self._fabric_catalog_policy(),
            catalog_observers=(self._peer_directory, discovery_observer),
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
        topic_adapter = TopicExchangeCatalogAdapter.for_exchange(
            self.topic_exchange, ttl_seconds=self.settings.fabric_catalog_ttl_seconds
        )
        self._fabric_topic_announcer = FabricTopicAnnouncer(
            adapter=topic_adapter,
            broadcaster=self._fabric_control_plane.broadcaster,
        )
        self._internal_discovery_subscription_announcer = (
            InternalDiscoverySubscriptionAnnouncer(
                schedule=self._schedule_discovery_query_refresh,
                announce=self._announce_internal_discovery_subscriptions,
            )
        )
        self.cluster.connection_event_bus.subscribe(
            self._internal_discovery_subscription_announcer
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
        self._fabric_control_plane.engine.node_load_provider = self._node_load_score
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
            send_to_cluster=lambda cluster_id, message, source_peer_url: (
                messenger.send_to_cluster(message, cluster_id, source_peer_url)
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

    def _resolve_peer_dial_diagnostics_enabled(self) -> bool:
        scopes = tuple(
            scope.strip().lower()
            for scope in self.settings.log_debug_scopes
            if scope.strip()
        )
        if "peer_dial" in scopes:
            return True
        env_value = os.environ.get("MPREG_DEBUG_PEER_DIAL", "").strip().lower()
        return env_value in _DIAG_TRUE_VALUES

    def _peer_dial_policy_diagnostics_enabled(self) -> bool:
        scopes = tuple(
            scope.strip().lower()
            for scope in self.settings.log_debug_scopes
            if scope.strip()
        )
        if "peer_dial_policy" in scopes:
            return True
        env_value = os.environ.get("MPREG_DEBUG_PEER_DIAL_POLICY", "").strip().lower()
        return env_value in _DIAG_TRUE_VALUES

    def _resolve_discovery_loop_diagnostics_enabled(self) -> bool:
        scopes = tuple(
            scope.strip().lower()
            for scope in self.settings.log_debug_scopes
            if scope.strip()
        )
        if "discovery_loop" in scopes:
            return True
        env_value = os.environ.get("MPREG_DEBUG_DISCOVERY_LOOP", "").strip().lower()
        return env_value in _DIAG_TRUE_VALUES

    def _summary_ingress_diagnostics_enabled(self) -> bool:
        scopes = tuple(
            scope.strip().lower()
            for scope in self.settings.log_debug_scopes
            if scope.strip()
        )
        if "summary_ingress" in scopes or "discovery_summary" in scopes:
            return True
        env_value = os.environ.get("MPREG_DEBUG_SUMMARY_INGRESS", "").strip().lower()
        return env_value in _DIAG_TRUE_VALUES

    def _peer_dial_target_count_hint(self) -> int:
        target_count = 0
        if self._peer_directory:
            target_count = max(target_count, len(self._peer_directory.nodes()))
        if self.settings.peers:
            target_count = max(target_count, len(self.settings.peers))
        if self.settings.connect:
            target_count = max(target_count, 1)
        return max(target_count, 1)

    def _select_peer_connection_policy(
        self,
        *,
        fast_connect: bool,
        peer_target_count: int,
        consecutive_failures: int,
        connected_ratio: float = 1.0,
    ) -> PeerDialConnectionPolicy:
        target_count = max(peer_target_count, 1)
        failure_count = max(consecutive_failures, 0)
        connectivity = min(max(connected_ratio, 0.0), 1.0)
        target_factor = target_count**0.5
        retry_bonus = min(4, failure_count // 2)
        connectivity_pressure = max(0.0, 0.45 - connectivity) / 0.45

        def _adaptive_retry_cap(*, fast_path: bool) -> int:
            if fast_path:
                if target_count >= 20:
                    if connectivity < 0.15:
                        # Rotate targets quickly under near-isolation instead of
                        # spending long retry chains on a single candidate.
                        return 0
                    # Large sparse fabrics should avoid retry storms.
                    if connectivity < 0.45:
                        return 1
                    return 2
                base_cap = max(2, int(6.0 / max(target_factor, 1.0)))
                if connectivity < 0.5:
                    base_cap += 1
                return base_cap + min(1, failure_count // 5)
            if target_count >= 20:
                if connectivity < 0.15:
                    return 0
                if connectivity < 0.45:
                    return 1
                return 2
            base_cap = max(2, int(8.0 / max(target_factor, 1.0)))
            return base_cap + min(2, failure_count // 5)

        def _pressure_timeout_floor(*, fast_path: bool) -> float:
            if target_count < 20:
                if fast_path:
                    return 1.4 + (connectivity_pressure * 1.4)
                return 2.0 + (connectivity_pressure * 1.8)
            if fast_path:
                return 1.8 + (connectivity_pressure * 1.2)
            return 2.4 + (connectivity_pressure * 1.6)

        if fast_connect:
            max_retries = min(6, max(1, 1 + int(target_factor // 2)) + retry_bonus)
            base_delay_seconds = min(
                1.5, 0.08 + (target_factor * 0.035) + (min(failure_count, 6) * 0.04)
            )
            connect_timeout_seconds = min(
                8.0, 1.1 + (target_factor * 0.16) + (min(failure_count, 8) * 0.18)
            )
            connect_timeout_seconds = max(
                connect_timeout_seconds, _pressure_timeout_floor(fast_path=True)
            )
            max_retries = min(max_retries, _adaptive_retry_cap(fast_path=True))
            policy = PeerDialConnectionPolicy(
                max_retries=max_retries,
                base_delay_seconds=base_delay_seconds,
                connect_timeout_seconds=connect_timeout_seconds,
                open_timeout_seconds=connect_timeout_seconds,
            )
            if self._peer_dial_policy_diagnostics_enabled():
                logger.warning(
                    "[DIAG_PEER_DIAL_POLICY] node={} mode=fast targets={} failures={} "
                    "connected_ratio={:.3f} pressure={:.3f} retries={} base_delay={:.4f}s "
                    "connect_timeout={:.4f}s",
                    self.cluster.local_url,
                    target_count,
                    failure_count,
                    connectivity,
                    connectivity_pressure,
                    policy.max_retries,
                    policy.base_delay_seconds,
                    policy.connect_timeout_seconds,
                )
            return policy

        max_retries = min(8, max(2, 1 + int(target_factor)) + retry_bonus)
        base_delay_seconds = min(
            2.2, 0.3 + (target_factor * 0.05) + (min(failure_count, 6) * 0.07)
        )
        connect_timeout_seconds = min(
            10.0, 2.4 + (target_factor * 0.10) + (min(failure_count, 10) * 0.22)
        )
        connect_timeout_seconds = max(
            connect_timeout_seconds, _pressure_timeout_floor(fast_path=False)
        )
        max_retries = min(max_retries, _adaptive_retry_cap(fast_path=False))
        policy = PeerDialConnectionPolicy(
            max_retries=max_retries,
            base_delay_seconds=base_delay_seconds,
            connect_timeout_seconds=connect_timeout_seconds,
            open_timeout_seconds=connect_timeout_seconds,
        )
        if self._peer_dial_policy_diagnostics_enabled():
            logger.warning(
                "[DIAG_PEER_DIAL_POLICY] node={} mode=steady targets={} failures={} "
                "connected_ratio={:.3f} pressure={:.3f} retries={} base_delay={:.4f}s "
                "connect_timeout={:.4f}s",
                self.cluster.local_url,
                target_count,
                failure_count,
                connectivity,
                connectivity_pressure,
                policy.max_retries,
                policy.base_delay_seconds,
                policy.connect_timeout_seconds,
            )
        return policy

    def _build_peer_dial_snapshot(
        self,
        *,
        peer_url: str,
        dial_url: str,
        context: str,
        fast_connect: bool,
        peer_target_count: int,
        connected_ratio: float,
        consecutive_failures: int,
        policy: PeerDialConnectionPolicy,
    ) -> PeerDialDiagnosticSnapshot:
        return PeerDialDiagnosticSnapshot(
            peer_url=peer_url,
            dial_url=dial_url,
            context=context,
            fast_connect=fast_connect,
            peer_target_count=peer_target_count,
            connected_ratio=connected_ratio,
            consecutive_failures=consecutive_failures,
            policy=policy,
            attempt_epoch_seconds=time.time(),
        )

    def _emit_peer_dial_diag(
        self,
        *,
        snapshot: PeerDialDiagnosticSnapshot,
        outcome: str,
        duration_seconds: float | None = None,
        error_message: str | None = None,
    ) -> None:
        if not self._peer_dial_diagnostics_enabled:
            return
        duration_fragment = (
            f" duration={duration_seconds:.3f}s" if duration_seconds is not None else ""
        )
        error_fragment = f" error={error_message}" if error_message else ""
        logger.error(
            "[DIAG_PEER_DIAL] node={} name={} outcome={} context={} peer={} dial={} "
            "fast_connect={} targets={} connected_ratio={:.2f} failures={} retries={} "
            "base_delay={:.3f}s connect_timeout={:.3f}s open_timeout={:.3f}s{}{}",
            self.cluster.local_url,
            self.settings.name,
            outcome,
            snapshot.context,
            snapshot.peer_url,
            snapshot.dial_url,
            snapshot.fast_connect,
            snapshot.peer_target_count,
            snapshot.connected_ratio,
            snapshot.consecutive_failures,
            snapshot.policy.max_retries,
            snapshot.policy.base_delay_seconds,
            snapshot.policy.connect_timeout_seconds,
            snapshot.policy.open_timeout_seconds,
            duration_fragment,
            error_fragment,
        )

    def _emit_peer_dial_loop_diag(self, snapshot: PeerDialLoopSnapshot) -> None:
        if not self._peer_dial_diagnostics_enabled:
            return
        now = time.time()
        if (
            snapshot.selected_candidates == 0
            and now - self._peer_dial_diag_last_summary_at < 1.0
        ):
            return
        self._peer_dial_diag_last_summary_at = now
        logger.error(
            "[DIAG_PEER_DIAL] node={} name={} loop targets={} connected_ratio={:.2f} discovery_ratio={:.2f} pressure={:.2f} desired_connected={} exploration_slots={} connected={} due={} selected={} "
            "parallelism={} budget={} not_due={} interval={:.3f}s",
            self.cluster.local_url,
            self.settings.name,
            snapshot.peer_target_count,
            snapshot.connected_ratio,
            snapshot.discovery_ratio,
            snapshot.pressure,
            snapshot.desired_connected,
            snapshot.exploration_slots,
            snapshot.connected_candidates,
            snapshot.due_candidates,
            snapshot.selected_candidates,
            snapshot.parallelism,
            snapshot.dial_budget,
            snapshot.not_due_candidates,
            snapshot.reconcile_interval_seconds,
        )

    def _emit_discovery_loop_diag(
        self,
        *,
        peer_target_count: int,
        connected_candidates: int,
        due_candidates: int,
        selected_candidates: int,
        not_due_candidates: int,
        desired_connected: int,
        connected_ratio: float,
        discovery_ratio: float,
        reconcile_interval_seconds: float,
    ) -> None:
        if not self._discovery_loop_diagnostics_enabled:
            return
        now = time.time()
        if now - self._discovery_diag_last_summary_at < 1.0:
            return
        self._discovery_diag_last_summary_at = now

        function_catalog_discovered = max(len(self.cluster.servers) - 1, 0)
        peer_directory_discovered = -1
        if self._peer_directory is not None:
            peer_directory_discovered = max(len(self._peer_directory.nodes()) - 1, 0)

        node_catalog_discovered = -1
        pending_catalog_updates = 0
        if self._fabric_control_plane is not None:
            node_entries = self._fabric_control_plane.index.catalog.nodes.entries(
                now=now
            )
            node_catalog_nodes = {entry.node_id for entry in node_entries}
            node_catalog_discovered = max(
                len(node_catalog_nodes - {self.cluster.local_url}),
                0,
            )
            from mpreg.fabric.gossip import GossipMessageType

            pending_catalog_updates = sum(
                1
                for message in self._fabric_control_plane.gossip.pending_messages
                if message.message_type is GossipMessageType.CATALOG_UPDATE
            )

        logger.error(
            "[DIAG_DISCOVERY_LOOP] node={} name={} targets={} connected={} desired={} "
            "due={} selected={} not_due={} connected_ratio={:.2f} discovery_ratio={:.2f} "
            "function_catalog_discovered={} node_catalog_discovered={} "
            "peer_directory_discovered={} pending_catalog_updates={} interval={:.3f}s",
            self.cluster.local_url,
            self.settings.name,
            peer_target_count,
            connected_candidates,
            desired_connected,
            due_candidates,
            selected_candidates,
            not_due_candidates,
            connected_ratio,
            discovery_ratio,
            function_catalog_discovered,
            node_catalog_discovered,
            peer_directory_discovered,
            pending_catalog_updates,
            reconcile_interval_seconds,
        )

    def _peer_dial_state_for(self, peer_url: str) -> PeerDialState:
        state = self._peer_dial_state.get(peer_url)
        if state is None:
            state = PeerDialState()
            self._peer_dial_state[peer_url] = state
        return state

    def _peer_dial_spread_fraction(self, peer_url: str) -> float:
        # Deterministic jitter prevents synchronized redials without global randomness.
        checksum = sum(ord(char) for char in peer_url) % 1000
        return checksum / 4000.0

    def _peer_dial_selection_spread(self, peer_url: str) -> float:
        # Per-node deterministic spread prevents large fabrics from hammering the
        # same low-sorted peers when dial budgets are constrained.
        basis = f"{self.cluster.local_url}|{peer_url}"
        digest = hashlib.blake2s(basis.encode("utf-8"), digest_size=8).digest()
        spread_value = int.from_bytes(digest, "big")
        return spread_value / float((1 << 64) - 1)

    def _peer_dial_pressure(
        self, peer_target_count: int, connected_ratio: float = 1.0
    ) -> float:
        target_count = max(peer_target_count, 1)
        connectivity = min(max(connected_ratio, 0.0), 1.0)
        connectivity_deficit = 1.0 - connectivity
        size_factor = max(target_count**0.5 - 2.0, 0.0) / 2.0
        return connectivity_deficit * size_factor

    def _peer_dial_backoff_base_seconds(
        self, peer_target_count: int, connected_ratio: float = 1.0
    ) -> float:
        target_count = max(peer_target_count, 1)
        gossip_interval = float(self.settings.gossip_interval)
        by_cluster_scale = gossip_interval / max(target_count**0.5, 1.0)
        startup_pressure_floor = gossip_interval * min(target_count / 100.0, 0.5)
        pressure = self._peer_dial_pressure(target_count, connected_ratio)
        pressure_multiplier = 1.0 + (min(pressure, 2.5) * 1.5)
        return max(0.2, by_cluster_scale, startup_pressure_floor) * pressure_multiplier

    def _peer_dial_backoff_cap_seconds(self, peer_target_count: int) -> float:
        target_count = max(peer_target_count, 1)
        return max(
            float(self.settings.gossip_interval),
            float(self.settings.gossip_interval) * (target_count**0.5),
        )

    def _peer_dial_parallelism(
        self, peer_target_count: int, connected_ratio: float = 1.0
    ) -> int:
        target_count = max(peer_target_count, 1)
        connectivity = min(max(connected_ratio, 0.0), 1.0)
        pressure = self._peer_dial_pressure(target_count, connectivity)
        parallelism_cap = 4
        if pressure >= 1.2:
            parallelism_cap = 1
        elif pressure >= 0.8:
            parallelism_cap = 2
        elif pressure >= 0.4:
            parallelism_cap = 3
        parallelism = max(1, min(parallelism_cap, int(target_count**0.5)))
        if target_count >= 24 and connectivity < 0.20:
            # Large-cluster recovery mode: avoid two-slot starvation when a node is
            # far behind on active links but has already discovered most peers.
            parallelism = max(parallelism, 3)
        elif target_count >= 20 and connectivity < 0.15:
            # Recovery mode for near-isolated nodes in large fabrics:
            # keep dial concurrency bounded, but avoid single-slot starvation.
            recovery_parallelism = 2 if target_count < 36 else 3
            parallelism = max(parallelism, recovery_parallelism)
        if self._peer_dial_policy_diagnostics_enabled():
            logger.warning(
                "[DIAG_PEER_DIAL_POLICY] node={} action=parallelism targets={} "
                "connected_ratio={:.3f} pressure={:.3f} cap={} selected={}",
                self.cluster.local_url,
                target_count,
                connectivity,
                pressure,
                parallelism_cap,
                parallelism,
            )
        return parallelism

    def _peer_discovery_ratio(self, peer_target_count: int) -> float:
        target_count = max(peer_target_count, 1)
        ratios: list[float] = []
        if self._peer_directory:
            discovered_nodes = max(len(self._peer_directory.nodes()) - 1, 0)
            ratios.append(min(discovered_nodes / target_count, 1.0))
        discovered_status_nodes = max(len(self.cluster.servers) - 1, 0)
        ratios.append(min(discovered_status_nodes / target_count, 1.0))
        if not ratios:
            return 1.0
        return min(ratios)

    def _peer_dial_exploration_slots(
        self,
        *,
        peer_target_count: int,
        connected_ratio: float,
        discovery_ratio: float,
    ) -> int:
        target_count = max(peer_target_count, 1)
        if target_count < 20:
            return 0
        if discovery_ratio >= 0.95:
            return 0
        if connected_ratio < 0.30:
            return max(2, int(target_count**0.5))
        if connected_ratio < 0.50:
            return max(2, int(target_count**0.5) // 2)
        return max(1, int(target_count**0.5) // 2)

    def _peer_target_connection_count(
        self, peer_target_count: int, connected_ratio: float = 1.0
    ) -> int:
        target_count = max(peer_target_count, 1)
        if target_count <= 20:
            return target_count
        baseline = max(6, int(target_count**0.5) + 4)
        connectivity = min(max(connected_ratio, 0.0), 1.0)
        discovery_ratio = self._peer_discovery_ratio(target_count)

        if discovery_ratio >= 0.90:
            # Once discovery is already broad, keep a modest stable mesh and
            # avoid expensive reconnect storms that starve propagation work.
            stability_target = max(6, int(target_count**0.5) + 2)
            if connectivity < 0.30:
                return min(target_count, stability_target + 1)
            return min(target_count, stability_target)

        if connectivity < 0.3:
            deficit_bonus = max(6, int(target_count**0.5))
        elif connectivity < 0.5:
            deficit_bonus = max(4, int(target_count**0.4))
        elif connectivity < 0.7:
            deficit_bonus = 2
        else:
            deficit_bonus = 0

        discovery_bonus = 0
        if target_count >= 24:
            if discovery_ratio < 0.5:
                discovery_bonus = max(discovery_bonus, int(target_count * 0.35))
            elif discovery_ratio < 0.7:
                discovery_bonus = max(discovery_bonus, int(target_count * 0.25))
            elif discovery_ratio < 0.85:
                discovery_bonus = max(discovery_bonus, int(target_count * 0.15))
            elif discovery_ratio < 0.95:
                discovery_bonus = max(discovery_bonus, int(target_count * 0.08))

        return min(target_count, baseline + max(deficit_bonus, discovery_bonus))

    def _peer_reconcile_interval_seconds(
        self, peer_target_count: int, connected_ratio: float = 1.0
    ) -> float:
        gossip_interval = float(self.settings.gossip_interval)
        if peer_target_count <= 0:
            return max(gossip_interval, 0.2)
        scaled_interval = gossip_interval / max(peer_target_count**0.5, 1.0)
        adaptive_floor = min(0.75, 0.2 + (peer_target_count / 200.0))
        base_interval = max(adaptive_floor, scaled_interval)
        pressure = self._peer_dial_pressure(peer_target_count, connected_ratio)
        pressure_multiplier = 1.0 + (min(pressure, 2.0) * 1.25)
        interval_cap = max(gossip_interval * 4.0, 1.5)
        return min(interval_cap, base_interval * pressure_multiplier)

    def _record_peer_dial_outcome(
        self,
        *,
        peer_url: str,
        success: bool,
        now: float,
        peer_target_count: int,
        connected_ratio: float = 1.0,
    ) -> None:
        state = self._peer_dial_state_for(peer_url)
        if success:
            state.record_success(now)
            return
        seed_peers = set(self.settings.peers or [])
        if self.settings.connect:
            seed_peers.add(self.settings.connect)
        is_unrecovered_seed = peer_url in seed_peers and state.last_success_at is None
        base_delay_seconds = self._peer_dial_backoff_base_seconds(
            peer_target_count,
            connected_ratio,
        )
        max_delay_seconds = self._peer_dial_backoff_cap_seconds(peer_target_count)
        if is_unrecovered_seed:
            gossip_interval = float(self.settings.gossip_interval)
            # Keep bootstrap links retriable under pressure until first success.
            base_delay_seconds = min(
                base_delay_seconds,
                max(0.2, gossip_interval * 0.5),
            )
            max_delay_seconds = min(
                max_delay_seconds,
                max(gossip_interval * 2.0, 1.0),
            )
        state.record_failure(
            now=now,
            base_delay_seconds=base_delay_seconds,
            max_delay_seconds=max_delay_seconds,
            spread_fraction=self._peer_dial_spread_fraction(peer_url),
        )

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
            base_policy = CatalogFilterPolicy(
                local_cluster=self.settings.cluster_id,
                allowed_clusters=allowed_clusters,
                node_filter=self._fabric_allows_node,
            )
            return self._namespace_catalog_policy(base_policy)
        security_policy = config.security_policy
        base_policy = CatalogFilterPolicy(
            local_cluster=self.settings.cluster_id,
            allowed_clusters=allowed_clusters,
            allowed_functions=security_policy.allowed_functions_cross_federation,
            blocked_functions=security_policy.blocked_functions_cross_federation,
            node_filter=self._fabric_allows_node,
        )
        return self._namespace_catalog_policy(base_policy)

    def _namespace_catalog_policy(
        self, base_policy: CatalogFilterPolicy
    ) -> CatalogFilterPolicy:
        engine = self._namespace_policy_engine
        if engine is None or not engine.enabled:
            return base_policy
        return NamespaceCatalogFilterPolicy(
            base_policy=base_policy,
            namespace_policy=engine,
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

    def _fabric_catalog_ttl_refresh_seconds(self) -> float:
        ttl_seconds = max(1.0, float(self.settings.fabric_catalog_ttl_seconds))
        target_hint = max(self._peer_dial_target_count_hint(), 1)
        if target_hint >= 30:
            return max(4.0, ttl_seconds * 0.25)
        if target_hint >= 10:
            return max(3.0, ttl_seconds * 0.33)
        return max(2.0, ttl_seconds * 0.5)

    def _local_function_catalog_signature(self) -> tuple[str, ...]:
        signature: list[str] = []
        for spec in self.registry.specs():
            resources = ",".join(sorted(spec.resources))
            digest = spec.spec_digest
            signature.append(
                f"{spec.identity.name}|{spec.identity.function_id}|"
                f"{spec.identity.version}|{digest}|{resources}"
            )
        signature.sort()
        return tuple(signature)

    def _local_service_catalog_signature(self) -> tuple[str, ...]:
        signature: list[str] = []
        for registration in self._service_registry.registrations():
            spec = registration.spec
            targets = ",".join(sorted(spec.targets))
            signature.append(
                f"{spec.namespace}|{spec.name}|{spec.protocol}|{spec.port}|"
                f"{spec.scope}|{targets}|{spec.priority}|{spec.weight}"
            )
        signature.sort()
        return tuple(signature)

    async def _announce_fabric_functions_if_due(
        self,
        *,
        now: float | None = None,
        force: bool = False,
    ) -> bool:
        timestamp = now if now is not None else time.time()
        signature = self._local_function_catalog_signature()
        changed = signature != self._fabric_function_catalog_signature
        due = (
            timestamp - self._fabric_function_catalog_last_announced_at
            >= self._fabric_catalog_ttl_refresh_seconds()
        )
        if not (force or changed or due):
            return False
        await self._announce_fabric_functions()
        self._fabric_function_catalog_signature = signature
        self._fabric_function_catalog_last_announced_at = timestamp
        return True

    async def _announce_fabric_services_if_due(
        self,
        *,
        now: float | None = None,
        force: bool = False,
    ) -> bool:
        timestamp = now if now is not None else time.time()
        signature = self._local_service_catalog_signature()
        changed = signature != self._fabric_service_catalog_signature
        due = (
            timestamp - self._fabric_service_catalog_last_announced_at
            >= self._fabric_catalog_ttl_refresh_seconds()
        )
        if not (force or changed or due):
            return False
        await self._announce_fabric_services()
        self._fabric_service_catalog_signature = signature
        self._fabric_service_catalog_last_announced_at = timestamp
        return True

    async def _announce_fabric_functions(self) -> None:
        if not self._fabric_control_plane:
            return
        from mpreg.fabric.adapters.function_registry import LocalFunctionCatalogAdapter
        from mpreg.fabric.announcers import FabricFunctionAnnouncer

        adapter = LocalFunctionCatalogAdapter(
            registry=self.registry,
            node_id=self.cluster.local_url,
            cluster_id=self.settings.cluster_id,
            node_resources=frozenset(self.settings.resources or set()),
            node_capabilities=self._fabric_node_capabilities(),
            transport_endpoints=self._fabric_transport_endpoints(),
            node_region=self.settings.cache_region,
            function_ttl_seconds=self.settings.fabric_catalog_ttl_seconds,
            spec_share_policy=self._rpc_spec_share_policy,
        )
        announcer = FabricFunctionAnnouncer(
            adapter=adapter, broadcaster=self._fabric_control_plane.broadcaster
        )
        await announcer.announce()

    async def _announce_fabric_services(self) -> None:
        if not self._fabric_control_plane:
            return
        from mpreg.fabric.adapters.service_registry import LocalServiceCatalogAdapter
        from mpreg.fabric.announcers import FabricServiceAnnouncer

        adapter = LocalServiceCatalogAdapter(
            registry=self._service_registry,
            node_id=self.cluster.local_url,
            cluster_id=self.settings.cluster_id,
            node_resources=frozenset(self.settings.resources or set()),
            node_capabilities=self._fabric_node_capabilities(),
            transport_endpoints=self._fabric_transport_endpoints(),
            node_region=self.settings.cache_region,
            service_ttl_seconds=self.settings.fabric_catalog_ttl_seconds,
        )
        announcer = FabricServiceAnnouncer(
            adapter=adapter, broadcaster=self._fabric_control_plane.broadcaster
        )
        await announcer.announce()

    def _publish_fabric_function_update(self, registration: RpcRegistration) -> None:
        if not self._fabric_control_plane:
            return
        import uuid

        from mpreg.fabric.catalog import (
            FunctionEndpoint,
            NodeDescriptor,
            normalize_endpoint_scope,
        )
        from mpreg.fabric.catalog_delta import RoutingCatalogDelta

        now = time.time()
        spec = registration.spec
        include_spec = self._rpc_spec_share_policy.include_spec(spec)
        endpoint = FunctionEndpoint(
            identity=spec.identity,
            resources=spec.resources,
            node_id=self.cluster.local_url,
            cluster_id=self.settings.cluster_id,
            scope=normalize_endpoint_scope(spec.scope),
            tags=spec.tags,
            rpc_summary=spec.summary(),
            rpc_spec=spec if include_spec else None,
            spec_digest=spec.spec_digest,
            advertised_at=now,
            ttl_seconds=self.settings.fabric_catalog_ttl_seconds,
        )
        node = NodeDescriptor(
            node_id=self.cluster.local_url,
            cluster_id=self.settings.cluster_id,
            region=self.settings.cache_region,
            resources=frozenset(self.settings.resources or set()),
            capabilities=self._fabric_node_capabilities(),
            transport_endpoints=self._fabric_transport_endpoints(),
            advertised_at=now,
            ttl_seconds=self.settings.fabric_catalog_ttl_seconds,
        )
        delta = RoutingCatalogDelta(
            update_id=str(uuid.uuid4()),
            cluster_id=self.settings.cluster_id,
            sent_at=now,
            functions=(endpoint,),
            nodes=(node,),
        )
        self._fabric_control_plane.applier.apply(delta, now=now)
        self._fabric_function_catalog_signature = (
            self._local_function_catalog_signature()
        )
        self._fabric_function_catalog_last_announced_at = now
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._track_background_task(
            loop.create_task(
                self._fabric_control_plane.broadcaster.publisher.publish(delta)
            )
        )
        # Push a direct snapshot to currently connected peers so newly
        # registered functions do not rely solely on eventual gossip cycles.
        self._schedule_catalog_snapshots_for_existing_peers()

    def _publish_fabric_service_update(self, registration: ServiceRegistration) -> None:
        if not self._fabric_control_plane:
            return
        import uuid

        from mpreg.fabric.catalog import NodeDescriptor, ServiceEndpoint
        from mpreg.fabric.catalog_delta import RoutingCatalogDelta

        now = time.time()
        spec = registration.spec
        ttl_seconds = (
            spec.ttl_seconds
            if spec.ttl_seconds is not None
            else self.settings.fabric_catalog_ttl_seconds
        )
        endpoint = ServiceEndpoint(
            name=spec.name,
            namespace=spec.namespace,
            protocol=spec.protocol,
            port=spec.port,
            targets=spec.targets,
            scope=spec.scope,
            tags=spec.tags,
            capabilities=spec.capabilities,
            metadata=spec.metadata,
            priority=spec.priority,
            weight=spec.weight,
            node_id=self.cluster.local_url,
            cluster_id=self.settings.cluster_id,
            advertised_at=now,
            ttl_seconds=ttl_seconds,
        )
        node = NodeDescriptor(
            node_id=self.cluster.local_url,
            cluster_id=self.settings.cluster_id,
            region=self.settings.cache_region,
            resources=frozenset(self.settings.resources or set()),
            capabilities=self._fabric_node_capabilities(),
            transport_endpoints=self._fabric_transport_endpoints(),
            advertised_at=now,
            ttl_seconds=self.settings.fabric_catalog_ttl_seconds,
        )
        delta = RoutingCatalogDelta(
            update_id=str(uuid.uuid4()),
            cluster_id=self.settings.cluster_id,
            sent_at=now,
            services=(endpoint,),
            nodes=(node,),
        )
        self._fabric_control_plane.applier.apply(delta, now=now)
        self._fabric_service_catalog_signature = self._local_service_catalog_signature()
        self._fabric_service_catalog_last_announced_at = now
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._track_background_task(
            loop.create_task(
                self._fabric_control_plane.broadcaster.publisher.publish(delta)
            )
        )

    def _publish_fabric_service_removal(
        self,
        *,
        namespace: str,
        name: str,
        protocol: str,
        port: int,
    ) -> None:
        if not self._fabric_control_plane:
            return
        import uuid

        from mpreg.fabric.catalog import ServiceKey
        from mpreg.fabric.catalog_delta import RoutingCatalogDelta

        now = time.time()
        key = ServiceKey(
            cluster_id=self.settings.cluster_id,
            node_id=self.cluster.local_url,
            namespace=namespace,
            name=name,
            protocol=protocol,
            port=port,
        )
        delta = RoutingCatalogDelta(
            update_id=str(uuid.uuid4()),
            cluster_id=self.settings.cluster_id,
            sent_at=now,
            service_removals=(key,),
        )
        self._fabric_control_plane.applier.apply(delta, now=now)
        self._fabric_service_catalog_signature = self._local_service_catalog_signature()
        self._fabric_service_catalog_last_announced_at = now
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

    async def _announce_internal_discovery_subscriptions(self) -> None:
        if not self._fabric_topic_announcer:
            return
        subscription_ids = (
            self._discovery_resolver_subscription_id,
            self._discovery_summary_resolver_subscription_id,
        )
        for subscription_id in subscription_ids:
            if not subscription_id:
                continue
            subscription = self.topic_exchange.subscriptions.get(subscription_id)
            if subscription is None:
                continue
            await self._announce_fabric_subscription(subscription)

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

        summary_topic_diag = DISCOVERY_SUMMARY_DIAG_ENABLED and (
            message.topic == DISCOVERY_SUMMARY_TOPIC
            or message.topic.startswith(f"{DISCOVERY_SUMMARY_TOPIC}.")
        )
        if summary_topic_diag and self._fabric_control_plane:
            topic_matches = self._fabric_control_plane.index.catalog.topics.match(
                message.topic, now=time.time()
            )
            logger.warning(
                "[DIAG_DISCOVERY_SUMMARY] node={} action=forward_lookup topic={} matches={}",
                self.cluster.local_url,
                message.topic,
                [
                    {
                        "subscription_id": sub.subscription_id,
                        "node_id": sub.node_id,
                        "cluster_id": sub.cluster_id,
                        "patterns": sub.patterns,
                    }
                    for sub in topic_matches
                ],
            )

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
        if summary_topic_diag:
            logger.warning(
                "[DIAG_DISCOVERY_SUMMARY] node={} action=forward_route topic={} reason={} targets={} cluster_routes={}",
                self.cluster.local_url,
                message.topic,
                route.reason.value,
                [
                    {
                        "node_id": target.node_id,
                        "cluster_id": target.cluster_id,
                        "target_id": target.target_id,
                    }
                    for target in route.targets
                ],
                {
                    cluster_id: {
                        "can_forward": plan.can_forward,
                        "is_local": plan.is_local,
                        "reason": plan.reason.value,
                        "next_hop": (
                            plan.forwarding.next_peer_url if plan.forwarding else None
                        ),
                    }
                    for cluster_id, plan in route.cluster_routes.items()
                },
            )
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
            await self._establish_peer_connection(
                reply_to,
                fast_connect=True,
                dial_context="rpc_forward_reply",
                peer_target_count=1,
            )
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
        viewer_cluster_id = message.headers.source_cluster or self.settings.cluster_id

        selector = FunctionSelector(
            name=payload.command,
            function_id=payload.function_id,
            version_constraint=VersionConstraint.parse(payload.version_constraint)
            if payload.version_constraint
            else None,
        )
        requested_resources = frozenset(payload.resources)
        directed_to_local = not (
            (
                payload.target_cluster
                and payload.target_cluster != self.settings.cluster_id
            )
            or (payload.target_node and payload.target_node != self.cluster.local_url)
        )
        if not directed_to_local:
            local_matches: list[Any] = []
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

        implementation = self.registry.resolve(selector) if directed_to_local else None
        if (
            implementation is not None
            and requested_resources
            and not requested_resources.issubset(implementation.spec.resources)
        ):
            implementation = None

        if local_matches or implementation is not None:
            with self._request_viewer_context(viewer_cluster_id):
                try:
                    resolved = implementation or self.registry.resolve(selector)
                    if not resolved:
                        raise CommandNotFoundException(command_name=payload.command)
                    answer_payload = await resolved.call_async(
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
        from mpreg.core.model import FabricMessageEnvelope
        from mpreg.fabric.message import UnifiedMessage
        from mpreg.fabric.message_codec import unified_message_to_dict
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
            await self._establish_peer_connection(
                response.reply_to,
                fast_connect=True,
                dial_context="rpc_response_reply_to",
                peer_target_count=1,
            )
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
        startup_now = time.time()
        await self._announce_fabric_functions_if_due(now=startup_now, force=True)
        await self._announce_fabric_services_if_due(now=startup_now, force=True)
        await self._announce_fabric_cache_role()
        await self._announce_fabric_topics()
        await self._announce_fabric_node()
        await self._start_fabric_topic_refresh()
        await self._start_fabric_node_refresh()
        if self._fabric_queue_federation:
            await self._fabric_queue_federation.advertise_existing_queues()
        await self._start_fabric_catalog_refresh()
        await self._start_fabric_route_key_refresh()

    def _seed_discovery_resolver(self) -> None:
        if (
            not self._discovery_resolver
            or not self.settings.discovery_resolver_seed_on_start
        ):
            return
        if not self._fabric_control_plane:
            return
        self._discovery_resolver.seed_from_catalog(self._fabric_control_plane.catalog)

    def _start_discovery_resolver_prune(self) -> None:
        if not self._discovery_resolver or self._shutdown_event.is_set():
            return
        if (
            self._discovery_resolver_prune_task
            and not self._discovery_resolver_prune_task.done()
        ):
            return
        interval = float(self.settings.discovery_resolver_prune_interval_seconds)
        if interval <= 0:
            return
        interval = max(1.0, interval)

        async def _prune_loop() -> None:
            while not self._shutdown_event.is_set():
                if self._discovery_resolver:
                    self._discovery_resolver.prune_expired(now=time.time())
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=interval
                    )
                except TimeoutError:
                    continue

        self._discovery_resolver_prune_task = asyncio.create_task(_prune_loop())
        self._track_background_task(self._discovery_resolver_prune_task)

    def _start_discovery_summary_resolver_prune(self) -> None:
        if not self._discovery_summary_resolver or self._shutdown_event.is_set():
            return
        if (
            self._discovery_summary_resolver_prune_task
            and not self._discovery_summary_resolver_prune_task.done()
        ):
            return
        interval = float(
            self.settings.discovery_summary_resolver_prune_interval_seconds
        )
        if interval <= 0:
            return
        interval = max(1.0, interval)

        async def _prune_loop() -> None:
            while not self._shutdown_event.is_set():
                if self._discovery_summary_resolver:
                    self._discovery_summary_resolver.prune_expired(now=time.time())
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=interval
                    )
                except TimeoutError:
                    continue

        self._discovery_summary_resolver_prune_task = asyncio.create_task(_prune_loop())
        self._track_background_task(self._discovery_summary_resolver_prune_task)

    def _start_discovery_resolver_resync(self) -> None:
        if (
            not self._discovery_resolver
            or not self._fabric_control_plane
            or self._shutdown_event.is_set()
        ):
            return
        if (
            self._discovery_resolver_resync_task
            and not self._discovery_resolver_resync_task.done()
        ):
            return
        interval = float(self.settings.discovery_resolver_resync_interval_seconds)
        if interval <= 0:
            return
        interval = max(1.0, interval)

        async def _resync_loop() -> None:
            while not self._shutdown_event.is_set():
                if self._discovery_resolver and self._fabric_control_plane:
                    self._discovery_resolver.seed_from_catalog(
                        self._fabric_control_plane.catalog, now=time.time()
                    )
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=interval
                    )
                except TimeoutError:
                    continue

        self._discovery_resolver_resync_task = asyncio.create_task(_resync_loop())
        self._track_background_task(self._discovery_resolver_resync_task)

    def _start_discovery_summary_export(self) -> None:
        if not self.settings.discovery_summary_export_enabled:
            return
        if self._shutdown_event.is_set():
            return
        if (
            self._discovery_summary_export_task
            and not self._discovery_summary_export_task.done()
        ):
            return
        interval = float(self.settings.discovery_summary_export_interval_seconds)
        if interval <= 0:
            self._publish_discovery_summary()
            return

        async def _export_loop() -> None:
            while not self._shutdown_event.is_set():
                try:
                    self._publish_discovery_summary()
                except Exception as exc:
                    logger.warning(
                        "[{}] Summary export failed: {}",
                        self.settings.name,
                        exc,
                    )
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=interval
                    )
                except TimeoutError:
                    continue

        self._discovery_summary_export_task = asyncio.create_task(_export_loop())
        self._track_background_task(self._discovery_summary_export_task)

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
                "services": catalog.services.entry_count(),
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
        if not self._fabric_control_plane or self._shutdown_event.is_set():
            return
        if (
            self._fabric_catalog_refresh_task
            and not self._fabric_catalog_refresh_task.done()
        ):
            return

        async def _refresh_loop() -> None:
            while not self._shutdown_event.is_set():
                try:
                    refresh_now = time.time()
                    await self._announce_fabric_functions_if_due(now=refresh_now)
                except Exception as exc:
                    logger.warning(
                        "[{}] Fabric catalog refresh failed: {}",
                        self.settings.name,
                        exc,
                    )
                try:
                    await self._announce_fabric_services_if_due(now=refresh_now)
                except Exception as exc:
                    logger.warning(
                        "[{}] Fabric service refresh failed: {}",
                        self.settings.name,
                        exc,
                    )
                refresh_interval = self._fabric_catalog_refresh_interval_seconds()
                refresh_interval += self._fabric_catalog_refresh_jitter_seconds(
                    refresh_interval
                )
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=refresh_interval
                    )
                except TimeoutError:
                    continue

        self._fabric_catalog_refresh_task = asyncio.create_task(_refresh_loop())
        self._track_background_task(self._fabric_catalog_refresh_task)

    async def _announce_fabric_node(self, *, now: float | None = None) -> None:
        if not self._fabric_control_plane:
            return
        import uuid

        from mpreg.fabric.catalog_delta import RoutingCatalogDelta

        timestamp = now if now is not None else time.time()
        node = self._build_local_node_descriptor(now=timestamp)
        delta = RoutingCatalogDelta(
            update_id=str(uuid.uuid4()),
            cluster_id=self.settings.cluster_id,
            sent_at=timestamp,
            nodes=(node,),
        )
        await self._fabric_control_plane.broadcaster.broadcast(delta, now=timestamp)

    async def _start_fabric_node_refresh(self) -> None:
        if not self._fabric_control_plane or self._shutdown_event.is_set():
            return
        if self._fabric_node_refresh_task and not self._fabric_node_refresh_task.done():
            return

        async def _refresh_loop() -> None:
            while not self._shutdown_event.is_set():
                try:
                    await self._announce_fabric_node()
                except Exception as exc:
                    logger.warning(
                        "[{}] Fabric node refresh failed: {}",
                        self.settings.name,
                        exc,
                    )
                try:
                    if self._should_schedule_node_snapshot_batch():
                        self._schedule_node_snapshots_for_existing_peers_batch()
                except Exception as exc:
                    logger.warning(
                        "[{}] Fabric node snapshot refresh failed: {}",
                        self.settings.name,
                        exc,
                    )
                refresh_interval = self._fabric_node_refresh_interval_seconds()
                refresh_interval += self._fabric_catalog_refresh_jitter_seconds(
                    refresh_interval
                )
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=refresh_interval
                    )
                except TimeoutError:
                    continue

        self._fabric_node_refresh_task = asyncio.create_task(_refresh_loop())
        self._track_background_task(self._fabric_node_refresh_task)

    def _should_schedule_node_snapshot_batch(self) -> bool:
        target_hint = max(1, self._peer_dial_target_count_hint())
        if target_hint <= 1:
            return False

        pending_per_target = self._fabric_catalog_pending_messages_per_target(
            target_hint
        )
        # Allow larger burst budget only when discovery gap is still significant.
        base_pending_budget = max(2.0, target_hint**0.5)

        if not self._peer_directory:
            return pending_per_target <= base_pending_budget

        discovered_nodes = max(len(self._peer_directory.nodes()) - 1, 0)
        if discovered_nodes >= target_hint:
            return False
        discovery_gap = max(target_hint - discovered_nodes, 0)
        pending_budget = base_pending_budget + min(
            discovery_gap**0.5, base_pending_budget
        )
        return pending_per_target <= pending_budget

    def _fabric_node_refresh_interval_seconds(self) -> float:
        ttl_quarter_life = max(
            1.0, float(self.settings.fabric_catalog_ttl_seconds) * 0.25
        )
        target_hint = max(1, self._peer_dial_target_count_hint())
        gossip_interval = float(self.settings.gossip_interval)
        adaptive_interval = max(
            2.0,
            gossip_interval * max(target_hint**0.5, 2.0),
        )
        adaptive_interval *= self._fabric_catalog_backpressure_multiplier(target_hint)
        return min(ttl_quarter_life, adaptive_interval)

    def _fabric_catalog_refresh_interval_seconds(self) -> float:
        ttl_half_life = max(1.0, float(self.settings.fabric_catalog_ttl_seconds) * 0.5)
        target_hint = max(1, self._peer_dial_target_count_hint())
        gossip_interval = float(self.settings.gossip_interval)
        sqrt_scaled = gossip_interval * max(target_hint**0.5, 2.0)
        linear_scaled = gossip_interval * max(target_hint / 2.0, 2.0)
        adaptive_interval = max(2.0, max(sqrt_scaled, linear_scaled))
        adaptive_interval *= self._fabric_catalog_backpressure_multiplier(target_hint)
        return min(ttl_half_life, adaptive_interval)

    def _fabric_catalog_pending_messages_per_target(self, target_hint: int) -> float:
        if not self._fabric_control_plane:
            return 0.0
        gossip = self._fabric_control_plane.gossip
        if gossip is None:
            return 0.0
        from mpreg.fabric.gossip import GossipMessageType

        pending_catalog_updates = sum(
            1
            for message in gossip.pending_messages
            if message.message_type == GossipMessageType.CATALOG_UPDATE
        )
        return pending_catalog_updates / max(target_hint, 1)

    def _fabric_catalog_backpressure_multiplier(self, target_hint: int) -> float:
        pending_per_target = self._fabric_catalog_pending_messages_per_target(
            max(target_hint, 1)
        )
        if target_hint < 30:
            if pending_per_target >= 20.0:
                return 1.5
            return 1.0
        if pending_per_target >= 30.0:
            return 4.0
        if pending_per_target >= 20.0:
            return 3.0
        if pending_per_target >= 10.0:
            return 2.0
        if pending_per_target >= 5.0:
            return 1.5
        return 1.0

    def _fabric_catalog_refresh_jitter_seconds(self, base_interval: float) -> float:
        """Deterministic per-node jitter to avoid synchronized refresh bursts."""
        node_basis = sum(ord(ch) for ch in self.cluster.local_url) % 100
        jitter_fraction = node_basis / 1000.0
        return base_interval * jitter_fraction

    async def _start_fabric_topic_refresh(self) -> None:
        if not self._fabric_topic_announcer or self._shutdown_event.is_set():
            return
        if (
            self._fabric_topic_refresh_task
            and not self._fabric_topic_refresh_task.done()
        ):
            return
        refresh_interval = max(
            1.0, float(self.settings.fabric_catalog_ttl_seconds) * 0.5
        )

        async def _refresh_loop() -> None:
            while not self._shutdown_event.is_set():
                try:
                    await self._announce_fabric_topics()
                except Exception as exc:
                    logger.warning(
                        "[{}] Fabric topic refresh failed: {}",
                        self.settings.name,
                        exc,
                    )
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=refresh_interval
                    )
                except TimeoutError:
                    continue

        self._fabric_topic_refresh_task = asyncio.create_task(_refresh_loop())
        self._track_background_task(self._fabric_topic_refresh_task)

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

                known_peer_urls = set(self._get_all_peer_connections().keys())
                if self._peer_directory:
                    for node in self._peer_directory.nodes():
                        known_peer_urls.add(node.node_id)
                known_peer_urls.update(self.settings.peers or [])
                if self.settings.connect:
                    known_peer_urls.add(self.settings.connect)
                known_peer_urls.discard(self.cluster.local_url)
                if known_peer_urls:
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
        await self._stop_dns_gateway()

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

    def _build_node_load_metrics(
        self, status: RPCServerStatus | None, *, now: float
    ) -> NodeLoadMetrics | None:
        if status is None:
            return None
        stale_after = max(self.settings.gossip_interval * 3.0, 1.0)
        return NodeLoadMetrics.from_status(
            status, now=now, stale_after_seconds=stale_after
        )

    def _peer_snapshot_disconnected_stale_after_seconds(self) -> float:
        """Adaptive grace window before hiding disconnected peers from snapshots."""
        target_hint = max(1, self._peer_dial_target_count_hint())
        gossip_scaled = float(self.settings.gossip_interval) * max(
            4.0, target_hint**0.5
        )
        return max(1.0, gossip_scaled)

    def _should_exclude_disconnected_peer_snapshot(
        self,
        *,
        peer_url: str,
        status: RPCServerStatus | None,
        connection: Connection | None,
        now: float,
    ) -> bool:
        if status is None:
            return False
        if connection is not None and connection.is_connected:
            return False
        status_age_seconds = now - float(status.timestamp)
        stale_after = self._peer_snapshot_disconnected_stale_after_seconds()
        if status.status == "disconnected":
            should_exclude = status_age_seconds >= stale_after
            if should_exclude and PEER_SNAPSHOT_DIAG_ENABLED:
                logger.warning(
                    "[DIAG_PEER_SNAPSHOT] node={} action=exclude_disconnected "
                    "peer={} age={:.3f} stale_after={:.3f}",
                    self.cluster.local_url,
                    peer_url,
                    status_age_seconds,
                    stale_after,
                )
            return should_exclude
        target_hint = max(1, self._peer_dial_target_count_hint())
        if target_hint <= 20:
            stale_connected_after = max(
                stale_after * 3.0,
                float(self.settings.gossip_interval) * 6.0,
                2.0,
            )
            should_exclude = status_age_seconds >= stale_connected_after
            if should_exclude and PEER_SNAPSHOT_DIAG_ENABLED:
                logger.warning(
                    "[DIAG_PEER_SNAPSHOT] node={} action=exclude_stale_status "
                    "peer={} status={} age={:.3f} stale_after={:.3f} target_hint={}",
                    self.cluster.local_url,
                    peer_url,
                    status.status,
                    status_age_seconds,
                    stale_connected_after,
                    target_hint,
                )
            return should_exclude
        return False

    def _node_load_score(self, node_id: str) -> float | None:
        now = time.time()
        if node_id == self.cluster.local_url:
            status = self._build_status_message()
        else:
            status = self.peer_status.get(node_id)
        load = self._build_node_load_metrics(status, now=now)
        return load.load_score if load else None

    def _build_local_node_descriptor(self, *, now: float) -> NodeDescriptor:
        from mpreg.fabric.catalog import NodeDescriptor

        return NodeDescriptor(
            node_id=self.cluster.local_url,
            cluster_id=self.settings.cluster_id,
            region=self.settings.cache_region,
            resources=frozenset(self.settings.resources or set()),
            capabilities=self._fabric_node_capabilities(),
            transport_endpoints=self._fabric_transport_endpoints(),
            advertised_at=now,
            ttl_seconds=self.settings.fabric_catalog_ttl_seconds,
        )

    def _build_cluster_map_snapshot(
        self, *, now: float | None = None
    ) -> ClusterMapSnapshot:
        timestamp = now if now is not None else time.time()
        nodes: list[NodeDescriptor] = []
        if self._fabric_control_plane:
            from mpreg.fabric.index import NodeQuery

            nodes = self._fabric_control_plane.index.find_nodes(
                NodeQuery(), now=timestamp
            )
        if not any(node.node_id == self.cluster.local_url for node in nodes):
            nodes.append(self._build_local_node_descriptor(now=timestamp))

        local_status: RPCServerStatus | None = None
        snapshots: list[ClusterNodeSnapshot] = []
        for node in nodes:
            if node.node_id == self.cluster.local_url:
                local_status = local_status or self._build_status_message()
                status = local_status
            else:
                status = self.peer_status.get(node.node_id)
            load = self._build_node_load_metrics(status, now=timestamp)
            advertised_urls: tuple[str, ...] = ()
            if status and status.advertised_urls:
                advertised_urls = tuple(status.advertised_urls)
            elif node.node_id:
                advertised_urls = (node.node_id,)
            snapshots.append(
                ClusterNodeSnapshot(
                    node_id=node.node_id,
                    cluster_id=node.cluster_id,
                    region=node.region or None,
                    scope=node.scope or None,
                    resources=tuple(sorted(node.resources)),
                    capabilities=tuple(sorted(node.capabilities)),
                    tags=tuple(sorted(node.tags)),
                    transport_endpoints=node.transport_endpoints,
                    advertised_urls=advertised_urls,
                    advertised_at=node.advertised_at,
                    load=load,
                )
            )
        return ClusterMapSnapshot(
            cluster_id=self.settings.cluster_id,
            generated_at=timestamp,
            nodes=tuple(
                sorted(
                    snapshots,
                    key=lambda entry: (entry.cluster_id, entry.node_id),
                )
            ),
        )

    def _get_cluster_map_snapshot(self) -> Payload:
        snapshot = self._build_cluster_map_snapshot()
        return snapshot.to_dict()

    def _parse_request(
        self,
        request_cls: type[RequestT],
        payload: PayloadMapping | RequestT | None,
        overrides: PayloadMapping | None = None,
    ) -> RequestT:
        if isinstance(payload, request_cls):
            request = payload
            if overrides:
                request = apply_overrides(request, overrides)
            return request
        return parse_request(request_cls, payload, overrides)

    def _discovery_resolver_enabled(self) -> bool:
        return bool(self.settings.discovery_resolver_mode and self._discovery_resolver)

    def _discovery_summary_resolver_enabled(self) -> bool:
        return bool(
            self.settings.discovery_summary_resolver_mode
            and self._discovery_summary_resolver
        )

    def _namespace_policy_enabled(self) -> bool:
        return bool(
            self._namespace_policy_engine and self._namespace_policy_engine.enabled
        )

    def _enforce_discovery_rate_limit(
        self,
        *,
        command: str,
        viewer_cluster_id: str,
        viewer_tenant_id: TenantId | None,
        namespace: str | None,
    ) -> None:
        limiter = self._discovery_rate_limiter
        if limiter is None:
            return
        viewer_id = self._discovery_viewer_identity(viewer_cluster_id, viewer_tenant_id)
        key = DiscoveryRateLimitKey(
            viewer_id=viewer_id,
            command=command,
            namespace=namespace or None,
        )
        if limiter.allow(key, now=time.time()):
            return
        self._record_discovery_access_denial(
            command=command,
            viewer_cluster_id=viewer_cluster_id,
            viewer_tenant_id=viewer_tenant_id,
            namespace=namespace or None,
            reason="rate_limited",
        )
        raise MPREGException(
            rpc_error=RPCError(
                code=429,
                message="discovery_rate_limited",
                details=(
                    f"Rate limit exceeded for {command} "
                    f"(viewer={viewer_id}, namespace={namespace or ''})"
                ),
            )
        )

    def _effective_viewer_cluster_id(self, requested: str | None = None) -> str:
        viewer = _current_viewer_cluster_id.get()
        if viewer:
            return viewer
        if requested:
            return requested
        return self.settings.cluster_id

    @contextlib.contextmanager
    def _request_viewer_context(
        self,
        viewer_cluster_id: str | None,
        *,
        viewer_tenant_id: TenantId | None = None,
    ) -> Any:
        token_cluster = _current_viewer_cluster_id.set(viewer_cluster_id)
        token_tenant = _current_viewer_tenant_id.set(viewer_tenant_id)
        try:
            yield
        finally:
            _current_viewer_cluster_id.reset(token_cluster)
            _current_viewer_tenant_id.reset(token_tenant)

    def _effective_viewer_tenant_id(
        self, requested: TenantId | None = None
    ) -> TenantId | None:
        viewer = _current_viewer_tenant_id.get()
        if viewer:
            return viewer
        if not self.settings.discovery_tenant_mode:
            return None
        if requested and self.settings.discovery_tenant_allow_request_override:
            return requested
        if self.settings.discovery_tenant_default_id:
            return self.settings.discovery_tenant_default_id
        return None

    def _tenant_credentials(self) -> tuple[DiscoveryTenantCredential, ...]:
        return tuple(self.settings.discovery_tenant_credentials or ())

    def _tenant_id_from_transport(
        self, transport: TransportInterface
    ) -> TenantId | None:
        if not self.settings.discovery_tenant_mode:
            return None
        headers = transport.peer_headers
        credentials = self._tenant_credentials()
        if credentials:
            auth_value = headers.get("authorization")
            if auth_value:
                scheme, token = self._parse_authorization_header(auth_value)
                if scheme == "bearer" and token:
                    match = self._match_tenant_credentials(
                        credentials, scheme=scheme, token=token
                    )
                    if match:
                        return match
            api_key = headers.get("x-api-key")
            if api_key:
                match = self._match_tenant_credentials(
                    credentials, scheme="api-key", token=api_key.strip()
                )
                if match:
                    return match
            return None
        header = self.settings.discovery_tenant_header
        if not header:
            return None
        value = headers.get(header)
        if value is None:
            return None
        tenant = value.split(",", 1)[0].strip()
        return tenant or None

    @staticmethod
    def _parse_authorization_header(value: str) -> tuple[str, str]:
        parts = value.split(None, 1)
        if len(parts) == 2:
            return parts[0].strip().lower(), parts[1].strip()
        return "bearer", value.strip()

    @staticmethod
    def _match_tenant_credentials(
        credentials: tuple[DiscoveryTenantCredential, ...],
        *,
        scheme: str,
        token: str,
    ) -> TenantId | None:
        for entry in credentials:
            if entry.matches(scheme=scheme, token=token):
                return entry.tenant_id
        return None

    def _discovery_viewer_identity(
        self, viewer_cluster_id: str, viewer_tenant_id: TenantId | None
    ) -> ViewerId:
        if viewer_tenant_id:
            return f"tenant:{viewer_tenant_id}"
        return f"cluster:{viewer_cluster_id}"

    def _namespace_policy_viewer_decision(
        self,
        namespace: str,
        viewer_cluster_id: str | None,
        viewer_tenant_id: TenantId | None,
    ) -> NamespacePolicyDecision:
        engine = self._namespace_policy_engine
        if engine is None or not engine.enabled:
            return NamespacePolicyDecision(True, "policy_disabled")
        viewer = self._effective_viewer_cluster_id(viewer_cluster_id)
        tenant = self._effective_viewer_tenant_id(viewer_tenant_id)
        return engine.allows_viewer(namespace, viewer, viewer_tenant_id=tenant)

    def _namespace_policy_allows_viewer(
        self,
        namespace: str,
        viewer_cluster_id: str | None,
        viewer_tenant_id: TenantId | None = None,
    ) -> bool:
        if not namespace:
            return True
        decision = self._namespace_policy_viewer_decision(
            namespace, viewer_cluster_id, viewer_tenant_id
        )
        return decision.allowed

    def _topic_namespace_prefixes(self, patterns: Iterable[str]) -> tuple[str, ...]:
        prefixes: list[str] = []
        for pattern in patterns:
            prefix = pattern
            if "*" in prefix:
                prefix = prefix.split("*", 1)[0]
            if "#" in prefix:
                prefix = prefix.split("#", 1)[0]
            prefix = prefix.rstrip(".")
            prefixes.append(prefix)
        return tuple(prefixes)

    def _summary_topic(self, scope: EndpointScope | None, namespace: str | None) -> str:
        topic = DISCOVERY_SUMMARY_TOPIC
        if scope:
            topic = f"{topic}.{scope}"
        if namespace:
            topic = f"{topic}.{namespace}"
        return topic

    def _namespace_policy_allows_topic_patterns(
        self,
        patterns: Iterable[str],
        viewer_cluster_id: str | None,
        viewer_tenant_id: TenantId | None = None,
    ) -> bool:
        prefixes = self._topic_namespace_prefixes(patterns)
        if not prefixes:
            return True
        for prefix in prefixes:
            if prefix and not self._namespace_policy_allows_viewer(
                prefix, viewer_cluster_id, viewer_tenant_id
            ):
                return False
        return True

    def _resolver_allows_namespaces(self, namespaces: Iterable[str]) -> bool:
        allowed = self.settings.discovery_resolver_namespaces
        if not allowed:
            return True
        allowed_prefixes = [prefix.strip() for prefix in allowed if prefix.strip()]
        if not allowed_prefixes:
            return True
        for namespace in namespaces:
            if not namespace:
                continue
            for prefix in allowed_prefixes:
                if self._namespace_filter_matches(prefix, namespace):
                    return True
        return False

    def _summary_resolver_allows_namespaces(self, namespaces: Iterable[str]) -> bool:
        allowed = self.settings.discovery_summary_resolver_namespaces
        if not allowed:
            return True
        allowed_prefixes = [prefix.strip() for prefix in allowed if prefix.strip()]
        if not allowed_prefixes:
            return True
        for namespace in namespaces:
            if not namespace:
                continue
            for prefix in allowed_prefixes:
                if self._namespace_filter_matches(prefix, namespace):
                    return True
        return False

    def _summary_resolver_allows_scope(self, scope: EndpointScope | None) -> bool:
        scopes = tuple(self.settings.discovery_summary_resolver_scopes or ())
        if not scopes:
            return True
        if scope is None:
            return False
        return scope in scopes

    def _ingress_urls_for_cluster(
        self,
        cluster_id: str,
        *,
        scope: EndpointScope | None,
        scope_strict: bool,
        capabilities: frozenset[str],
        tags: frozenset[str],
        limit: int | None,
        now: float,
    ) -> tuple[str, ...]:
        if not cluster_id:
            return tuple()
        ingress_diag = self._summary_ingress_diagnostics_enabled()
        if ingress_diag:
            logger.warning(
                "[DIAG_SUMMARY_INGRESS] node={} action=ingress_lookup_start cluster={} "
                "scope={} scope_strict={} capabilities={} tags={} limit={}",
                self.cluster.local_url,
                cluster_id,
                scope,
                scope_strict,
                sorted(capabilities),
                sorted(tags),
                limit,
            )
        nodes: list[NodeDescriptor] = []
        resolver = (
            self._discovery_resolver if self._discovery_resolver_enabled() else None
        )
        if resolver:
            from mpreg.fabric.index import NodeQuery

            with resolver.locked():
                nodes = resolver.index.find_nodes(
                    NodeQuery(cluster_id=cluster_id), now=now
                )
        elif self._fabric_control_plane:
            from mpreg.fabric.index import NodeQuery

            nodes = self._fabric_control_plane.index.find_nodes(
                NodeQuery(cluster_id=cluster_id), now=now
            )

        if cluster_id == self.settings.cluster_id and not any(
            node.node_id == self.cluster.local_url for node in nodes
        ):
            nodes.append(self._build_local_node_descriptor(now=now))

        if scope is not None:
            from mpreg.fabric.catalog import endpoint_scope_rank

            scoped = [
                node
                for node in nodes
                if endpoint_scope_rank(node.scope) >= endpoint_scope_rank(scope)
            ]
            if scoped:
                nodes = scoped
            elif scope_strict:
                if ingress_diag:
                    logger.warning(
                        "[DIAG_SUMMARY_INGRESS] node={} action=ingress_lookup_end cluster={} "
                        "result=empty reason=strict_scope_no_match",
                        self.cluster.local_url,
                        cluster_id,
                    )
                return tuple()

        if capabilities:
            nodes = [node for node in nodes if capabilities.issubset(node.capabilities)]
        if tags:
            nodes = [node for node in nodes if tags.issubset(node.tags)]
        if not nodes:
            if ingress_diag:
                logger.warning(
                    "[DIAG_SUMMARY_INGRESS] node={} action=ingress_lookup_end cluster={} "
                    "result=empty reason=no_nodes_after_filters",
                    self.cluster.local_url,
                    cluster_id,
                )
            return tuple()

        urls: list[str] = []
        seen: set[str] = set()
        for node in sorted(nodes, key=lambda entry: entry.node_id):
            if node.node_id == self.cluster.local_url:
                status = self._build_status_message()
            else:
                status = self.peer_status.get(node.node_id)
            if status and status.advertised_urls:
                candidates = tuple(status.advertised_urls)
            elif node.node_id:
                candidates = (node.node_id,)
            else:
                candidates = ()
            for candidate in candidates:
                if not candidate or candidate in seen:
                    continue
                seen.add(candidate)
                urls.append(candidate)
                if limit is not None and len(urls) >= limit:
                    if ingress_diag:
                        logger.warning(
                            "[DIAG_SUMMARY_INGRESS] node={} action=ingress_lookup_end cluster={} "
                            "result=limited urls={}",
                            self.cluster.local_url,
                            cluster_id,
                            urls,
                        )
                    return tuple(urls)
        if ingress_diag:
            logger.warning(
                "[DIAG_SUMMARY_INGRESS] node={} action=ingress_lookup_end cluster={} "
                "result=ok urls={} node_count={}",
                self.cluster.local_url,
                cluster_id,
                urls,
                len(nodes),
            )
        return tuple(urls)

    def _summary_store_forward_enabled(self) -> bool:
        return bool(
            self.settings.discovery_summary_export_store_forward_seconds > 0
            and self.settings.discovery_summary_export_store_forward_max_messages > 0
        )

    def _topic_pattern_matches(self, topic: str, pattern: str) -> bool:
        if pattern == "#":
            return True
        escaped = pattern.replace(".", r"\.")
        escaped = escaped.replace("*", r"[^.]+")
        escaped = escaped.replace("#", r".*")
        regex_pattern = f"^{escaped}$"
        return re.match(regex_pattern, topic) is not None

    def _subscription_matches_topic(
        self, subscription: PubSubSubscription, topic: str
    ) -> bool:
        for pattern in subscription.patterns:
            if self._topic_pattern_matches(topic, pattern.pattern):
                return True
        return False

    def _normalize_discovery_scope(self, scope: str | None) -> str | None:
        if scope is None:
            return None
        value = scope.strip().lower()
        if not value:
            return None
        alias = "zone" if value == "cluster" else value
        if alias not in {"local", "zone", "region", "global"}:
            raise ValueError(f"Unsupported discovery scope: {scope}")
        return alias

    def _normalize_rpc_detail_level(self, detail_level: str | None) -> str:
        if detail_level is None:
            return "full"
        value = detail_level.strip().lower()
        if value not in {"summary", "full"}:
            return "full"
        return value

    def _rpc_namespace_for_name(self, name: str) -> str:
        if "." in name:
            return name.rsplit(".", 1)[0]
        return ""

    def _namespace_filter_matches(self, namespace_filter: str, value: str) -> bool:
        if not namespace_filter:
            return True
        if not value:
            return False
        if value == namespace_filter:
            return True
        return value.startswith(f"{namespace_filter}.")

    def _rpc_describe_identity_filters(
        self, request: RpcDescribeRequest
    ) -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
        digest_filter = frozenset(request.spec_digests)
        name_filter = frozenset(
            {name for name in request.function_names} | {request.function_name}
            if request.function_name
            else request.function_names
        )
        id_filter = frozenset(
            {identifier for identifier in request.function_ids} | {request.function_id}
            if request.function_id
            else request.function_ids
        )
        return digest_filter, name_filter, id_filter

    def _rpc_matches_query(
        self,
        *,
        name: str,
        namespace: str,
        summary: str | None,
        function_id: str,
        query: str,
    ) -> bool:
        needle = query.strip().lower()
        if not needle:
            return True
        if needle in name.lower():
            return True
        if namespace and needle in namespace.lower():
            return True
        if summary and needle in summary.lower():
            return True
        return needle in function_id.lower()

    def _schedule_discovery_query_refresh(self, coroutine: Any) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._track_background_task(loop.create_task(coroutine))

    def _scope_limits(self, scope: str | None) -> tuple[str | None, str | None]:
        if scope == "local":
            return self.settings.cluster_id, self.cluster.local_url
        if scope == "zone":
            return self.settings.cluster_id, None
        return None, None

    def _node_matches_namespace_filter(
        self, node: NodeDescriptor, namespace_filter: str
    ) -> bool:
        if node.node_id.startswith(namespace_filter):
            return True
        if node.cluster_id.startswith(namespace_filter):
            return True
        for value in node.resources:
            if value.startswith(namespace_filter):
                return True
        for value in node.capabilities:
            if value.startswith(namespace_filter):
                return True
        return False

    def _delta_namespaces(self, delta: RoutingCatalogDelta) -> tuple[str, ...]:
        namespaces: set[str] = set()

        for endpoint in delta.functions:
            name = endpoint.identity.name
            if "." in name:
                namespaces.add(name.rsplit(".", 1)[0])
            elif name:
                namespaces.add(name)

        for endpoint in delta.queues:
            name = endpoint.queue_name
            if "." in name:
                namespaces.add(name.rsplit(".", 1)[0])
            elif name:
                namespaces.add(name)

        for endpoint in delta.services:
            name = endpoint.namespace or ""
            if name:
                namespaces.add(name)
            elif endpoint.name:
                namespaces.add(endpoint.name)

        for subscription in delta.topics:
            for pattern in subscription.patterns:
                prefix = pattern
                if "*" in prefix:
                    prefix = prefix.split("*", 1)[0]
                if "#" in prefix:
                    prefix = prefix.split("#", 1)[0]
                prefix = prefix.rstrip(".")
                if prefix:
                    namespaces.add(prefix)

        if not namespaces and delta.cluster_id:
            namespaces.add(delta.cluster_id)

        return tuple(sorted(namespaces))

    def _publish_discovery_delta(
        self, delta: RoutingCatalogDelta, counts: dict[str, int]
    ) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        message = DiscoveryDeltaMessage(
            delta=delta,
            counts=CatalogDeltaCounts.from_dict(counts),
            namespaces=self._delta_namespaces(delta),
            source_node=self.cluster.local_url,
            source_cluster=self.settings.cluster_id,
            published_at=time.time(),
        )
        payload = message.to_dict()
        pubsub_message = PubSubMessage(
            message_id=delta.update_id,
            topic=DISCOVERY_DELTA_TOPIC,
            payload=payload,
            publisher=self.settings.name,
            headers={
                "event": "discovery.catalog_delta",
                "cluster_id": self.settings.cluster_id,
            },
            timestamp=time.time(),
        )
        notifications = self.topic_exchange.publish_message(pubsub_message)
        for notification in notifications:
            self._track_background_task(
                loop.create_task(self._send_notification_to_client(notification))
            )
        for namespace in message.namespaces:
            topic = f"{DISCOVERY_DELTA_TOPIC}.{namespace}"
            namespaced_message = PubSubMessage(
                message_id=str(ulid.new()),
                topic=topic,
                payload=payload,
                publisher=self.settings.name,
                headers={
                    "event": "discovery.catalog_delta",
                    "cluster_id": self.settings.cluster_id,
                },
                timestamp=pubsub_message.timestamp,
            )
            namespaced_notifications = self.topic_exchange.publish_message(
                namespaced_message
            )
            for notification in namespaced_notifications:
                self._track_background_task(
                    loop.create_task(self._send_notification_to_client(notification))
                )

    def _publish_discovery_summary(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        timestamp = time.time()
        export_scope = self.settings.discovery_summary_export_scope
        include_unscoped = bool(self.settings.discovery_summary_export_include_unscoped)
        resolver = (
            self._discovery_resolver if self._discovery_resolver_enabled() else None
        )
        if resolver:
            catalog = resolver.catalog
        elif self._fabric_control_plane:
            catalog = self._fabric_control_plane.index.catalog
        else:
            from mpreg.fabric.catalog import RoutingCatalog

            catalog = RoutingCatalog()

        summaries = summarize_functions(
            catalog,
            now=timestamp,
            region=self.settings.cache_region,
            ttl_seconds=self.settings.fabric_catalog_ttl_seconds,
            policy_engine=self._namespace_policy_engine,
            viewer_cluster_id=self.settings.cluster_id,
            enforce_summary_exports=True,
            source_cluster_id=self.settings.cluster_id,
            export_scope=export_scope,
            summary_scope=export_scope,
        )

        export_namespaces = tuple(
            prefix.strip()
            for prefix in self.settings.discovery_summary_export_namespaces
            if prefix and prefix.strip()
        )
        if export_namespaces:
            summaries = [
                summary
                for summary in summaries
                if any(
                    self._namespace_filter_matches(prefix, summary.namespace)
                    for prefix in export_namespaces
                )
            ]

        summaries = self._summary_export_state.prepare_export(
            summaries,
            timestamp=timestamp,
            ttl_seconds=self.settings.fabric_catalog_ttl_seconds,
            hold_down_seconds=float(
                self.settings.discovery_summary_export_hold_down_seconds
            ),
        )
        namespaces = tuple(
            sorted({summary.namespace for summary in summaries if summary.namespace})
        )
        self._summary_export_state.record_export(summaries, timestamp)
        message_scope = export_scope
        if DISCOVERY_SUMMARY_DIAG_ENABLED:
            logger.warning(
                "[DIAG_DISCOVERY_SUMMARY] node={} action=publish_prepare scope={} include_unscoped={} summaries={} namespaces={}",
                self.cluster.local_url,
                export_scope,
                include_unscoped,
                len(summaries),
                namespaces,
            )

        def _publish_summary_message(
            *,
            topic: str,
            message_summaries: tuple[ServiceSummary, ...],
            message_namespaces: tuple[str, ...],
            scope: EndpointScope | None,
            store_forward: bool,
        ) -> None:
            message = DiscoverySummaryMessage(
                summaries=message_summaries,
                namespaces=message_namespaces,
                source_node=self.cluster.local_url,
                source_cluster=self.settings.cluster_id,
                published_at=timestamp,
                scope=scope,
            )
            pubsub_message = PubSubMessage(
                message_id=str(ulid.new()),
                topic=topic,
                payload=message.to_dict(),
                publisher=self.settings.name,
                headers={
                    "event": "discovery.summary_export",
                    "cluster_id": self.settings.cluster_id,
                },
                timestamp=timestamp,
            )
            notifications = self.topic_exchange.publish_message(pubsub_message)
            for notification in notifications:
                self._track_background_task(
                    loop.create_task(self._send_notification_to_client(notification))
                )
            self._track_background_task(
                loop.create_task(self._forward_pubsub_publish(pubsub_message))
            )
            if store_forward:
                self._summary_export_state.record_store_forward(
                    pubsub_message,
                    now=timestamp,
                    max_age_seconds=float(
                        self.settings.discovery_summary_export_store_forward_seconds
                    ),
                    max_messages=int(
                        self.settings.discovery_summary_export_store_forward_max_messages
                    ),
                )
            if DISCOVERY_SUMMARY_DIAG_ENABLED:
                logger.warning(
                    "[DIAG_DISCOVERY_SUMMARY] node={} action=publish topic={} scope={} namespaces={} summaries={} notifications={} store_forward={}",
                    self.cluster.local_url,
                    topic,
                    scope,
                    message_namespaces,
                    len(message_summaries),
                    len(notifications),
                    store_forward,
                )

        scopes_to_publish: tuple[str | None, ...]
        if export_scope:
            scopes_to_publish = (export_scope,)
            if include_unscoped:
                scopes_to_publish = (export_scope, None)
        else:
            scopes_to_publish = (None,)

        summaries_by_namespace: dict[str, list[ServiceSummary]] = {}
        for summary in summaries:
            summaries_by_namespace.setdefault(summary.namespace, []).append(summary)
        for namespace, items in summaries_by_namespace.items():
            for scope in scopes_to_publish:
                topic = self._summary_topic(scope, namespace)
                _publish_summary_message(
                    topic=topic,
                    message_summaries=tuple(items),
                    message_namespaces=(namespace,),
                    scope=message_scope,
                    store_forward=True,
                )
        for scope in scopes_to_publish:
            base_topic = self._summary_topic(scope, None)
            _publish_summary_message(
                topic=base_topic,
                message_summaries=tuple(summaries),
                message_namespaces=namespaces,
                scope=message_scope,
                store_forward=scope is not None or include_unscoped,
            )

    def _get_cluster_map_v2(
        self,
        payload: PayloadMapping | ClusterMapRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(ClusterMapRequest, payload, kwargs)
        scope = self._normalize_discovery_scope(request.scope)
        timestamp = time.time()
        namespace_filter = (
            request.namespace_filter.strip() if request.namespace_filter else ""
        )
        viewer_cluster_id = self._effective_viewer_cluster_id(None)
        viewer_tenant_id = self._effective_viewer_tenant_id(request.viewer_tenant_id)
        self._enforce_discovery_rate_limit(
            command="cluster_map_v2",
            viewer_cluster_id=viewer_cluster_id,
            viewer_tenant_id=viewer_tenant_id,
            namespace=namespace_filter or None,
        )
        if namespace_filter:
            decision = self._namespace_policy_viewer_decision(
                namespace_filter,
                viewer_cluster_id,
                viewer_tenant_id,
            )
            if not decision.allowed:
                self._record_discovery_access_denial(
                    command="cluster_map_v2",
                    viewer_cluster_id=viewer_cluster_id,
                    viewer_tenant_id=viewer_tenant_id,
                    namespace=namespace_filter,
                    reason=decision.reason,
                )
                return ClusterMapResponse(
                    cluster_id=self.settings.cluster_id,
                    generated_at=timestamp,
                    nodes=tuple(),
                ).to_dict()
        scope_cluster_id, scope_node_id = self._scope_limits(scope)
        if (
            request.cluster_id
            and scope_cluster_id
            and request.cluster_id != scope_cluster_id
        ):
            return ClusterMapResponse(
                cluster_id=self.settings.cluster_id,
                generated_at=timestamp,
                nodes=tuple(),
            ).to_dict()
        cluster_id = request.cluster_id or scope_cluster_id
        requested_resources = frozenset(request.resources)
        requested_capabilities = frozenset(request.capabilities)
        resource_filter = requested_resources or None
        capability_filter = requested_capabilities or None
        from mpreg.fabric.catalog import endpoint_scope_rank

        def _scope_allows(endpoint_scope: str | None) -> bool:
            if scope is None:
                return True
            return endpoint_scope_rank(endpoint_scope) >= endpoint_scope_rank(scope)

        def _build_nodes(
            index: RoutingIndex | None, *, include_local: bool
        ) -> list[ClusterNodeSnapshot]:
            nodes: list[NodeDescriptor] = []
            if index is not None:
                from mpreg.fabric.index import NodeQuery

                nodes = index.find_nodes(
                    NodeQuery(
                        cluster_id=cluster_id,
                        node_id=scope_node_id,
                        resources=resource_filter,
                        capabilities=capability_filter,
                    ),
                    now=timestamp,
                )
            if (
                include_local
                and (cluster_id is None or cluster_id == self.settings.cluster_id)
                and (scope_node_id is None or scope_node_id == self.cluster.local_url)
            ):
                local_node = self._build_local_node_descriptor(now=timestamp)
                if resource_filter and not resource_filter.issubset(
                    local_node.resources
                ):
                    local_node = None
                if capability_filter and not capability_filter.issubset(
                    local_node.capabilities
                ):
                    local_node = None
                if local_node and not any(
                    node.node_id == local_node.node_id for node in nodes
                ):
                    nodes.append(local_node)

            if namespace_filter:
                nodes = [
                    node
                    for node in nodes
                    if self._node_matches_namespace_filter(node, namespace_filter)
                ]
            if scope is not None:
                nodes = [node for node in nodes if _scope_allows(node.scope)]

            local_status: RPCServerStatus | None = None
            snapshots: list[ClusterNodeSnapshot] = []
            for node in nodes:
                if node.node_id == self.cluster.local_url:
                    local_status = local_status or self._build_status_message()
                    status = local_status
                else:
                    status = self.peer_status.get(node.node_id)
                load = self._build_node_load_metrics(status, now=timestamp)
                advertised_urls: tuple[str, ...] = ()
                if status and status.advertised_urls:
                    advertised_urls = tuple(status.advertised_urls)
                elif node.node_id:
                    advertised_urls = (node.node_id,)
                snapshots.append(
                    ClusterNodeSnapshot(
                        node_id=node.node_id,
                        cluster_id=node.cluster_id,
                        region=node.region or None,
                        scope=node.scope or None,
                        resources=tuple(sorted(node.resources)),
                        capabilities=tuple(sorted(node.capabilities)),
                        tags=tuple(sorted(node.tags)),
                        transport_endpoints=node.transport_endpoints,
                        advertised_urls=advertised_urls,
                        advertised_at=node.advertised_at,
                        load=load,
                    )
                )

            ordered = sorted(
                snapshots, key=lambda entry: (entry.cluster_id, entry.node_id)
            )
            return ordered

        def _build_response(
            nodes: tuple[ClusterNodeSnapshot, ...], *, generated_at: Timestamp
        ) -> Payload:
            paged_nodes, next_token = paginate_items(
                nodes, limit=request.limit, page_token=request.page_token
            )
            response = ClusterMapResponse(
                cluster_id=self.settings.cluster_id,
                generated_at=generated_at,
                nodes=paged_nodes,
                next_page_token=next_token,
            )
            return response.to_dict()

        resolver = (
            self._discovery_resolver if self._discovery_resolver_enabled() else None
        )
        if resolver and resolver.query_cache_config.enabled:
            cache_key = ClusterMapQueryCacheKey(
                scope=scope,
                namespace_filter=namespace_filter or None,
                capabilities=tuple(sorted(requested_capabilities)),
                resources=tuple(sorted(requested_resources)),
                cluster_id=cluster_id,
            )
            cache_state: QueryCacheState
            cache_entry: ClusterMapQueryCacheEntry | None
            with resolver.locked():
                cache_state, cache_entry = resolver.query_cache.cluster_map_lookup(
                    cache_key, now=timestamp
                )
                if cache_state == QueryCacheState.MISS or cache_entry is None:
                    nodes = _build_nodes(resolver.index, include_local=False)
                    cache_entry = resolver.query_cache.cluster_map_store(
                        cache_key,
                        tuple(nodes),
                        now=timestamp,
                        negative=not nodes,
                    )
            if cache_state == QueryCacheState.STALE:

                async def _refresh_cache() -> None:
                    resolver_local = (
                        self._discovery_resolver
                        if self._discovery_resolver_enabled()
                        else None
                    )
                    if resolver_local is None:
                        return
                    now_refresh = time.time()
                    with resolver_local.locked():
                        nodes = _build_nodes(resolver_local.index, include_local=False)
                        resolver_local.query_cache.cluster_map_store(
                            cache_key,
                            tuple(nodes),
                            now=now_refresh,
                            negative=not nodes,
                        )
                        resolver_local.query_cache.cluster_map_refresh_recorded()

                self._schedule_discovery_query_refresh(_refresh_cache())
            if cache_entry is not None:
                return _build_response(
                    cache_entry.nodes, generated_at=cache_entry.generated_at
                )

        if resolver:
            with resolver.locked():
                nodes = _build_nodes(resolver.index, include_local=False)
            return _build_response(tuple(nodes), generated_at=timestamp)

        index = self._fabric_control_plane.index if self._fabric_control_plane else None
        nodes = _build_nodes(index, include_local=True)
        return _build_response(tuple(nodes), generated_at=timestamp)

    def _normalize_catalog_entry_type(self, entry_type: str) -> str:
        value = entry_type.strip().lower()
        aliases = {
            "function": "functions",
            "node": "nodes",
            "queue": "queues",
            "topic": "topics",
            "cache": "caches",
            "cache-profile": "cache_profiles",
            "cache_profile": "cache_profiles",
            "service": "services",
        }
        value = aliases.get(value, value)
        if value not in {
            "functions",
            "nodes",
            "queues",
            "topics",
            "services",
            "caches",
            "cache_profiles",
        }:
            raise ValueError(f"Unsupported catalog entry_type: {entry_type}")
        return value

    def _catalog_query(
        self,
        payload: PayloadMapping | CatalogQueryRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(CatalogQueryRequest, payload, kwargs)
        entry_type = self._normalize_catalog_entry_type(request.entry_type)
        scope = self._normalize_discovery_scope(request.scope)
        scope_cluster_id, scope_node_id = self._scope_limits(scope)
        timestamp = time.time()
        if (
            request.cluster_id
            and scope_cluster_id
            and request.cluster_id != scope_cluster_id
        ):
            return CatalogQueryResponse(
                entry_type=entry_type,
                generated_at=timestamp,
                items=tuple(),
            ).to_dict()
        cluster_id = request.cluster_id or scope_cluster_id
        node_id = request.node_id
        if scope_node_id:
            if node_id and node_id != scope_node_id:
                return CatalogQueryResponse(
                    entry_type=entry_type,
                    generated_at=timestamp,
                    items=tuple(),
                ).to_dict()
            node_id = scope_node_id

        requested_resources = frozenset(request.resources)
        requested_capabilities = frozenset(request.capabilities)
        requested_tags = frozenset(request.tags)
        resource_filter = requested_resources or None
        capability_filter = requested_capabilities or None
        tag_filter = requested_tags or None
        namespace_filter = request.namespace or ""
        namespace_filter = namespace_filter.strip()
        viewer_cluster_id = self._effective_viewer_cluster_id(request.viewer_cluster_id)
        viewer_tenant_id = self._effective_viewer_tenant_id(request.viewer_tenant_id)
        self._enforce_discovery_rate_limit(
            command="catalog_query",
            viewer_cluster_id=viewer_cluster_id,
            viewer_tenant_id=viewer_tenant_id,
            namespace=namespace_filter or None,
        )
        if namespace_filter:
            decision = self._namespace_policy_viewer_decision(
                namespace_filter,
                viewer_cluster_id,
                viewer_tenant_id,
            )
            if not decision.allowed:
                self._record_discovery_access_denial(
                    command="catalog_query",
                    viewer_cluster_id=viewer_cluster_id,
                    viewer_tenant_id=viewer_tenant_id,
                    namespace=namespace_filter,
                    reason=decision.reason,
                )
                return CatalogQueryResponse(
                    entry_type=entry_type,
                    generated_at=timestamp,
                    items=tuple(),
                ).to_dict()

        from mpreg.fabric.catalog import endpoint_scope_rank

        def _scope_allows(endpoint_scope: str | None) -> bool:
            if scope is None:
                return True
            return endpoint_scope_rank(endpoint_scope) >= endpoint_scope_rank(scope)

        def _build_items(
            index: RoutingIndex | None,
            catalog: RoutingCatalog | None,
            *,
            include_local: bool,
        ) -> list[Payload]:
            items: list[Payload] = []

            if entry_type == "nodes":
                nodes: list[NodeDescriptor] = []
                if index is not None:
                    from mpreg.fabric.index import NodeQuery

                    nodes = index.find_nodes(
                        NodeQuery(
                            cluster_id=cluster_id,
                            node_id=node_id,
                            resources=resource_filter,
                            capabilities=capability_filter,
                        ),
                        now=timestamp,
                    )
                if (
                    include_local
                    and (cluster_id is None or cluster_id == self.settings.cluster_id)
                    and (node_id is None or node_id == self.cluster.local_url)
                ):
                    local_node = self._build_local_node_descriptor(now=timestamp)
                    if resource_filter and not resource_filter.issubset(
                        local_node.resources
                    ):
                        local_node = None
                    if capability_filter and not capability_filter.issubset(
                        local_node.capabilities
                    ):
                        local_node = None
                    if local_node and not any(
                        node.node_id == local_node.node_id for node in nodes
                    ):
                        nodes.append(local_node)

                nodes = sorted(nodes, key=lambda node: (node.cluster_id, node.node_id))
                items = [node.to_dict() for node in nodes]

            elif entry_type == "functions":
                selector = FunctionSelector(
                    name=request.function_name,
                    function_id=request.function_id,
                    version_constraint=VersionConstraint.parse(
                        request.version_constraint
                    )
                    if request.version_constraint
                    else None,
                )
                endpoints: list[Any] = []
                if index is not None:
                    from mpreg.fabric.index import FunctionQuery

                    endpoints = index.find_functions(
                        FunctionQuery(
                            selector=selector,
                            resources=resource_filter or frozenset(),
                            cluster_id=cluster_id,
                            node_id=node_id,
                        ),
                        now=timestamp,
                    )
                elif include_local:
                    from mpreg.fabric.catalog import (
                        FunctionEndpoint,
                        normalize_endpoint_scope,
                    )

                    for spec in self.registry.specs():
                        endpoints.append(
                            FunctionEndpoint(
                                identity=spec.identity,
                                resources=spec.resources,
                                node_id=self.cluster.local_url,
                                cluster_id=self.settings.cluster_id,
                                scope=normalize_endpoint_scope(spec.scope),
                                tags=spec.tags,
                                rpc_summary=spec.summary(),
                                spec_digest=spec.spec_digest,
                                advertised_at=timestamp,
                                ttl_seconds=self.settings.fabric_catalog_ttl_seconds,
                            )
                        )

                if capability_filter and index is not None:
                    from mpreg.fabric.index import NodeQuery

                    nodes_with_caps = index.find_nodes(
                        NodeQuery(
                            cluster_id=cluster_id,
                            node_id=node_id,
                            capabilities=capability_filter,
                        ),
                        now=timestamp,
                    )
                    allowed_nodes = {node.node_id for node in nodes_with_caps}
                    endpoints = [
                        endpoint
                        for endpoint in endpoints
                        if endpoint.node_id in allowed_nodes
                    ]

                filtered_endpoints = []
                for endpoint in endpoints:
                    if namespace_filter:
                        endpoint_namespace = (
                            endpoint.rpc_summary.namespace
                            if endpoint.rpc_summary
                            else self._rpc_namespace_for_name(endpoint.identity.name)
                        )
                        if not self._namespace_filter_matches(
                            namespace_filter, endpoint_namespace
                        ):
                            continue
                    if not _scope_allows(endpoint.scope):
                        continue
                    if tag_filter and not tag_filter.issubset(endpoint.tags):
                        continue
                    if not self._namespace_policy_allows_viewer(
                        endpoint.identity.name,
                        viewer_cluster_id,
                        viewer_tenant_id,
                    ):
                        continue
                    filtered_endpoints.append(endpoint)

                filtered_endpoints = sorted(
                    filtered_endpoints,
                    key=lambda entry: (
                        entry.identity.name,
                        entry.identity.function_id,
                        entry.identity.version,
                        entry.cluster_id,
                        entry.node_id,
                    ),
                )
                items = [entry.to_dict() for entry in filtered_endpoints]

            elif entry_type == "queues":
                endpoints = []
                if index is not None:
                    if request.queue_name:
                        from mpreg.fabric.index import QueueQuery

                        endpoints = index.find_queues(
                            QueueQuery(
                                queue_name=request.queue_name, cluster_id=cluster_id
                            ),
                            now=timestamp,
                        )
                    elif catalog is not None:
                        endpoints = catalog.queues.entries(now=timestamp)

                filtered = []
                for endpoint in endpoints:
                    if cluster_id and endpoint.cluster_id != cluster_id:
                        continue
                    if node_id and endpoint.node_id != node_id:
                        continue
                    if namespace_filter and not self._namespace_filter_matches(
                        namespace_filter, endpoint.queue_name
                    ):
                        continue
                    if not _scope_allows(endpoint.scope):
                        continue
                    if tag_filter and not tag_filter.issubset(endpoint.tags):
                        continue
                    if not self._namespace_policy_allows_viewer(
                        endpoint.queue_name,
                        viewer_cluster_id,
                        viewer_tenant_id,
                    ):
                        continue
                    filtered.append(endpoint)

                filtered = sorted(
                    filtered,
                    key=lambda entry: (
                        entry.queue_name,
                        entry.cluster_id,
                        entry.node_id,
                    ),
                )
                items = [entry.to_dict() for entry in filtered]

            elif entry_type == "services":
                endpoints = []
                if index is not None:
                    from mpreg.fabric.index import ServiceQuery

                    endpoints = index.find_services(
                        ServiceQuery(
                            namespace=request.namespace or None,
                            name=request.service_name,
                            protocol=request.service_protocol,
                            port=request.service_port,
                            cluster_id=cluster_id,
                            node_id=node_id,
                        ),
                        now=timestamp,
                    )
                elif catalog is not None:
                    endpoints = catalog.services.entries(now=timestamp)

                filtered = []
                for endpoint in endpoints:
                    if cluster_id and endpoint.cluster_id != cluster_id:
                        continue
                    if node_id and endpoint.node_id != node_id:
                        continue
                    if namespace_filter and not self._namespace_filter_matches(
                        namespace_filter, endpoint.namespace
                    ):
                        continue
                    if request.service_name and endpoint.name != request.service_name:
                        continue
                    if (
                        request.service_protocol
                        and endpoint.protocol != request.service_protocol
                    ):
                        continue
                    if (
                        request.service_port is not None
                        and endpoint.port != request.service_port
                    ):
                        continue
                    if not _scope_allows(endpoint.scope):
                        continue
                    if tag_filter and not tag_filter.issubset(endpoint.tags):
                        continue
                    if capability_filter and not capability_filter.issubset(
                        endpoint.capabilities
                    ):
                        continue
                    if not self._namespace_policy_allows_viewer(
                        endpoint.namespace,
                        viewer_cluster_id,
                        viewer_tenant_id,
                    ):
                        continue
                    filtered.append(endpoint)

                filtered = sorted(
                    filtered,
                    key=lambda entry: (
                        entry.namespace,
                        entry.name,
                        entry.protocol,
                        entry.port,
                        entry.cluster_id,
                        entry.node_id,
                    ),
                )
                items = [entry.to_dict() for entry in filtered]

            elif entry_type == "topics":
                subscriptions = []
                if index is not None:
                    if request.topic:
                        from mpreg.fabric.index import TopicQuery

                        subscriptions = index.match_topics(
                            TopicQuery(topic=request.topic), now=timestamp
                        )
                    elif catalog is not None:
                        subscriptions = catalog.topics.entries(now=timestamp)

                filtered_subs = []
                for subscription in subscriptions:
                    if cluster_id and subscription.cluster_id != cluster_id:
                        continue
                    if node_id and subscription.node_id != node_id:
                        continue
                    if namespace_filter:
                        if not any(
                            self._namespace_filter_matches(namespace_filter, pattern)
                            for pattern in subscription.patterns
                        ):
                            continue
                    if not _scope_allows(subscription.scope):
                        continue
                    if tag_filter and not tag_filter.issubset(subscription.tags):
                        continue
                    if not self._namespace_policy_allows_topic_patterns(
                        subscription.patterns,
                        viewer_cluster_id,
                        viewer_tenant_id,
                    ):
                        continue
                    filtered_subs.append(subscription)

                filtered_subs = sorted(
                    filtered_subs,
                    key=lambda entry: (
                        entry.cluster_id,
                        entry.node_id,
                        entry.subscription_id,
                    ),
                )
                items = [entry.to_dict() for entry in filtered_subs]

            elif entry_type == "caches":
                cache_entries = []
                if catalog is not None:
                    cache_entries = catalog.caches.entries(now=timestamp)

                filtered = []
                for entry in cache_entries:
                    if cluster_id and entry.cluster_id != cluster_id:
                        continue
                    if node_id and entry.node_id != node_id:
                        continue
                    if not _scope_allows(entry.scope):
                        continue
                    if tag_filter and not tag_filter.issubset(entry.tags):
                        continue
                    filtered.append(entry)

                filtered = sorted(
                    filtered,
                    key=lambda entry: (
                        entry.role.value,
                        entry.cluster_id,
                        entry.node_id,
                    ),
                )
                items = [entry.to_dict() for entry in filtered]

            elif entry_type == "cache_profiles":
                profiles = []
                if catalog is not None:
                    profiles = catalog.cache_profiles.entries(now=timestamp)

                filtered = []
                for profile in profiles:
                    if cluster_id and profile.cluster_id != cluster_id:
                        continue
                    if node_id and profile.node_id != node_id:
                        continue
                    if not _scope_allows(profile.scope):
                        continue
                    if tag_filter and not tag_filter.issubset(profile.tags):
                        continue
                    filtered.append(profile)

                filtered = sorted(
                    filtered, key=lambda entry: (entry.cluster_id, entry.node_id)
                )
                items = [entry.to_dict() for entry in filtered]

            return items

        def _build_response(
            items: tuple[Payload, ...], *, generated_at: Timestamp
        ) -> Payload:
            paged_items, next_token = paginate_items(
                items, limit=request.limit, page_token=request.page_token
            )
            response = CatalogQueryResponse(
                entry_type=entry_type,
                generated_at=generated_at,
                items=paged_items,
                next_page_token=next_token,
            )
            return response.to_dict()

        resolver = (
            self._discovery_resolver if self._discovery_resolver_enabled() else None
        )
        if resolver and resolver.query_cache_config.enabled:
            cache_key = CatalogQueryCacheKey(
                entry_type=entry_type,
                scope=scope,
                namespace=namespace_filter or None,
                viewer_cluster_id=viewer_cluster_id,
                capabilities=tuple(sorted(requested_capabilities)),
                resources=tuple(sorted(requested_resources)),
                cluster_id=cluster_id,
                node_id=node_id,
                function_name=request.function_name,
                function_id=request.function_id,
                version_constraint=request.version_constraint,
                queue_name=request.queue_name,
                service_name=request.service_name,
                service_protocol=request.service_protocol,
                service_port=request.service_port,
                topic=request.topic,
                tags=tuple(sorted(requested_tags)),
            )
            cache_state: QueryCacheState
            cache_entry: CatalogQueryCacheEntry | None
            with resolver.locked():
                cache_state, cache_entry = resolver.query_cache.catalog_lookup(
                    cache_key, now=timestamp
                )
                if cache_state == QueryCacheState.MISS or cache_entry is None:
                    items = _build_items(
                        resolver.index, resolver.catalog, include_local=False
                    )
                    cache_entry = resolver.query_cache.catalog_store(
                        cache_key,
                        tuple(items),
                        now=timestamp,
                        negative=not items,
                    )
            if cache_state == QueryCacheState.STALE:

                async def _refresh_cache() -> None:
                    resolver_local = (
                        self._discovery_resolver
                        if self._discovery_resolver_enabled()
                        else None
                    )
                    if resolver_local is None:
                        return
                    now_refresh = time.time()
                    with resolver_local.locked():
                        items = _build_items(
                            resolver_local.index,
                            resolver_local.catalog,
                            include_local=False,
                        )
                        resolver_local.query_cache.catalog_store(
                            cache_key,
                            tuple(items),
                            now=now_refresh,
                            negative=not items,
                        )
                        resolver_local.query_cache.catalog_refresh_recorded()

                self._schedule_discovery_query_refresh(_refresh_cache())
            if cache_entry is not None:
                return _build_response(
                    cache_entry.items, generated_at=cache_entry.generated_at
                )

        if resolver:
            with resolver.locked():
                items = _build_items(
                    resolver.index, resolver.catalog, include_local=False
                )
            return _build_response(tuple(items), generated_at=timestamp)

        if self._fabric_control_plane:
            index = self._fabric_control_plane.index
            catalog = index.catalog
        else:
            index = None
            catalog = None

        items = _build_items(index, catalog, include_local=True)
        return _build_response(tuple(items), generated_at=timestamp)

    def _dns_register(
        self,
        payload: PayloadMapping | DnsRegisterRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(DnsRegisterRequest, payload, kwargs)
        from mpreg.datastructures.service_spec import DEFAULT_SERVICE_SCOPE, ServiceSpec

        spec = ServiceSpec(
            name=request.name,
            namespace=request.namespace,
            protocol=request.protocol,
            port=request.port,
            targets=request.targets,
            scope=request.scope or DEFAULT_SERVICE_SCOPE,
            tags=frozenset(request.tags),
            capabilities=frozenset(request.capabilities),
            metadata=request.metadata,
            priority=request.priority,
            weight=request.weight,
            ttl_seconds=request.ttl_seconds,
        )
        registration = self._service_registry.register(spec, now=time.time())
        self._publish_fabric_service_update(registration)
        response = DnsRegisterResponse(
            registration_id=registration.registration_id,
            service=spec,
            registered_at=registration.registered_at,
        )
        return response.to_dict()

    def _dns_unregister(
        self,
        payload: PayloadMapping | DnsUnregisterRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(DnsUnregisterRequest, payload, kwargs)
        removed = self._service_registry.unregister(
            namespace=request.namespace,
            name=request.name,
            protocol=request.protocol,
            port=request.port,
        )
        if removed is not None:
            self._publish_fabric_service_removal(
                namespace=request.namespace,
                name=request.name,
                protocol=request.protocol,
                port=request.port,
            )
        response = DnsUnregisterResponse(
            removed=removed is not None,
            generated_at=time.time(),
        )
        return response.to_dict()

    def _dns_list(
        self,
        payload: PayloadMapping | DnsListRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(DnsListRequest, payload, kwargs)
        catalog_request = CatalogQueryRequest(
            entry_type="services",
            namespace=request.namespace,
            scope=request.scope,
            viewer_cluster_id=self._effective_viewer_cluster_id(None),
            viewer_tenant_id=self._effective_viewer_tenant_id(None),
            capabilities=request.capabilities,
            resources=tuple(),
            tags=request.tags,
            cluster_id=request.cluster_id,
            node_id=request.node_id,
            service_name=request.name,
            service_protocol=request.protocol,
            service_port=request.port,
            limit=request.limit,
            page_token=request.page_token,
        )
        catalog_result = self._catalog_query(catalog_request)
        response = DnsListResponse(
            generated_at=float(catalog_result.get("generated_at", time.time())),
            items=tuple(catalog_result.get("items", []) or []),
            next_page_token=(
                str(catalog_result.get("next_page_token"))
                if catalog_result.get("next_page_token") is not None
                else None
            ),
        )
        return response.to_dict()

    def _dns_describe(
        self,
        payload: PayloadMapping | DnsDescribeRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(DnsDescribeRequest, payload, kwargs)
        catalog_request = CatalogQueryRequest(
            entry_type="services",
            namespace=request.namespace,
            scope=request.scope,
            viewer_cluster_id=self._effective_viewer_cluster_id(None),
            viewer_tenant_id=self._effective_viewer_tenant_id(None),
            capabilities=request.capabilities,
            resources=tuple(),
            tags=request.tags,
            cluster_id=request.cluster_id,
            node_id=request.node_id,
            service_name=request.name,
            service_protocol=request.protocol,
            service_port=request.port,
            limit=request.limit,
            page_token=request.page_token,
        )
        catalog_result = self._catalog_query(catalog_request)
        response = DnsDescribeResponse(
            generated_at=float(catalog_result.get("generated_at", time.time())),
            items=tuple(catalog_result.get("items", []) or []),
            next_page_token=(
                str(catalog_result.get("next_page_token"))
                if catalog_result.get("next_page_token") is not None
                else None
            ),
        )
        return response.to_dict()

    def _rpc_list(
        self, payload: PayloadMapping | RpcListRequest | None = None, **kwargs: object
    ) -> Payload:
        request = self._parse_request(RpcListRequest, payload, kwargs)
        catalog_request = CatalogQueryRequest.from_rpc_list(
            request, include_pagination=not request.query
        )
        catalog_result = self._catalog_query(catalog_request)
        items_payload = catalog_result.get("items", [])
        from mpreg.fabric.catalog import FunctionEndpoint

        endpoints = [
            FunctionEndpoint.from_dict(item)  # type: ignore[arg-type]
            for item in items_payload
        ]
        if request.query:
            endpoints = [
                endpoint
                for endpoint in endpoints
                if self._rpc_matches_query(
                    name=endpoint.identity.name,
                    namespace=endpoint.rpc_summary.namespace
                    if endpoint.rpc_summary
                    else self._rpc_namespace_for_name(endpoint.identity.name),
                    summary=endpoint.rpc_summary.summary
                    if endpoint.rpc_summary
                    else None,
                    function_id=endpoint.identity.function_id,
                    query=request.query,
                )
            ]
        list_items = []
        for endpoint in endpoints:
            summary = endpoint.rpc_summary
            namespace = (
                summary.namespace
                if summary is not None
                else self._rpc_namespace_for_name(endpoint.identity.name)
            )
            list_items.append(
                RpcListItem(
                    identity=endpoint.identity,
                    namespace=namespace,
                    node_id=endpoint.node_id,
                    cluster_id=endpoint.cluster_id,
                    resources=tuple(sorted(endpoint.resources)),
                    tags=tuple(sorted(endpoint.tags)),
                    scope=endpoint.scope,
                    summary=summary,
                    spec_digest=endpoint.spec_digest
                    or (summary.spec_digest if summary else None),
                )
            )
        if request.query:
            paged_items, next_token = paginate_items(
                list_items, limit=request.limit, page_token=request.page_token
            )
        else:
            paged_items = tuple(list_items)
            next_token = (
                str(catalog_result.get("next_page_token"))
                if catalog_result.get("next_page_token") is not None
                else None
            )
        response = RpcListResponse(
            generated_at=float(catalog_result.get("generated_at", time.time())),
            items=paged_items,
            next_page_token=next_token,
        )
        return response.to_dict()

    def _rpc_describe_catalog_endpoints(
        self, request: RpcDescribeRequest
    ) -> tuple[tuple[FunctionEndpoint, ...], str | None, float]:
        catalog_request = CatalogQueryRequest.from_rpc_describe(
            request, include_pagination=not request.query
        )
        catalog_result = self._catalog_query(catalog_request)
        from mpreg.fabric.catalog import FunctionEndpoint

        endpoints = [
            FunctionEndpoint.from_dict(item)  # type: ignore[arg-type]
            for item in catalog_result.get("items", [])
        ]
        digest_filter, name_filter, id_filter = self._rpc_describe_identity_filters(
            request
        )
        if digest_filter or name_filter or id_filter:
            filtered = []
            for endpoint in endpoints:
                endpoint_digest = endpoint.spec_digest
                if endpoint_digest is None and endpoint.rpc_summary is not None:
                    endpoint_digest = endpoint.rpc_summary.spec_digest
                if digest_filter and (
                    endpoint_digest is None or endpoint_digest not in digest_filter
                ):
                    continue
                if name_filter and endpoint.identity.name not in name_filter:
                    continue
                if id_filter and endpoint.identity.function_id not in id_filter:
                    continue
                filtered.append(endpoint)
            endpoints = filtered
        if request.query:
            endpoints = [
                endpoint
                for endpoint in endpoints
                if self._rpc_matches_query(
                    name=endpoint.identity.name,
                    namespace=endpoint.rpc_summary.namespace
                    if endpoint.rpc_summary
                    else self._rpc_namespace_for_name(endpoint.identity.name),
                    summary=endpoint.rpc_summary.summary
                    if endpoint.rpc_summary
                    else None,
                    function_id=endpoint.identity.function_id,
                    query=request.query,
                )
            ]
        if request.query:
            paged_endpoints, next_token = paginate_items(
                endpoints, limit=request.limit, page_token=request.page_token
            )
        else:
            paged_endpoints = tuple(endpoints)
            next_token = (
                str(catalog_result.get("next_page_token"))
                if catalog_result.get("next_page_token") is not None
                else None
            )
        generated_at = float(catalog_result.get("generated_at", time.time()))
        return tuple(paged_endpoints), next_token, generated_at

    def _rpc_describe_local(
        self,
        payload: PayloadMapping | RpcDescribeRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(RpcDescribeRequest, payload, kwargs)
        detail_level = self._normalize_rpc_detail_level(request.detail_level)
        scope = self._normalize_discovery_scope(request.scope)
        namespace_filter = request.namespace.strip() if request.namespace else ""
        viewer_cluster_id = self._effective_viewer_cluster_id(request.viewer_cluster_id)
        viewer_tenant_id = self._effective_viewer_tenant_id(request.viewer_tenant_id)
        self._enforce_discovery_rate_limit(
            command="rpc_describe",
            viewer_cluster_id=viewer_cluster_id,
            viewer_tenant_id=viewer_tenant_id,
            namespace=namespace_filter or None,
        )
        if namespace_filter:
            decision = self._namespace_policy_viewer_decision(
                namespace_filter,
                viewer_cluster_id,
                viewer_tenant_id,
            )
            if not decision.allowed:
                self._record_discovery_access_denial(
                    command="rpc_describe",
                    viewer_cluster_id=viewer_cluster_id,
                    viewer_tenant_id=viewer_tenant_id,
                    namespace=namespace_filter,
                    reason=decision.reason,
                )
                response = RpcDescribeResponse(
                    generated_at=time.time(),
                    items=tuple(),
                    errors=tuple(),
                )
                return response.to_dict()
        if request.cluster_id and request.cluster_id != self.settings.cluster_id:
            response = RpcDescribeResponse(
                generated_at=time.time(),
                items=tuple(),
                errors=tuple(),
            )
            return response.to_dict()
        if request.node_id and request.node_id != self.cluster.local_url:
            response = RpcDescribeResponse(
                generated_at=time.time(),
                items=tuple(),
                errors=tuple(),
            )
            return response.to_dict()

        from mpreg.fabric.catalog import endpoint_scope_rank

        requested_resources = frozenset(request.resources)
        requested_tags = frozenset(request.tags)
        requested_capabilities = frozenset(request.capabilities)
        digest_filter, name_filter, id_filter = self._rpc_describe_identity_filters(
            request
        )
        version_constraint = (
            VersionConstraint.parse(request.version_constraint)
            if request.version_constraint
            else None
        )

        items = []
        for spec in self.registry.specs():
            if namespace_filter and not self._namespace_filter_matches(
                namespace_filter, spec.namespace
            ):
                continue
            if scope and endpoint_scope_rank(spec.scope) < endpoint_scope_rank(scope):
                continue
            if requested_resources and not requested_resources.issubset(spec.resources):
                continue
            if requested_tags and not requested_tags.issubset(spec.tags):
                continue
            if requested_capabilities and not requested_capabilities.issubset(
                spec.capabilities
            ):
                continue
            if digest_filter and spec.spec_digest not in digest_filter:
                continue
            if name_filter and spec.identity.name not in name_filter:
                continue
            if id_filter and spec.identity.function_id not in id_filter:
                continue
            if version_constraint and not version_constraint.matches(
                spec.identity.version
            ):
                continue
            if request.query and not self._rpc_matches_query(
                name=spec.identity.name,
                namespace=spec.namespace,
                summary=spec.doc.summary,
                function_id=spec.identity.function_id,
                query=request.query,
            ):
                continue
            if not self._namespace_policy_allows_viewer(
                spec.identity.name,
                viewer_cluster_id,
                viewer_tenant_id,
            ):
                continue
            summary = spec.summary()
            spec_payload = spec if detail_level == "full" else None
            items.append(
                RpcDescribeItem(
                    identity=spec.identity,
                    namespace=spec.namespace,
                    node_id=self.cluster.local_url,
                    cluster_id=self.settings.cluster_id,
                    resources=tuple(sorted(spec.resources)),
                    tags=tuple(sorted(spec.tags)),
                    scope=spec.scope,
                    summary=summary,
                    spec=spec_payload,
                    spec_digest=spec.spec_digest,
                )
            )
        items = sorted(
            items,
            key=lambda entry: (
                entry.identity.name,
                entry.identity.function_id,
                entry.identity.version,
            ),
        )
        paged_items, next_token = paginate_items(
            items, limit=request.limit, page_token=request.page_token
        )
        response = RpcDescribeResponse(
            generated_at=time.time(),
            items=paged_items,
            errors=tuple(),
            next_page_token=next_token,
        )
        return response.to_dict()

    async def _rpc_describe(
        self,
        payload: PayloadMapping | RpcDescribeRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(RpcDescribeRequest, payload, kwargs)
        mode = request.mode.strip().lower()
        if mode in {"local", "node"}:
            return self._rpc_describe_local(request)
        if mode == "catalog":
            response = self._rpc_describe_catalog(request)
            return response.to_dict()
        if mode == "auto":
            response = await self._rpc_describe_auto(request)
            return response.to_dict()
        if mode != "scatter":
            raise ValueError(f"Unsupported rpc_describe mode: {request.mode}")
        response = await self._rpc_describe_scatter(request)
        return response.to_dict()

    def _rpc_describe_catalog(self, request: RpcDescribeRequest) -> RpcDescribeResponse:
        detail_level = self._normalize_rpc_detail_level(request.detail_level)
        endpoints, next_token, generated_at = self._rpc_describe_catalog_endpoints(
            request
        )
        items = []
        for endpoint in endpoints:
            summary = endpoint.rpc_summary
            namespace = (
                summary.namespace
                if summary is not None
                else self._rpc_namespace_for_name(endpoint.identity.name)
            )
            spec_payload = endpoint.rpc_spec if detail_level == "full" else None
            items.append(
                RpcDescribeItem(
                    identity=endpoint.identity,
                    namespace=namespace,
                    node_id=endpoint.node_id,
                    cluster_id=endpoint.cluster_id,
                    resources=tuple(sorted(endpoint.resources)),
                    tags=tuple(sorted(endpoint.tags)),
                    scope=endpoint.scope,
                    summary=summary,
                    spec=spec_payload,
                    spec_digest=endpoint.spec_digest
                    or (summary.spec_digest if summary else None),
                )
            )
        return RpcDescribeResponse(
            generated_at=generated_at,
            items=tuple(items),
            errors=tuple(),
            next_page_token=next_token,
        )

    async def _rpc_describe_auto(
        self, request: RpcDescribeRequest
    ) -> RpcDescribeResponse:
        detail_level = self._normalize_rpc_detail_level(request.detail_level)
        endpoints, next_token, generated_at = self._rpc_describe_catalog_endpoints(
            request
        )
        catalog_items = []
        for endpoint in endpoints:
            summary = endpoint.rpc_summary
            namespace = (
                summary.namespace
                if summary is not None
                else self._rpc_namespace_for_name(endpoint.identity.name)
            )
            spec_payload = endpoint.rpc_spec if detail_level == "full" else None
            catalog_items.append(
                RpcDescribeItem(
                    identity=endpoint.identity,
                    namespace=namespace,
                    node_id=endpoint.node_id,
                    cluster_id=endpoint.cluster_id,
                    resources=tuple(sorted(endpoint.resources)),
                    tags=tuple(sorted(endpoint.tags)),
                    scope=endpoint.scope,
                    summary=summary,
                    spec=spec_payload,
                    spec_digest=endpoint.spec_digest
                    or (summary.spec_digest if summary else None),
                )
            )
        if detail_level != "full":
            return RpcDescribeResponse(
                generated_at=generated_at,
                items=tuple(catalog_items),
                errors=tuple(),
                next_page_token=next_token,
            )

        missing_endpoints = tuple(
            endpoint for endpoint in endpoints if endpoint.rpc_spec is None
        )
        if not missing_endpoints:
            return RpcDescribeResponse(
                generated_at=generated_at,
                items=tuple(catalog_items),
                errors=tuple(),
                next_page_token=next_token,
            )

        scatter_response = await self._rpc_describe_scatter_missing(
            request,
            endpoints=missing_endpoints,
            generated_at=generated_at,
            next_page_token=next_token,
        )
        items_by_key = {
            (
                item.identity.name,
                item.identity.function_id,
                item.identity.version,
                item.cluster_id,
                item.node_id,
            ): item
            for item in catalog_items
        }
        for item in scatter_response.items:
            key = (
                item.identity.name,
                item.identity.function_id,
                item.identity.version,
                item.cluster_id,
                item.node_id,
            )
            existing = items_by_key.get(key)
            if existing is None or existing.spec is None:
                items_by_key[key] = item
        merged_items = sorted(
            items_by_key.values(),
            key=lambda entry: (
                entry.identity.name,
                entry.identity.function_id,
                entry.identity.version,
                entry.cluster_id,
                entry.node_id,
            ),
        )
        return RpcDescribeResponse(
            generated_at=generated_at,
            items=tuple(merged_items),
            errors=scatter_response.errors,
            next_page_token=next_token,
        )

    async def _rpc_describe_scatter_missing(
        self,
        request: RpcDescribeRequest,
        *,
        endpoints: tuple[FunctionEndpoint, ...],
        generated_at: float,
        next_page_token: str | None,
    ) -> RpcDescribeResponse:
        if not endpoints:
            return RpcDescribeResponse(
                generated_at=generated_at,
                items=tuple(),
                errors=tuple(),
                next_page_token=next_page_token,
            )
        grouped: dict[tuple[str, str], list[FunctionEndpoint]] = {}
        for endpoint in endpoints:
            key = (endpoint.cluster_id, endpoint.node_id)
            grouped.setdefault(key, []).append(endpoint)
        if request.max_nodes is not None and len(grouped) > request.max_nodes:
            selected_keys = sorted(grouped.keys())[: request.max_nodes]
            grouped = {key: grouped[key] for key in selected_keys}

        errors: list[RpcDescribeError] = []
        items: list[RpcDescribeItem] = []
        timeout = request.timeout_seconds
        if timeout is None:
            timeout = 5.0
        elif timeout <= 0:
            timeout = None
        parallel_limit = min(16, len(grouped) or 1)
        semaphore = asyncio.Semaphore(parallel_limit)

        async def _describe_node(
            cluster_id: str, node_id: str, node_endpoints: list[FunctionEndpoint]
        ) -> None:
            digests = [
                entry.spec_digest for entry in node_endpoints if entry.spec_digest
            ]
            names = [
                entry.identity.name for entry in node_endpoints if not entry.spec_digest
            ]
            ids = [
                entry.identity.function_id
                for entry in node_endpoints
                if not entry.spec_digest
            ]
            allowed_identities = {
                (
                    entry.identity.name,
                    entry.identity.function_id,
                    str(entry.identity.version),
                )
                for entry in node_endpoints
            }
            local_request = RpcDescribeRequest(
                mode="local",
                detail_level=request.detail_level,
                namespace=request.namespace,
                scope=request.scope,
                viewer_cluster_id=request.viewer_cluster_id,
                viewer_tenant_id=request.viewer_tenant_id,
                capabilities=request.capabilities,
                resources=request.resources,
                tags=request.tags,
                function_name=request.function_name,
                function_id=request.function_id,
                version_constraint=request.version_constraint,
                query=request.query,
                spec_digests=tuple(digests),
                function_names=tuple(names),
                function_ids=tuple(ids),
            )
            if (
                cluster_id == self.settings.cluster_id
                and node_id == self.cluster.local_url
            ):
                try:
                    response = RpcDescribeResponse.from_dict(
                        self._rpc_describe_local(local_request)
                    )
                except Exception as exc:
                    errors.append(
                        RpcDescribeError(
                            node_id=node_id,
                            cluster_id=cluster_id,
                            error=str(exc),
                        )
                    )
                    return
                for item in response.items:
                    identity_key = (
                        item.identity.name,
                        item.identity.function_id,
                        str(item.identity.version),
                    )
                    if identity_key not in allowed_identities:
                        continue
                    items.append(item)
                return

            async with semaphore:
                try:
                    request_payload = local_request.to_dict()
                    rpc_command = RPCCommand(
                        name="rpc_describe_local",
                        fun="rpc_describe_local",
                        args=(request_payload,),
                        locs=frozenset(),
                        target_cluster=cluster_id,
                    )
                    call = self.cluster._execute_remote_command(
                        rpc_command,
                        {},
                        Connection(url=node_id),
                        target_cluster=cluster_id,
                        target_node=node_id,
                        retry_remaining=0,
                        allow_fallback=False,
                    )
                    result = (
                        await asyncio.wait_for(call, timeout=timeout)
                        if timeout
                        else await call
                    )
                except TimeoutError:
                    errors.append(
                        RpcDescribeError(
                            node_id=node_id,
                            cluster_id=cluster_id,
                            error="timeout",
                        )
                    )
                    return
                except Exception as exc:
                    errors.append(
                        RpcDescribeError(
                            node_id=node_id,
                            cluster_id=cluster_id,
                            error=str(exc),
                        )
                    )
                    return
                if not isinstance(result, dict):
                    errors.append(
                        RpcDescribeError(
                            node_id=node_id,
                            cluster_id=cluster_id,
                            error="unexpected_response_type",
                        )
                    )
                    return
                if (
                    "error" in result
                    and result.get("command") == "rpc_describe_local"
                    and result.get("error_type")
                ):
                    errors.append(
                        RpcDescribeError(
                            node_id=node_id,
                            cluster_id=cluster_id,
                            error=str(result.get("error")),
                        )
                    )
                    return
                response = RpcDescribeResponse.from_dict(result)
                for item in response.items:
                    identity_key = (
                        item.identity.name,
                        item.identity.function_id,
                        str(item.identity.version),
                    )
                    if identity_key not in allowed_identities:
                        continue
                    items.append(item)

        await asyncio.gather(
            *[
                _describe_node(cluster_id, node_id, node_endpoints)
                for (cluster_id, node_id), node_endpoints in grouped.items()
            ],
            return_exceptions=True,
        )

        items = sorted(
            items,
            key=lambda entry: (
                entry.identity.name,
                entry.identity.function_id,
                entry.identity.version,
                entry.cluster_id,
                entry.node_id,
            ),
        )
        return RpcDescribeResponse(
            generated_at=generated_at,
            items=tuple(items),
            errors=tuple(errors),
            next_page_token=next_page_token,
        )

    async def _rpc_describe_scatter(
        self, request: RpcDescribeRequest
    ) -> RpcDescribeResponse:
        catalog_request = CatalogQueryRequest.from_rpc_describe(
            request, include_pagination=not request.query
        )
        catalog_result = self._catalog_query(catalog_request)
        from mpreg.fabric.catalog import FunctionEndpoint

        endpoints = [
            FunctionEndpoint.from_dict(item)  # type: ignore[arg-type]
            for item in catalog_result.get("items", [])
        ]
        digest_filter, name_filter, id_filter = self._rpc_describe_identity_filters(
            request
        )
        if digest_filter or name_filter or id_filter:
            filtered = []
            for endpoint in endpoints:
                endpoint_digest = endpoint.spec_digest
                if endpoint_digest is None and endpoint.rpc_summary is not None:
                    endpoint_digest = endpoint.rpc_summary.spec_digest
                if digest_filter and (
                    endpoint_digest is None or endpoint_digest not in digest_filter
                ):
                    continue
                if name_filter and endpoint.identity.name not in name_filter:
                    continue
                if id_filter and endpoint.identity.function_id not in id_filter:
                    continue
                filtered.append(endpoint)
            endpoints = filtered
        if request.query:
            endpoints = [
                endpoint
                for endpoint in endpoints
                if self._rpc_matches_query(
                    name=endpoint.identity.name,
                    namespace=endpoint.rpc_summary.namespace
                    if endpoint.rpc_summary
                    else self._rpc_namespace_for_name(endpoint.identity.name),
                    summary=endpoint.rpc_summary.summary
                    if endpoint.rpc_summary
                    else None,
                    function_id=endpoint.identity.function_id,
                    query=request.query,
                )
            ]
        if request.query:
            endpoints, next_token = paginate_items(
                endpoints, limit=request.limit, page_token=request.page_token
            )
        else:
            next_token = (
                str(catalog_result.get("next_page_token"))
                if catalog_result.get("next_page_token") is not None
                else None
            )
            endpoints = tuple(endpoints)

        grouped: dict[tuple[str, str], list[FunctionEndpoint]] = {}
        for endpoint in endpoints:
            key = (endpoint.cluster_id, endpoint.node_id)
            grouped.setdefault(key, []).append(endpoint)
        if request.max_nodes is not None and len(grouped) > request.max_nodes:
            selected_keys = sorted(grouped.keys())[: request.max_nodes]
            grouped = {key: grouped[key] for key in selected_keys}

        errors: list[RpcDescribeError] = []
        items: list[RpcDescribeItem] = []
        timeout = request.timeout_seconds
        if timeout is None:
            timeout = 5.0
        elif timeout <= 0:
            timeout = None
        parallel_limit = min(16, len(grouped) or 1)
        semaphore = asyncio.Semaphore(parallel_limit)

        async def _describe_node(
            cluster_id: str, node_id: str, node_endpoints: list[FunctionEndpoint]
        ) -> None:
            digests = [
                entry.spec_digest for entry in node_endpoints if entry.spec_digest
            ]
            names = [
                entry.identity.name for entry in node_endpoints if not entry.spec_digest
            ]
            ids = [
                entry.identity.function_id
                for entry in node_endpoints
                if not entry.spec_digest
            ]
            allowed_identities = {
                (
                    entry.identity.name,
                    entry.identity.function_id,
                    str(entry.identity.version),
                )
                for entry in node_endpoints
            }
            local_request = RpcDescribeRequest(
                mode="local",
                detail_level=request.detail_level,
                namespace=request.namespace,
                scope=request.scope,
                viewer_cluster_id=request.viewer_cluster_id,
                viewer_tenant_id=request.viewer_tenant_id,
                capabilities=request.capabilities,
                resources=request.resources,
                tags=request.tags,
                function_name=request.function_name,
                function_id=request.function_id,
                version_constraint=request.version_constraint,
                query=request.query,
                spec_digests=tuple(digests),
                function_names=tuple(names),
                function_ids=tuple(ids),
            )
            if (
                cluster_id == self.settings.cluster_id
                and node_id == self.cluster.local_url
            ):
                try:
                    response = RpcDescribeResponse.from_dict(
                        self._rpc_describe_local(local_request)
                    )
                except Exception as exc:
                    errors.append(
                        RpcDescribeError(
                            node_id=node_id,
                            cluster_id=cluster_id,
                            error=str(exc),
                        )
                    )
                    return
                for item in response.items:
                    identity_key = (
                        item.identity.name,
                        item.identity.function_id,
                        str(item.identity.version),
                    )
                    if identity_key not in allowed_identities:
                        continue
                    items.append(item)
                return

            async with semaphore:
                try:
                    request_payload = local_request.to_dict()
                    rpc_command = RPCCommand(
                        name="rpc_describe_local",
                        fun="rpc_describe_local",
                        args=(request_payload,),
                        locs=frozenset(),
                        target_cluster=cluster_id,
                    )
                    call = self.cluster._execute_remote_command(
                        rpc_command,
                        {},
                        Connection(url=node_id),
                        target_cluster=cluster_id,
                        target_node=node_id,
                        retry_remaining=0,
                        allow_fallback=False,
                    )
                    result = (
                        await asyncio.wait_for(call, timeout=timeout)
                        if timeout
                        else await call
                    )
                except TimeoutError:
                    errors.append(
                        RpcDescribeError(
                            node_id=node_id,
                            cluster_id=cluster_id,
                            error="timeout",
                        )
                    )
                    return
                except Exception as exc:
                    errors.append(
                        RpcDescribeError(
                            node_id=node_id,
                            cluster_id=cluster_id,
                            error=str(exc),
                        )
                    )
                    return
                if not isinstance(result, dict):
                    errors.append(
                        RpcDescribeError(
                            node_id=node_id,
                            cluster_id=cluster_id,
                            error="unexpected_response_type",
                        )
                    )
                    return
                if (
                    "error" in result
                    and result.get("command") == "rpc_describe_local"
                    and result.get("error_type")
                ):
                    errors.append(
                        RpcDescribeError(
                            node_id=node_id,
                            cluster_id=cluster_id,
                            error=str(result.get("error")),
                        )
                    )
                    return
                response = RpcDescribeResponse.from_dict(result)
                for item in response.items:
                    identity_key = (
                        item.identity.name,
                        item.identity.function_id,
                        str(item.identity.version),
                    )
                    if identity_key not in allowed_identities:
                        continue
                    items.append(item)

        await asyncio.gather(
            *[
                _describe_node(cluster_id, node_id, node_endpoints)
                for (cluster_id, node_id), node_endpoints in grouped.items()
            ],
            return_exceptions=True,
        )

        items = sorted(
            items,
            key=lambda entry: (
                entry.identity.name,
                entry.identity.function_id,
                entry.identity.version,
                entry.cluster_id,
                entry.node_id,
            ),
        )
        if request.query:
            paged_items, page_token = paginate_items(
                items, limit=request.limit, page_token=request.page_token
            )
            next_page_token = page_token
        else:
            paged_items = tuple(items)
            next_page_token = next_token
        return RpcDescribeResponse(
            generated_at=float(catalog_result.get("generated_at", time.time())),
            items=paged_items,
            errors=tuple(errors),
            next_page_token=next_page_token,
        )

    def _rpc_report(
        self, payload: PayloadMapping | RpcReportRequest | None = None, **kwargs: object
    ) -> Payload:
        request = self._parse_request(RpcReportRequest, payload, kwargs)
        catalog_request = CatalogQueryRequest.from_rpc_report(request)
        catalog_result = self._catalog_query(catalog_request)
        from mpreg.fabric.catalog import FunctionEndpoint

        endpoints = [
            FunctionEndpoint.from_dict(item)  # type: ignore[arg-type]
            for item in catalog_result.get("items", [])
        ]
        namespace_counts: dict[str, int] = {}
        cluster_counts: dict[str, int] = {}
        tag_counts: dict[str, int] = {}
        for endpoint in endpoints:
            summary = endpoint.rpc_summary
            namespace = (
                summary.namespace
                if summary is not None
                else self._rpc_namespace_for_name(endpoint.identity.name)
            )
            namespace_counts[namespace] = namespace_counts.get(namespace, 0) + 1
            cluster_counts[endpoint.cluster_id] = (
                cluster_counts.get(endpoint.cluster_id, 0) + 1
            )
            for tag in endpoint.tags:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
        namespace_items = tuple(
            RpcReportCount(key=key, count=count)
            for key, count in sorted(
                namespace_counts.items(), key=lambda item: (-item[1], item[0])
            )
        )
        cluster_items = tuple(
            RpcReportCount(key=key, count=count)
            for key, count in sorted(
                cluster_counts.items(), key=lambda item: (-item[1], item[0])
            )
        )
        tag_items = tuple(
            RpcReportCount(key=key, count=count)
            for key, count in sorted(
                tag_counts.items(), key=lambda item: (-item[1], item[0])
            )
        )
        response = RpcReportResponse(
            generated_at=float(catalog_result.get("generated_at", time.time())),
            total_functions=len(endpoints),
            namespace_counts=namespace_items,
            cluster_counts=cluster_items,
            tag_counts=tag_items,
        )
        return response.to_dict()

    def _catalog_watch(
        self,
        payload: PayloadMapping | CatalogWatchRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(CatalogWatchRequest, payload, kwargs)
        scope = self._normalize_discovery_scope(request.scope)
        timestamp = time.time()
        namespace = request.namespace.strip() if request.namespace else None
        viewer_cluster_id = self._effective_viewer_cluster_id(None)
        viewer_tenant_id = self._effective_viewer_tenant_id(request.viewer_tenant_id)
        self._enforce_discovery_rate_limit(
            command="catalog_watch",
            viewer_cluster_id=viewer_cluster_id,
            viewer_tenant_id=viewer_tenant_id,
            namespace=namespace,
        )
        if namespace:
            decision = self._namespace_policy_viewer_decision(
                namespace,
                viewer_cluster_id,
                viewer_tenant_id,
            )
            if not decision.allowed:
                self._record_discovery_access_denial(
                    command="catalog_watch",
                    viewer_cluster_id=viewer_cluster_id,
                    viewer_tenant_id=viewer_tenant_id,
                    namespace=namespace,
                    reason=decision.reason,
                )
                raise MPREGException(
                    rpc_error=RPCError(
                        code=403,
                        message="discovery_access_denied",
                        details=f"catalog_watch denied for namespace {namespace}",
                    )
                )
        topic = (
            f"{DISCOVERY_DELTA_TOPIC}.{namespace}"
            if namespace
            else DISCOVERY_DELTA_TOPIC
        )
        response = CatalogWatchResponse(
            topic=topic,
            generated_at=timestamp,
            scope=scope,
            namespace=request.namespace,
            cluster_id=request.cluster_id or self.settings.cluster_id,
        )
        return response.to_dict()

    def _summary_query(
        self,
        payload: PayloadMapping | SummaryQueryRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(SummaryQueryRequest, payload, kwargs)
        timestamp = time.time()
        scope = self._normalize_discovery_scope(request.scope)
        viewer_cluster_id = self._effective_viewer_cluster_id(request.viewer_cluster_id)
        viewer_tenant_id = self._effective_viewer_tenant_id(request.viewer_tenant_id)
        namespace_filter = request.namespace.strip() if request.namespace else ""
        service_id_filter = request.service_id.strip() if request.service_id else ""
        self._enforce_discovery_rate_limit(
            command="summary_query",
            viewer_cluster_id=viewer_cluster_id,
            viewer_tenant_id=viewer_tenant_id,
            namespace=namespace_filter or None,
        )
        if namespace_filter:
            decision = self._namespace_policy_viewer_decision(
                namespace_filter,
                viewer_cluster_id,
                viewer_tenant_id,
            )
            if not decision.allowed:
                self._record_discovery_access_denial(
                    command="summary_query",
                    viewer_cluster_id=viewer_cluster_id,
                    viewer_tenant_id=viewer_tenant_id,
                    namespace=namespace_filter,
                    reason=decision.reason,
                )
                return SummaryQueryResponse(
                    generated_at=timestamp,
                    items=tuple(),
                    ingress=None,
                    next_page_token=None,
                ).to_dict()

        use_summary_cache = bool(
            scope in {"region", "global"} and self._discovery_summary_resolver_enabled()
        )
        if use_summary_cache:
            resolver = self._discovery_summary_resolver
            summaries = (
                resolver.query(
                    namespace=namespace_filter or None,
                    service_id=service_id_filter or None,
                    scope=scope,
                    now=timestamp,
                )
                if resolver is not None
                else tuple()
            )
            if DISCOVERY_SUMMARY_DIAG_ENABLED:
                logger.warning(
                    "[DIAG_DISCOVERY_SUMMARY] node={} action=summary_query_cache scope={} namespace={} service_id={} returned={} cache_entries={}",
                    self.cluster.local_url,
                    scope,
                    namespace_filter or None,
                    service_id_filter or None,
                    len(summaries),
                    (resolver.entry_counts().summaries if resolver is not None else 0),
                )
        else:
            resolver = (
                self._discovery_resolver if self._discovery_resolver_enabled() else None
            )
            if resolver:
                catalog = resolver.catalog
            elif self._fabric_control_plane:
                catalog = self._fabric_control_plane.index.catalog
            else:
                from mpreg.fabric.catalog import RoutingCatalog

                catalog = RoutingCatalog()

            summaries = summarize_functions(
                catalog,
                now=timestamp,
                region=self.settings.cache_region,
                ttl_seconds=self.settings.fabric_catalog_ttl_seconds,
                policy_engine=self._namespace_policy_engine,
                viewer_cluster_id=viewer_cluster_id,
                viewer_tenant_id=viewer_tenant_id,
                source_cluster_id=self.settings.cluster_id,
                summary_scope=scope,
            )

        filtered: list[Any] = []
        for summary in summaries:
            if namespace_filter and not self._namespace_filter_matches(
                namespace_filter, summary.namespace
            ):
                continue
            if service_id_filter and summary.service_id != service_id_filter:
                continue
            if not self._namespace_policy_allows_viewer(
                summary.namespace,
                viewer_cluster_id,
                viewer_tenant_id,
            ):
                continue
            filtered.append(summary)

        ordered = sorted(
            filtered,
            key=lambda entry: (entry.namespace, entry.service_id),
        )
        paged_items, next_token = paginate_items(
            ordered, limit=request.limit, page_token=request.page_token
        )
        ingress: dict[str, tuple[str, ...]] | None = None
        if request.include_ingress:
            ingress_scope = (
                request.ingress_scope
                if request.ingress_scope is not None
                else (scope if scope in {"region", "global"} else None)
            )
            ingress_scope_strict = request.ingress_scope is not None
            ingress_capabilities = frozenset(request.ingress_capabilities)
            ingress_tags = frozenset(request.ingress_tags)
            clusters = sorted(
                {entry.source_cluster for entry in paged_items if entry.source_cluster}
            )
            ingress_map: dict[str, tuple[str, ...]] = {}
            for cluster in clusters:
                urls = self._ingress_urls_for_cluster(
                    cluster,
                    scope=ingress_scope,
                    scope_strict=ingress_scope_strict,
                    capabilities=ingress_capabilities,
                    tags=ingress_tags,
                    limit=request.ingress_limit,
                    now=timestamp,
                )
                if urls:
                    ingress_map[cluster] = urls
            if ingress_map:
                ingress = ingress_map
        response = SummaryQueryResponse(
            generated_at=timestamp,
            items=tuple(paged_items),
            ingress=ingress,
            next_page_token=next_token,
        )
        return response.to_dict()

    def _summary_watch(
        self,
        payload: PayloadMapping | SummaryWatchRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(SummaryWatchRequest, payload, kwargs)
        scope = self._normalize_discovery_scope(request.scope)
        timestamp = time.time()
        namespace = request.namespace.strip() if request.namespace else None
        topic = self._summary_topic(scope, namespace)
        viewer_cluster_id = self._effective_viewer_cluster_id(None)
        viewer_tenant_id = self._effective_viewer_tenant_id(request.viewer_tenant_id)
        self._enforce_discovery_rate_limit(
            command="summary_watch",
            viewer_cluster_id=viewer_cluster_id,
            viewer_tenant_id=viewer_tenant_id,
            namespace=namespace,
        )
        if namespace:
            decision = self._namespace_policy_viewer_decision(
                namespace,
                viewer_cluster_id,
                viewer_tenant_id,
            )
            if not decision.allowed:
                self._record_discovery_access_denial(
                    command="summary_watch",
                    viewer_cluster_id=viewer_cluster_id,
                    viewer_tenant_id=viewer_tenant_id,
                    namespace=namespace,
                    reason=decision.reason,
                )
                raise MPREGException(
                    rpc_error=RPCError(
                        code=403,
                        message="discovery_access_denied",
                        details=f"summary_watch denied for namespace {namespace}",
                    )
                )
        response = SummaryWatchResponse(
            topic=topic,
            generated_at=timestamp,
            scope=scope,
            namespace=request.namespace,
            cluster_id=request.cluster_id or self.settings.cluster_id,
        )
        return response.to_dict()

    def _resolver_cache_stats(
        self, payload: PayloadMapping | None = None, **kwargs: object
    ) -> Payload:
        timestamp = time.time()
        namespaces = tuple(self.settings.discovery_resolver_namespaces)
        resolver = (
            self._discovery_resolver if self._discovery_resolver_enabled() else None
        )
        if resolver is None:
            response = DiscoveryResolverCacheStatsResponse(
                enabled=False,
                generated_at=timestamp,
                namespaces=namespaces,
                entry_counts=CatalogEntryCounts(),
                stats=None,
                query_cache=None,
            )
            return response.to_dict()

        counts = resolver.entry_counts()
        stats = resolver.stats_snapshot()
        query_cache = resolver.query_cache_snapshot()
        response = DiscoveryResolverCacheStatsResponse(
            enabled=True,
            generated_at=timestamp,
            namespaces=namespaces,
            entry_counts=counts,
            stats=stats,
            query_cache=query_cache,
        )
        return response.to_dict()

    def _resolver_resync(
        self, payload: PayloadMapping | None = None, **kwargs: object
    ) -> Payload:
        timestamp = time.time()
        resolver = (
            self._discovery_resolver if self._discovery_resolver_enabled() else None
        )
        if resolver is None:
            response = DiscoveryResolverResyncResponse(
                enabled=False,
                generated_at=timestamp,
                resynced=False,
                entry_counts=None,
                stats=None,
                error="resolver_mode_disabled",
            )
            return response.to_dict()
        if not self._fabric_control_plane:
            response = DiscoveryResolverResyncResponse(
                enabled=True,
                generated_at=timestamp,
                resynced=False,
                entry_counts=None,
                stats=None,
                error="fabric_control_plane_unavailable",
            )
            return response.to_dict()

        counts = resolver.seed_from_catalog(
            self._fabric_control_plane.catalog, now=timestamp
        )
        stats = resolver.stats_snapshot()
        response = DiscoveryResolverResyncResponse(
            enabled=True,
            generated_at=timestamp,
            resynced=True,
            entry_counts=counts,
            stats=stats,
        )
        return response.to_dict()

    def _namespace_status(
        self,
        payload: PayloadMapping | NamespaceStatusRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(NamespaceStatusRequest, payload, kwargs)
        namespace = request.namespace.strip()
        viewer_cluster_id = self._effective_viewer_cluster_id(request.viewer_cluster_id)
        viewer_tenant_id = self._effective_viewer_tenant_id(request.viewer_tenant_id)
        self._enforce_discovery_rate_limit(
            command="namespace_status",
            viewer_cluster_id=viewer_cluster_id,
            viewer_tenant_id=viewer_tenant_id,
            namespace=namespace or None,
        )
        timestamp = time.time()
        summaries_exported, last_export_at = (
            self._summary_export_state.summary_for_namespace(namespace)
        )
        engine = self._namespace_policy_engine
        if engine is None or not engine.enabled:
            response = NamespaceStatusResponse(
                namespace=namespace,
                generated_at=timestamp,
                viewer_cluster_id=viewer_cluster_id,
                viewer_tenant_id=viewer_tenant_id,
                allowed=True,
                reason="policy_disabled",
                rule=None,
                summaries_exported=summaries_exported,
                last_export_at=last_export_at,
            )
            return response.to_dict()
        decision = engine.allows_viewer(
            namespace,
            viewer_cluster_id,
            viewer_tenant_id=viewer_tenant_id,
        )
        if not decision.allowed:
            self._record_discovery_access_denial(
                command="namespace_status",
                viewer_cluster_id=viewer_cluster_id,
                viewer_tenant_id=viewer_tenant_id,
                namespace=namespace,
                reason=decision.reason,
            )
        response = NamespaceStatusResponse(
            namespace=namespace,
            generated_at=timestamp,
            viewer_cluster_id=viewer_cluster_id,
            viewer_tenant_id=viewer_tenant_id,
            allowed=decision.allowed,
            reason=decision.reason,
            rule=decision.rule,
            summaries_exported=summaries_exported,
            last_export_at=last_export_at,
        )
        return response.to_dict()

    def _namespace_policy_validate(
        self,
        payload: PayloadMapping | NamespacePolicyApplyRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(NamespacePolicyApplyRequest, payload, kwargs)
        rules = (
            request.rules
            if request.rules is not None
            else self.settings.discovery_policy_rules
        )
        errors = validate_namespace_policy_rules(rules)
        response = NamespacePolicyValidationResponse(
            valid=not errors,
            generated_at=time.time(),
            rule_count=len(rules),
            errors=errors,
        )
        self._record_namespace_policy_audit(
            event="validate",
            valid=not errors,
            rule_count=len(rules),
            errors=errors,
            actor=request.actor,
        )
        return response.to_dict()

    def _namespace_policy_apply(
        self,
        payload: PayloadMapping | NamespacePolicyApplyRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(NamespacePolicyApplyRequest, payload, kwargs)
        rules = (
            request.rules
            if request.rules is not None
            else self.settings.discovery_policy_rules
        )
        errors = validate_namespace_policy_rules(rules)
        if errors:
            response = NamespacePolicyApplyResponse(
                applied=False,
                valid=False,
                generated_at=time.time(),
                rule_count=len(rules),
                errors=errors,
            )
            self._record_namespace_policy_audit(
                event="apply",
                valid=False,
                rule_count=len(rules),
                errors=errors,
                actor=request.actor,
            )
            return response.to_dict()

        enabled = (
            request.enabled
            if request.enabled is not None
            else self.settings.discovery_policy_enabled
        )
        default_allow = (
            request.default_allow
            if request.default_allow is not None
            else self.settings.discovery_policy_default_allow
        )
        self.settings.discovery_policy_enabled = enabled
        self.settings.discovery_policy_default_allow = default_allow
        self.settings.discovery_policy_rules = tuple(rules)
        self._namespace_policy_engine = NamespacePolicyEngine(
            enabled=enabled,
            default_allow=default_allow,
            rules=tuple(rules),
        )
        if self._fabric_control_plane:
            self._fabric_control_plane.applier.policy = self._fabric_catalog_policy()

        response = NamespacePolicyApplyResponse(
            applied=True,
            valid=True,
            generated_at=time.time(),
            rule_count=len(rules),
            errors=(),
        )
        self._record_namespace_policy_audit(
            event="apply",
            valid=True,
            rule_count=len(rules),
            errors=(),
            actor=request.actor,
        )
        return response.to_dict()

    def _namespace_policy_export(
        self, payload: PayloadMapping | None = None, **kwargs: object
    ) -> Payload:
        timestamp = time.time()
        engine = self._namespace_policy_engine
        if engine is None:
            enabled = self.settings.discovery_policy_enabled
            default_allow = self.settings.discovery_policy_default_allow
            rules = self.settings.discovery_policy_rules
        else:
            enabled = engine.enabled
            default_allow = engine.default_allow
            rules = engine.rules
        response = NamespacePolicyExportResponse(
            enabled=enabled,
            default_allow=default_allow,
            generated_at=timestamp,
            rule_count=len(rules),
            rules=tuple(rules),
        )
        return response.to_dict()

    def _namespace_policy_audit(
        self,
        payload: PayloadMapping | NamespacePolicyAuditRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(NamespacePolicyAuditRequest, payload, kwargs)
        limit_value = request.limit
        entries = (
            self._namespace_policy_audit_log.snapshot(limit=limit_value)
            if self._namespace_policy_audit_log is not None
            else tuple()
        )
        response = NamespacePolicyAuditResponse(
            generated_at=time.time(),
            entries=entries,
        )
        return response.to_dict()

    def _record_namespace_policy_audit(
        self,
        *,
        event: str,
        valid: bool,
        rule_count: int,
        errors: tuple[str, ...],
        actor: str | None,
    ) -> None:
        if self._namespace_policy_audit_log is None:
            return
        entry = NamespacePolicyAuditEntry(
            event=event,
            timestamp=time.time(),
            actor=actor,
            valid=valid,
            rule_count=rule_count,
            errors=errors,
        )
        self._namespace_policy_audit_log.record(entry)

    def _record_discovery_access_denial(
        self,
        *,
        command: str,
        viewer_cluster_id: str,
        viewer_tenant_id: TenantId | None,
        namespace: str | None,
        reason: str,
    ) -> None:
        log = self._discovery_access_audit_log
        if log is None:
            return
        entry = DiscoveryAccessAuditEntry(
            event=command,
            timestamp=time.time(),
            viewer_cluster_id=viewer_cluster_id,
            viewer_tenant_id=viewer_tenant_id,
            namespace=namespace,
            allowed=False,
            reason=reason,
        )
        log.record(entry)

    def _discovery_access_audit(
        self,
        payload: PayloadMapping | DiscoveryAccessAuditRequest | None = None,
        **kwargs: object,
    ) -> Payload:
        request = self._parse_request(DiscoveryAccessAuditRequest, payload, kwargs)
        limit_value = request.limit
        audit_log = self._discovery_access_audit_log
        entries = audit_log.snapshot(limit=limit_value) if audit_log else tuple()
        response = DiscoveryAccessAuditResponse(
            generated_at=time.time(),
            entries=entries,
        )
        return response.to_dict()

    def _get_peers_snapshot(
        self, payload: PayloadMapping | ListPeersRequest | None = None, **kwargs: object
    ) -> list[Payload]:
        """Return a JSON-safe snapshot of current peer info."""
        peers = []
        if not self._peer_directory:
            return peers
        request = self._parse_request(ListPeersRequest, payload, kwargs)
        scope = self._normalize_discovery_scope(request.scope)
        if scope == "local":
            return peers
        scope_cluster_id, _ = self._scope_limits(scope)
        if (
            request.cluster_id
            and scope_cluster_id
            and request.cluster_id != scope_cluster_id
        ):
            return peers
        cluster_id = request.cluster_id or scope_cluster_id
        now = time.time()
        all_connections = self._get_all_peer_connections()
        for node in self._peer_directory.nodes():
            if node.node_id == self.cluster.local_url:
                continue
            if self._is_peer_departed(
                node.node_id, self._peer_instance_ids.get(node.node_id)
            ):
                continue
            if cluster_id and node.cluster_id != cluster_id:
                continue
            status = self.peer_status.get(node.node_id)
            connection = all_connections.get(node.node_id)
            if self._should_exclude_disconnected_peer_snapshot(
                peer_url=node.node_id,
                status=status,
                connection=connection,
                now=now,
            ):
                continue
            funs, locs = self._peer_function_snapshot(node.node_id)
            advertised_urls: tuple[str, ...] = ()
            if status and status.advertised_urls:
                advertised_urls = tuple(status.advertised_urls)
            elif node.node_id:
                advertised_urls = (node.node_id,)
            load = self._build_node_load_metrics(status, now=now)
            peer_region = str(node.region or "")
            if not peer_region and status and isinstance(status.metrics, dict):
                cache_metrics = status.metrics.get("cache_metrics")
                if isinstance(cache_metrics, dict):
                    cache_region = cache_metrics.get("cache_region")
                    if cache_region:
                        peer_region = str(cache_region)
            if node.cluster_id == self.settings.cluster_id:
                peer_scope = "zone"
            elif peer_region and peer_region == self.settings.cache_region:
                peer_scope = "region"
            elif peer_region:
                peer_scope = "global"
            else:
                peer_scope = "region"
            snapshot = PeerSnapshot(
                url=node.node_id,
                cluster_id=node.cluster_id,
                funs=funs,
                locs=tuple(sorted(locs)),
                last_seen=node.advertised_at,
                status=status.status if status else "unknown",
                status_timestamp=status.timestamp if status else None,
                scope=peer_scope,
                region=peer_region or None,
                advertised_urls=advertised_urls,
                transport_endpoints=node.transport_endpoints,
                load=load,
            )
            peers.append(snapshot.to_dict())
        return sorted(peers, key=lambda peer: peer["url"])

    def _current_function_catalog(self) -> tuple[RPCFunctionDescriptor, ...]:
        catalog: list[RPCFunctionDescriptor] = []
        for spec in self.registry.specs():
            catalog.append(
                RPCFunctionDescriptor(
                    name=spec.identity.name,
                    function_id=spec.identity.function_id,
                    version=str(spec.identity.version),
                    resources=tuple(sorted(spec.resources)),
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
        if peer_url == self.cluster.local_url:
            return
        dispatch = self._catalog_snapshot_dispatch
        dispatch.pending_peers.add(peer_url)
        dispatch.enqueued_events += 1
        if dispatch.flush_task and not dispatch.flush_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        flush_task = loop.create_task(self._flush_catalog_snapshots())
        dispatch.flush_task = flush_task
        self._track_background_task(flush_task)

    async def _flush_catalog_snapshots(self) -> None:
        dispatch = self._catalog_snapshot_dispatch
        try:
            while not self._shutdown_event.is_set():
                if not dispatch.pending_peers:
                    return
                # Coalesce bursty registration updates into one snapshot pass.
                await asyncio.sleep(0)
                peer_batch = tuple(sorted(dispatch.pending_peers))
                dispatch.pending_peers.clear()
                dispatch.flush_batches += 1
                dispatch.peers_flushed += len(peer_batch)
                if CATALOG_SNAPSHOT_DIAG_ENABLED:
                    logger.error(
                        "[DIAG_CATALOG_SNAPSHOT] action=flush "
                        "node={} batch_index={} peers_in_batch={} "
                        "pending_after_pop={} enqueued_events={} peers_flushed_total={}",
                        self.settings.name,
                        dispatch.flush_batches,
                        len(peer_batch),
                        len(dispatch.pending_peers),
                        dispatch.enqueued_events,
                        dispatch.peers_flushed,
                    )
                for peer_url in peer_batch:
                    await self._send_catalog_snapshot_to_peer(peer_url)
        finally:
            dispatch.flush_task = None

    def _schedule_catalog_snapshots_for_existing_peers(self) -> None:
        if not self._fabric_control_plane or not self._fabric_gossip_transport:
            return
        for peer_url, connection in self._get_all_peer_connections().items():
            if not connection.is_connected:
                continue
            if peer_url == self.cluster.local_url:
                continue
            self._schedule_catalog_snapshot(peer_url)

    def _schedule_node_snapshots_for_existing_peers_batch(self) -> None:
        if not self._fabric_control_plane or not self._fabric_gossip_transport:
            return
        connected_peer_urls: list[str] = []
        for peer_url, connection in self._get_all_peer_connections().items():
            if not connection.is_connected:
                continue
            if peer_url == self.cluster.local_url:
                continue
            connected_peer_urls.append(peer_url)
        if not connected_peer_urls:
            return

        target_hint = max(1, self._peer_dial_target_count_hint())
        batch_size = max(1, int(target_hint**0.5))
        batch_size = min(batch_size, len(connected_peer_urls))
        start_index = self._fabric_node_snapshot_cursor % len(connected_peer_urls)
        selected_peers = [
            connected_peer_urls[(start_index + offset) % len(connected_peer_urls)]
            for offset in range(batch_size)
        ]
        self._fabric_node_snapshot_cursor = (start_index + batch_size) % len(
            connected_peer_urls
        )
        for peer_url in selected_peers:
            self._schedule_node_snapshot(peer_url)

    def _schedule_node_snapshot(self, peer_url: str) -> None:
        if not self._fabric_control_plane or not self._fabric_gossip_transport:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._track_background_task(
            loop.create_task(self._send_node_snapshot_to_peer(peer_url))
        )

    async def _send_node_snapshot_to_peer(self, peer_url: str) -> None:
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
        nodes = tuple(catalog.nodes.entries(now=now))
        if not nodes:
            return
        delta = RoutingCatalogDelta(
            update_id=f"node-snapshot:{uuid.uuid4()}",
            cluster_id=self.settings.cluster_id,
            sent_at=now,
            nodes=nodes,
        )
        gossip = self._fabric_control_plane.gossip
        message = GossipMessage(
            message_id=f"{gossip.node_id}:node-snapshot:{uuid.uuid4()}",
            message_type=GossipMessageType.CATALOG_UPDATE,
            sender_id=gossip.node_id,
            payload=delta.to_dict(),
            vector_clock=gossip.vector_clock.copy(),
            sequence_number=gossip.next_sequence_number(),
            ttl=0,
            max_hops=0,
        )
        await self._fabric_gossip_transport.send_message(peer_url, message)

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
        delta_payload = delta.to_dict()
        if CATALOG_SNAPSHOT_DIAG_ENABLED:
            payload_size_bytes = len(self.serializer.serialize(delta_payload))
            logger.error(
                "[DIAG_CATALOG_SNAPSHOT] action=send "
                "node={} peer={} functions={} topics={} queues={} "
                "caches={} cache_profiles={} nodes={} payload_bytes={}",
                self.settings.name,
                peer_url,
                len(delta.functions),
                len(delta.topics),
                len(delta.queues),
                len(delta.caches),
                len(delta.cache_profiles),
                len(delta.nodes),
                payload_size_bytes,
            )
        gossip = self._fabric_control_plane.gossip
        message = GossipMessage(
            message_id=f"{gossip.node_id}:snapshot:{uuid.uuid4()}",
            message_type=GossipMessageType.CATALOG_UPDATE,
            sender_id=gossip.node_id,
            payload=delta_payload,
            vector_clock=gossip.vector_clock.copy(),
            sequence_number=gossip.next_sequence_number(),
            ttl=0,
            max_hops=0,
        )
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
        self._peer_dial_state.pop(goodbye.departing_node_url, None)
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
        existing_status = self.peer_status.get(peer_url)
        disconnected_since = (
            float(existing_status.timestamp)
            if existing_status is not None and existing_status.status == "disconnected"
            else time.time()
        )
        self.peer_status[peer_url] = RPCServerStatus(
            server_url=peer_url,
            cluster_id=cluster_id or self.settings.cluster_id,
            timestamp=disconnected_since,
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
        """Registers the default RPC commands (echo, echos)."""
        logger.debug(f"🚀 {self.settings.name}: _register_default_commands() called")
        self.register_command("echo", echo, [])
        self.register_command("echos", echos, [])
        self.register_command("list_peers", self._get_peers_snapshot, [])
        self.register_command("cluster_map", self._get_cluster_map_snapshot, [])
        self.register_command("cluster_map_v2", self._get_cluster_map_v2, [])
        self.register_command("catalog_query", self._catalog_query, [])
        self.register_command("catalog_watch", self._catalog_watch, [])
        self.register_command("dns_register", self._dns_register, [])
        self.register_command("dns_unregister", self._dns_unregister, [])
        self.register_command("dns_list", self._dns_list, [])
        self.register_command("dns_describe", self._dns_describe, [])
        self.register_command("summary_query", self._summary_query, [])
        self.register_command("summary_watch", self._summary_watch, [])
        self.register_command("resolver_cache_stats", self._resolver_cache_stats, [])
        self.register_command("resolver_resync", self._resolver_resync, [])
        self.register_command(
            "discovery_access_audit", self._discovery_access_audit, []
        )
        self.register_command("rpc_list", self._rpc_list, [])
        self.register_command("rpc_describe_local", self._rpc_describe_local, [])
        self.register_command("rpc_describe", self._rpc_describe, [])
        self.register_command("rpc_report", self._rpc_report, [])
        self.register_command("namespace_status", self._namespace_status, [])
        self.register_command(
            "namespace_policy_validate", self._namespace_policy_validate, []
        )
        self.register_command(
            "namespace_policy_apply", self._namespace_policy_apply, []
        )
        self.register_command(
            "namespace_policy_export", self._namespace_policy_export, []
        )
        self.register_command(
            "namespace_policy_audit", self._namespace_policy_audit, []
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

    async def run_rpc(
        self,
        req: RPCRequest,
        *,
        viewer_cluster_id: str | None = None,
        viewer_tenant_id: TenantId | None = None,
    ) -> RPCResponse:
        """Run a client RPC request command in the cluster.

        This method takes an RPCRequest, constructs an RPC execution graph,
        and then runs the commands across the cluster.

        Supports enhanced debugging features including intermediate results
        and execution summaries based on request parameters.
        """
        start_time = time.time()
        success = False
        with self._request_viewer_context(
            viewer_cluster_id, viewer_tenant_id=viewer_tenant_id
        ):
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

            except MPREGException as exc:
                return RPCResponse(r=None, error=exc.rpc_error, u=req.u)
            except Exception:
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
        connection.tenant_id = self._tenant_id_from_transport(transport)
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
                pending_summary_backlog: PubSubSubscription | None = None
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
                        viewer_cluster_id = (
                            peer_cluster_id
                            if is_server_connection and peer_cluster_id
                            else self.settings.cluster_id
                        )
                        response_model = await self.run_rpc(
                            rpc_request,
                            viewer_cluster_id=viewer_cluster_id,
                            viewer_tenant_id=connection.tenant_id,
                        )
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
                            if (
                                self._summary_store_forward_enabled()
                                and subscribe_req.subscription.get_backlog
                            ):
                                pending_summary_backlog = subscribe_req.subscription
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
                    except Exception as exc:
                        logger.debug(
                            "[{}] Client disconnected before reply delivery: {}",
                            peer_label,
                            exc,
                        )
                if pending_summary_backlog is not None:
                    await self._send_summary_store_forward_backlog(
                        pending_summary_backlog
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

    async def _send_summary_store_forward_backlog(
        self, subscription: PubSubSubscription
    ) -> None:
        if not self._summary_store_forward_enabled():
            return
        max_age_seconds = float(
            self.settings.discovery_summary_export_store_forward_seconds
        )
        if subscription.backlog_seconds:
            max_age_seconds = min(max_age_seconds, float(subscription.backlog_seconds))
        if max_age_seconds <= 0:
            return
        now = time.time()
        backlog = self._summary_export_state.store_forward_backlog(
            now=now, max_age_seconds=max_age_seconds
        )
        for message in backlog:
            if not self._subscription_matches_topic(subscription, message.topic):
                continue
            notification = PubSubNotification(
                message=message,
                subscription_id=subscription.subscription_id,
                u=f"backlog_{message.message_id}_{subscription.subscription_id}",
            )
            await self._send_notification_to_client(notification)

    async def _establish_peer_connection(
        self,
        peer_url: str,
        *,
        fast_connect: bool = False,
        dial_url: str | None = None,
        dial_context: str = "peer_reconcile",
        peer_target_count: int | None = None,
        connected_ratio_hint: float | None = None,
        allow_inbound_reuse: bool = True,
    ) -> bool:
        """Establishes a persistent connection to a peer for RPC forwarding.

        Args:
            peer_url: The URL of the peer to connect to.
        """
        # Do not establish new connections during shutdown
        if self._shutdown_event.is_set():
            logger.debug(
                f"[{self.settings.name}] Skipping peer connection to {peer_url} - shutdown in progress"
            )
            return False
        lock = self._peer_connection_locks.get(peer_url)
        if lock is None:
            lock = asyncio.Lock()
            self._peer_connection_locks[peer_url] = lock
        async with lock:
            if self._shutdown_event.is_set():
                return False
            existing_connection = self.peer_connections.get(peer_url)
            if existing_connection and existing_connection.is_connected:
                logger.debug(
                    "[{}] Already connected to peer: {}",
                    self.settings.name,
                    peer_url,
                )
                return True
            if allow_inbound_reuse:
                inbound_connection = self._inbound_peer_connections.get(peer_url)
                if inbound_connection and inbound_connection.is_connected:
                    logger.debug(
                        "[{}] Reusing inbound connection for peer: {}",
                        self.settings.name,
                        peer_url,
                    )
                    return True
            if peer_url in self.peer_connections:
                # Check if existing connection is still valid
                if self.peer_connections[peer_url].is_connected:
                    logger.debug(
                        "[{}] Already connected to peer: {}",
                        self.settings.name,
                        peer_url,
                    )
                    return True
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
            snapshot: PeerDialDiagnosticSnapshot | None = None
            try:
                target_count = max(
                    peer_target_count
                    if peer_target_count is not None
                    else self._peer_dial_target_count_hint(),
                    1,
                )
                connected_ratio = connected_ratio_hint
                if connected_ratio is None:
                    connected_count = sum(
                        1
                        for connection in self._get_all_peer_connections().values()
                        if connection.is_connected
                    )
                    connected_ratio = connected_count / max(target_count, 1)
                connected_ratio = min(max(connected_ratio, 0.0), 1.0)
                peer_state = self._peer_dial_state_for(peer_url)
                policy = self._select_peer_connection_policy(
                    fast_connect=fast_connect,
                    peer_target_count=target_count,
                    consecutive_failures=peer_state.consecutive_failures,
                    connected_ratio=connected_ratio,
                )
                if dial_context in {
                    "startup_connect",
                    "startup_seed",
                    "bootstrap_seed",
                }:
                    startup_retry_floor = 3 if connected_ratio < 0.5 else 2
                    startup_timeout_floor = max(
                        3.0,
                        float(self.settings.gossip_interval) * 3.0,
                    )
                    adjusted_retries = max(policy.max_retries, startup_retry_floor)
                    adjusted_timeout = max(
                        policy.connect_timeout_seconds,
                        startup_timeout_floor,
                    )
                    if (
                        adjusted_retries != policy.max_retries
                        or adjusted_timeout != policy.connect_timeout_seconds
                        or adjusted_timeout != policy.open_timeout_seconds
                    ):
                        policy = PeerDialConnectionPolicy(
                            max_retries=adjusted_retries,
                            base_delay_seconds=policy.base_delay_seconds,
                            connect_timeout_seconds=adjusted_timeout,
                            open_timeout_seconds=adjusted_timeout,
                        )
                snapshot = self._build_peer_dial_snapshot(
                    peer_url=peer_url,
                    dial_url=target_url,
                    context=dial_context,
                    fast_connect=fast_connect,
                    peer_target_count=target_count,
                    connected_ratio=connected_ratio,
                    consecutive_failures=peer_state.consecutive_failures,
                    policy=policy,
                )
                self._emit_peer_dial_diag(snapshot=snapshot, outcome="attempt")

                connection = Connection(
                    url=target_url,
                    max_retries=policy.max_retries,
                    base_delay=policy.base_delay_seconds,
                    open_timeout=policy.open_timeout_seconds,
                    connect_timeout=policy.connect_timeout_seconds,
                )

                connect_start = time.monotonic()
                await connection.connect()
                connect_duration = time.monotonic() - connect_start
                self.peer_connections[peer_url] = connection
                self._emit_peer_dial_diag(
                    snapshot=snapshot,
                    outcome="success",
                    duration_seconds=connect_duration,
                )

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
                return True
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if snapshot is not None:
                    self._emit_peer_dial_diag(
                        snapshot=snapshot,
                        outcome="failure",
                        error_message=str(e),
                    )
                transient_contexts = {
                    "peer_reconcile",
                    "startup_connect",
                    "startup_seed",
                    "bootstrap_seed",
                    "goodbye_temp_dial",
                }
                if dial_context in transient_contexts:
                    logger.debug(
                        "[{}] Failed to connect to peer {} (context={}): {}",
                        self.settings.name,
                        peer_url,
                        dial_context,
                        e,
                    )
                else:
                    logger.error(
                        "[{}] Failed to connect to peer {} (context={}): {}",
                        self.settings.name,
                        peer_url,
                        dial_context,
                        e,
                    )
                if dial_context == "peer_reconcile":
                    target_hint_for_failure = max(
                        peer_target_count
                        if peer_target_count is not None
                        else self._peer_dial_target_count_hint(),
                        1,
                    )
                    if target_hint_for_failure <= 20:
                        self._mark_peer_disconnected(peer_url)
                        if PEER_SNAPSHOT_DIAG_ENABLED:
                            logger.warning(
                                "[DIAG_PEER_SNAPSHOT] node={} action=mark_disconnected_on_dial_failure "
                                "peer={} context={} target_hint={}",
                                self.cluster.local_url,
                                peer_url,
                                dial_context,
                                target_hint_for_failure,
                            )
                return False

    async def _bootstrap_seed_peers(self) -> None:
        """Quickly retry seed peers during startup to avoid missing early status."""
        seed_peers = set(self.settings.peers or [])
        if self.settings.connect:
            seed_peers.add(self.settings.connect)
        if not seed_peers:
            return

        deadline = time.time() + max(2.5, self.settings.gossip_interval)
        while time.time() < deadline and not self._shutdown_event.is_set():
            managed_connections = self.peer_connections
            pending = [
                peer_url
                for peer_url in seed_peers
                if peer_url not in managed_connections
                or not managed_connections[peer_url].is_connected
            ]
            if not pending:
                return
            now = time.time()
            peer_target_count = len(seed_peers)
            connected_seed_peers = sum(
                1
                for peer_url in seed_peers
                if (
                    (connection := managed_connections.get(peer_url)) is not None
                    and connection.is_connected
                )
            )
            connected_ratio = connected_seed_peers / max(peer_target_count, 1)
            for peer_url in pending:
                state = self._peer_dial_state_for(peer_url)
                if not state.can_attempt(now):
                    continue
                state.record_attempt(now)
                success = await self._establish_peer_connection(
                    peer_url,
                    fast_connect=True,
                    dial_context="bootstrap_seed",
                    peer_target_count=peer_target_count,
                    connected_ratio_hint=connected_ratio,
                    allow_inbound_reuse=False,
                )
                self._record_peer_dial_outcome(
                    peer_url=peer_url,
                    success=success,
                    now=time.time(),
                    peer_target_count=peer_target_count,
                    connected_ratio=connected_ratio,
                )
            await asyncio.sleep(
                max(
                    0.1,
                    self._peer_reconcile_interval_seconds(
                        peer_target_count, connected_ratio
                    ),
                )
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
        active_connections = {
            peer_url: connection
            for peer_url, connection in all_connections.items()
            if connection.is_connected
        }
        inactive_peer_urls = sorted(peer_urls - set(active_connections))
        peer_count = len(peer_urls)
        active_count = len(active_connections)
        temp_dial_budget = len(inactive_peer_urls)
        if inactive_peer_urls:
            temp_dial_budget = min(
                len(inactive_peer_urls),
                max(0, int(peer_count**0.5) - max(active_count // 8, 0)),
            )
            if peer_count <= 8:
                temp_dial_budget = len(inactive_peer_urls)
        temp_dial_targets = inactive_peer_urls[:temp_dial_budget]
        skipped_temp_targets = len(inactive_peer_urls) - len(temp_dial_targets)
        if skipped_temp_targets > 0:
            logger.info(
                f"[{self.settings.name}] Skipping temp GOODBYE dials to {skipped_temp_targets} disconnected peers (budget={temp_dial_budget})"
            )

        send_parallelism = max(2, min(8, int(max(peer_count, 1) ** 0.5) + 1))
        send_semaphore = asyncio.Semaphore(send_parallelism)

        async def _send_to_connected_peer(
            peer_url: str, connection: Connection
        ) -> bool:
            try:
                async with send_semaphore:
                    await connection.send(message_bytes)
                logger.debug(f"[{self.settings.name}] Sent GOODBYE to {peer_url}")
                return True
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Failed to send GOODBYE to {peer_url}: {e}"
                )
                return False

        async def _send_to_disconnected_peer(peer_url: str) -> bool:
            temp_connection: Connection | None = None
            try:
                async with send_semaphore:
                    temp_connection = Connection(
                        url=peer_url,
                        max_retries=0,
                        base_delay=0.05,
                        open_timeout=0.35,
                        connect_timeout=0.35,
                    )
                    await temp_connection.connect()
                    await temp_connection.send(message_bytes)
                logger.debug(
                    f"[{self.settings.name}] Sent GOODBYE to {peer_url} (temp)"
                )
                return True
            except Exception as e:
                logger.warning(
                    f"[{self.settings.name}] Failed to send GOODBYE to {peer_url}: {e}"
                )
                return False
            finally:
                if temp_connection and temp_connection.is_connected:
                    with contextlib.suppress(Exception):
                        await temp_connection.disconnect()

        send_tasks = [
            _send_to_connected_peer(peer_url, connection)
            for peer_url, connection in active_connections.items()
        ]
        send_tasks.extend(
            _send_to_disconnected_peer(peer_url) for peer_url in temp_dial_targets
        )
        send_results = await asyncio.gather(*send_tasks, return_exceptions=True)
        successful_sends = sum(result is True for result in send_results)

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
            candidates: list[tuple[str, str | None]] = []
            candidate_urls: set[str] = set()

            def _add_candidate(peer_url: str, dial_url: str | None) -> None:
                if peer_url == self.cluster.local_url:
                    return
                if peer_url in candidate_urls:
                    return
                candidate_urls.add(peer_url)
                candidates.append((peer_url, dial_url))

            for peer_url in sorted(seed_peers):
                if self._shutdown_event.is_set():
                    return
                if self._is_peer_departed(peer_url, None):
                    continue
                _add_candidate(peer_url, None)

            if self._peer_directory:
                for node in self._iter_peer_nodes_for_connection():
                    if self._shutdown_event.is_set():
                        logger.info(
                            f"[{self.settings.name}] Stopping peer connection loop - shutdown in progress"
                        )
                        return
                    peer_url = node.node_id
                    if self._is_peer_departed(
                        peer_url, self._peer_instance_ids.get(peer_url)
                    ):
                        continue
                    auth_result = self.federation_manager.federation_manager.config.is_cross_federation_connection_allowed(
                        node.cluster_id
                    )
                    is_local_cluster = node.cluster_id == self.settings.cluster_id
                    if not is_local_cluster and auth_result.name.startswith("REJECTED"):
                        logger.debug(
                            "[{}] Skipping peer {} due to federation policy (cluster: {}, mode: {})",
                            self.settings.name,
                            peer_url,
                            node.cluster_id,
                            self.federation_manager.federation_manager.config.federation_mode.value,
                        )
                        continue
                    dial_url = self._select_peer_dial_url(node)
                    _add_candidate(peer_url, dial_url)
                    if not is_local_cluster:
                        logger.debug(
                            "[{}] Establishing cross-federation connection to peer {} (cluster: {})",
                            self.settings.name,
                            peer_url,
                            node.cluster_id,
                        )

            peer_target_count = len(candidates)
            now = time.time()
            managed_connections = self.peer_connections
            observed_connections = self._get_all_peer_connections()
            # In large fabrics, treat active inbound links as valid connectivity so
            # dial pressure does not spiral into redundant outbound storms.
            connection_view = (
                observed_connections if peer_target_count >= 20 else managed_connections
            )
            due_candidates: list[tuple[str, str | None]] = []
            connected_candidates = 0
            not_due_candidates = 0
            for peer_url, dial_url in candidates:
                connection = connection_view.get(peer_url)
                if connection and connection.is_connected:
                    connected_candidates += 1
                    self._record_peer_dial_outcome(
                        peer_url=peer_url,
                        success=True,
                        now=now,
                        peer_target_count=peer_target_count,
                    )
                    continue
                state = self._peer_dial_state_for(peer_url)
                if not state.can_attempt(now):
                    not_due_candidates += 1
                    continue
                due_candidates.append((peer_url, dial_url))

            connected_ratio = (
                connected_candidates / max(peer_target_count, 1)
                if peer_target_count > 0
                else 1.0
            )
            discovery_ratio = self._peer_discovery_ratio(peer_target_count)
            desired_connected = self._peer_target_connection_count(
                peer_target_count,
                connected_ratio,
            )
            exploration_slots = self._peer_dial_exploration_slots(
                peer_target_count=peer_target_count,
                connected_ratio=connected_ratio,
                discovery_ratio=discovery_ratio,
            )
            if (
                due_candidates
                and connected_candidates >= desired_connected
                and exploration_slots > 0
            ):
                desired_connected = min(
                    peer_target_count,
                    connected_candidates + exploration_slots,
                )
            parallelism = 0
            dial_budget = 0
            selected_candidate_count = 0
            dial_pressure = self._peer_dial_pressure(peer_target_count, connected_ratio)
            if due_candidates:
                parallelism = self._peer_dial_parallelism(
                    peer_target_count, connected_ratio
                )
                burst_budget = max(
                    0,
                    int(parallelism * max(0.0, 1.0 - min(dial_pressure, 1.0))),
                )
                dial_budget = max(
                    parallelism,
                    min(
                        len(due_candidates),
                        parallelism + burst_budget,
                    ),
                )
                if connected_ratio < 0.35 and dial_pressure > 0.5:
                    # Under sustained deficit, probe additional due peers per loop
                    # so lagging nodes can recover connectivity before propagation
                    # windows close.
                    recovery_extra_budget = max(
                        1,
                        int(parallelism * min(dial_pressure, 1.0)),
                    )
                    dial_budget = max(
                        dial_budget,
                        min(
                            len(due_candidates),
                            parallelism + recovery_extra_budget,
                        ),
                    )
                available_slots = max(0, desired_connected - connected_candidates)
                dial_budget = min(dial_budget, available_slots)
                if dial_budget <= 0:
                    selected_candidates: list[tuple[str, str | None]] = []
                else:

                    def _dial_priority(
                        candidate: tuple[str, str | None],
                    ) -> tuple[int, int, int, float]:
                        peer_url, _ = candidate
                        state = self._peer_dial_state_for(peer_url)
                        is_seed_peer = peer_url in seed_peers
                        has_success = state.last_success_at is not None
                        if is_seed_peer and not has_success:
                            seed_priority = 0
                        elif is_seed_peer:
                            seed_priority = 1
                        else:
                            seed_priority = 2
                        unrecovered_priority = (
                            0 if not has_success and connected_ratio < 0.7 else 1
                        )
                        return (
                            seed_priority,
                            unrecovered_priority,
                            state.consecutive_failures,
                            state.last_attempt_at,
                            self._peer_dial_selection_spread(peer_url),
                        )

                    selected_candidates = sorted(due_candidates, key=_dial_priority)[
                        :dial_budget
                    ]
                selected_candidate_count = len(selected_candidates)
                if selected_candidate_count <= 0:
                    selected_candidates = []
                semaphore = asyncio.Semaphore(max(parallelism, 1))

                async def _dial_candidate(peer_url: str, dial_url: str | None) -> None:
                    async with semaphore:
                        self._peer_dial_state_for(peer_url).record_attempt(time.time())
                        success = await self._establish_peer_connection(
                            peer_url,
                            dial_url=dial_url,
                            fast_connect=True,
                            dial_context="peer_reconcile",
                            peer_target_count=peer_target_count,
                            connected_ratio_hint=connected_ratio,
                            allow_inbound_reuse=False,
                        )
                        self._record_peer_dial_outcome(
                            peer_url=peer_url,
                            success=success,
                            now=time.time(),
                            peer_target_count=peer_target_count,
                            connected_ratio=connected_ratio,
                        )

                await asyncio.gather(
                    *(
                        _dial_candidate(peer_url, dial_url)
                        for peer_url, dial_url in selected_candidates
                    )
                )

            stale_peer_dial_entries = [
                peer_url
                for peer_url in self._peer_dial_state
                if peer_url not in candidate_urls
                and peer_url not in managed_connections
            ]
            for peer_url in stale_peer_dial_entries:
                self._peer_dial_state.pop(peer_url, None)

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

            peer_reconcile_interval = self._peer_reconcile_interval_seconds(
                peer_target_count,
                connected_ratio,
            )
            self._emit_peer_dial_loop_diag(
                PeerDialLoopSnapshot(
                    peer_target_count=peer_target_count,
                    connected_ratio=connected_ratio,
                    discovery_ratio=discovery_ratio,
                    pressure=dial_pressure,
                    desired_connected=desired_connected,
                    exploration_slots=exploration_slots,
                    connected_candidates=connected_candidates,
                    due_candidates=len(due_candidates),
                    selected_candidates=selected_candidate_count,
                    parallelism=parallelism,
                    dial_budget=dial_budget,
                    not_due_candidates=not_due_candidates,
                    reconcile_interval_seconds=peer_reconcile_interval,
                )
            )
            self._emit_discovery_loop_diag(
                peer_target_count=peer_target_count,
                connected_candidates=connected_candidates,
                due_candidates=len(due_candidates),
                selected_candidates=selected_candidate_count,
                not_due_candidates=not_due_candidates,
                desired_connected=desired_connected,
                connected_ratio=connected_ratio,
                discovery_ratio=discovery_ratio,
                reconcile_interval_seconds=peer_reconcile_interval,
            )
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=peer_reconcile_interval
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
        functions = tuple(
            sorted({spec.identity.name for spec in self.registry.specs()})
        )
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
            discovery_summary_provider=self._discovery_summary_metrics,
            discovery_cache_provider=self._discovery_cache_metrics,
            discovery_policy_provider=self._discovery_policy_metrics,
            discovery_lag_provider=self._discovery_lag_metrics,
            dns_metrics_provider=self._dns_metrics,
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

    async def _start_dns_gateway(self) -> None:
        if not self.settings.dns_gateway_enabled:
            return
        from mpreg.core.port_allocator import allocate_port
        from mpreg.dns import DnsGateway, DnsResolver, DnsResolverConfig

        host = self.settings.dns_listen_host or self.settings.host
        if self.settings.dns_udp_port in (None, 0):
            udp_port = allocate_port("dns-udp")
            self.settings.dns_udp_port = udp_port
            self._auto_allocated_dns_udp_port = udp_port
        else:
            udp_port = self.settings.dns_udp_port
        if self.settings.dns_tcp_port in (None, 0):
            tcp_port = allocate_port("dns-tcp")
            self.settings.dns_tcp_port = tcp_port
            self._auto_allocated_dns_tcp_port = tcp_port
        else:
            tcp_port = self.settings.dns_tcp_port

        resolver_config = DnsResolverConfig(
            zones=tuple(self.settings.dns_zones or ()),
            min_ttl_seconds=int(self.settings.dns_min_ttl_seconds),
            max_ttl_seconds=int(self.settings.dns_max_ttl_seconds),
            allow_external_names=bool(self.settings.dns_allow_external_names),
        )
        viewer_cluster_id = (
            self.settings.dns_viewer_cluster_id or self.settings.cluster_id
        )
        viewer_tenant_id = self.settings.dns_viewer_tenant_id

        def _catalog_query_with_viewer(request: CatalogQueryRequest) -> PayloadMapping:
            with self._request_viewer_context(
                viewer_cluster_id, viewer_tenant_id=viewer_tenant_id
            ):
                return self._catalog_query(request)

        resolver = DnsResolver(
            catalog_query=_catalog_query_with_viewer, config=resolver_config
        )
        gateway = DnsGateway(
            resolver=resolver,
            host=host,
            udp_port=int(udp_port or 0),
            tcp_port=int(tcp_port or 0),
        )
        await gateway.start()
        self._dns_gateway = gateway
        logger.debug(
            "[{}] DNS gateway listening on {} UDP:{} TCP:{}",
            self.settings.name,
            host,
            gateway.bound_udp_port,
            gateway.bound_tcp_port,
        )

    async def _stop_dns_gateway(self) -> None:
        if self._dns_gateway is not None:
            try:
                await self._dns_gateway.stop()
            except Exception as exc:
                logger.warning(
                    f"[{self.settings.name}] Error stopping DNS gateway: {exc}"
                )
            self._dns_gateway = None
        if self._auto_allocated_dns_udp_port is not None:
            from mpreg.core.port_allocator import release_port

            try:
                release_port(self._auto_allocated_dns_udp_port)
            except Exception as exc:
                logger.warning(
                    f"[{self.settings.name}] Error releasing DNS UDP port: {exc}"
                )
            self._auto_allocated_dns_udp_port = None
        if self._auto_allocated_dns_tcp_port is not None:
            from mpreg.core.port_allocator import release_port

            try:
                release_port(self._auto_allocated_dns_tcp_port)
            except Exception as exc:
                logger.warning(
                    f"[{self.settings.name}] Error releasing DNS TCP port: {exc}"
                )
            self._auto_allocated_dns_tcp_port = None

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
                "services": catalog.services.entry_count(),
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

    def _dns_metrics(self) -> JsonDict:
        if not self.settings.dns_gateway_enabled:
            return {"enabled": False}
        gateway = self._dns_gateway
        if gateway is None:
            return {"enabled": True, "status": "starting"}
        return {
            "enabled": True,
            "status": "running",
            "udp_port": gateway.bound_udp_port,
            "tcp_port": gateway.bound_tcp_port,
            "zones": list(self.settings.dns_zones or ()),
            "metrics": gateway.metrics_snapshot(),
        }

    def _discovery_summary_metrics(self) -> dict[str, Any]:
        timestamp = time.time()
        snapshot = self._summary_export_state.snapshot(
            enabled=self.settings.discovery_summary_export_enabled,
            interval_seconds=self.settings.discovery_summary_export_interval_seconds,
            export_scope=self.settings.discovery_summary_export_scope,
            hold_down_seconds=float(
                self.settings.discovery_summary_export_hold_down_seconds
            ),
            store_forward_seconds=float(
                self.settings.discovery_summary_export_store_forward_seconds
            ),
            store_forward_max_messages=int(
                self.settings.discovery_summary_export_store_forward_max_messages
            ),
            configured_namespaces=tuple(
                self.settings.discovery_summary_export_namespaces
            ),
            generated_at=timestamp,
        )
        return snapshot.to_dict()

    def _discovery_cache_metrics(self) -> dict[str, Any]:
        timestamp = time.time()
        namespaces = tuple(self.settings.discovery_resolver_namespaces)
        resolver = (
            self._discovery_resolver if self._discovery_resolver_enabled() else None
        )
        if resolver is None:
            resolver_response = DiscoveryResolverCacheStatsResponse(
                enabled=False,
                generated_at=timestamp,
                namespaces=namespaces,
                entry_counts=CatalogEntryCounts(),
                stats=None,
                query_cache=None,
            )
        else:
            counts = resolver.entry_counts()
            stats = resolver.stats_snapshot()
            query_cache = resolver.query_cache_snapshot()
            resolver_response = DiscoveryResolverCacheStatsResponse(
                enabled=True,
                generated_at=timestamp,
                namespaces=namespaces,
                entry_counts=counts,
                stats=stats,
                query_cache=query_cache,
            )

        summary_namespaces = tuple(self.settings.discovery_summary_resolver_namespaces)
        summary_resolver = (
            self._discovery_summary_resolver
            if self._discovery_summary_resolver_enabled()
            else None
        )
        if summary_resolver is None:
            summary_response = DiscoverySummaryCacheStatsResponse(
                enabled=False,
                generated_at=timestamp,
                namespaces=summary_namespaces,
                entry_counts=SummaryCacheEntryCounts(),
                stats=None,
            )
        else:
            summary_counts = summary_resolver.entry_counts()
            summary_stats = summary_resolver.stats_snapshot()
            summary_response = DiscoverySummaryCacheStatsResponse(
                enabled=True,
                generated_at=timestamp,
                namespaces=summary_namespaces,
                entry_counts=summary_counts,
                stats=summary_stats,
            )

        return {
            "resolver_cache": resolver_response.to_dict(),
            "summary_cache": summary_response.to_dict(),
        }

    def _discovery_policy_metrics(self) -> dict[str, Any]:
        timestamp = time.time()
        engine = self._namespace_policy_engine
        audit_log = self._namespace_policy_audit_log
        access_log = self._discovery_access_audit_log
        rules = engine.rules if engine else ()
        entries = audit_log.snapshot() if audit_log else ()
        recent_entries = entries[-20:] if entries else ()
        if access_log:
            access_entries_all = access_log.snapshot()
            access_total = len(access_entries_all)
            access_entries = access_entries_all[-20:]
        else:
            access_entries = ()
            access_total = 0
        status = DiscoveryPolicyStatus(
            enabled=bool(engine and engine.enabled),
            generated_at=timestamp,
            default_allow=engine.default_allow if engine else True,
            rule_count=len(rules),
            rules=rules,
            audit_entries=recent_entries,
            audit_total=len(entries),
            access_entries=access_entries,
            access_total=access_total,
        )
        return status.to_dict()

    def _discovery_lag_metrics(self) -> dict[str, Any]:
        timestamp = time.time()
        resolver = (
            self._discovery_resolver if self._discovery_resolver_enabled() else None
        )
        last_delta_at = resolver.stats.last_delta_at if resolver else None
        delta_lag_seconds = (
            timestamp - last_delta_at if last_delta_at is not None else None
        )
        last_seed_at = resolver.stats.last_seed_at if resolver else None
        last_summary_export_at = self._summary_export_state.last_export_at
        summary_export_lag_seconds = (
            timestamp - last_summary_export_at
            if last_summary_export_at is not None
            else None
        )
        status = DiscoveryLagStatus(
            generated_at=timestamp,
            resolver_enabled=bool(resolver),
            last_delta_at=last_delta_at,
            delta_lag_seconds=delta_lag_seconds,
            last_seed_at=last_seed_at,
            summary_export_enabled=self.settings.discovery_summary_export_enabled,
            last_summary_export_at=last_summary_export_at,
            summary_export_lag_seconds=summary_export_lag_seconds,
        )
        return status.to_dict()

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
        # Use all active links (outbound and inbound) so consensus propagation
        # works even when a peer relationship is currently inbound-only.
        active_connections = [
            (url, conn)
            for url, conn in self._get_all_peer_connections().items()
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

        # Use all active links (outbound and inbound) so vote propagation does
        # not depend on which side initiated the connection.
        active_connections = [
            (url, conn)
            for url, conn in self._get_all_peer_connections().items()
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
        scope: str | None = None,
        tags: Iterable[str] | None = None,
        namespace: str | None = None,
        capabilities: Iterable[str] | None = None,
        doc: RpcDocSpec | None = None,
        examples: Iterable[RpcExampleSpec] | None = None,
    ) -> None:
        """Register a command with the server.

        Args:
            name: The name of the command.
            func: The callable function that implements the command.
            resources: An iterable of resource strings associated with the command.
            scope: Optional discovery scope for the function endpoint.
            tags: Optional discovery tags for the function endpoint.
            namespace: Optional namespace override for discovery grouping.
            capabilities: Optional capability tags for routing/discovery filters.
            doc: Optional doc metadata override for the RPC spec.
            examples: Optional RPC examples for discovery tooling.

        Raises:
            ValueError: If the command name is already registered on this server.
        """
        logger.debug(
            "[{}] Registering function '{}' with resources {}",
            self.settings.name,
            name,
            list(resources),
        )
        registration = RpcRegistration.from_callable(
            func,
            name=name,
            function_id=function_id,
            version=version,
            namespace=namespace,
            resources=resources,
            tags=tags or (),
            scope=scope,
            capabilities=capabilities or (),
            doc=doc,
            examples=examples or (),
        )
        if self._fabric_control_plane:
            self._fabric_control_plane.catalog.functions.validate_identity(
                registration.spec.identity
            )
        self.registry.register(registration)
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
                await self._establish_peer_connection(
                    peer_url,
                    fast_connect=True,
                    dial_context="startup_seed",
                    peer_target_count=len(self.settings.peers),
                    allow_inbound_reuse=False,
                )
        if self.settings.connect:
            await self._establish_peer_connection(
                self.settings.connect,
                fast_connect=True,
                dial_context="startup_connect",
                peer_target_count=max(len(self.settings.peers or []), 1),
                allow_inbound_reuse=False,
            )

        # Start a background task to manage peer connections based on the catalog.
        # Use the proper task management system for consistent cleanup
        self._track_background_task(
            asyncio.create_task(self._manage_peer_connections())
        )

        self._track_background_task(asyncio.create_task(self._bootstrap_seed_peers()))

        await self._start_monitoring_services()
        await self._start_dns_gateway()

        # Start consensus manager for intra-cluster consensus
        await self.consensus_manager.start()

        # Populate consensus manager with known cluster peers
        self._update_consensus_known_nodes()

        # Initialize optional cache/queue systems with live event loop
        await self._initialize_default_systems()
        await self._start_fabric_control_plane()
        self._schedule_catalog_snapshots_for_existing_peers()
        await self._announce_internal_discovery_subscriptions()
        self._seed_discovery_resolver()
        self._start_discovery_resolver_prune()
        self._start_discovery_resolver_resync()
        self._start_discovery_summary_resolver_prune()
        self._start_discovery_summary_export()

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
            await self._stop_dns_gateway()
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
