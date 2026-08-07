"""
Federation Monitoring Endpoints for Production MPREG Deployments.

This module provides comprehensive HTTP endpoints for monitoring federation health,
performance, and connectivity across distributed MPREG clusters. It integrates with
the unified monitoring system and provides real-time observability into federation
operations for production environments.

Key Features:
- RESTful endpoints for federation metrics and health data
- Real-time federation graph topology visualization
- Cross-cluster performance monitoring and alerting
- Federation configuration and policy monitoring
- Integration with existing unified monitoring infrastructure
- Production-ready observability for operations teams
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field, is_dataclass
from enum import Enum
from typing import Any

from aiohttp import web
from loguru import logger

from mpreg.fabric.federation_config import FederationConfig, FederationMode
from mpreg.fabric.federation_graph import (
    FederationGraph,
    FederationGraphEdge,
    FederationGraphNode,
)

from ..core.config import MPREGSettings
from ..core.monitoring.unified_monitoring import (
    HealthStatus,
    UnifiedSystemMonitor,
)
from ..core.transport.adapter_registry import (
    AdapterEndpointRegistry,
    get_adapter_endpoint_registry,
)
from ..core.transport.enhanced_health import HealthScore
from ..datastructures.type_aliases import ClusterId
from .connection_manager import FederationConnectionManager
from .performance_metrics import (
    ClusterMetrics,
    PerformanceAlert,
    PerformanceMetricsService,
)

# Type aliases for monitoring endpoints
type EndpointPath = str
type JsonResponse = dict[str, Any]
type QueryParameters = dict[str, str]
type TimeSeriesData = list[tuple[float, float]]  # [(timestamp, value), ...]
type RouteTraceProvider = Callable[[ClusterId, tuple[ClusterId, ...]], JsonResponse]
type LinkStateStatusProvider = Callable[[], JsonResponse]
type RaftStatusProvider = Callable[[], JsonResponse]
type PersistenceSnapshotProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]
type DiscoverySummaryProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]
type DiscoveryCacheProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]
type DiscoveryPolicyProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]
type DiscoveryLagProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]
type DnsMetricsProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]
type StrongMetricsProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]
type SharedAuditMetricsProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]

def _is_dataclass_obj(value: Any) -> bool:
    return is_dataclass(value) and not isinstance(value, type)

class MonitoringEndpointType(Enum):
    """Types of federation monitoring endpoints."""

    HEALTH = "health"
    METRICS = "metrics"
    TOPOLOGY = "topology"
    PERFORMANCE = "performance"
    ALERTS = "alerts"
    CONFIGURATION = "configuration"

@dataclass(frozen=True, slots=True)
class FederationHealthSummary:
    """Summary of federation health across all clusters."""

    total_clusters: int
    healthy_clusters: int
    degraded_clusters: int
    critical_clusters: int
    unavailable_clusters: int
    total_connections: int
    active_connections: int
    overall_health_score: HealthScore
    overall_health_status: HealthStatus
    last_updated: float
    federation_mode: FederationMode
    cross_cluster_latency_p95_ms: float
    connection_success_rate_percent: float

@dataclass(frozen=True, slots=True)
class FederationTopologySnapshot:
    """Current federation topology with connection states."""

    nodes: list[FederationTopologyNode]  # Node information with health and metrics
    edges: list[FederationTopologyEdge]  # Connection information with performance data
    clusters: dict[
        ClusterId, FederationTopologyClusterSummary
    ]  # Cluster-level aggregations
    total_nodes: int
    total_edges: int
    graph_diameter: int  # Maximum shortest path between any two nodes
    clustering_coefficient: float  # Graph connectivity measure
    snapshot_timestamp: float

@dataclass(frozen=True, slots=True)
class FederationPerformanceSummary:
    """Performance summary across federation."""

    requests_per_second: float
    average_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    error_rate_percent: float
    cross_cluster_hops_average: float
    federation_efficiency_score: float  # 0.0 to 1.0
    bottleneck_clusters: list[ClusterId]
    top_performing_clusters: list[ClusterId]
    recent_performance_trend: str  # "improving", "stable", "degrading"

@dataclass(frozen=True, slots=True)
class FederationConfigurationStatus:
    """Current federation configuration and policy status."""

    federation_mode: FederationMode
    total_policies: int
    active_policies: int
    policy_compliance_percent: float
    configuration_version: str
    last_configuration_update: float
    pending_configuration_changes: int
    configuration_validation_errors: list[str]

@dataclass(frozen=True, slots=True)
class FederationTopologyPathSummary:
    """Routing path summary for topology analysis."""

    total_paths: int
    average_path_length: float
    path_efficiency: float
    redundant_paths: int

@dataclass(frozen=True, slots=True)
class FederationTopologyAnalysis:
    """High-level topology analysis summary."""

    cluster_connectivity: list[FederationTopologyNode]
    network_health: HealthStatus
    topology_score: float
    recommendations: list[str]

@dataclass(frozen=True, slots=True)
class FederationClusterHealth:
    """Health status for a specific cluster."""

    cluster_id: ClusterId
    status: HealthStatus
    health_score: float
    node_count: int
    active_connections: int
    last_heartbeat: float

@dataclass(frozen=True, slots=True)
class FederationConnectionStats:
    """Connection statistics for federation links."""

    total_connections: int
    active_connections: int
    connection_success_rate: float
    average_connection_latency_ms: float
    connections_by_cluster: dict[ClusterId, int]

@dataclass(frozen=True, slots=True)
class FederationTopologyNode:
    """Topology node details for monitoring views."""

    node_id: str
    cluster_id: ClusterId
    host: str
    port: int
    health_score: float
    status: HealthStatus
    last_seen: float

@dataclass(frozen=True, slots=True)
class FederationTopologyEdge:
    """Topology edge details for monitoring views."""

    source: ClusterId
    target: ClusterId
    status: HealthStatus
    latency_ms: float | None = None

@dataclass(frozen=True, slots=True)
class TopologyEdgeKey:
    """Typed edge key for topology snapshots."""

    source: ClusterId
    target: ClusterId

    @classmethod
    def normalized(cls, source: ClusterId, target: ClusterId) -> TopologyEdgeKey:
        if source <= target:
            return cls(source=source, target=target)
        return cls(source=target, target=source)

@dataclass(frozen=True, slots=True)
class FederationTopologyClusterSummary:
    """Cluster summary data for topology snapshots."""

    cluster_id: ClusterId
    node_count: int
    health_score: float
    status: HealthStatus

@dataclass(slots=True)
class FederationMonitoringSystem:
    """
    Production monitoring system for MPREG federation deployments.

    Provides comprehensive HTTP endpoints for monitoring federation health,
    performance, and operations across distributed clusters.
    """

    settings: MPREGSettings
    federation_config: FederationConfig
    federation_manager: FederationConnectionManager
    unified_monitor: UnifiedSystemMonitor

    # Monitoring components
    performance_service: PerformanceMetricsService | None = None
    federation_graph: FederationGraph | None = None
    route_trace_provider: RouteTraceProvider | None = None
    link_state_status_provider: LinkStateStatusProvider | None = None
    raft_status_provider: RaftStatusProvider | None = None
    adapter_endpoint_registry: AdapterEndpointRegistry | None = None
    persistence_snapshot_provider: PersistenceSnapshotProvider | None = None
    discovery_summary_provider: DiscoverySummaryProvider | None = None
    discovery_cache_provider: DiscoveryCacheProvider | None = None
    discovery_policy_provider: DiscoveryPolicyProvider | None = None
    discovery_lag_provider: DiscoveryLagProvider | None = None
    dns_metrics_provider: DnsMetricsProvider | None = None
    strong_metrics_provider: StrongMetricsProvider | None = None
    shared_audit_metrics_provider: SharedAuditMetricsProvider | None = None
    # Optional callable returning mgmt summary dicts (cluster/nodes/routes/catalog)
    mgmt_summary_provider: (
        Callable[[], Awaitable[JsonResponse] | JsonResponse] | None
    ) = None
    policy_dry_run_provider: (
        Callable[[JsonResponse], Awaitable[JsonResponse] | JsonResponse] | None
    ) = None
    # Management mutation providers (bound from MPREGServer).
    mgmt_drain_provider: (
        Callable[[JsonResponse], Awaitable[JsonResponse] | JsonResponse] | None
    ) = None
    mgmt_detach_provider: (
        Callable[[JsonResponse], Awaitable[JsonResponse] | JsonResponse] | None
    ) = None
    mgmt_policy_apply_provider: (
        Callable[[JsonResponse], Awaitable[JsonResponse] | JsonResponse] | None
    ) = None
    mgmt_audit_provider: Callable[[], list[dict[str, Any]] | dict[str, Any]] | None = (
        None
    )
    # Optional readiness override: when True, /ready returns 503 (drain).
    draining_provider: Callable[[], bool] | None = None
    # Per-server route decision audit log (bound from FabricRouter).
    route_decision_log: object | None = None
    # Optional ServerMetricsTracker (or duck-type with prometheus_lines).
    server_metrics_tracker: object | None = None

    # Web server components
    app: web.Application = field(init=False)
    runner: web.AppRunner | None = field(default=None, init=False)
    site: web.TCPSite | None = field(default=None, init=False)

    # Performance tracking
    endpoint_metrics: dict[EndpointPath, list[float]] = field(default_factory=dict)
    active_alerts: list[PerformanceAlert] = field(default_factory=list)

    # Configuration
    monitoring_port: int = 9090
    monitoring_host: str | None = None
    enable_cors: bool = False
    auth_token: str | None = None
    metrics_retention_hours: int = 24
    metrics_ingest_interval_seconds: float = 5.0
    # PERF-T10-08: scrape snapshot cache TTL (seconds); 0 disables.
    prom_cache_ttl_seconds: float = 1.0
    _prom_cache_text: list[str] | None = field(default=None, init=False, repr=False)
    _prom_cache_at: float = field(default=0.0, init=False, repr=False)

    _metrics_task: asyncio.Task | None = field(default=None, init=False)
    _metrics_running: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        """Initialize the web application and routes."""
        if self.adapter_endpoint_registry is None:
            self.adapter_endpoint_registry = get_adapter_endpoint_registry()
        self.app = web.Application()
        self._setup_routes()
        self._setup_middleware()

    def _setup_routes(self) -> None:
        """Configure HTTP routes for monitoring endpoints."""
        # Health endpoints (/live = process up; /ready = traffic-admissible)
        self.app.router.add_get("/health", self._get_federation_health)
        self.app.router.add_get("/live", self._get_live)
        self.app.router.add_get("/ready", self._get_ready)
        self.app.router.add_get("/health/summary", self._get_health_summary)
        self.app.router.add_get("/health/clusters", self._get_cluster_health)
        self.app.router.add_get(
            "/health/clusters/{cluster_id}", self._get_cluster_health_detail
        )

        # Metrics endpoints
        self.app.router.add_get("/metrics", self._get_federation_metrics)
        self.app.router.add_get("/metrics/performance", self._get_performance_metrics)
        self.app.router.add_get("/metrics/connections", self._get_connection_metrics)
        self.app.router.add_get("/metrics/timeseries", self._get_metrics_timeseries)
        self.app.router.add_get("/metrics/unified", self._get_unified_metrics)
        self.app.router.add_get("/metrics/rpc", self._get_rpc_metrics)
        self.app.router.add_get("/metrics/pubsub", self._get_pubsub_metrics)
        self.app.router.add_get("/metrics/queue", self._get_queue_metrics)
        self.app.router.add_get("/metrics/cache", self._get_cache_metrics)
        self.app.router.add_get("/metrics/transport", self._get_transport_metrics)
        self.app.router.add_get("/metrics/persistence", self._get_persistence_metrics)
        self.app.router.add_get("/metrics/strong", self._get_strong_metrics)
        self.app.router.add_get("/metrics/shared-audit", self._get_shared_audit_metrics)
        self.app.router.add_get("/metrics/prometheus", self._get_prometheus_metrics)
        self.app.router.add_get("/transport/endpoints", self._get_transport_endpoints)

        # Management read API (v1)
        self.app.router.add_get("/mgmt/v1/cluster", self._get_mgmt_cluster)
        self.app.router.add_get("/mgmt/v1/nodes", self._get_mgmt_nodes)
        self.app.router.add_get("/mgmt/v1/routes", self._get_mgmt_routes)
        self.app.router.add_get("/mgmt/v1/catalog", self._get_mgmt_catalog)
        self.app.router.add_get("/mgmt/v1/health", self._get_mgmt_health)
        self.app.router.add_get("/mgmt/v1/raft", self._get_mgmt_raft)
        self.app.router.add_get("/mgmt/v1/strong", self._get_mgmt_strong)

        # Discovery endpoints
        self.app.router.add_get("/discovery/summary", self._get_discovery_summary)
        self.app.router.add_get("/discovery/cache", self._get_discovery_cache)
        self.app.router.add_get("/discovery/policy", self._get_discovery_policy)
        self.app.router.add_get("/discovery/lag", self._get_discovery_lag)
        self.app.router.add_get("/dns/metrics", self._get_dns_metrics)

        # Topology endpoints
        self.app.router.add_get("/topology", self._get_federation_topology)
        self.app.router.add_get("/topology/graph", self._get_topology_graph)
        self.app.router.add_get("/topology/paths", self._get_topology_paths)
        self.app.router.add_get("/topology/analysis", self._get_topology_analysis)

        # Performance endpoints
        self.app.router.add_get("/performance", self._get_performance_summary)
        self.app.router.add_get(
            "/performance/bottlenecks", self._get_performance_bottlenecks
        )
        self.app.router.add_get("/performance/trends", self._get_performance_trends)
        self.app.router.add_get(
            "/performance/clusters/{cluster_id}", self._get_cluster_performance
        )

        # Alert endpoints
        self.app.router.add_get("/alerts", self._get_active_alerts)
        self.app.router.add_get("/alerts/history", self._get_alert_history)
        self.app.router.add_post("/alerts/acknowledge", self._acknowledge_alert)

        # Configuration endpoints
        self.app.router.add_get("/config", self._get_federation_configuration)
        self.app.router.add_get("/config/policies", self._get_federation_policies)
        self.app.router.add_get("/config/validation", self._validate_federation_config)

        # Routing endpoints
        self.app.router.add_get("/routing/trace", self._get_route_trace)
        self.app.router.add_get("/routing/decisions", self._get_route_decisions)
        self.app.router.add_get("/routing/link-state", self._get_link_state_status)
        self.app.router.add_post("/mgmt/v1/policy/dry-run", self._post_policy_dry_run)
        self.app.router.add_post("/mgmt/v1/nodes/drain", self._post_mgmt_drain)
        self.app.router.add_post("/mgmt/v1/peers/detach", self._post_mgmt_detach)
        self.app.router.add_post("/mgmt/v1/policy/apply", self._post_mgmt_policy_apply)
        self.app.router.add_get("/mgmt/v1/audit", self._get_mgmt_audit)

        # Utility endpoints
        self.app.router.add_get("/", self._get_monitoring_index)
        self.app.router.add_get("/endpoints", self._get_available_endpoints)
        self.app.router.add_get("/openapi.json", self._get_openapi)
        self.app.router.add_get("/mgmt/v1/schema", self._get_openapi)

    def _ready_min_score(self) -> float:
        """OBS-T10-07: minimum federation health score for /ready 200.

        Default 0.4 admits DEGRADED. Operators may raise via settings.ready_min_score
        or instance attribute ready_min_score (e.g. 0.85 for strict).
        """
        raw = getattr(self, "ready_min_score", None)
        if raw is None and getattr(self, "settings", None) is not None:
            raw = getattr(self.settings, "ready_min_score", None)
        try:
            return float(raw if raw is not None else 0.4)
        except TypeError, ValueError:
            return 0.4

    def _setup_middleware(self) -> None:
        """Configure middleware for request processing."""

        @web.middleware
        async def auth_middleware(request: web.Request, handler) -> web.Response:
            """Bearer-token gate for monitoring endpoints.

            ERG-T10-04: when no token is configured, mutation methods (POST/PUT/
            PATCH/DELETE) are refused except from loopback — fail-closed by default.
            GET/HEAD/OPTIONS stay open for local doctor/scrape unless a token is set
            (then all methods require the token).
            """
            token = self.auth_token or getattr(
                self.settings, "monitoring_auth_token", None
            )
            method = (request.method or "GET").upper()
            is_mutation = method in {"POST", "PUT", "PATCH", "DELETE"}
            if token:
                auth_header = request.headers.get("Authorization", "")
                alt = request.headers.get("X-MPREG-Monitoring-Token", "")
                expected = f"Bearer {token}"
                if auth_header != expected and alt != token:
                    return web.json_response(
                        {
                            "error": "unauthorized",
                            "detail": "valid monitoring token required",
                        },
                        status=401,
                    )
                return await handler(request)
            peer = ""
            try:
                peer = str(request.remote or "")
            except Exception:
                peer = ""
            loopback = peer in {"127.0.0.1", "::1", "localhost"} or peer.startswith(
                "127."
            )
            bind_host = str(
                getattr(self, "host", None)
                or getattr(getattr(self, "settings", None), "monitoring_host", "")
                or "127.0.0.1"
            ).strip()
            bind_loopback = bind_host in {
                "127.0.0.1",
                "::1",
                "localhost",
            } or bind_host.startswith("127.")
            require_reads = bool(
                getattr(self, "monitoring_auth_required_for_reads", False)
                or getattr(
                    getattr(self, "settings", None),
                    "monitoring_auth_required_for_reads",
                    False,
                )
            )
            # Default ERG-T11-03: public bind without token → require token for
            # non-loopback clients on ALL methods (scrapers must set token or
            # bind mon to loopback).
            if not token and not bind_loopback and not loopback:
                return web.json_response(
                    {
                        "error": "unauthorized",
                        "detail": (
                            "monitoring requires monitoring_auth_token when bound "
                            "non-loopback (ERG-T11-03); use 127.0.0.1 or set token"
                        ),
                    },
                    status=401,
                )
            if is_mutation and not token and not loopback:
                return web.json_response(
                    {
                        "error": "unauthorized",
                        "detail": (
                            "monitoring mutations require monitoring_auth_token "
                            "when not on loopback (ERG-T10-04)"
                        ),
                    },
                    status=401,
                )
            if require_reads and not token and not loopback:
                return web.json_response(
                    {
                        "error": "unauthorized",
                        "detail": "monitoring_auth_required_for_reads",
                    },
                    status=401,
                )
            return await handler(request)

        @web.middleware
        async def cors_middleware(request: web.Request, handler) -> web.Response:
            """Handle CORS headers for browser access when explicitly enabled."""
            if request.method == "OPTIONS" and self.enable_cors:
                response = web.Response(status=204)
            else:
                response = await handler(request)
            if self.enable_cors:
                response.headers["Access-Control-Allow-Origin"] = "*"
                response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
                response.headers["Access-Control-Allow-Headers"] = (
                    "Content-Type, Authorization, X-MPREG-Monitoring-Token"
                )
            return response

        @web.middleware
        async def metrics_middleware(request: web.Request, handler) -> web.Response:
            """Track endpoint performance metrics."""
            start_time = time.time()
            try:
                response = await handler(request)
                duration_ms = (time.time() - start_time) * 1000.0

                # Track endpoint performance
                endpoint = request.path
                if endpoint not in self.endpoint_metrics:
                    self.endpoint_metrics[endpoint] = []

                self.endpoint_metrics[endpoint].append(duration_ms)
                # Keep only recent measurements
                if len(self.endpoint_metrics[endpoint]) > 1000:
                    self.endpoint_metrics[endpoint] = self.endpoint_metrics[endpoint][
                        -1000:
                    ]

                # Add performance headers
                response.headers["X-Response-Time-Ms"] = str(duration_ms)
                response.headers["X-Monitoring-System"] = "MPREG-Federation-Monitor"

                return response
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000.0
                logger.error(
                    f"Error processing {request.path}: {e} (took {duration_ms:.1f}ms)"
                )
                raise

        # aiohttp applies middlewares in reverse order of append.
        self.app.middlewares.append(metrics_middleware)
        self.app.middlewares.append(cors_middleware)
        self.app.middlewares.append(auth_middleware)

    async def start(self) -> None:
        """Start the federation monitoring HTTP server."""
        try:
            if self.performance_service:
                self._metrics_running = True
                self._metrics_task = asyncio.create_task(self._metrics_ingest_loop())
            await self._start_monitoring_server()
        except OSError as e:
            if self.monitoring_port != 0:
                logger.warning(
                    f"Monitoring port {self.monitoring_port} unavailable: {e}. Falling back to an ephemeral port."
                )
                self.monitoring_port = 0
                await self._start_monitoring_server()
            else:
                logger.error(f"Failed to start federation monitoring server: {e}")
                raise
        except Exception as e:
            logger.error(f"Failed to start federation monitoring server: {e}")
            raise

    async def _start_monitoring_server(self) -> None:
        """Initialize and start the monitoring server with current port settings."""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()

        self.site = web.TCPSite(
            self.runner,
            host=self.monitoring_host or self.settings.host,
            port=self.monitoring_port,
        )
        await self.site.start()

        if (
            self.monitoring_port == 0
            and self.site._server
            and self.site._server.sockets
        ):
            self.monitoring_port = self.site._server.sockets[0].getsockname()[1]

        logger.debug(
            f"Federation monitoring server started on "
            f"http://{self.monitoring_host or self.settings.host}:{self.monitoring_port}"
        )

    async def stop(self) -> None:
        """Stop the federation monitoring HTTP server."""
        try:
            if self._metrics_task is not None:
                self._metrics_running = False
                self._metrics_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._metrics_task
                self._metrics_task = None

            if self.site:
                await self.site.stop()
                self.site = None

            if self.runner:
                try:
                    await self.runner.cleanup()
                except AttributeError:
                    logger.debug(
                        "Monitoring runner already closed or missing app; skipping cleanup."
                    )
                self.runner = None

            await self.unified_monitor.stop()

            logger.debug("Federation monitoring server stopped")

        except Exception as e:
            logger.error(f"Error stopping federation monitoring server: {e}")

    # Health monitoring endpoints

    async def _get_live(self, request: web.Request) -> web.Response:
        """probe-style liveness: process is up and serving HTTP."""
        return web.json_response(
            {"status": "live", "timestamp": time.time()},
            status=200,
        )

    async def _get_ready(self, request: web.Request) -> web.Response:
        """Readiness: refuse traffic when draining or federation health is bad.

        OBS-07/T10-07: returns ready when health score >= ready_min_score (default 0.4) and the
        node is not draining. Operators must not treat HTTP 200 here as "fully healthy";
        use /health and federation health_score for full status. Drain alone forces 503.
        """
        try:
            draining = False
            drain_fn = getattr(self, "draining_provider", None)
            if drain_fn is not None:
                try:
                    draining = bool(drain_fn())
                except Exception as exc:  # noqa: BLE001
                    logger.debug("draining_provider failed: {}", exc)
                    draining = False

            health_summary = await self._collect_health_summary()
            status = health_summary.overall_health_status
            score = float(health_summary.overall_health_score)
            # CRITICAL / UNAVAILABLE → not ready (503). DEGRADED still admits traffic.
            not_ready_values = {"critical", "unavailable", "unhealthy"}
            status_value = getattr(status, "value", str(status)).lower()
            ready = (
                not draining
                and status_value not in not_ready_values
                and score >= self._ready_min_score()
            )
            body = {
                "status": "ready" if ready else "not_ready",
                "ready": ready,
                "draining": draining,
                "health_score": score,
                "overall_status": status_value,
                "ready_min_score": self._ready_min_score(),
                "timestamp": time.time(),
            }
            # OBS-T11-03 / OBS-T15-01: mirror admission into Prom gauge
            # (field is server_metrics_tracker — prior lookup missed it)
            tracker = self._resolve_server_metrics_tracker()
            if tracker is not None and hasattr(tracker, "set_ready"):
                tracker.set_ready(bool(ready))
            return web.json_response(body, status=200 if ready else 503)
        except Exception as e:
            logger.error("Error computing readiness: {}", e)
            return web.json_response(
                {
                    "status": "not_ready",
                    "ready": False,
                    "message": str(e),
                    "timestamp": time.time(),
                },
                status=503,
            )

    async def _get_federation_health(self, request: web.Request) -> web.Response:
        """Get overall federation health status.

        Always returns HTTP 200 when the process can answer (liveness-shaped).
        Use ``/ready`` for traffic admission based on health score/status.
        """
        try:
            health_summary = await self._collect_health_summary()
            status_value = health_summary.overall_health_status.value
            score = float(health_summary.overall_health_score)

            return web.json_response(
                {
                    "status": "ok",
                    "ready": (
                        status_value.lower()
                        not in {"critical", "unavailable", "unhealthy"}
                        and score >= self._ready_min_score()
                        and not (
                            bool(self.draining_provider())
                            if callable(getattr(self, "draining_provider", None))
                            else False
                        )
                    ),
                    "federation_health": {
                        "overall_status": status_value,
                        "health_score": health_summary.overall_health_score,
                        "total_clusters": health_summary.total_clusters,
                        "healthy_clusters": health_summary.healthy_clusters,
                        "degraded_clusters": health_summary.degraded_clusters,
                        "critical_clusters": health_summary.critical_clusters,
                        "unavailable_clusters": health_summary.unavailable_clusters,
                        "connection_health": {
                            "total_connections": health_summary.total_connections,
                            "active_connections": health_summary.active_connections,
                            "success_rate_percent": health_summary.connection_success_rate_percent,
                        },
                        "performance": {
                            "cross_cluster_latency_p95_ms": health_summary.cross_cluster_latency_p95_ms,
                            "federation_mode": health_summary.federation_mode.value,
                        },
                        "last_updated": health_summary.last_updated,
                    },
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error("Error getting federation health: {}", e)
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_health_summary(self, request: web.Request) -> web.Response:
        """Get detailed health summary with cluster breakdown."""
        try:
            health_summary = await self._collect_health_summary()
            cluster_health = await self._collect_cluster_health_details()

            return web.json_response(
                {
                    "status": "ok",
                    "health_summary": {
                        "overall": {
                            "health_score": health_summary.overall_health_score,
                            "status": health_summary.overall_health_status.value,
                            "federation_mode": health_summary.federation_mode.value,
                        },
                        "cluster_counts": {
                            "total": health_summary.total_clusters,
                            "healthy": health_summary.healthy_clusters,
                            "degraded": health_summary.degraded_clusters,
                            "critical": health_summary.critical_clusters,
                            "unavailable": health_summary.unavailable_clusters,
                        },
                        "connections": {
                            "total": health_summary.total_connections,
                            "active": health_summary.active_connections,
                            "success_rate_percent": health_summary.connection_success_rate_percent,
                        },
                        "cluster_details": {
                            cluster_id: self._serialize_cluster_health(details)
                            for cluster_id, details in cluster_health.items()
                        },
                    },
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting health summary: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_cluster_health(self, request: web.Request) -> web.Response:
        """Get health status for all clusters."""
        try:
            cluster_health = await self._collect_cluster_health_details()

            return web.json_response(
                {
                    "status": "ok",
                    "clusters": {
                        cluster_id: self._serialize_cluster_health(details)
                        for cluster_id, details in cluster_health.items()
                    },
                    "total_clusters": len(cluster_health),
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting cluster health: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_cluster_health_detail(self, request: web.Request) -> web.Response:
        """Get detailed health information for a specific cluster."""
        cluster_id = request.match_info.get("cluster_id")
        if not cluster_id:
            return web.json_response(
                {"status": "error", "message": "cluster_id required"}, status=400
            )

        try:
            cluster_details = await self._collect_cluster_detail(cluster_id)

            if not cluster_details:
                return web.json_response(
                    {"status": "error", "message": f"Cluster {cluster_id} not found"},
                    status=404,
                )

            return web.json_response(
                {
                    "status": "ok",
                    "cluster": self._serialize_cluster_health(cluster_details),
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting cluster {cluster_id} health: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    # Metrics endpoints

    async def _get_federation_metrics(self, request: web.Request) -> web.Response:
        """Get comprehensive federation metrics."""
        try:
            unified_metrics = await self.unified_monitor.get_unified_metrics()

            return web.json_response(
                {
                    "status": "ok",
                    "federation_metrics": {
                        "federation_system": {
                            "requests_per_second": unified_metrics.federation_metrics.requests_per_second
                            if unified_metrics.federation_metrics
                            else 0.0,
                            "average_latency_ms": unified_metrics.federation_metrics.average_latency_ms
                            if unified_metrics.federation_metrics
                            else 0.0,
                            "p95_latency_ms": unified_metrics.federation_metrics.p95_latency_ms
                            if unified_metrics.federation_metrics
                            else 0.0,
                            "error_rate_percent": unified_metrics.federation_metrics.error_rate_percent
                            if unified_metrics.federation_metrics
                            else 0.0,
                            "active_connections": unified_metrics.federation_metrics.active_connections
                            if unified_metrics.federation_metrics
                            else 0,
                        },
                        "correlation_metrics": {
                            "total_cross_system_correlations": unified_metrics.correlation_metrics.total_cross_system_correlations,
                            "federation_success_rate_percent": unified_metrics.correlation_metrics.federation_success_rate_percent,
                            "cross_cluster_latency_p95_ms": unified_metrics.correlation_metrics.cross_cluster_latency_p95_ms,
                            "federation_message_hops": unified_metrics.correlation_metrics.federation_message_hops,
                        },
                        "overall_health": {
                            "health_score": unified_metrics.overall_health_score,
                            "status": unified_metrics.overall_health_status.value,
                            "systems_healthy": unified_metrics.systems_healthy,
                            "systems_degraded": unified_metrics.systems_degraded,
                            "systems_critical": unified_metrics.systems_critical,
                        },
                    },
                    "collection_info": {
                        "collection_timestamp": unified_metrics.collection_timestamp,
                        "collection_duration_ms": unified_metrics.collection_duration_ms,
                    },
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting federation metrics: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_performance_metrics(self, request: web.Request) -> web.Response:
        """Get federation performance metrics."""
        try:
            performance_summary = await self._collect_performance_summary()

            return web.json_response(
                {
                    "status": "ok",
                    "performance": {
                        "throughput": {
                            "requests_per_second": performance_summary.requests_per_second
                        },
                        "latency": {
                            "average_ms": performance_summary.average_latency_ms,
                            "p95_ms": performance_summary.p95_latency_ms,
                            "p99_ms": performance_summary.p99_latency_ms,
                        },
                        "reliability": {
                            "error_rate_percent": performance_summary.error_rate_percent,
                            "federation_efficiency_score": performance_summary.federation_efficiency_score,
                        },
                        "topology": {
                            "cross_cluster_hops_average": performance_summary.cross_cluster_hops_average,
                            "bottleneck_clusters": performance_summary.bottleneck_clusters,
                            "top_performing_clusters": performance_summary.top_performing_clusters,
                        },
                        "trend": performance_summary.recent_performance_trend,
                    },
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting performance metrics: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_connection_metrics(self, request: web.Request) -> web.Response:
        """Get federation connection metrics."""
        try:
            connection_stats = await self._collect_connection_statistics()

            return web.json_response(
                {
                    "status": "ok",
                    "connections": self._serialize_connection_stats(connection_stats),
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting connection metrics: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_metrics_timeseries(self, request: web.Request) -> web.Response:
        """Get time series metrics data."""
        try:
            # Parse query parameters
            metric_name = request.query.get("metric", "health_score")
            duration_hours = int(request.query.get("duration", "1"))
            resolution_minutes = int(request.query.get("resolution", "1"))

            timeseries_data = await self._collect_timeseries_data(
                metric_name, duration_hours, resolution_minutes
            )

            return web.json_response(
                {
                    "status": "ok",
                    "metric": metric_name,
                    "duration_hours": duration_hours,
                    "resolution_minutes": resolution_minutes,
                    "data_points": len(timeseries_data),
                    "data": timeseries_data,
                    "timestamp": time.time(),
                }
            )

        except ValueError as e:
            return web.json_response(
                {"status": "error", "message": f"Invalid parameter: {e}"}, status=400
            )
        except Exception as e:
            logger.error(f"Error getting metrics timeseries: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_unified_metrics(self, request: web.Request) -> web.Response:
        """Get raw unified monitoring metrics across all systems."""
        try:
            unified_metrics = await self.unified_monitor.get_unified_metrics()
            return web.json_response(
                {
                    "status": "ok",
                    "unified_metrics": self._serialize_unified_metrics(unified_metrics),
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error getting unified metrics: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_rpc_metrics(self, request: web.Request) -> web.Response:
        """Get RPC system metrics."""
        return await self._get_system_metrics("rpc")

    async def _get_pubsub_metrics(self, request: web.Request) -> web.Response:
        """Get Pub/Sub system metrics."""
        return await self._get_system_metrics("pubsub")

    async def _get_queue_metrics(self, request: web.Request) -> web.Response:
        """Get queue system metrics."""
        return await self._get_system_metrics("queue")

    async def _get_cache_metrics(self, request: web.Request) -> web.Response:
        """Get cache system metrics."""
        return await self._get_system_metrics("cache")

    async def _get_transport_metrics(self, request: web.Request) -> web.Response:
        """Get transport monitoring metrics."""
        try:
            unified_metrics = await self.unified_monitor.get_unified_metrics()
            snapshots = {
                endpoint: asdict(snapshot)
                for endpoint, snapshot in unified_metrics.transport_health_snapshots.items()
            }
            return web.json_response(
                {
                    "status": "ok",
                    "transport_health_snapshots": snapshots,
                    "correlation_results": [
                        asdict(result)
                        for result in unified_metrics.transport_correlation_results
                    ],
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error getting transport metrics: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_persistence_metrics(self, request: web.Request) -> web.Response:
        """Get persistence snapshot metrics."""
        provider = self.persistence_snapshot_provider
        if provider is None:
            return web.json_response(
                {
                    "status": "ok",
                    "persistence_snapshots": {"enabled": False},
                    "timestamp": time.time(),
                }
            )
        try:
            payload = provider()
            if inspect.isawaitable(payload):
                payload = await payload
            return web.json_response(
                {
                    "status": "ok",
                    "persistence_snapshots": payload,
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error getting persistence metrics: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_strong_metrics(self, request: web.Request) -> web.Response:
        """STRONG majority-commit put metrics (process-local; not WAN SLA)."""
        provider = self.strong_metrics_provider
        if provider is None:
            return web.json_response(
                {
                    "status": "ok",
                    "strong": {
                        "enabled_flag": False,
                        "coordinator_bound": False,
                        "health": "unwired",
                        "counters": {},
                        "latency_ms": {},
                    },
                    "timestamp": time.time(),
                }
            )
        try:
            payload = provider()
            if inspect.isawaitable(payload):
                payload = await payload
            return web.json_response(
                {
                    "status": "ok",
                    "strong": payload if isinstance(payload, dict) else {"raw": payload},
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error getting strong metrics: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_shared_audit_metrics(self, request: web.Request) -> web.Response:
        """Shared audit G-Set epidemic metrics for operators."""
        provider = self.shared_audit_metrics_provider
        if provider is None:
            return web.json_response(
                {
                    "status": "ok",
                    "shared_audit": {
                        "enabled_flag": False,
                        "store_present": False,
                        "status": "unwired",
                        "counters": {},
                    },
                    "timestamp": time.time(),
                }
            )
        try:
            payload = provider()
            if inspect.isawaitable(payload):
                payload = await payload
            return web.json_response(
                {
                    "status": "ok",
                    "shared_audit": (
                        payload if isinstance(payload, dict) else {"raw": payload}
                    ),
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error getting shared audit metrics: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_mgmt_strong(self, request: web.Request) -> web.Response:
        """Management snapshot for STRONG readiness (same payload as /metrics/strong)."""
        return await self._get_strong_metrics(request)

    async def _get_discovery_summary(self, request: web.Request) -> web.Response:
        """Get discovery summary export status."""
        provider = self.discovery_summary_provider
        if provider is None:
            return web.json_response(
                {
                    "status": "ok",
                    "summary_export": {"enabled": False},
                    "timestamp": time.time(),
                }
            )
        try:
            payload = provider()
            if inspect.isawaitable(payload):
                payload = await payload
            return web.json_response(
                {
                    "status": "ok",
                    "summary_export": payload,
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error getting discovery summary status: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_discovery_cache(self, request: web.Request) -> web.Response:
        """Get discovery resolver cache status."""
        provider = self.discovery_cache_provider
        if provider is None:
            return web.json_response(
                {
                    "status": "ok",
                    "resolver_cache": {"enabled": False},
                    "timestamp": time.time(),
                }
            )
        try:
            payload = provider()
            if inspect.isawaitable(payload):
                payload = await payload
            if isinstance(payload, dict) and (
                "resolver_cache" in payload or "summary_cache" in payload
            ):
                response_payload = dict(payload)
                return web.json_response(
                    {
                        "status": "ok",
                        **response_payload,
                        "timestamp": time.time(),
                    }
                )
            return web.json_response(
                {
                    "status": "ok",
                    "resolver_cache": payload,
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error getting discovery cache status: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_discovery_policy(self, request: web.Request) -> web.Response:
        """Get discovery namespace policy status."""
        provider = self.discovery_policy_provider
        if provider is None:
            return web.json_response(
                {
                    "status": "ok",
                    "policy": {"enabled": False},
                    "timestamp": time.time(),
                }
            )
        try:
            payload = provider()
            if inspect.isawaitable(payload):
                payload = await payload
            return web.json_response(
                {
                    "status": "ok",
                    "policy": payload,
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error getting discovery policy status: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_discovery_lag(self, request: web.Request) -> web.Response:
        """Get discovery lag status."""
        provider = self.discovery_lag_provider
        if provider is None:
            return web.json_response(
                {
                    "status": "ok",
                    "lag": {"resolver_enabled": False},
                    "timestamp": time.time(),
                }
            )
        try:
            payload = provider()
            if inspect.isawaitable(payload):
                payload = await payload
            return web.json_response(
                {
                    "status": "ok",
                    "lag": payload,
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error getting discovery lag status: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_dns_metrics(self, request: web.Request) -> web.Response:
        """Get DNS gateway metrics."""
        provider = self.dns_metrics_provider
        if provider is None:
            return web.json_response(
                {
                    "status": "ok",
                    "dns_gateway": {"enabled": False},
                    "timestamp": time.time(),
                }
            )
        try:
            payload = provider()
            if inspect.isawaitable(payload):
                payload = await payload
            return web.json_response(
                {
                    "status": "ok",
                    "dns_gateway": payload,
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error getting DNS metrics: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_transport_endpoints(self, request: web.Request) -> web.Response:
        """Get transport endpoint assignments (auto-allocated ports)."""
        try:
            registry = self.adapter_endpoint_registry
            snapshot = registry.detailed_snapshot() if registry else {}
            return web.json_response(
                {
                    "status": "ok",
                    "transport_endpoints": snapshot,
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error getting transport endpoints: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_system_metrics(self, system_name: str) -> web.Response:
        """Return metrics for a specific system from unified monitoring."""
        try:
            unified_metrics = await self.unified_monitor.get_unified_metrics()
            system_map = {
                "rpc": unified_metrics.rpc_metrics,
                "pubsub": unified_metrics.pubsub_metrics,
                "queue": unified_metrics.queue_metrics,
                "cache": unified_metrics.cache_metrics,
                "federation": unified_metrics.federation_metrics,
            }
            metrics = system_map.get(system_name)
            return web.json_response(
                {
                    "status": "ok",
                    "system": system_name,
                    "metrics": self._serialize_system_metrics(metrics),
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error getting {system_name} metrics: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    def _serialize_system_metrics(self, metrics: Any) -> dict[str, Any] | None:
        if metrics is None:
            return None
        return {
            "system_type": metrics.system_type.value,
            "system_name": metrics.system_name,
            "requests_per_second": metrics.requests_per_second,
            "average_latency_ms": metrics.average_latency_ms,
            "p95_latency_ms": metrics.p95_latency_ms,
            "p99_latency_ms": metrics.p99_latency_ms,
            "error_rate_percent": metrics.error_rate_percent,
            "active_connections": metrics.active_connections,
            "total_operations_last_hour": metrics.total_operations_last_hour,
            "last_updated": metrics.last_updated,
        }

    def _serialize_unified_metrics(self, metrics: Any) -> dict[str, Any]:
        return {
            "rpc": self._serialize_system_metrics(metrics.rpc_metrics),
            "pubsub": self._serialize_system_metrics(metrics.pubsub_metrics),
            "queue": self._serialize_system_metrics(metrics.queue_metrics),
            "cache": self._serialize_system_metrics(metrics.cache_metrics),
            "federation": self._serialize_system_metrics(metrics.federation_metrics),
            "transport_health_snapshots": {
                endpoint: asdict(snapshot)
                for endpoint, snapshot in metrics.transport_health_snapshots.items()
            },
            "correlation_metrics": {
                "rpc_to_pubsub_events": metrics.correlation_metrics.rpc_to_pubsub_events,
                "rpc_to_queue_events": metrics.correlation_metrics.rpc_to_queue_events,
                "rpc_to_cache_events": metrics.correlation_metrics.rpc_to_cache_events,
                "pubsub_to_queue_events": metrics.correlation_metrics.pubsub_to_queue_events,
                "pubsub_to_cache_events": metrics.correlation_metrics.pubsub_to_cache_events,
                "queue_to_cache_events": metrics.correlation_metrics.queue_to_cache_events,
                "federation_message_hops": metrics.correlation_metrics.federation_message_hops,
                "cross_cluster_latency_p95_ms": metrics.correlation_metrics.cross_cluster_latency_p95_ms,
                "federation_success_rate_percent": metrics.correlation_metrics.federation_success_rate_percent,
                "transport_health_scores": metrics.correlation_metrics.transport_health_scores,
                "circuit_breaker_events": metrics.correlation_metrics.circuit_breaker_events,
                "correlation_tracking_success_rate": metrics.correlation_metrics.correlation_tracking_success_rate,
                "end_to_end_latency_p95_ms": metrics.correlation_metrics.end_to_end_latency_p95_ms,
                "total_cross_system_correlations": metrics.correlation_metrics.total_cross_system_correlations,
                "correlation_success_rate_percent": metrics.correlation_metrics.correlation_success_rate_percent,
            },
            "overall_health": {
                "health_score": metrics.overall_health_score,
                "status": metrics.overall_health_status.value,
                "systems_healthy": metrics.systems_healthy,
                "systems_degraded": metrics.systems_degraded,
                "systems_critical": metrics.systems_critical,
                "systems_unavailable": metrics.systems_unavailable,
            },
            "performance_summary": {
                "total_requests_per_second": metrics.total_requests_per_second,
                "average_cross_system_latency_ms": metrics.average_cross_system_latency_ms,
                "total_active_correlations": metrics.total_active_correlations,
            },
            "collection_info": {
                "collection_timestamp": metrics.collection_timestamp,
                "collection_duration_ms": metrics.collection_duration_ms,
            },
        }

    # Topology endpoints

    async def _get_federation_topology(self, request: web.Request) -> web.Response:
        """Get current federation topology."""
        try:
            topology = await self._collect_topology_snapshot()

            return web.json_response(
                {
                    "status": "ok",
                    "topology": {
                        "summary": {
                            "total_nodes": topology.total_nodes,
                            "total_edges": topology.total_edges,
                            "graph_diameter": topology.graph_diameter,
                            "clustering_coefficient": topology.clustering_coefficient,
                        },
                        "nodes": [
                            self._serialize_topology_node(node)
                            for node in topology.nodes
                        ],
                        "edges": [
                            self._serialize_topology_edge(edge)
                            for edge in topology.edges
                        ],
                        "clusters": {
                            cluster_id: self._serialize_topology_cluster(cluster_info)
                            for cluster_id, cluster_info in topology.clusters.items()
                        },
                        "snapshot_timestamp": topology.snapshot_timestamp,
                    },
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting federation topology: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    # Configuration endpoints

    async def _get_federation_configuration(self, request: web.Request) -> web.Response:
        """Get current federation configuration."""
        try:
            config_status = await self._collect_configuration_status()

            return web.json_response(
                {
                    "status": "ok",
                    "configuration": {
                        "federation_mode": config_status.federation_mode.value,
                        "policies": {
                            "total": config_status.total_policies,
                            "active": config_status.active_policies,
                            "compliance_percent": config_status.policy_compliance_percent,
                        },
                        "version_info": {
                            "configuration_version": config_status.configuration_version,
                            "last_update": config_status.last_configuration_update,
                            "pending_changes": config_status.pending_configuration_changes,
                        },
                        "validation": {
                            "errors": config_status.configuration_validation_errors
                        },
                    },
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting federation configuration: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    # Utility endpoints

    async def _get_monitoring_index(self, request: web.Request) -> web.Response:
        """Get monitoring system information and available endpoints."""
        endpoints = await self._collect_available_endpoints()

        return web.json_response(
            {
                "service": "MPREG Federation Monitoring",
                "version": "1.0.0",
                "federation_mode": self.federation_config.federation_mode.value,
                "cluster_id": self.settings.cluster_id,
                "monitoring_port": self.monitoring_port,
                "endpoints": endpoints,
                "timestamp": time.time(),
            }
        )

    async def _get_available_endpoints(self, request: web.Request) -> web.Response:
        """Get list of available monitoring endpoints."""
        endpoints = await self._collect_available_endpoints()

        return web.json_response(
            {
                "status": "ok",
                "endpoints": endpoints,
                "total_endpoints": len(endpoints),
                "timestamp": time.time(),
            }
        )

    # Data collection methods

    async def _collect_health_summary(self) -> FederationHealthSummary:
        """Collect comprehensive federation health summary."""
        # This would integrate with the federation connection manager
        # and unified monitoring system to collect real health data

        # For now, return a structured response based on available data
        unified_metrics = await self.unified_monitor.get_unified_metrics()
        cluster_health = await self._collect_cluster_health_details()

        total_clusters = len(cluster_health) if cluster_health else 1
        status_counts = {status.value: 0 for status in HealthStatus}
        for details in cluster_health.values():
            status_counts[details.status.value] = (
                status_counts.get(details.status.value, 0) + 1
            )

        connection_stats = await self._collect_connection_statistics()

        return FederationHealthSummary(
            total_clusters=total_clusters,
            healthy_clusters=status_counts.get("healthy", 0),
            degraded_clusters=status_counts.get("degraded", 0),
            critical_clusters=status_counts.get("critical", 0),
            unavailable_clusters=status_counts.get("unavailable", 0),
            total_connections=connection_stats.total_connections,
            active_connections=connection_stats.active_connections,
            overall_health_score=unified_metrics.overall_health_score,
            overall_health_status=unified_metrics.overall_health_status,
            last_updated=time.time(),
            federation_mode=self.federation_config.federation_mode,
            cross_cluster_latency_p95_ms=unified_metrics.correlation_metrics.cross_cluster_latency_p95_ms,
            connection_success_rate_percent=connection_stats.connection_success_rate,
        )

    async def _collect_cluster_health_details(
        self,
    ) -> dict[ClusterId, FederationClusterHealth]:
        """Collect detailed health information for all clusters."""
        cluster_health: dict[ClusterId, FederationClusterHealth] = {
            self.settings.cluster_id: FederationClusterHealth(
                cluster_id=self.settings.cluster_id,
                status=HealthStatus.HEALTHY,
                health_score=1.0,
                node_count=1,
                active_connections=0,
                last_heartbeat=time.time(),
            )
        }

        active_connections = (
            self.federation_manager.federation_manager.active_connections
        )

        for cluster_id, connections in active_connections.items():
            if not connections:
                cluster_health[cluster_id] = FederationClusterHealth(
                    cluster_id=cluster_id,
                    status=HealthStatus.UNAVAILABLE,
                    health_score=0.0,
                    node_count=0,
                    active_connections=0,
                    last_heartbeat=0.0,
                )
                continue

            health_scores = [conn.success_rate for conn in connections]
            avg_health = sum(health_scores) / len(health_scores)
            healthy_connections = sum(1 for conn in connections if conn.is_healthy)

            if avg_health >= 0.9:
                status = HealthStatus.HEALTHY
            elif avg_health >= 0.7:
                status = HealthStatus.DEGRADED
            else:
                status = HealthStatus.CRITICAL

            cluster_health[cluster_id] = FederationClusterHealth(
                cluster_id=cluster_id,
                status=status,
                health_score=avg_health,
                node_count=len(connections),
                active_connections=healthy_connections,
                last_heartbeat=max(conn.last_health_check_at for conn in connections),
            )

        return cluster_health

    async def _collect_cluster_detail(
        self, cluster_id: ClusterId
    ) -> FederationClusterHealth | None:
        """Collect detailed information for a specific cluster."""
        cluster_health = await self._collect_cluster_health_details()
        return cluster_health.get(cluster_id)

    async def _collect_performance_summary(self) -> FederationPerformanceSummary:
        """Collect federation performance summary."""
        unified_metrics = await self.unified_monitor.get_unified_metrics()
        connection_stats = await self._collect_connection_statistics()
        trend = "stable"
        if self.performance_service:
            trend = self._calculate_trend_from_federation_metrics(
                "federation_avg_latency_ms", hours=1
            )

        return FederationPerformanceSummary(
            requests_per_second=unified_metrics.total_requests_per_second,
            average_latency_ms=unified_metrics.average_cross_system_latency_ms,
            p95_latency_ms=unified_metrics.correlation_metrics.cross_cluster_latency_p95_ms,
            p99_latency_ms=unified_metrics.correlation_metrics.cross_cluster_latency_p95_ms
            * 1.2,  # Estimate
            error_rate_percent=max(
                0.0, 100.0 - connection_stats.connection_success_rate
            ),
            cross_cluster_hops_average=sum(
                unified_metrics.correlation_metrics.federation_message_hops
            )
            / max(len(unified_metrics.correlation_metrics.federation_message_hops), 1),
            federation_efficiency_score=(
                connection_stats.connection_success_rate / 100.0
                if connection_stats.total_connections > 0
                else 1.0
            ),
            bottleneck_clusters=[],  # Would be identified from performance analysis
            top_performing_clusters=[self.settings.cluster_id],
            recent_performance_trend=trend,
        )

    async def _collect_connection_statistics(self) -> FederationConnectionStats:
        """Collect federation connection statistics."""
        active_connections = (
            self.federation_manager.federation_manager.active_connections
        )
        total_connections = 0
        active_count = 0
        success_rates = []
        connections_by_cluster: dict[str, int] = {}

        for cluster_id, connections in active_connections.items():
            total_connections += len(connections)
            connections_by_cluster[cluster_id] = len(connections)
            active_count += sum(1 for conn in connections if conn.is_healthy)
            success_rates.extend([conn.success_rate for conn in connections])

        success_rate = (
            sum(success_rates) / len(success_rates) * 100.0 if success_rates else 100.0
        )

        return FederationConnectionStats(
            total_connections=total_connections,
            active_connections=active_count,
            connection_success_rate=success_rate,
            average_connection_latency_ms=0.0,
            connections_by_cluster=connections_by_cluster,
        )

    async def _collect_timeseries_data(
        self, metric_name: str, duration_hours: int, resolution_minutes: int
    ) -> TimeSeriesData:
        """Collect time series data for a specific metric."""
        current_time = time.time()
        data_points: list[tuple[float, float]] = []

        if self.unified_monitor.system_metrics_history:
            history = list(self.unified_monitor.system_metrics_history)
            # Filter to requested time window
            cutoff = current_time - (duration_hours * 3600)
            for metrics in history:
                if metrics.collection_timestamp < cutoff:
                    continue
                if metric_name == "health_score":
                    value = metrics.overall_health_score
                elif metric_name == "latency_ms":
                    value = metrics.average_cross_system_latency_ms
                elif metric_name == "requests_per_second":
                    value = metrics.total_requests_per_second
                else:
                    value = metrics.total_active_correlations
                data_points.append((metrics.collection_timestamp, float(value)))

            return data_points

        if self.performance_service:
            data_points = self._collect_federation_timeseries(
                metric_name, duration_hours
            )
            if data_points:
                return data_points

        # Fallback: empty series if no history yet
        return []

    def _collect_federation_timeseries(
        self, metric_name: str, duration_hours: int
    ) -> TimeSeriesData:
        if not self.performance_service:
            return []
        data_points: list[tuple[float, float]] = []
        history = self.performance_service.get_federation_history(hours=duration_hours)
        field_map = {
            "health_score": "federation_health_score",
            "latency_ms": "federation_avg_latency_ms",
            "requests_per_second": "federation_total_throughput_rps",
            "error_rate_percent": "federation_error_rate_percent",
            "cross_cluster_latency_ms": "avg_cross_cluster_latency_ms",
        }
        field_name = field_map.get(metric_name, metric_name)
        for metrics in history:
            value = getattr(metrics, field_name, None)
            if value is None:
                continue
            data_points.append((metrics.collected_at, float(value)))
        return data_points

    def _calculate_trend_from_federation_metrics(
        self, field_name: str, hours: int
    ) -> str:
        if not self.performance_service:
            return "insufficient_data"
        history = self.performance_service.get_federation_history(hours=hours)
        values = [
            getattr(metrics, field_name, 0.0)
            for metrics in history
            if getattr(metrics, field_name, None) is not None
        ]
        if len(values) < 6:
            return "insufficient_data"
        split = max(1, len(values) // 3)
        earlier = values[:split]
        recent = values[-split:]
        earlier_avg = sum(earlier) / len(earlier)
        recent_avg = sum(recent) / len(recent)
        if recent_avg < earlier_avg * 0.9:
            return "improving"
        if recent_avg > earlier_avg * 1.1:
            return "degrading"
        return "stable"

    async def _metrics_ingest_loop(self) -> None:
        """Continuously ingest unified metrics into the performance collector."""
        try:
            while self._metrics_running:
                await asyncio.sleep(self.metrics_ingest_interval_seconds)
                if not self._metrics_running or not self.performance_service:
                    continue

                try:
                    unified_metrics = await self.unified_monitor.get_unified_metrics()
                    connection_stats = await self._collect_connection_statistics()
                    error_rate = (
                        unified_metrics.federation_metrics.error_rate_percent
                        if unified_metrics.federation_metrics
                        else max(0.0, 100.0 - connection_stats.connection_success_rate)
                    )
                    hops = unified_metrics.correlation_metrics.federation_message_hops
                    avg_hops = sum(hops) / len(hops) if hops else 0.0
                    cluster_metrics = ClusterMetrics(
                        cluster_id=self.settings.cluster_id,
                        cluster_name=self.settings.name,
                        region=self.settings.cache_region,
                        avg_latency_ms=unified_metrics.average_cross_system_latency_ms,
                        p95_latency_ms=unified_metrics.correlation_metrics.cross_cluster_latency_p95_ms,
                        p99_latency_ms=unified_metrics.correlation_metrics.cross_cluster_latency_p95_ms
                        * 1.2,
                        throughput_rps=unified_metrics.total_requests_per_second,
                        error_rate_percent=error_rate,
                        health_score=unified_metrics.overall_health_score * 100.0,
                        active_connections=connection_stats.active_connections,
                        cross_cluster_messages=int(avg_hops),
                        federation_latency_ms=unified_metrics.correlation_metrics.cross_cluster_latency_p95_ms,
                    )
                    await self.performance_service.ingest_cluster_metrics(
                        cluster_metrics
                    )
                except Exception as e:
                    logger.error(f"Error ingesting monitoring metrics: {e}")

        except asyncio.CancelledError:
            logger.debug("Monitoring metrics ingest loop cancelled")
        except Exception as e:
            logger.error(f"Monitoring metrics ingest loop error: {e}")

    async def _collect_topology_snapshot(self) -> FederationTopologySnapshot:
        """Collect current federation topology snapshot."""
        if self.federation_graph is not None:
            return await self._collect_graph_topology_snapshot()

        active_connections = (
            self.federation_manager.federation_manager.active_connections
        )
        nodes = [
            FederationTopologyNode(
                node_id=self.settings.name,
                cluster_id=self.settings.cluster_id,
                host=self.settings.host,
                port=self.settings.port,
                health_score=1.0,
                status=HealthStatus.HEALTHY,
                last_seen=time.time(),
            )
        ]
        edges = []

        for cluster_id, connections in active_connections.items():
            for conn in connections:
                nodes.append(
                    FederationTopologyNode(
                        node_id=conn.remote_node_id,
                        cluster_id=cluster_id,
                        host=conn.remote_url,
                        port=0,
                        health_score=conn.success_rate,
                        status=HealthStatus.HEALTHY
                        if conn.is_healthy
                        else HealthStatus.DEGRADED,
                        last_seen=conn.last_health_check_at,
                    )
                )
                edges.append(
                    FederationTopologyEdge(
                        source=self.settings.cluster_id,
                        target=cluster_id,
                        status=HealthStatus.HEALTHY
                        if conn.is_healthy
                        else HealthStatus.DEGRADED,
                    )
                )

        return FederationTopologySnapshot(
            nodes=nodes,
            edges=edges,
            clusters={
                self.settings.cluster_id: FederationTopologyClusterSummary(
                    cluster_id=self.settings.cluster_id,
                    node_count=len(nodes),
                    health_score=1.0,
                    status=HealthStatus.HEALTHY,
                )
            },
            total_nodes=len(nodes),
            total_edges=len(edges),
            graph_diameter=0,
            clustering_coefficient=1.0 if edges else 0.0,
            snapshot_timestamp=time.time(),
        )

    async def _collect_graph_topology_snapshot(self) -> FederationTopologySnapshot:
        """Collect topology snapshot from federation graph routing state."""
        graph = self.federation_graph
        if graph is None:
            return await self._collect_topology_snapshot()

        nodes = [self._build_graph_topology_node(node) for node in graph.nodes.values()]
        edges: list[FederationTopologyEdge] = []
        seen_edges: set[TopologyEdgeKey] = set()
        for source_id, adjacency in graph.adjacency.items():
            for target_id, edge in adjacency.items():
                edge_key = TopologyEdgeKey.normalized(source_id, target_id)
                if edge_key in seen_edges:
                    continue
                seen_edges.add(edge_key)
                edges.append(self._build_graph_topology_edge(edge))

        graph_stats = graph.get_statistics()
        graph_diameter = self._compute_graph_diameter(graph)
        clustering = self._compute_clustering_coefficient(graph)

        clusters = {
            node.node_id: FederationTopologyClusterSummary(
                cluster_id=node.node_id,
                node_count=1,
                health_score=node.health_score,
                status=HealthStatus.HEALTHY
                if node.is_healthy()
                else HealthStatus.DEGRADED,
            )
            for node in graph.nodes.values()
        }

        return FederationTopologySnapshot(
            nodes=nodes,
            edges=edges,
            clusters=clusters,
            total_nodes=graph_stats.total_nodes,
            total_edges=graph_stats.total_edges,
            graph_diameter=graph_diameter,
            clustering_coefficient=clustering,
            snapshot_timestamp=time.time(),
        )

    def _build_graph_topology_node(
        self, node: FederationGraphNode
    ) -> FederationTopologyNode:
        return FederationTopologyNode(
            node_id=node.node_id,
            cluster_id=node.node_id,
            host=node.region,
            port=0,
            health_score=node.health_score,
            status=HealthStatus.HEALTHY if node.is_healthy() else HealthStatus.DEGRADED,
            last_seen=node.last_updated,
        )

    def _build_graph_topology_edge(
        self, edge: FederationGraphEdge
    ) -> FederationTopologyEdge:
        if edge.is_usable():
            status = HealthStatus.HEALTHY
        elif edge.reliability_score > 0.1:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.CRITICAL
        return FederationTopologyEdge(
            source=edge.source_id,
            target=edge.target_id,
            status=status,
            latency_ms=edge.latency_ms,
        )

    def _compute_graph_diameter(self, graph: FederationGraph) -> int:
        if not graph.nodes:
            return 0
        max_distance = 0
        for node_id in graph.nodes:
            distances = self._bfs_distances(graph, node_id)
            if distances:
                max_distance = max(max_distance, max(distances.values()))
        return max_distance

    def _bfs_distances(self, graph: FederationGraph, start_node: str) -> dict[str, int]:
        distances: dict[str, int] = {start_node: 0}
        queue: list[str] = [start_node]
        while queue:
            current = queue.pop(0)
            for neighbor in graph.get_neighbors(current):
                if neighbor not in distances:
                    distances[neighbor] = distances[current] + 1
                    queue.append(neighbor)
        return distances

    def _compute_clustering_coefficient(self, graph: FederationGraph) -> float:
        if not graph.nodes:
            return 0.0
        coefficients: list[float] = []
        for node_id in graph.nodes:
            neighbors = graph.get_neighbors(node_id)
            if len(neighbors) < 2:
                coefficients.append(0.0)
                continue
            set(neighbors)
            links = 0
            for i, neighbor in enumerate(neighbors):
                for other in neighbors[i + 1 :]:
                    if other in graph.adjacency.get(neighbor, {}):
                        links += 1
            possible_links = len(neighbors) * (len(neighbors) - 1) / 2
            coefficients.append(links / possible_links if possible_links else 0.0)
        return sum(coefficients) / len(coefficients)

    def _build_topology_path_summary(
        self, topology: FederationTopologySnapshot
    ) -> FederationTopologyPathSummary:
        total_paths = topology.total_edges
        average_length = self._estimate_average_path_length(topology)
        path_efficiency = max(0.0, 100.0 - (average_length * 15.0))
        redundant_paths = self._estimate_redundant_paths(topology)
        return FederationTopologyPathSummary(
            total_paths=total_paths,
            average_path_length=average_length,
            path_efficiency=path_efficiency,
            redundant_paths=redundant_paths,
        )

    def _estimate_average_path_length(
        self, topology: FederationTopologySnapshot
    ) -> float:
        if topology.total_nodes <= 1:
            return 0.0
        if self.federation_graph is None:
            return 1.0
        graph = self.federation_graph
        if graph is None:
            return 1.0
        node_ids = list(graph.nodes.keys())
        if len(node_ids) > 30:
            node_ids = node_ids[:30]
        distances: list[int] = []
        for node_id in node_ids:
            for distance in self._bfs_distances(graph, node_id).values():
                if distance > 0:
                    distances.append(distance)
        if not distances:
            return 1.0
        return sum(distances) / len(distances)

    def _estimate_redundant_paths(self, topology: FederationTopologySnapshot) -> int:
        if topology.total_nodes <= 1:
            return 0
        components = self._count_components(topology)
        min_edges = max(0, topology.total_nodes - components)
        return max(0, topology.total_edges - min_edges)

    def _count_components(self, topology: FederationTopologySnapshot) -> int:
        if self.federation_graph is None:
            return 1 if topology.total_nodes else 0
        graph = self.federation_graph
        if graph is None:
            return 0
        visited: set[str] = set()
        components = 0
        for node_id in graph.nodes:
            if node_id in visited:
                continue
            components += 1
            for neighbor in self._bfs_distances(graph, node_id):
                visited.add(neighbor)
        return components

    def _build_topology_analysis(
        self, topology: FederationTopologySnapshot
    ) -> FederationTopologyAnalysis:
        if topology.total_edges == 0 and topology.total_nodes > 1:
            health = HealthStatus.DEGRADED
        elif topology.total_edges == 0:
            health = HealthStatus.UNAVAILABLE
        else:
            health = HealthStatus.HEALTHY

        topology_score = max(
            0.0,
            100.0
            - (topology.graph_diameter * 10.0)
            - (max(0.0, 1.0 - topology.clustering_coefficient) * 20.0),
        )
        recommendations: list[str] = []
        if topology.total_edges == 0 and topology.total_nodes > 1:
            recommendations.append("Add federation edges to improve connectivity.")
        if topology.clustering_coefficient < 0.3 and topology.total_nodes > 2:
            recommendations.append("Increase redundancy between clusters.")

        return FederationTopologyAnalysis(
            cluster_connectivity=topology.nodes,
            network_health=health,
            topology_score=topology_score,
            recommendations=recommendations,
        )

    async def _collect_configuration_status(self) -> FederationConfigurationStatus:
        """Collect current federation configuration status."""
        return FederationConfigurationStatus(
            federation_mode=self.federation_config.federation_mode,
            total_policies=1,  # Would be calculated from real policy data
            active_policies=1,
            policy_compliance_percent=100.0,
            configuration_version="1.0.0",
            last_configuration_update=time.time(),
            pending_configuration_changes=0,
            configuration_validation_errors=[],
        )

    async def _get_route_decisions(self, request: web.Request) -> web.Response:
        """Return recent fabric route decisions (ring buffer audit log)."""
        from mpreg.fabric.route_decision_log import (
            RouteDecisionLog,
            get_default_route_decision_log,
        )

        try:
            limit = int(request.query.get("limit", "50"))
        except ValueError:
            limit = 50
        message_id = request.query.get("message_id")
        correlation_id = request.query.get("correlation_id")
        traceparent = request.query.get("traceparent")
        log = getattr(self, "route_decision_log", None)
        if log is None:
            # Backward-compatible fallback for tests that don't bind a server log.
            log = get_default_route_decision_log()
        assert isinstance(log, RouteDecisionLog) or hasattr(log, "recent")
        records = log.recent(
            limit=limit,
            message_id=message_id,
            correlation_id=correlation_id,
            traceparent=traceparent,
        )
        return web.json_response(
            {
                "decisions": [r.to_dict() for r in records],
                "stats": log.stats() if hasattr(log, "stats") else {},
            }
        )

    async def _read_json_body(
        self, request: web.Request
    ) -> tuple[dict[str, Any] | None, web.Response | None]:
        try:
            body = await request.json()
        except Exception:
            return None, web.json_response({"error": "invalid_json"}, status=400)
        if body is None:
            return {}, None
        if not isinstance(body, dict):
            return None, web.json_response({"error": "body_must_be_object"}, status=400)
        return body, None

    async def _invoke_mgmt_provider(
        self,
        provider: Callable[[JsonResponse], Awaitable[JsonResponse] | JsonResponse]
        | None,
        body: dict[str, Any],
        *,
        missing_code: str,
    ) -> web.Response:
        if provider is None:
            return web.json_response(
                {
                    "applied": False,
                    "error": missing_code,
                    "message": "Management mutation provider is not bound on this node.",
                },
                status=503,
            )
        result = provider(body)
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, dict):
            result = {"result": result}
        status = 200 if result.get("applied", True) is not False else 400
        if result.get("error") == "namespace_policy_unavailable":
            status = 503
        return web.json_response(result, status=status)

    async def _post_mgmt_drain(self, request: web.Request) -> web.Response:
        """Mark the local node as draining (or clear drain). Affects /ready."""
        body, err = await self._read_json_body(request)
        if err is not None:
            return err
        assert body is not None
        return await self._invoke_mgmt_provider(
            self.mgmt_drain_provider, body, missing_code="drain_provider_unbound"
        )

    async def _post_mgmt_detach(self, request: web.Request) -> web.Response:
        """Detach a peer connection from this node."""
        body, err = await self._read_json_body(request)
        if err is not None:
            return err
        assert body is not None
        return await self._invoke_mgmt_provider(
            self.mgmt_detach_provider, body, missing_code="detach_provider_unbound"
        )

    async def _post_mgmt_policy_apply(self, request: web.Request) -> web.Response:
        """Apply namespace policy rules (same path as RPC namespace_policy_apply)."""
        body, err = await self._read_json_body(request)
        if err is not None:
            return err
        assert body is not None
        return await self._invoke_mgmt_provider(
            self.mgmt_policy_apply_provider,
            body,
            missing_code="policy_apply_provider_unbound",
        )

    async def _get_mgmt_audit(self, request: web.Request) -> web.Response:
        """Admin mutation audit plus optional route-decision read path."""
        from mpreg.fabric.route_decision_log import get_default_route_decision_log
        from mpreg.server_pkg.shared_audit.response import build_audit_response

        try:
            limit = int(request.query.get("limit", "50"))
        except ValueError:
            limit = 50
        scope_raw = str(request.query.get("scope", "local") or "local").lower()
        scope = "cluster" if scope_raw == "cluster" else "local"
        origin_filter = request.query.get("origin_node") or None

        # Provider may return list, dict, or full pre-built response.
        audit_fn = getattr(self, "mgmt_audit_provider", None)
        if audit_fn is not None:
            try:
                # Prefer signature with scope kwargs when available
                import inspect

                try:
                    sig = inspect.signature(audit_fn)
                    if "scope" in sig.parameters:
                        snap = audit_fn(
                            scope=scope, limit=limit, origin_node=origin_filter
                        )
                    else:
                        snap = audit_fn()
                except TypeError, ValueError:
                    snap = audit_fn()
                if isinstance(snap, dict) and "mutations" in snap and "scope" in snap:
                    return web.json_response(snap)
                if isinstance(snap, dict) and (
                    "mutations" in snap or "entries" in snap
                ):
                    mutations = list(snap.get("mutations") or snap.get("entries") or [])
                    log = (
                        getattr(self, "route_decision_log", None)
                        or get_default_route_decision_log()
                    )
                    route_records = (
                        log.recent(limit=min(limit, 20))
                        if hasattr(log, "recent")
                        else []
                    )
                    return web.json_response(
                        build_audit_response(
                            store=None,
                            local_entries=mutations,
                            route_records=route_records,
                            scope=scope,  # type: ignore[arg-type]
                            limit=limit,
                            origin_node_filter=origin_filter,
                            shared_enabled=bool(snap.get("shared_enabled")),
                            health=None,
                        )
                    )
                if isinstance(snap, list):
                    mutations = snap
                    log = (
                        getattr(self, "route_decision_log", None)
                        or get_default_route_decision_log()
                    )
                    route_records = (
                        log.recent(limit=min(limit, 20))
                        if hasattr(log, "recent")
                        else []
                    )
                    return web.json_response(
                        build_audit_response(
                            store=None,
                            local_entries=mutations,
                            route_records=route_records,
                            scope=scope,  # type: ignore[arg-type]
                            limit=limit,
                            origin_node_filter=origin_filter,
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                logger.debug("mgmt_audit_provider failed: {}", exc)

        log = (
            getattr(self, "route_decision_log", None)
            or get_default_route_decision_log()
        )
        route_records = (
            log.recent(limit=min(limit, 20)) if hasattr(log, "recent") else []
        )
        return web.json_response(
            build_audit_response(
                store=None,
                local_entries=[],
                route_records=route_records,
                scope=scope,  # type: ignore[arg-type]
                limit=limit,
                origin_node_filter=origin_filter,
            )
        )

    async def _get_openapi(self, request: web.Request) -> web.Response:
        """Minimal OpenAPI 3 surface for monitoring + mgmt read APIs."""
        from mpreg.server_pkg.openapi_surface import build_monitoring_openapi

        return web.json_response(build_monitoring_openapi())

    async def _post_policy_dry_run(self, request: web.Request) -> web.Response:
        """Dry-run namespace policy evaluation without applying changes."""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "invalid_json"}, status=400)
        if not isinstance(body, dict):
            return web.json_response({"error": "body_must_be_object"}, status=400)

        # Prefer live server policy engine via mgmt provider extension
        provider = getattr(self, "policy_dry_run_provider", None)
        if provider is not None:
            result = provider(body)
            if inspect.isawaitable(result):
                result = await result
            return web.json_response(
                result if isinstance(result, dict) else {"result": result}
            )

        # Fallback: local structural validation only
        namespace = str(body.get("namespace", ""))
        action = str(body.get("action", "query"))
        return web.json_response(
            {
                "dry_run": True,
                "namespace": namespace,
                "action": action,
                "allowed": True,
                "reason": "no_policy_engine_bound",
                "note": "Wire policy_dry_run_provider on the monitoring system for live evaluation",
            }
        )

    async def _get_prometheus_metrics(self, request: web.Request) -> web.Response:
        """Expose golden-signal metrics in Prometheus text exposition format."""
        # PERF-T10-08: short TTL cache cuts concurrent scrape p99.
        now = time.time()
        ttl = float(getattr(self, "prom_cache_ttl_seconds", 1.0) or 0.0)
        cached = getattr(self, "_prom_cache_text", None)
        cached_at = float(getattr(self, "_prom_cache_at", 0.0) or 0.0)
        if cached is not None and ttl > 0 and (now - cached_at) < ttl:
            lines = cached
        else:
            lines = await self._build_prometheus_text()
            self._prom_cache_text = lines
            self._prom_cache_at = now
        body = "\n".join(lines) + "\n"
        return web.Response(
            text=body,
            content_type="text/plain",
            charset="utf-8",
        )

    def _resolve_server_metrics_tracker(self) -> object | None:
        """OBS-T15-01: canonical ServerMetricsTracker resolution for mon handlers."""
        tracker = getattr(self, "server_metrics_tracker", None)
        if tracker is not None:
            return tracker
        tracker = getattr(self, "metrics_tracker", None) or getattr(
            self, "_metrics_tracker", None
        )
        if tracker is not None:
            return tracker
        provider = getattr(self, "metrics_tracker_provider", None)
        if callable(provider):
            try:
                return provider()
            except Exception:
                return None
        return None

    def _refresh_raft_bridge_metrics(self) -> None:
        """OBS-T14-01: pull Raft node metrics into ServerMetricsTracker for Prom."""
        tracker = self.server_metrics_tracker
        if tracker is None or not hasattr(tracker, "set_raft_bridge"):
            return
        provider = getattr(self, "raft_status_provider", None)
        if provider is None:
            return
        payload = provider()
        if inspect.isawaitable(payload):
            return  # scrape path is sync; async provider skipped
        if not isinstance(payload, dict):
            return
        nodes = payload.get("nodes") or []
        if not isinstance(nodes, list) or not nodes:
            return
        # Aggregate: prefer LEADER node, else first
        chosen = None
        for n in nodes:
            if not isinstance(n, dict):
                continue
            role = str(n.get("role") or "").lower()
            if role == "leader":
                chosen = n
                break
            if chosen is None:
                chosen = n
        if not isinstance(chosen, dict):
            return
        metrics = (
            chosen.get("metrics") if isinstance(chosen.get("metrics"), dict) else {}
        )
        tracker.set_raft_bridge(
            term=int(chosen.get("term") or metrics.get("current_term") or 0),
            commit_index=int(
                chosen.get("commit_index") or metrics.get("commit_index") or 0
            ),
            last_applied=int(
                chosen.get("last_applied") or metrics.get("last_applied") or 0
            ),
            log_size=int(metrics.get("log_size") or chosen.get("log_size") or 0),
            elections_started=int(metrics.get("elections_started") or 0),
            elections_won=int(metrics.get("elections_won") or 0),
            append_entries_success=int(metrics.get("append_entries_success") or 0),
            append_entries_failure=int(metrics.get("append_entries_failure") or 0),
            commands_applied=int(metrics.get("commands_applied") or 0),
            state=str(chosen.get("role") or metrics.get("current_state") or "unknown"),
        )

    async def _build_prometheus_text(self) -> list[str]:
        """Build OpenMetrics-ish Prometheus text from available monitors."""
        cluster = self._prom_escape(str(self.settings.cluster_id))
        name = self._prom_escape(str(self.settings.name))
        labels = f'cluster_id="{cluster}",node="{name}"'
        lines: list[str] = [
            "# HELP mpreg_info Static MPREG node labels.",
            "# TYPE mpreg_info gauge",
            f"mpreg_info{{{labels}}} 1",
            # Prefer Prometheus `up` / absent(mpreg_info) for scrape-down alerts.
            # This gauge is always 1 when the process can answer the scrape.
            "# HELP mpreg_monitoring_up 1 if this process is serving /metrics/prometheus.",
            "# TYPE mpreg_monitoring_up gauge",
            f"mpreg_monitoring_up{{{labels}}} 1",
        ]

        # Endpoint latency samples
        for path, samples in self.endpoint_metrics.items():
            if not samples:
                continue
            safe_path = self._prom_escape(path)
            avg = sum(samples) / len(samples)
            lines.append(
                "# HELP mpreg_http_request_duration_ms Average monitoring handler latency."
            )
            lines.append("# TYPE mpreg_http_request_duration_ms gauge")
            lines.append(
                f'mpreg_http_request_duration_ms{{{labels},path="{safe_path}"}} {avg:.3f}'
            )
            lines.append(
                f'mpreg_http_request_samples{{{labels},path="{safe_path}"}} {len(samples)}'
            )

        try:
            unified = await self.unified_monitor.get_unified_metrics()
            payload = (
                unified.to_dict()
                if hasattr(unified, "to_dict")
                else asdict(unified)
                if _is_dataclass_obj(unified)
                else {}
            )
            self._prom_flatten(lines, "mpreg_unified", payload, labels)
        except Exception as exc:  # noqa: BLE001 - metrics must not crash scrape
            logger.debug("Prometheus unified metrics unavailable: {}", exc)

        try:
            health = await self._collect_health_summary()
            lines.append(
                "# HELP mpreg_federation_health_score Overall federation health 0-1."
            )
            lines.append("# TYPE mpreg_federation_health_score gauge")
            score = getattr(health, "overall_health_score", 0.0)
            try:
                score_f = float(score)
            except TypeError, ValueError:
                score_f = 0.0
            lines.append(f"mpreg_federation_health_score{{{labels}}} {score_f}")
            lines.append(
                f"mpreg_federation_healthy_clusters{{{labels}}} {getattr(health, 'healthy_clusters', 0)}"
            )
            lines.append(
                f"mpreg_federation_total_clusters{{{labels}}} {getattr(health, 'total_clusters', 0)}"
            )
            lines.append(
                f"mpreg_federation_active_connections{{{labels}}} {getattr(health, 'active_connections', 0)}"
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Prometheus health metrics unavailable: {}", exc)

        # Route decision blackhole / reachability (process-local ring buffer)
        try:
            log = getattr(self, "route_decision_log", None)
            if log is None:
                from mpreg.fabric.route_decision_log import (
                    get_default_route_decision_log,
                )

                log = get_default_route_decision_log()
            stats = log.stats() if hasattr(log, "stats") else {}
            lines.append(
                "# HELP mpreg_route_blackhole_total Unreachable route decisions recorded."
            )
            lines.append("# TYPE mpreg_route_blackhole_total counter")
            lines.append(
                f"mpreg_route_blackhole_total{{{labels}}} "
                f"{int(stats.get('blackhole_count', 0))}"
            )
            lines.append(
                "# HELP mpreg_route_decisions_total Total route decisions recorded."
            )
            lines.append("# TYPE mpreg_route_decisions_total counter")
            lines.append(
                f"mpreg_route_decisions_total{{{labels}}} "
                f"{int(stats.get('total_recorded', 0))}"
            )
            lines.append(
                "# HELP mpreg_route_reachable_ratio Fraction of decisions that were reachable."
            )
            lines.append("# TYPE mpreg_route_reachable_ratio gauge")
            lines.append(
                f"mpreg_route_reachable_ratio{{{labels}}} "
                f"{float(stats.get('reachable_ratio', 1.0)):.6f}"
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Prometheus route decision metrics unavailable: {}", exc)

        # OBS-T14-01: bridge Raft internal metrics onto tracker before emit
        try:
            self._refresh_raft_bridge_metrics()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Raft bridge refresh unavailable: {}", exc)

        # OBS-T15-01: at least clear ready when draining without requiring /ready hit
        try:
            tracker = self._resolve_server_metrics_tracker()
            drain_fn = getattr(self, "draining_provider", None)
            if (
                tracker is not None
                and drain_fn is not None
                and hasattr(tracker, "set_draining")
            ):
                draining = bool(drain_fn())
                tracker.set_draining(draining)
                # set_draining already clears ready when True; when False leave gauge
                # until /ready recomputes score-based admission (avoid false ready=1).
        except Exception as exc:  # noqa: BLE001
            logger.debug("Prom drain/ready refresh unavailable: {}", exc)

        # RPC / pubsub counters + latency histograms + per-error-code
        try:
            tracker = self.server_metrics_tracker
            if tracker is not None and hasattr(tracker, "prometheus_lines"):
                lines.extend(tracker.prometheus_lines(labels))
        except Exception as exc:  # noqa: BLE001
            logger.debug("Prometheus server metrics unavailable: {}", exc)

        if self.persistence_snapshot_provider is not None:
            try:
                snap = self.persistence_snapshot_provider()
                if inspect.isawaitable(snap):
                    snap = await snap
                if isinstance(snap, dict):
                    self._prom_flatten(lines, "mpreg_persistence", snap, labels)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Prometheus persistence metrics unavailable: {}", exc)

        # STRONG put counters + latency (lab SLI; not production WAN SLA)
        if self.strong_metrics_provider is not None:
            try:
                strong = self.strong_metrics_provider()
                if inspect.isawaitable(strong):
                    strong = await strong
                if isinstance(strong, dict):
                    counters = strong.get("counters") or {}
                    lat = strong.get("latency_ms") or {}
                    lines.append(
                        "# HELP mpreg_strong_enabled 1 if STRONG coordinator is bound."
                    )
                    lines.append("# TYPE mpreg_strong_enabled gauge")
                    lines.append(
                        f"mpreg_strong_enabled{{{labels}}} "
                        f"{1 if strong.get('coordinator_bound') else 0}"
                    )
                    lines.append(
                        "# HELP mpreg_strong_puts_ok_total Successful STRONG puts."
                    )
                    lines.append("# TYPE mpreg_strong_puts_ok_total counter")
                    lines.append(
                        f"mpreg_strong_puts_ok_total{{{labels}}} "
                        f"{int(counters.get('puts_ok', 0) or 0)}"
                    )
                    lines.append(
                        "# HELP mpreg_strong_puts_fail_total Failed STRONG puts."
                    )
                    lines.append("# TYPE mpreg_strong_puts_fail_total counter")
                    lines.append(
                        f"mpreg_strong_puts_fail_total{{{labels}}} "
                        f"{int(counters.get('puts_fail', 0) or 0)}"
                    )
                    lines.append(
                        "# HELP mpreg_strong_refused_disabled_total STRONG puts refused (1012)."
                    )
                    lines.append("# TYPE mpreg_strong_refused_disabled_total counter")
                    lines.append(
                        f"mpreg_strong_refused_disabled_total{{{labels}}} "
                        f"{int(counters.get('refused_disabled', 0) or 0)}"
                    )
                    lines.append(
                        "# HELP mpreg_strong_gets_refused_total STRONG gets refused (1012; quorum get is v1.1)."
                    )
                    lines.append("# TYPE mpreg_strong_gets_refused_total counter")
                    lines.append(
                        f"mpreg_strong_gets_refused_total{{{labels}}} "
                        f"{int(counters.get('gets_refused', 0) or 0)}"
                    )
                    lines.append(
                        "# HELP mpreg_strong_deletes_refused_total STRONG deletes refused (1012; quorum delete is v1.1)."
                    )
                    lines.append("# TYPE mpreg_strong_deletes_refused_total counter")
                    lines.append(
                        f"mpreg_strong_deletes_refused_total{{{labels}}} "
                        f"{int(counters.get('deletes_refused', 0) or 0)}"
                    )
                    lines.append(
                        "# HELP mpreg_strong_aborts_peer_ok_total Successful peer ABORT deliveries (CFT best-effort)."
                    )
                    lines.append("# TYPE mpreg_strong_aborts_peer_ok_total counter")
                    lines.append(
                        f"mpreg_strong_aborts_peer_ok_total{{{labels}}} "
                        f"{int(counters.get('aborts_peer_ok', 0) or 0)}"
                    )
                    lines.append(
                        "# HELP mpreg_strong_aborts_peer_fail_total Failed peer ABORT deliveries "
                        "(lost ABORT; may leave peer L1 until delivered ABORT "
                        "or LWW success put — not BFT; not pending TTL)."
                    )
                    lines.append("# TYPE mpreg_strong_aborts_peer_fail_total counter")
                    lines.append(
                        f"mpreg_strong_aborts_peer_fail_total{{{labels}}} "
                        f"{int(counters.get('aborts_peer_fail', 0) or 0)}"
                    )
                    # T39: ops-driven retry_abort counters (not automatic heal)
                    lines.append(
                        "# HELP mpreg_strong_retry_abort_calls_total "
                        "Ops-driven strong_retry_abort invocations (CFT best-effort; "
                        "not automatic background heal)."
                    )
                    lines.append("# TYPE mpreg_strong_retry_abort_calls_total counter")
                    lines.append(
                        f"mpreg_strong_retry_abort_calls_total{{{labels}}} "
                        f"{int(counters.get('retry_abort_calls', 0) or strong.get('retry_abort_calls', 0) or 0)}"
                    )
                    lines.append(
                        "# HELP mpreg_strong_retry_abort_cleared_total "
                        "retry_abort runs that cleared all targeted residual candidates."
                    )
                    lines.append(
                        "# TYPE mpreg_strong_retry_abort_cleared_total counter"
                    )
                    lines.append(
                        f"mpreg_strong_retry_abort_cleared_total{{{labels}}} "
                        f"{int(counters.get('retry_abort_cleared', 0) or strong.get('retry_abort_cleared', 0) or 0)}"
                    )
                    lines.append(
                        "# HELP mpreg_strong_retry_abort_still_fail_total "
                        "retry_abort runs that still had abort_fail peers (CFT)."
                    )
                    lines.append(
                        "# TYPE mpreg_strong_retry_abort_still_fail_total counter"
                    )
                    lines.append(
                        f"mpreg_strong_retry_abort_still_fail_total{{{labels}}} "
                        f"{int(counters.get('retry_abort_still_fail', 0) or strong.get('retry_abort_still_fail', 0) or 0)}"
                    )
                    lines.append(
                        "# HELP mpreg_strong_pending Pending prepare entries on local backend."
                    )
                    lines.append("# TYPE mpreg_strong_pending gauge")
                    lines.append(
                        f"mpreg_strong_pending{{{labels}}} "
                        f"{int(strong.get('pending_count', 0) or 0)}"
                    )
                    lines.append(
                        "# HELP mpreg_strong_visible Local visible L1 strong entries "
                        "(may include CFT residuals; process-local)."
                    )
                    lines.append("# TYPE mpreg_strong_visible gauge")
                    lines.append(
                        f"mpreg_strong_visible{{{labels}}} "
                        f"{int(strong.get('visible_count', 0) or 0)}"
                    )
                    lines.append(
                        "# HELP mpreg_strong_backups Pre-commit backups for live ops "
                        "(orphan GC on commit/abort; process-local)."
                    )
                    lines.append("# TYPE mpreg_strong_backups gauge")
                    lines.append(
                        f"mpreg_strong_backups{{{labels}}} "
                        f"{int(strong.get('backups_count', 0) or 0)}"
                    )
                    lines.append(
                        "# HELP mpreg_strong_backups_pruned_total Orphan pre-commit backups "
                        "dropped (T30 GC; not residual L1 clear)."
                    )
                    lines.append("# TYPE mpreg_strong_backups_pruned_total counter")
                    lines.append(
                        f"mpreg_strong_backups_pruned_total{{{labels}}} "
                        f"{int(strong.get('backups_pruned_total', 0) or counters.get('backups_pruned', 0) or 0)}"
                    )
                    # T73/T80: count of CFT residual candidate peers (ops guidance only)
                    from mpreg.core.cache_strong import count_abort_fail_peers

                    n_fail_peers = count_abort_fail_peers(body=strong)
                    lines.append(
                        "# HELP mpreg_strong_abort_fail_peers Count of last_abort_fail_peers "
                        "(CFT residual candidates; process-local; not residual-free proof; "
                        "not automatic heal)."
                    )
                    lines.append("# TYPE mpreg_strong_abort_fail_peers gauge")
                    lines.append(
                        f"mpreg_strong_abort_fail_peers{{{labels}}} {n_fail_peers}"
                    )
                    if lat.get("sample_count"):
                        lines.append(
                            "# HELP mpreg_strong_put_latency_p99_ms Process-local p99 put latency (not WAN SLA)."
                        )
                        lines.append("# TYPE mpreg_strong_put_latency_p99_ms gauge")
                        lines.append(
                            f"mpreg_strong_put_latency_p99_ms{{{labels}}} "
                            f"{float(lat.get('p99_ms', 0.0) or 0.0):.3f}"
                        )
                        lines.append(
                            "# HELP mpreg_strong_put_latency_p50_ms Process-local p50 put latency."
                        )
                        lines.append("# TYPE mpreg_strong_put_latency_p50_ms gauge")
                        lines.append(
                            f"mpreg_strong_put_latency_p50_ms{{{labels}}} "
                            f"{float(lat.get('p50_ms', 0.0) or 0.0):.3f}"
                        )
                    # Capability honesty gauges (v1 put-only MVP; always 0 for get/delete quorum)
                    caps = strong.get("capabilities") or {}
                    cap_specs = (
                        (
                            "mpreg_strong_cap_put_majority_commit",
                            "1 if STRONG put majority-commit is available.",
                            bool(caps.get("put_majority_commit")),
                        ),
                        (
                            "mpreg_strong_cap_get_quorum",
                            "Always 0 in v1 (STRONG get quorum is v1.1 non-goal).",
                            bool(caps.get("get_quorum")),
                        ),
                        (
                            "mpreg_strong_cap_delete_quorum",
                            "Always 0 in v1 (STRONG delete quorum is v1.1 non-goal).",
                            bool(caps.get("delete_quorum")),
                        ),
                        (
                            "mpreg_strong_cap_local_ryw_after_put",
                            "1 if local RYW via EVENTUAL/WEAK get after STRONG put.",
                            bool(caps.get("local_ryw_after_put", True)),
                        ),
                        (
                            "mpreg_strong_cap_cft_only",
                            "Always 1 — STRONG is CFT only (not BFT).",
                            bool(caps.get("cft_only", True)),
                        ),
                        (
                            "mpreg_strong_cap_abort_best_effort",
                            "Always 1 — ABORT delivery is best-effort (lost ABORT CFT limit).",
                            bool(caps.get("abort_best_effort", True)),
                        ),
                        (
                            "mpreg_strong_cap_pending_ttl_clears_residual_l1",
                            "Always 0 — pending TTL purge does not clear residual L1 after COMMIT.",
                            bool(caps.get("pending_ttl_clears_residual_l1", False)),
                        ),
                        (
                            "mpreg_strong_cap_retry_abort_ops_driven",
                            "Always 1 — strong_retry_abort is ops-driven CFT, not automatic heal.",
                            bool(caps.get("retry_abort_ops_driven", True)),
                        ),
                    )
                    for mname, help_s, val in cap_specs:
                        lines.append(f"# HELP {mname} {help_s}")
                        lines.append(f"# TYPE {mname} gauge")
                        lines.append(f"{mname}{{{labels}}} {1 if val else 0}")
            except Exception as exc:  # noqa: BLE001
                logger.debug("Prometheus strong metrics unavailable: {}", exc)

        if self.shared_audit_metrics_provider is not None:
            try:
                audit = self.shared_audit_metrics_provider()
                if inspect.isawaitable(audit):
                    audit = await audit
                if isinstance(audit, dict):
                    counters = audit.get("counters") or {}
                    lines.append(
                        "# HELP mpreg_shared_audit_enabled 1 if shared audit store is present."
                    )
                    lines.append("# TYPE mpreg_shared_audit_enabled gauge")
                    lines.append(
                        f"mpreg_shared_audit_enabled{{{labels}}} "
                        f"{1 if audit.get('store_present') else 0}"
                    )
                    lines.append(
                        "# HELP mpreg_shared_audit_store_size Local G-Set record count."
                    )
                    lines.append("# TYPE mpreg_shared_audit_store_size gauge")
                    lines.append(
                        f"mpreg_shared_audit_store_size{{{labels}}} "
                        f"{int(audit.get('store_size', 0) or 0)}"
                    )
                    for cname in (
                        "publish_dropped",
                        "deltas_sent",
                        "deltas_recv",
                        "pulls_sent",
                        "pulls_recv",
                        "digests_sent",
                        "merge_conflicts",
                    ):
                        if cname in counters:
                            lines.append(
                                f"# HELP mpreg_shared_audit_{cname}_total Shared audit counter."
                            )
                            lines.append(
                                f"# TYPE mpreg_shared_audit_{cname}_total counter"
                            )
                            lines.append(
                                f"mpreg_shared_audit_{cname}_total{{{labels}}} "
                                f"{int(counters.get(cname, 0) or 0)}"
                            )
                    # Capability honesty gauges (never SIEM/BFT/infinite retention)
                    acaps = audit.get("capabilities") or {}
                    acap_specs = (
                        (
                            "mpreg_shared_audit_cap_gset_epidemic",
                            "1 if shared-audit G-Set epidemic is active.",
                            bool(acaps.get("gset_epidemic")),
                        ),
                        (
                            "mpreg_shared_audit_cap_siem",
                            "Always 0 — shared audit is not a SIEM.",
                            bool(acaps.get("siem")),
                        ),
                        (
                            "mpreg_shared_audit_cap_bft",
                            "Always 0 — CFT gossip only, not BFT.",
                            bool(acaps.get("bft")),
                        ),
                        (
                            "mpreg_shared_audit_cap_infinite_retention",
                            "Always 0 — bounded watermark window.",
                            bool(acaps.get("infinite_retention")),
                        ),
                        (
                            "mpreg_shared_audit_cap_linearizable_cluster_ops",
                            "Always 0 — audit visibility is not linearizable ops.",
                            bool(acaps.get("linearizable_cluster_ops")),
                        ),
                    )
                    for mname, help_s, val in acap_specs:
                        lines.append(f"# HELP {mname} {help_s}")
                        lines.append(f"# TYPE {mname} gauge")
                        lines.append(f"{mname}{{{labels}}} {1 if val else 0}")
            except Exception as exc:  # noqa: BLE001
                logger.debug("Prometheus shared audit metrics unavailable: {}", exc)

        # Deduplicate HELP/TYPE lines while preserving order of metric samples
        return lines

    @staticmethod
    def _prom_escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')

    def _prom_flatten(
        self,
        lines: list[str],
        prefix: str,
        payload: dict[str, Any],
        labels: str,
        depth: int = 0,
    ) -> None:
        # Cap recursion: explicit golden series preferred over deep trees.
        # PERF-04: skip empty dicts; avoid re-allocating safe keys for Nones.
        if depth > 2 or not isinstance(payload, dict) or not payload:
            return
        for key, value in payload.items():
            if value is None:
                continue
            if isinstance(value, dict) and not value:
                continue
            safe_key = "".join(
                ch if ch.isalnum() or ch == "_" else "_" for ch in str(key)
            ).strip("_")
            metric = f"{prefix}_{safe_key}" if safe_key else prefix
            if isinstance(value, bool):
                lines.append(f"{metric}{{{labels}}} {1 if value else 0}")
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                lines.append(f"{metric}{{{labels}}} {float(value)}")
            elif isinstance(value, dict):
                self._prom_flatten(lines, metric, value, labels, depth + 1)

    async def _resolve_mgmt_summary(self) -> JsonResponse:
        if self.mgmt_summary_provider is None:
            # Minimal fallback from local settings + health
            health_status = "unknown"
            try:
                summary = await self._get_health_summary_payload()
                health_status = str(summary.get("overall_health_status", "unknown"))
            except Exception:  # noqa: BLE001
                summary = {}
            return {
                "cluster": {
                    "cluster_id": self.settings.cluster_id,
                    "node_name": self.settings.name,
                    "health": health_status,
                },
                "nodes": [],
                "routes": [],
                "catalog": {
                    "functions": 0,
                    "queues": 0,
                    "topics": 0,
                    "caches": 0,
                },
                "health": summary if isinstance(summary, dict) else {},
            }
        result = self.mgmt_summary_provider()
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, dict):
            return {"error": "invalid_mgmt_summary"}
        return result

    async def _get_health_summary_payload(self) -> JsonResponse:
        """Best-effort JSON health summary for mgmt fallback."""
        try:
            health = await self._collect_health_summary()
            return {
                "overall_health_status": getattr(
                    health.overall_health_status,
                    "value",
                    str(health.overall_health_status),
                ),
                "overall_health_score": float(health.overall_health_score),
                "total_clusters": health.total_clusters,
                "healthy_clusters": health.healthy_clusters,
                "active_connections": health.active_connections,
            }
        except Exception:  # noqa: BLE001
            return {"overall_health_status": "unknown"}

    async def _get_mgmt_cluster(self, request: web.Request) -> web.Response:
        summary = await self._resolve_mgmt_summary()
        return web.json_response(summary.get("cluster", summary))

    async def _get_mgmt_nodes(self, request: web.Request) -> web.Response:
        summary = await self._resolve_mgmt_summary()
        return web.json_response({"nodes": summary.get("nodes", [])})

    async def _get_mgmt_routes(self, request: web.Request) -> web.Response:
        summary = await self._resolve_mgmt_summary()
        return web.json_response({"routes": summary.get("routes", [])})

    async def _get_mgmt_catalog(self, request: web.Request) -> web.Response:
        summary = await self._resolve_mgmt_summary()
        return web.json_response({"catalog": summary.get("catalog", {})})

    async def _get_mgmt_health(self, request: web.Request) -> web.Response:
        summary = await self._resolve_mgmt_summary()
        return web.json_response({"health": summary.get("health", {})})

    async def _collect_available_endpoints(self) -> list[dict[str, Any]]:
        """Collect information about available monitoring endpoints."""
        return [
            {
                "path": "/health",
                "method": "GET",
                "description": "Overall federation health status",
            },
            {
                "path": "/health/summary",
                "method": "GET",
                "description": "Detailed health summary",
            },
            {
                "path": "/health/clusters",
                "method": "GET",
                "description": "Health status for all clusters",
            },
            {
                "path": "/health/clusters/{cluster_id}",
                "method": "GET",
                "description": "Health details for a specific cluster",
            },
            {
                "path": "/metrics",
                "method": "GET",
                "description": "Comprehensive federation metrics",
            },
            {
                "path": "/metrics/performance",
                "method": "GET",
                "description": "Performance metrics",
            },
            {
                "path": "/metrics/connections",
                "method": "GET",
                "description": "Connection metrics",
            },
            {
                "path": "/metrics/unified",
                "method": "GET",
                "description": "Unified system metrics",
            },
            {
                "path": "/metrics/rpc",
                "method": "GET",
                "description": "RPC system metrics",
            },
            {
                "path": "/metrics/pubsub",
                "method": "GET",
                "description": "Pub/Sub system metrics",
            },
            {
                "path": "/metrics/queue",
                "method": "GET",
                "description": "Queue system metrics",
            },
            {
                "path": "/metrics/cache",
                "method": "GET",
                "description": "Cache system metrics",
            },
            {
                "path": "/metrics/transport",
                "method": "GET",
                "description": "Transport system metrics",
            },
            {
                "path": "/metrics/persistence",
                "method": "GET",
                "description": "Persistence snapshot metrics",
            },
            {
                "path": "/metrics/strong",
                "method": "GET",
                "description": "STRONG majority-commit put metrics (process-local)",
            },
            {
                "path": "/metrics/shared-audit",
                "method": "GET",
                "description": "Shared audit G-Set epidemic metrics",
            },
            {
                "path": "/metrics/prometheus",
                "method": "GET",
                "description": "Prometheus text exposition of golden-signal metrics",
            },
            {
                "path": "/mgmt/v1/cluster",
                "method": "GET",
                "description": "Management API: cluster summary",
            },
            {
                "path": "/mgmt/v1/nodes",
                "method": "GET",
                "description": "Management API: node summaries",
            },
            {
                "path": "/mgmt/v1/routes",
                "method": "GET",
                "description": "Management API: route summaries",
            },
            {
                "path": "/mgmt/v1/catalog",
                "method": "GET",
                "description": "Management API: catalog summary",
            },
            {
                "path": "/mgmt/v1/health",
                "method": "GET",
                "description": "Management API: health summary",
            },
            {
                "path": "/mgmt/v1/raft",
                "method": "GET",
                "description": "Management API: Raft consensus status",
            },
            {
                "path": "/mgmt/v1/strong",
                "method": "GET",
                "description": "Management API: STRONG cache put readiness",
            },
            {
                "path": "/transport/endpoints",
                "method": "GET",
                "description": "Adapter endpoints and auto-assigned transport ports",
            },
            {
                "path": "/discovery/summary",
                "method": "GET",
                "description": "Discovery summary export status",
            },
            {
                "path": "/discovery/cache",
                "method": "GET",
                "description": "Discovery resolver cache status",
            },
            {
                "path": "/discovery/policy",
                "method": "GET",
                "description": "Namespace policy status and audit summary",
            },
            {
                "path": "/discovery/lag",
                "method": "GET",
                "description": "Discovery delta and summary export lag",
            },
            {
                "path": "/metrics/timeseries",
                "method": "GET",
                "description": "Time series metrics data",
            },
            {
                "path": "/topology",
                "method": "GET",
                "description": "Current federation topology",
            },
            {
                "path": "/topology/graph",
                "method": "GET",
                "description": "Topology graph visualization data",
            },
            {
                "path": "/topology/paths",
                "method": "GET",
                "description": "Topology routing path analysis",
            },
            {
                "path": "/topology/analysis",
                "method": "GET",
                "description": "Topology analysis and recommendations",
            },
            {
                "path": "/performance",
                "method": "GET",
                "description": "Federation performance summary",
            },
            {
                "path": "/performance/bottlenecks",
                "method": "GET",
                "description": "Performance bottlenecks analysis",
            },
            {
                "path": "/performance/trends",
                "method": "GET",
                "description": "Performance trends analysis",
            },
            {
                "path": "/performance/clusters/{cluster_id}",
                "method": "GET",
                "description": "Performance for a specific cluster",
            },
            {
                "path": "/alerts",
                "method": "GET",
                "description": "Active federation alerts",
            },
            {
                "path": "/alerts/history",
                "method": "GET",
                "description": "Federation alert history",
            },
            {
                "path": "/alerts/acknowledge",
                "method": "POST",
                "description": "Acknowledge an alert",
            },
            {
                "path": "/config",
                "method": "GET",
                "description": "Federation configuration status",
            },
            {
                "path": "/config/policies",
                "method": "GET",
                "description": "Federation policy configuration",
            },
            {
                "path": "/config/validation",
                "method": "GET",
                "description": "Federation configuration validation",
            },
            {
                "path": "/routing/decisions",
                "method": "GET",
                "description": "Recent fabric route decision audit log",
            },
            {
                "path": "/mgmt/v1/policy/dry-run",
                "method": "POST",
                "description": "Dry-run namespace/routing policy evaluation",
            },
            {
                "path": "/routing/trace",
                "method": "GET",
                "description": "Route selection trace for a destination cluster",
            },
            {
                "path": "/routing/link-state",
                "method": "GET",
                "description": "Link-state routing status and area counters",
            },
            {
                "path": "/endpoints",
                "method": "GET",
                "description": "Available monitoring endpoints",
            },
        ]

    async def _get_topology_graph(self, request: web.Request) -> web.Response:
        """Get federation topology graph visualization data."""
        try:
            topology = await self._collect_topology_snapshot()
            nodes = [self._serialize_topology_node(node) for node in topology.nodes]

            return web.json_response(
                {
                    "status": "ok",
                    "graph_data": {
                        "nodes": nodes,
                        "edges": len(topology.edges),
                        "timestamp": topology.snapshot_timestamp,
                    },
                    "visualization_ready": True,
                }
            )

        except Exception as e:
            logger.error(f"Error getting topology graph: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_topology_paths(self, request: web.Request) -> web.Response:
        """Get federation routing paths analysis."""
        try:
            topology = await self._collect_topology_snapshot()
            path_summary = self._build_topology_path_summary(topology)

            return web.json_response(
                {
                    "status": "ok",
                    "routing_paths": self._serialize_topology_path_summary(
                        path_summary
                    ),
                    "timestamp": topology.snapshot_timestamp,
                }
            )

        except Exception as e:
            logger.error(f"Error getting topology paths: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_topology_analysis(self, request: web.Request) -> web.Response:
        """Get advanced federation topology analysis."""
        try:
            topology = await self._collect_topology_snapshot()
            nodes = [self._serialize_topology_node(node) for node in topology.nodes]
            analysis = self._build_topology_analysis(topology)

            return web.json_response(
                {
                    "status": "ok",
                    "analysis": {
                        "cluster_connectivity": nodes,
                        "network_health": analysis.network_health.value,
                        "topology_score": analysis.topology_score,
                        "recommendations": analysis.recommendations,
                    },
                    "timestamp": topology.snapshot_timestamp,
                }
            )

        except Exception as e:
            logger.error(f"Error getting topology analysis: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_performance_summary(self, request: web.Request) -> web.Response:
        """Get performance summary across all federation clusters."""
        try:
            performance_metrics = await self._collect_performance_summary()

            return web.json_response(
                {
                    "status": "ok",
                    "performance_summary": asdict(performance_metrics),
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting performance summary: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_performance_bottlenecks(self, request: web.Request) -> web.Response:
        """Get performance bottlenecks analysis."""
        try:
            bottlenecks = {
                "cpu_bottlenecks": [],
                "memory_bottlenecks": [],
                "network_bottlenecks": [],
                "federation_bottlenecks": [],
            }

            if self.performance_service:
                active_alerts = self.performance_service.get_active_alerts()
                bottlenecks["federation_bottlenecks"] = [
                    self._serialize_alert(alert) for alert in active_alerts
                ]
            return web.json_response(
                {
                    "status": "ok",
                    "bottlenecks": bottlenecks,
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting performance bottlenecks: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_performance_trends(self, request: web.Request) -> web.Response:
        """Get performance trends analysis."""
        try:
            trends = {
                "latency_trend": "stable",
                "throughput_trend": "increasing",
                "error_rate_trend": "decreasing",
                "resource_utilization_trend": "stable",
            }
            if self.performance_service:
                trends = {
                    "latency_trend": self._calculate_trend_from_federation_metrics(
                        "federation_avg_latency_ms", hours=1
                    ),
                    "throughput_trend": self._calculate_trend_from_federation_metrics(
                        "federation_total_throughput_rps", hours=1
                    ),
                    "error_rate_trend": self._calculate_trend_from_federation_metrics(
                        "federation_error_rate_percent", hours=1
                    ),
                    "resource_utilization_trend": self._calculate_trend_from_federation_metrics(
                        "avg_cpu_usage_percent", hours=1
                    ),
                }
            return web.json_response(
                {
                    "status": "ok",
                    "trends": trends,
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting performance trends: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_cluster_performance(self, request: web.Request) -> web.Response:
        """Get performance metrics for a specific cluster."""
        cluster_id = request.match_info.get("cluster_id")
        if not cluster_id:
            return web.json_response(
                {"status": "error", "message": "cluster_id required"}, status=400
            )

        try:
            connections = (
                self.federation_manager.federation_manager.active_connections.get(
                    cluster_id, []
                )
            )
            success_rates = [conn.success_rate for conn in connections]
            avg_success = (
                sum(success_rates) / len(success_rates) if success_rates else 0.0
            )
            health = "healthy" if avg_success >= 0.9 else "degraded"

            return web.json_response(
                {
                    "status": "ok",
                    "cluster_id": cluster_id,
                    "performance": {
                        "cpu_usage": None,
                        "memory_usage": None,
                        "network_latency": None,
                        "federation_health": health,
                        "success_rate_percent": avg_success * 100.0,
                        "active_connections": len(connections),
                    },
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting cluster performance for {cluster_id}: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_active_alerts(self, request: web.Request) -> web.Response:
        """Get active federation alerts."""
        try:
            active_alerts: list[PerformanceAlert] = []
            if self.performance_service:
                active_alerts = self.performance_service.get_active_alerts()
            return web.json_response(
                {
                    "status": "ok",
                    "active_alerts": [
                        self._serialize_alert(alert) for alert in active_alerts
                    ],
                    "alert_count": len(active_alerts),
                    "severity_breakdown": self._summarize_alert_severity(active_alerts),
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting active alerts: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_alert_history(self, request: web.Request) -> web.Response:
        """Get federation alert history."""
        try:
            alerts: list[PerformanceAlert] = []
            if self.performance_service:
                alerts = self.performance_service.get_alert_history()
            return web.json_response(
                {
                    "status": "ok",
                    "alert_history": [self._serialize_alert(alert) for alert in alerts],
                    "total_alerts": len(alerts),
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting alert history: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _acknowledge_alert(self, request: web.Request) -> web.Response:
        """Acknowledge a federation alert."""
        try:
            data = await request.json()
            alert_id = data.get("alert_id")

            if not alert_id:
                return web.json_response(
                    {"status": "error", "message": "alert_id required"}, status=400
                )

            if not self.performance_service:
                return web.json_response(
                    {"status": "error", "message": "alerting not configured"},
                    status=503,
                )

            if not self.performance_service.resolve_alert(alert_id):
                return web.json_response(
                    {"status": "error", "message": "alert not found"}, status=404
                )

            return web.json_response(
                {
                    "status": "ok",
                    "message": f"Alert {alert_id} acknowledged",
                    "alert_id": alert_id,
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error acknowledging alert: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_federation_policies(self, request: web.Request) -> web.Response:
        """Get federation policies configuration."""
        try:
            config_status = await self._collect_configuration_status()

            return web.json_response(
                {
                    "status": "ok",
                    "policies": {
                        "federation_mode": config_status.federation_mode.value,
                        "total_policies": config_status.total_policies,
                        "active_policies": config_status.active_policies,
                        "compliance_percent": config_status.policy_compliance_percent,
                    },
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting federation policies: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _validate_federation_config(self, request: web.Request) -> web.Response:
        """Validate current federation configuration."""
        try:
            config_status = await self._collect_configuration_status()

            return web.json_response(
                {
                    "status": "ok",
                    "validation": {
                        "is_valid": len(config_status.configuration_validation_errors)
                        == 0,
                        "errors": config_status.configuration_validation_errors,
                        "warnings": [],
                        "configuration_version": config_status.configuration_version,
                    },
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error validating federation config: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_route_trace(self, request: web.Request) -> web.Response:
        """Get a route selection trace for a destination cluster."""
        destination = request.query.get("destination") or ""
        if not destination:
            return web.json_response(
                {"status": "error", "message": "destination required"}, status=400
            )
        if not self.route_trace_provider:
            return web.json_response(
                {"status": "error", "message": "route tracing not available"},
                status=503,
            )
        avoid_raw = request.query.get("avoid", "")
        avoid = tuple(part.strip() for part in avoid_raw.split(",") if part.strip())
        try:
            trace = self.route_trace_provider(destination, avoid)
            return web.json_response(
                {
                    "status": "ok",
                    "destination": destination,
                    "avoid_clusters": list(avoid),
                    "trace": trace,
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error generating route trace: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_mgmt_raft(self, request: web.Request) -> web.Response:
        """Raft group status for operators (term, role, commit_index)."""
        if not self.raft_status_provider:
            return web.json_response(
                {
                    "status": "ok",
                    "raft": {
                        "configured": False,
                        "nodes": [],
                        "membership_change_supported": False,
                    },
                    "timestamp": time.time(),
                }
            )
        try:
            payload = self.raft_status_provider()
            if not isinstance(payload, dict):
                payload = {"nodes": payload}
            body = {
                "status": "ok",
                "raft": payload,
                "timestamp": time.time(),
            }
            return web.json_response(body)
        except Exception as e:
            logger.error(f"Error getting raft status: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_link_state_status(self, request: web.Request) -> web.Response:
        """Get link-state routing status and area mismatch counters."""
        if not self.link_state_status_provider:
            return web.json_response(
                {"status": "error", "message": "link-state status not available"},
                status=503,
            )
        try:
            status = self.link_state_status_provider()
            return web.json_response(
                {
                    "status": "ok",
                    "link_state": status,
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            logger.error(f"Error getting link-state status: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    def _serialize_cluster_health(
        self, health: FederationClusterHealth
    ) -> dict[str, Any]:
        return {
            "cluster_id": health.cluster_id,
            "status": health.status.value,
            "health_score": health.health_score,
            "node_count": health.node_count,
            "active_connections": health.active_connections,
            "last_heartbeat": health.last_heartbeat,
        }

    def _serialize_connection_stats(
        self, stats: FederationConnectionStats
    ) -> dict[str, Any]:
        return {
            "total_connections": stats.total_connections,
            "active_connections": stats.active_connections,
            "connection_success_rate": stats.connection_success_rate,
            "average_connection_latency_ms": stats.average_connection_latency_ms,
            "connections_by_cluster": stats.connections_by_cluster,
        }

    def _serialize_topology_node(self, node: FederationTopologyNode) -> dict[str, Any]:
        return {
            "node_id": node.node_id,
            "cluster_id": node.cluster_id,
            "host": node.host,
            "port": node.port,
            "health_score": node.health_score,
            "status": node.status.value,
            "last_seen": node.last_seen,
        }

    def _serialize_topology_edge(self, edge: FederationTopologyEdge) -> dict[str, Any]:
        return {
            "source": edge.source,
            "target": edge.target,
            "status": edge.status.value,
            "latency_ms": edge.latency_ms,
        }

    def _serialize_topology_cluster(
        self, cluster: FederationTopologyClusterSummary
    ) -> dict[str, Any]:
        return {
            "cluster_id": cluster.cluster_id,
            "node_count": cluster.node_count,
            "health_score": cluster.health_score,
            "status": cluster.status.value,
        }

    def _serialize_topology_path_summary(
        self, summary: FederationTopologyPathSummary
    ) -> dict[str, Any]:
        return {
            "total_paths": summary.total_paths,
            "average_path_length": summary.average_path_length,
            "path_efficiency": summary.path_efficiency,
            "redundant_paths": summary.redundant_paths,
        }

    def _serialize_alert(self, alert: PerformanceAlert) -> dict[str, Any]:
        return {
            "alert_id": alert.alert_id,
            "severity": alert.severity.value,
            "metric_name": alert.metric_name,
            "current_value": alert.current_value,
            "threshold_value": alert.threshold_value,
            "cluster_id": alert.cluster_id,
            "node_id": alert.node_id,
            "timestamp": alert.timestamp,
            "message": alert.message,
            "resolved": alert.resolved,
        }

    def _summarize_alert_severity(
        self, alerts: list[PerformanceAlert]
    ) -> dict[str, int]:
        severity_counts = {"critical": 0, "error": 0, "warning": 0, "info": 0}
        for alert in alerts:
            severity = alert.severity.value
            if severity not in severity_counts:
                severity_counts[severity] = 0
            severity_counts[severity] += 1
        return severity_counts

# Factory function for creating federation monitoring systems
def create_federation_monitoring_system(
    settings: MPREGSettings,
    federation_config: FederationConfig,
    federation_manager: FederationConnectionManager,
    unified_monitor: UnifiedSystemMonitor,
    monitoring_port: int = 9090,
    monitoring_host: str | None = None,
    enable_cors: bool = False,
    auth_token: str | None = None,
    performance_service: PerformanceMetricsService | None = None,
    federation_graph: FederationGraph | None = None,
    route_trace_provider: RouteTraceProvider | None = None,
    link_state_status_provider: LinkStateStatusProvider | None = None,
    adapter_endpoint_registry: AdapterEndpointRegistry | None = None,
    persistence_snapshot_provider: PersistenceSnapshotProvider | None = None,
    discovery_summary_provider: DiscoverySummaryProvider | None = None,
    discovery_cache_provider: DiscoveryCacheProvider | None = None,
    discovery_policy_provider: DiscoveryPolicyProvider | None = None,
    discovery_lag_provider: DiscoveryLagProvider | None = None,
    dns_metrics_provider: DnsMetricsProvider | None = None,
    strong_metrics_provider: StrongMetricsProvider | None = None,
    shared_audit_metrics_provider: SharedAuditMetricsProvider | None = None,
    mgmt_summary_provider: Callable[[], Awaitable[JsonResponse] | JsonResponse]
    | None = None,
    policy_dry_run_provider: Callable[
        [JsonResponse], Awaitable[JsonResponse] | JsonResponse
    ]
    | None = None,
    mgmt_drain_provider: Callable[
        [JsonResponse], Awaitable[JsonResponse] | JsonResponse
    ]
    | None = None,
    mgmt_detach_provider: Callable[
        [JsonResponse], Awaitable[JsonResponse] | JsonResponse
    ]
    | None = None,
    mgmt_policy_apply_provider: Callable[
        [JsonResponse], Awaitable[JsonResponse] | JsonResponse
    ]
    | None = None,
    mgmt_audit_provider: Callable[[], list[dict[str, Any]] | dict[str, Any]]
    | None = None,
    draining_provider: Callable[[], bool] | None = None,
    route_decision_log: object | None = None,
    raft_status_provider: RaftStatusProvider | None = None,
    server_metrics_tracker: object | None = None,
) -> FederationMonitoringSystem:
    """Create a federation monitoring system with specified configuration."""

    if performance_service is None:
        performance_service = PerformanceMetricsService()

    resolved_token = auth_token
    if resolved_token is None:
        resolved_token = getattr(settings, "monitoring_auth_token", None)

    monitoring_system = FederationMonitoringSystem(
        settings=settings,
        federation_config=federation_config,
        federation_manager=federation_manager,
        unified_monitor=unified_monitor,
        performance_service=performance_service,
        federation_graph=federation_graph,
        route_trace_provider=route_trace_provider,
        link_state_status_provider=link_state_status_provider,
        adapter_endpoint_registry=adapter_endpoint_registry,
        persistence_snapshot_provider=persistence_snapshot_provider,
        discovery_summary_provider=discovery_summary_provider,
        discovery_cache_provider=discovery_cache_provider,
        discovery_policy_provider=discovery_policy_provider,
        discovery_lag_provider=discovery_lag_provider,
        dns_metrics_provider=dns_metrics_provider,
        strong_metrics_provider=strong_metrics_provider,
        shared_audit_metrics_provider=shared_audit_metrics_provider,
        mgmt_summary_provider=mgmt_summary_provider,
        policy_dry_run_provider=policy_dry_run_provider,
        mgmt_drain_provider=mgmt_drain_provider,
        mgmt_detach_provider=mgmt_detach_provider,
        mgmt_policy_apply_provider=mgmt_policy_apply_provider,
        mgmt_audit_provider=mgmt_audit_provider,
        draining_provider=draining_provider,
        route_decision_log=route_decision_log,
        raft_status_provider=raft_status_provider,
        server_metrics_tracker=server_metrics_tracker,
    )

    monitoring_system.monitoring_port = monitoring_port
    monitoring_system.monitoring_host = monitoring_host
    monitoring_system.enable_cors = enable_cors
    monitoring_system.auth_token = resolved_token
    return monitoring_system
