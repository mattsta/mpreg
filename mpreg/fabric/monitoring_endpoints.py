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
from dataclasses import asdict, dataclass, field
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
type PersistenceSnapshotProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]
type DiscoverySummaryProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]
type DiscoveryCacheProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]
type DiscoveryPolicyProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]
type DiscoveryLagProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]
type DnsMetricsProvider = Callable[[], Awaitable[JsonResponse] | JsonResponse]


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
    adapter_endpoint_registry: AdapterEndpointRegistry | None = None
    persistence_snapshot_provider: PersistenceSnapshotProvider | None = None
    discovery_summary_provider: DiscoverySummaryProvider | None = None
    discovery_cache_provider: DiscoveryCacheProvider | None = None
    discovery_policy_provider: DiscoveryPolicyProvider | None = None
    discovery_lag_provider: DiscoveryLagProvider | None = None
    dns_metrics_provider: DnsMetricsProvider | None = None

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
    enable_cors: bool = True
    metrics_retention_hours: int = 24
    metrics_ingest_interval_seconds: float = 5.0

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
        # Health endpoints
        self.app.router.add_get("/health", self._get_federation_health)
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
        self.app.router.add_get("/transport/endpoints", self._get_transport_endpoints)

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
        self.app.router.add_get("/routing/link-state", self._get_link_state_status)

        # Utility endpoints
        self.app.router.add_get("/", self._get_monitoring_index)
        self.app.router.add_get("/endpoints", self._get_available_endpoints)

    def _setup_middleware(self) -> None:
        """Configure middleware for request processing."""

        @web.middleware
        async def cors_middleware(request: web.Request, handler) -> web.Response:
            """Handle CORS headers for browser access."""
            response = await handler(request)
            if self.enable_cors:
                response.headers["Access-Control-Allow-Origin"] = "*"
                response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
                response.headers["Access-Control-Allow-Headers"] = "Content-Type"
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

        self.app.middlewares.append(cors_middleware)
        self.app.middlewares.append(metrics_middleware)

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

    async def _get_federation_health(self, request: web.Request) -> web.Response:
        """Get overall federation health status."""
        try:
            health_summary = await self._collect_health_summary()

            return web.json_response(
                {
                    "status": "ok",
                    "federation_health": {
                        "overall_status": health_summary.overall_health_status.value,
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
            logger.error(f"Error getting federation health: {e}")
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
            neighbor_set = set(neighbors)
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
    enable_cors: bool = True,
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
) -> FederationMonitoringSystem:
    """Create a federation monitoring system with specified configuration."""

    if performance_service is None:
        performance_service = PerformanceMetricsService()

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
    )

    monitoring_system.monitoring_port = monitoring_port
    monitoring_system.monitoring_host = monitoring_host
    monitoring_system.enable_cors = enable_cors
    return monitoring_system
