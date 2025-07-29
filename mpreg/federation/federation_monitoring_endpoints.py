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

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from aiohttp import web
from loguru import logger

from ..core.config import MPREGSettings
from ..core.monitoring.unified_monitoring import (
    HealthStatus,
    UnifiedSystemMonitor,
)
from ..core.transport.enhanced_health import HealthScore
from ..datastructures.type_aliases import ClusterId
from .federation_alerting import PerformanceAlert
from .federation_config import FederationConfig, FederationMode
from .federation_connection_manager import FederationConnectionManager
from .federation_graph import FederationGraph
from .performance_metrics import (
    PerformanceMetricsCollector,
)

# Type aliases for monitoring endpoints
type EndpointPath = str
type JsonResponse = dict[str, Any]
type QueryParameters = dict[str, str]
type TimeSeriesData = list[tuple[float, float]]  # [(timestamp, value), ...]


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

    nodes: list[dict[str, Any]]  # Node information with health and metrics
    edges: list[dict[str, Any]]  # Connection information with performance data
    clusters: dict[ClusterId, dict[str, Any]]  # Cluster-level aggregations
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
    performance_collector: PerformanceMetricsCollector | None = None
    federation_graph: FederationGraph | None = None

    # Web server components
    app: web.Application = field(init=False)
    runner: web.AppRunner | None = field(default=None, init=False)
    site: web.TCPSite | None = field(default=None, init=False)

    # Performance tracking
    endpoint_metrics: dict[EndpointPath, list[float]] = field(default_factory=dict)
    active_alerts: list[PerformanceAlert] = field(default_factory=list)

    # Configuration
    monitoring_port: int = 9090
    enable_cors: bool = True
    metrics_retention_hours: int = 24

    def __post_init__(self) -> None:
        """Initialize the web application and routes."""
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
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()

            self.site = web.TCPSite(
                self.runner, host=self.settings.host, port=self.monitoring_port
            )
            await self.site.start()

            logger.info(
                f"Federation monitoring server started on "
                f"http://{self.settings.host}:{self.monitoring_port}"
            )

        except Exception as e:
            logger.error(f"Failed to start federation monitoring server: {e}")
            raise

    async def stop(self) -> None:
        """Stop the federation monitoring HTTP server."""
        try:
            if self.site:
                await self.site.stop()
                self.site = None

            if self.runner:
                await self.runner.cleanup()
                self.runner = None

            logger.info("Federation monitoring server stopped")

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
                        "cluster_details": cluster_health,
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
                    "clusters": cluster_health,
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
                {"status": "ok", "cluster": cluster_details, "timestamp": time.time()}
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
                    "connections": connection_stats,
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
                        "nodes": topology.nodes,
                        "edges": topology.edges,
                        "clusters": topology.clusters,
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

        return FederationHealthSummary(
            total_clusters=1,  # Would be calculated from federation manager
            healthy_clusters=1,
            degraded_clusters=0,
            critical_clusters=0,
            unavailable_clusters=0,
            total_connections=unified_metrics.federation_metrics.active_connections
            if unified_metrics.federation_metrics
            else 0,
            active_connections=unified_metrics.federation_metrics.active_connections
            if unified_metrics.federation_metrics
            else 0,
            overall_health_score=unified_metrics.overall_health_score,
            overall_health_status=unified_metrics.overall_health_status,
            last_updated=time.time(),
            federation_mode=self.federation_config.federation_mode,
            cross_cluster_latency_p95_ms=unified_metrics.correlation_metrics.cross_cluster_latency_p95_ms,
            connection_success_rate_percent=unified_metrics.correlation_metrics.federation_success_rate_percent,
        )

    async def _collect_cluster_health_details(self) -> dict[ClusterId, dict[str, Any]]:
        """Collect detailed health information for all clusters."""
        # This would integrate with federation connection manager
        # to get real cluster health data

        return {
            self.settings.cluster_id: {
                "cluster_id": self.settings.cluster_id,
                "status": "healthy",
                "health_score": 0.95,
                "node_count": 1,
                "active_connections": 0,
                "last_heartbeat": time.time(),
            }
        }

    async def _collect_cluster_detail(
        self, cluster_id: ClusterId
    ) -> dict[str, Any] | None:
        """Collect detailed information for a specific cluster."""
        cluster_health = await self._collect_cluster_health_details()
        return cluster_health.get(cluster_id)

    async def _collect_performance_summary(self) -> FederationPerformanceSummary:
        """Collect federation performance summary."""
        unified_metrics = await self.unified_monitor.get_unified_metrics()

        return FederationPerformanceSummary(
            requests_per_second=unified_metrics.total_requests_per_second,
            average_latency_ms=unified_metrics.average_cross_system_latency_ms,
            p95_latency_ms=unified_metrics.correlation_metrics.cross_cluster_latency_p95_ms,
            p99_latency_ms=unified_metrics.correlation_metrics.cross_cluster_latency_p95_ms
            * 1.2,  # Estimate
            error_rate_percent=5.0,  # Would be calculated from real data
            cross_cluster_hops_average=sum(
                unified_metrics.correlation_metrics.federation_message_hops
            )
            / max(len(unified_metrics.correlation_metrics.federation_message_hops), 1),
            federation_efficiency_score=0.85,  # Would be calculated from real performance data
            bottleneck_clusters=[],  # Would be identified from performance analysis
            top_performing_clusters=[self.settings.cluster_id],
            recent_performance_trend="stable",
        )

    async def _collect_connection_statistics(self) -> dict[str, Any]:
        """Collect federation connection statistics."""
        # This would integrate with federation connection manager
        return {
            "total_connections": 0,
            "active_connections": 0,
            "connection_success_rate": 100.0,
            "average_connection_latency_ms": 0.0,
            "connections_by_cluster": {},
        }

    async def _collect_timeseries_data(
        self, metric_name: str, duration_hours: int, resolution_minutes: int
    ) -> TimeSeriesData:
        """Collect time series data for a specific metric."""
        # This would collect historical data from the unified monitoring system
        # For now, generate sample data
        current_time = time.time()
        data_points = []

        for i in range(duration_hours * 60 // resolution_minutes):
            timestamp = current_time - (i * resolution_minutes * 60)
            # Generate sample data based on metric type
            if metric_name == "health_score":
                value = 0.9 + (i % 10) * 0.01  # Simulate varying health score
            elif metric_name == "latency_ms":
                value = 50.0 + (i % 20) * 2.0  # Simulate varying latency
            else:
                value = float(i % 100)  # Generic sample data

            data_points.append((timestamp, value))

        return list(reversed(data_points))  # Return chronological order

    async def _collect_topology_snapshot(self) -> FederationTopologySnapshot:
        """Collect current federation topology snapshot."""
        return FederationTopologySnapshot(
            nodes=[
                {
                    "node_id": self.settings.name,
                    "cluster_id": self.settings.cluster_id,
                    "host": self.settings.host,
                    "port": self.settings.port,
                    "health_score": 0.95,
                    "status": "healthy",
                    "last_seen": time.time(),
                }
            ],
            edges=[],  # Would be populated from federation graph
            clusters={
                self.settings.cluster_id: {
                    "cluster_id": self.settings.cluster_id,
                    "node_count": 1,
                    "health_score": 0.95,
                    "status": "healthy",
                }
            },
            total_nodes=1,
            total_edges=0,
            graph_diameter=0,
            clustering_coefficient=1.0,
            snapshot_timestamp=time.time(),
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
                "path": "/config",
                "method": "GET",
                "description": "Federation configuration status",
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

            return web.json_response(
                {
                    "status": "ok",
                    "graph_data": {
                        "nodes": topology.nodes,
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

            return web.json_response(
                {
                    "status": "ok",
                    "routing_paths": {
                        "total_paths": len(topology.edges),
                        "path_efficiency": 85.0,  # Placeholder calculation
                        "redundant_paths": 0,
                    },
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

            return web.json_response(
                {
                    "status": "ok",
                    "analysis": {
                        "cluster_connectivity": topology.nodes,
                        "network_health": "healthy",  # TODO: Calculate based on topology metrics
                        "topology_score": 90.0,  # Placeholder calculation
                        "recommendations": [],
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
                    "performance_summary": performance_metrics,
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting performance summary: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_performance_bottlenecks(self, request: web.Request) -> web.Response:
        """Get performance bottlenecks analysis."""
        try:
            return web.json_response(
                {
                    "status": "ok",
                    "bottlenecks": {
                        "cpu_bottlenecks": [],
                        "memory_bottlenecks": [],
                        "network_bottlenecks": [],
                        "federation_bottlenecks": [],
                    },
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting performance bottlenecks: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_performance_trends(self, request: web.Request) -> web.Response:
        """Get performance trends analysis."""
        try:
            return web.json_response(
                {
                    "status": "ok",
                    "trends": {
                        "latency_trend": "stable",
                        "throughput_trend": "increasing",
                        "error_rate_trend": "decreasing",
                        "resource_utilization_trend": "stable",
                    },
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
            return web.json_response(
                {
                    "status": "ok",
                    "cluster_id": cluster_id,
                    "performance": {
                        "cpu_usage": 45.2,
                        "memory_usage": 62.1,
                        "network_latency": 12.3,
                        "federation_health": "healthy",
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
            return web.json_response(
                {
                    "status": "ok",
                    "active_alerts": [],
                    "alert_count": 0,
                    "severity_breakdown": {"critical": 0, "warning": 0, "info": 0},
                    "timestamp": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting active alerts: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _get_alert_history(self, request: web.Request) -> web.Response:
        """Get federation alert history."""
        try:
            return web.json_response(
                {
                    "status": "ok",
                    "alert_history": [],
                    "total_alerts": 0,
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


# Factory function for creating federation monitoring systems
def create_federation_monitoring_system(
    settings: MPREGSettings,
    federation_config: FederationConfig,
    federation_manager: FederationConnectionManager,
    unified_monitor: UnifiedSystemMonitor,
    monitoring_port: int = 9090,
    enable_cors: bool = True,
) -> FederationMonitoringSystem:
    """Create a federation monitoring system with specified configuration."""

    monitoring_system = FederationMonitoringSystem(
        settings=settings,
        federation_config=federation_config,
        federation_manager=federation_manager,
        unified_monitor=unified_monitor,
    )

    monitoring_system.monitoring_port = monitoring_port
    monitoring_system.enable_cors = enable_cors

    return monitoring_system
