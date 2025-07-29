"""
Federation Performance Metrics and Monitoring System.

This module provides comprehensive performance monitoring, metrics collection,
and alerting capabilities for federated MPREG deployments. It tracks federation-wide
performance, cluster health, and provides real-time insights into system behavior.

Features:
- Real-time performance metrics collection
- Federation-wide latency and throughput tracking
- Resource utilization monitoring
- Health score calculation and trending
- Configurable metric collection intervals
- Historical data retention and analysis
- Integration with alerting and visualization systems
"""

import asyncio
import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from threading import RLock
from typing import Any, Protocol

from loguru import logger

from ..core.statistics import (
    AlertsSummary,
    ClusterPerformanceSummary,
    CollectionStatus,
    FederationPerformanceMetrics,
    PerformanceSummary,
)
from ..datastructures.type_aliases import ClusterId, NodeId, Timestamp


class MetricType(Enum):
    """Types of metrics collected."""

    COUNTER = "counter"  # Monotonically increasing values
    GAUGE = "gauge"  # Point-in-time values
    HISTOGRAM = "histogram"  # Distribution of values
    SUMMARY = "summary"  # Statistical summaries
    TIMER = "timer"  # Duration measurements


class AlertSeverity(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


@dataclass(frozen=True, slots=True)
class PerformanceAlert:
    """Performance alert with severity and context."""

    alert_id: str
    severity: AlertSeverity
    metric_name: str
    current_value: float
    threshold_value: float
    cluster_id: ClusterId
    node_id: NodeId | None
    timestamp: Timestamp
    message: str
    resolved: bool = False


@dataclass(frozen=True, slots=True)
class MetricSnapshot:
    """Single metric measurement snapshot."""

    metric_name: str
    metric_type: MetricType
    value: float
    timestamp: Timestamp
    labels: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class PerformanceMetricsCollector:
    """
    Performance metrics collection system for federation monitoring.

    Collects, aggregates, and analyzes performance metrics across
    federated MPREG clusters with alerting and trend analysis.
    """

    # Configuration
    collection_interval_ms: float = 5000.0  # 5 seconds
    metrics_retention_hours: int = 24
    alert_thresholds: dict[str, float] = field(default_factory=dict)

    # Storage
    metrics_history: deque[MetricSnapshot] = field(
        default_factory=lambda: deque(maxlen=10000)
    )
    active_alerts: list[PerformanceAlert] = field(default_factory=list)
    cluster_metrics: dict[ClusterId, dict[str, float]] = field(default_factory=dict)

    # Collection state
    _collection_task: asyncio.Task | None = field(default=None, init=False)
    _running: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        """Initialize default alert thresholds."""
        if not self.alert_thresholds:
            self.alert_thresholds = {
                "latency_ms": 500.0,
                "error_rate_percent": 5.0,
                "cpu_usage_percent": 80.0,
                "memory_usage_percent": 85.0,
                "disk_usage_percent": 90.0,
            }

    async def start(self) -> None:
        """Start metrics collection."""
        if self._running:
            return

        self._running = True
        self._collection_task = asyncio.create_task(self._collection_loop())
        logger.info("Performance metrics collector started")

    async def stop(self) -> None:
        """Stop metrics collection."""
        if not self._running:
            return

        self._running = False

        if self._collection_task:
            self._collection_task.cancel()
            try:
                await self._collection_task
            except asyncio.CancelledError:
                pass

        logger.info("Performance metrics collector stopped")

    def record_metric(
        self,
        metric_name: str,
        value: float,
        metric_type: MetricType = MetricType.GAUGE,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Record a single metric measurement."""
        snapshot = MetricSnapshot(
            metric_name=metric_name,
            metric_type=metric_type,
            value=value,
            timestamp=time.time(),
            labels=labels or {},
        )

        self.metrics_history.append(snapshot)

        # Check for alert conditions
        if metric_name in self.alert_thresholds:
            threshold = self.alert_thresholds[metric_name]
            if value > threshold:
                self._generate_alert(metric_name, value, threshold, labels or {})

    def get_metric_history(
        self, metric_name: str, duration_hours: int = 1
    ) -> list[MetricSnapshot]:
        """Get historical data for a specific metric."""
        cutoff_time = time.time() - (duration_hours * 3600)

        return [
            snapshot
            for snapshot in self.metrics_history
            if snapshot.metric_name == metric_name and snapshot.timestamp >= cutoff_time
        ]

    def get_cluster_summary(self, cluster_id: ClusterId) -> dict[str, float]:
        """Get performance summary for a specific cluster."""
        return self.cluster_metrics.get(cluster_id, {})

    def calculate_performance_trend(
        self, metric_name: str, duration_hours: int = 1
    ) -> str:
        """Calculate performance trend for a metric."""
        history = self.get_metric_history(metric_name, duration_hours)

        if len(history) < 10:
            return "insufficient_data"

        # Simple trend analysis
        recent_third = len(history) // 3
        recent_avg = sum(s.value for s in history[-recent_third:]) / recent_third
        earlier_avg = sum(s.value for s in history[:recent_third]) / recent_third

        if recent_avg < earlier_avg * 0.9:
            return "improving"
        elif recent_avg > earlier_avg * 1.1:
            return "degrading"
        else:
            return "stable"

    def get_active_alerts(
        self, severity: AlertSeverity | None = None
    ) -> list[PerformanceAlert]:
        """Get active alerts, optionally filtered by severity."""
        alerts = [alert for alert in self.active_alerts if not alert.resolved]

        if severity:
            alerts = [alert for alert in alerts if alert.severity == severity]

        return sorted(alerts, key=lambda a: a.timestamp, reverse=True)

    def resolve_alert(self, alert_id: str) -> bool:
        """Mark an alert as resolved."""
        for alert in self.active_alerts:
            if alert.alert_id == alert_id:
                # Create resolved version (frozen dataclass)
                resolved_alert = PerformanceAlert(
                    alert_id=alert.alert_id,
                    severity=alert.severity,
                    metric_name=alert.metric_name,
                    current_value=alert.current_value,
                    threshold_value=alert.threshold_value,
                    cluster_id=alert.cluster_id,
                    node_id=alert.node_id,
                    timestamp=alert.timestamp,
                    message=alert.message,
                    resolved=True,
                )
                # Replace in list
                index = self.active_alerts.index(alert)
                self.active_alerts[index] = resolved_alert
                return True

        return False

    async def collect_system_metrics(self) -> FederationPerformanceMetrics:
        """Collect comprehensive system performance metrics."""
        # This would integrate with actual system monitoring
        # For now, return structured metrics based on collected data

        current_time = time.time()
        recent_metrics = [
            snapshot
            for snapshot in self.metrics_history
            if current_time - snapshot.timestamp < 300  # Last 5 minutes
        ]

        if not recent_metrics:
            return FederationPerformanceMetrics(
                total_clusters=len(self.cluster_metrics),
                healthy_clusters=len(self.cluster_metrics),
                degraded_clusters=0,
                unhealthy_clusters=0,
                federation_avg_latency_ms=0.0,
                federation_p95_latency_ms=0.0,
                federation_total_throughput_rps=0.0,
                federation_error_rate_percent=0.0,
            )

        # Calculate aggregated metrics
        latency_metrics = [m for m in recent_metrics if "latency" in m.metric_name]
        throughput_metrics = [m for m in recent_metrics if "rps" in m.metric_name]
        error_metrics = [m for m in recent_metrics if "error" in m.metric_name]

        avg_latency = (
            sum(m.value for m in latency_metrics) / len(latency_metrics)
            if latency_metrics
            else 0.0
        )

        total_rps = sum(m.value for m in throughput_metrics)

        avg_error_rate = (
            sum(m.value for m in error_metrics) / len(error_metrics)
            if error_metrics
            else 0.0
        )

        return FederationPerformanceMetrics(
            total_clusters=len(self.cluster_metrics),
            healthy_clusters=max(
                0, len(self.cluster_metrics) - int(avg_error_rate / 20)
            ),
            degraded_clusters=int(avg_error_rate / 20),
            unhealthy_clusters=0,
            federation_avg_latency_ms=avg_latency,
            federation_p95_latency_ms=avg_latency * 1.5,  # Estimate
            federation_total_throughput_rps=total_rps,
            federation_error_rate_percent=avg_error_rate,
        )

    def _generate_alert(
        self, metric_name: str, value: float, threshold: float, labels: dict[str, str]
    ) -> None:
        """Generate performance alert."""
        import ulid

        # Check if similar alert already exists
        existing_alert = None
        for alert in self.active_alerts:
            if (
                not alert.resolved
                and alert.metric_name == metric_name
                and alert.cluster_id == labels.get("cluster_id", "unknown")
            ):
                existing_alert = alert
                break

        if existing_alert:
            return  # Don't duplicate alerts

        severity = AlertSeverity.WARNING
        if metric_name == "error_rate_percent" and value > 10.0:
            severity = AlertSeverity.CRITICAL
        elif (
            metric_name in ["cpu_usage_percent", "memory_usage_percent"]
            and value > 95.0
        ):
            severity = AlertSeverity.CRITICAL

        alert = PerformanceAlert(
            alert_id=str(ulid.new()),
            severity=severity,
            metric_name=metric_name,
            current_value=value,
            threshold_value=threshold,
            cluster_id=labels.get("cluster_id", "unknown"),
            node_id=labels.get("node_id"),
            timestamp=time.time(),
            message=f"{metric_name} exceeded threshold: {value:.2f} > {threshold:.2f}",
            resolved=False,
        )

        self.active_alerts.append(alert)
        logger.warning(f"Performance alert generated: {alert.message}")

    async def _collection_loop(self) -> None:
        """Main metrics collection loop."""
        try:
            while self._running:
                await asyncio.sleep(self.collection_interval_ms / 1000.0)

                if not self._running:
                    break

                try:
                    # Simulate collection of system metrics
                    current_time = time.time()

                    # Example metrics that would be collected from real system
                    self.record_metric(
                        "system_latency_ms",
                        50.0 + (current_time % 10) * 5,  # Varying latency
                        MetricType.GAUGE,
                        {"cluster_id": "default-cluster"},
                    )

                    self.record_metric(
                        "system_rps",
                        100.0 + (current_time % 20) * 2,  # Varying throughput
                        MetricType.GAUGE,
                        {"cluster_id": "default-cluster"},
                    )

                    # Clean up old metrics
                    self._cleanup_old_metrics()

                except Exception as e:
                    logger.error(f"Error in metrics collection: {e}")

        except asyncio.CancelledError:
            logger.info("Metrics collection loop cancelled")
        except Exception as e:
            logger.error(f"Metrics collection loop error: {e}")

    def _cleanup_old_metrics(self) -> None:
        """Clean up old metrics beyond retention period."""
        cutoff_time = time.time() - (self.metrics_retention_hours * 3600)

        # Remove old resolved alerts
        self.active_alerts = [
            alert
            for alert in self.active_alerts
            if not alert.resolved or alert.timestamp >= cutoff_time
        ]


class MetricPoint:
    """Individual metric data point."""

    name: str
    value: float
    timestamp: float
    labels: dict[str, str] = field(default_factory=dict)
    metric_type: MetricType = MetricType.GAUGE


@dataclass(frozen=True, slots=True)
class PerformanceThresholds:
    """Performance thresholds for alerting."""

    # Latency thresholds (milliseconds)
    latency_warning: float = 100.0
    latency_error: float = 500.0
    latency_critical: float = 1000.0

    # Throughput thresholds (requests/second)
    throughput_warning: float = 10.0
    throughput_error: float = 5.0
    throughput_critical: float = 1.0

    # Error rate thresholds (percentage)
    error_rate_warning: float = 1.0
    error_rate_error: float = 5.0
    error_rate_critical: float = 10.0

    # Health score thresholds (0-100)
    health_score_warning: float = 80.0
    health_score_error: float = 60.0
    health_score_critical: float = 40.0

    # Resource utilization thresholds (percentage)
    cpu_warning: float = 70.0
    cpu_error: float = 85.0
    cpu_critical: float = 95.0

    memory_warning: float = 80.0
    memory_error: float = 90.0
    memory_critical: float = 95.0


@dataclass(frozen=True, slots=True)
class ClusterMetrics:
    """Performance metrics for a single cluster."""

    cluster_id: str
    cluster_name: str
    region: str

    # Performance metrics
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    throughput_rps: float = 0.0
    error_rate_percent: float = 0.0

    # Health and availability
    health_score: float = 100.0
    uptime_seconds: float = 0.0
    last_seen: float = field(default_factory=time.time)

    # Resource utilization
    cpu_usage_percent: float = 0.0
    memory_usage_percent: float = 0.0
    network_io_mbps: float = 0.0
    disk_io_mbps: float = 0.0

    # Federation-specific metrics
    active_connections: int = 0
    cross_cluster_messages: int = 0
    federation_latency_ms: float = 0.0
    bridge_health: float = 100.0

    # Message statistics
    messages_sent: int = 0
    messages_received: int = 0
    messages_failed: int = 0
    queue_depth: int = 0


# FederationMetrics is now imported from core.statistics as FederationPerformanceMetrics


# Duplicate PerformanceAlert class removed - using the one defined at line 59


class MetricsCollector(Protocol):
    """Protocol for metrics collection backends."""

    async def collect_metrics(self, cluster_id: str) -> ClusterMetrics:
        """Collect metrics for a specific cluster."""
        ...

    async def health_check(self) -> bool:
        """Check if metrics collection is working."""
        ...


@dataclass(slots=True)
class PerformanceMetricsService:
    """
    Comprehensive performance metrics collection and monitoring service.

    Provides real-time metrics collection, performance tracking, threshold monitoring,
    and alerting capabilities for federated MPREG deployments.
    """

    # Configuration
    collection_interval: float = 30.0
    retention_hours: int = 24
    thresholds: PerformanceThresholds = field(default_factory=PerformanceThresholds)

    # Internal state
    cluster_metrics: dict[str, ClusterMetrics] = field(default_factory=dict)
    federation_metrics_history: deque[FederationPerformanceMetrics] = field(
        default_factory=lambda: deque(maxlen=1000)
    )
    cluster_metrics_history: dict[str, deque[ClusterMetrics]] = field(
        default_factory=lambda: defaultdict(lambda: deque(maxlen=1000))
    )

    # Performance tracking
    latency_measurements: dict[str, deque[float]] = field(
        default_factory=lambda: defaultdict(lambda: deque(maxlen=100))
    )
    throughput_measurements: dict[str, deque[float]] = field(
        default_factory=lambda: defaultdict(lambda: deque(maxlen=100))
    )
    error_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    # Collection and alerting
    collectors: list[MetricsCollector] = field(default_factory=list)
    active_alerts: dict[str, PerformanceAlert] = field(default_factory=dict)
    alert_callbacks: list[Callable[[PerformanceAlert], None]] = field(
        default_factory=list
    )

    # Runtime state
    collecting: bool = False
    collection_task: asyncio.Task[Any] | None = None
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """Initialize the metrics service."""
        logger.info("Performance metrics service initialized")

    def add_collector(self, collector: MetricsCollector) -> None:
        """Add a metrics collector."""
        with self._lock:
            self.collectors.append(collector)
            logger.info(f"Added metrics collector: {type(collector).__name__}")

    def add_alert_callback(self, callback: Callable[[PerformanceAlert], None]) -> None:
        """Add an alert notification callback."""
        with self._lock:
            self.alert_callbacks.append(callback)
            logger.info("Added alert callback")

    def record_latency(self, cluster_id: str, latency_ms: float) -> None:
        """Record a latency measurement."""
        with self._lock:
            self.latency_measurements[cluster_id].append(latency_ms)

    def record_throughput(self, cluster_id: str, throughput_rps: float) -> None:
        """Record a throughput measurement."""
        with self._lock:
            self.throughput_measurements[cluster_id].append(throughput_rps)

    def record_error(self, cluster_id: str) -> None:
        """Record an error occurrence."""
        with self._lock:
            self.error_counts[cluster_id] += 1

    def get_cluster_metrics(self, cluster_id: str) -> ClusterMetrics | None:
        """Get current metrics for a specific cluster."""
        with self._lock:
            return self.cluster_metrics.get(cluster_id)

    def get_federation_metrics(self) -> FederationPerformanceMetrics | None:
        """Get current federation-wide metrics."""
        with self._lock:
            if self.federation_metrics_history:
                return self.federation_metrics_history[-1]
            return None

    def get_cluster_history(
        self, cluster_id: str, hours: int = 1
    ) -> list[ClusterMetrics]:
        """Get historical metrics for a cluster."""
        with self._lock:
            if cluster_id not in self.cluster_metrics_history:
                return []

            cutoff_time = time.time() - (hours * 3600)
            history = self.cluster_metrics_history[cluster_id]

            return [metrics for metrics in history if metrics.last_seen >= cutoff_time]

    def get_federation_history(
        self, hours: int = 1
    ) -> list[FederationPerformanceMetrics]:
        """Get historical federation metrics."""
        with self._lock:
            cutoff_time = time.time() - (hours * 3600)
            return [
                metrics
                for metrics in self.federation_metrics_history
                if metrics.collected_at >= cutoff_time
            ]

    async def start_collection(self) -> None:
        """Start automated metrics collection."""
        if self.collecting:
            logger.warning("Metrics collection already running")
            return

        self.collecting = True
        self.collection_task = asyncio.create_task(self._collection_loop())
        logger.info(
            f"Started metrics collection with {self.collection_interval}s interval"
        )

    async def stop_collection(self) -> None:
        """Stop automated metrics collection."""
        if not self.collecting:
            return

        self.collecting = False
        if self.collection_task:
            self.collection_task.cancel()
            try:
                await self.collection_task
            except asyncio.CancelledError:
                pass

        logger.info("Stopped metrics collection")

    async def _collection_loop(self) -> None:
        """Main metrics collection loop."""
        while self.collecting:
            try:
                await self._collect_all_metrics()
                await self._generate_federation_metrics()
                await self._check_thresholds()
                await asyncio.sleep(self.collection_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in metrics collection loop: {e}")
                await asyncio.sleep(5.0)  # Brief pause before retry

    async def _collect_all_metrics(self) -> None:
        """Collect metrics from all configured collectors."""
        if not self.collectors:
            return

        # Get list of cluster IDs to collect for
        cluster_ids = set(self.cluster_metrics.keys())

        # If no clusters yet, try to discover from collectors
        if not cluster_ids:
            # For now, we'll add clusters as they report metrics
            # In a real implementation, this would integrate with auto-discovery
            pass

        for collector in self.collectors:
            try:
                # For each known cluster, collect metrics
                for cluster_id in cluster_ids:
                    metrics = await collector.collect_metrics(cluster_id)
                    await self._update_cluster_metrics(metrics)
            except Exception as e:
                logger.error(f"Error collecting from {type(collector).__name__}: {e}")

    async def _update_cluster_metrics(self, metrics: ClusterMetrics) -> None:
        """Update cluster metrics and add to history."""
        with self._lock:
            self.cluster_metrics[metrics.cluster_id] = metrics
            self.cluster_metrics_history[metrics.cluster_id].append(metrics)

            # Update internal tracking
            if metrics.avg_latency_ms > 0:
                self.latency_measurements[metrics.cluster_id].append(
                    metrics.avg_latency_ms
                )
            if metrics.throughput_rps > 0:
                self.throughput_measurements[metrics.cluster_id].append(
                    metrics.throughput_rps
                )

    async def _generate_federation_metrics(self) -> None:
        """Generate aggregated federation-wide metrics."""
        with self._lock:
            if not self.cluster_metrics:
                return

            clusters = list(self.cluster_metrics.values())

            # Count cluster health states
            healthy = sum(1 for c in clusters if c.health_score >= 80)
            degraded = sum(1 for c in clusters if 60 <= c.health_score < 80)
            unhealthy = sum(1 for c in clusters if c.health_score < 60)

            # Calculate aggregated performance metrics
            avg_latency = (
                sum(c.avg_latency_ms for c in clusters) / len(clusters)
                if clusters
                else 0
            )
            p95_latency = (
                sorted([c.p95_latency_ms for c in clusters])[int(len(clusters) * 0.95)]
                if clusters
                else 0
            )
            total_throughput = sum(c.throughput_rps for c in clusters)
            avg_error_rate = (
                sum(c.error_rate_percent for c in clusters) / len(clusters)
                if clusters
                else 0
            )

            # Resource utilization
            avg_cpu = (
                sum(c.cpu_usage_percent for c in clusters) / len(clusters)
                if clusters
                else 0
            )
            avg_memory = (
                sum(c.memory_usage_percent for c in clusters) / len(clusters)
                if clusters
                else 0
            )
            total_network = sum(c.network_io_mbps for c in clusters)

            # Cross-cluster metrics
            total_cross_cluster = sum(c.cross_cluster_messages for c in clusters)
            avg_cross_cluster_latency = (
                sum(c.federation_latency_ms for c in clusters) / len(clusters)
                if clusters
                else 0
            )

            # Health and messaging
            federation_health = (
                sum(c.health_score for c in clusters) / len(clusters)
                if clusters
                else 100
            )
            total_messages_sent = sum(c.messages_sent for c in clusters)
            total_messages_received = sum(c.messages_received for c in clusters)
            total_messages_failed = sum(c.messages_failed for c in clusters)
            avg_queue_depth = (
                sum(c.queue_depth for c in clusters) / len(clusters) if clusters else 0
            )

            federation_metrics = FederationPerformanceMetrics(
                total_clusters=len(clusters),
                healthy_clusters=healthy,
                degraded_clusters=degraded,
                unhealthy_clusters=unhealthy,
                federation_avg_latency_ms=avg_latency,
                federation_p95_latency_ms=p95_latency,
                federation_total_throughput_rps=total_throughput,
                federation_error_rate_percent=avg_error_rate,
                total_cross_cluster_messages=total_cross_cluster,
                avg_cross_cluster_latency_ms=avg_cross_cluster_latency,
                avg_cpu_usage_percent=avg_cpu,
                avg_memory_usage_percent=avg_memory,
                total_network_io_mbps=total_network,
                federation_health_score=federation_health,
                total_messages_sent=total_messages_sent,
                total_messages_received=total_messages_received,
                total_messages_failed=total_messages_failed,
                average_queue_depth=avg_queue_depth,
            )

            self.federation_metrics_history.append(federation_metrics)

    async def _check_thresholds(self) -> None:
        """Check performance thresholds and trigger alerts."""
        current_time = time.time()

        with self._lock:
            for cluster_id, metrics in self.cluster_metrics.items():
                alerts_to_trigger = []

                # Check latency thresholds
                if metrics.avg_latency_ms >= self.thresholds.latency_critical:
                    alerts_to_trigger.append(
                        self._create_alert(
                            cluster_id,
                            AlertSeverity.CRITICAL,
                            "avg_latency_ms",
                            metrics.avg_latency_ms,
                            self.thresholds.latency_critical,
                            f"Critical latency in cluster {cluster_id}: {metrics.avg_latency_ms:.1f}ms",
                        )
                    )
                elif metrics.avg_latency_ms >= self.thresholds.latency_error:
                    alerts_to_trigger.append(
                        self._create_alert(
                            cluster_id,
                            AlertSeverity.CRITICAL,
                            "avg_latency_ms",
                            metrics.avg_latency_ms,
                            self.thresholds.latency_error,
                            f"High latency in cluster {cluster_id}: {metrics.avg_latency_ms:.1f}ms",
                        )
                    )
                elif metrics.avg_latency_ms >= self.thresholds.latency_warning:
                    alerts_to_trigger.append(
                        self._create_alert(
                            cluster_id,
                            AlertSeverity.WARNING,
                            "avg_latency_ms",
                            metrics.avg_latency_ms,
                            self.thresholds.latency_warning,
                            f"Elevated latency in cluster {cluster_id}: {metrics.avg_latency_ms:.1f}ms",
                        )
                    )

                # Check throughput thresholds
                if metrics.throughput_rps <= self.thresholds.throughput_critical:
                    alerts_to_trigger.append(
                        self._create_alert(
                            cluster_id,
                            AlertSeverity.CRITICAL,
                            "throughput_rps",
                            metrics.throughput_rps,
                            self.thresholds.throughput_critical,
                            f"Critical low throughput in cluster {cluster_id}: {metrics.throughput_rps:.1f} RPS",
                        )
                    )
                elif metrics.throughput_rps <= self.thresholds.throughput_error:
                    alerts_to_trigger.append(
                        self._create_alert(
                            cluster_id,
                            AlertSeverity.CRITICAL,
                            "throughput_rps",
                            metrics.throughput_rps,
                            self.thresholds.throughput_error,
                            f"Low throughput in cluster {cluster_id}: {metrics.throughput_rps:.1f} RPS",
                        )
                    )

                # Check error rate thresholds
                if metrics.error_rate_percent >= self.thresholds.error_rate_critical:
                    alerts_to_trigger.append(
                        self._create_alert(
                            cluster_id,
                            AlertSeverity.CRITICAL,
                            "error_rate_percent",
                            metrics.error_rate_percent,
                            self.thresholds.error_rate_critical,
                            f"Critical error rate in cluster {cluster_id}: {metrics.error_rate_percent:.1f}%",
                        )
                    )
                elif metrics.error_rate_percent >= self.thresholds.error_rate_error:
                    alerts_to_trigger.append(
                        self._create_alert(
                            cluster_id,
                            AlertSeverity.CRITICAL,
                            "error_rate_percent",
                            metrics.error_rate_percent,
                            self.thresholds.error_rate_error,
                            f"High error rate in cluster {cluster_id}: {metrics.error_rate_percent:.1f}%",
                        )
                    )
                elif metrics.error_rate_percent >= self.thresholds.error_rate_warning:
                    alerts_to_trigger.append(
                        self._create_alert(
                            cluster_id,
                            AlertSeverity.WARNING,
                            "error_rate_percent",
                            metrics.error_rate_percent,
                            self.thresholds.error_rate_warning,
                            f"Elevated error rate in cluster {cluster_id}: {metrics.error_rate_percent:.1f}%",
                        )
                    )

                # Check health score thresholds
                if metrics.health_score <= self.thresholds.health_score_critical:
                    alerts_to_trigger.append(
                        self._create_alert(
                            cluster_id,
                            AlertSeverity.CRITICAL,
                            "health_score",
                            metrics.health_score,
                            self.thresholds.health_score_critical,
                            f"Critical health score in cluster {cluster_id}: {metrics.health_score:.1f}",
                        )
                    )
                elif metrics.health_score <= self.thresholds.health_score_error:
                    alerts_to_trigger.append(
                        self._create_alert(
                            cluster_id,
                            AlertSeverity.CRITICAL,
                            "health_score",
                            metrics.health_score,
                            self.thresholds.health_score_error,
                            f"Low health score in cluster {cluster_id}: {metrics.health_score:.1f}",
                        )
                    )
                elif metrics.health_score <= self.thresholds.health_score_warning:
                    alerts_to_trigger.append(
                        self._create_alert(
                            cluster_id,
                            AlertSeverity.WARNING,
                            "health_score",
                            metrics.health_score,
                            self.thresholds.health_score_warning,
                            f"Degraded health score in cluster {cluster_id}: {metrics.health_score:.1f}",
                        )
                    )

                # Trigger all new alerts
                for alert in alerts_to_trigger:
                    await self._trigger_alert(alert)

    def _create_alert(
        self,
        cluster_id: str,
        severity: AlertSeverity,
        metric_name: str,
        current_value: float,
        threshold_value: float,
        message: str,
    ) -> PerformanceAlert:
        """Create a performance alert."""
        alert_id = f"{cluster_id}_{metric_name}_{severity.value}"

        return PerformanceAlert(
            alert_id=alert_id,
            cluster_id=cluster_id,
            node_id=None,
            severity=severity,
            metric_name=metric_name,
            current_value=current_value,
            threshold_value=threshold_value,
            timestamp=time.time(),
            message=message,
        )

    async def _trigger_alert(self, alert: PerformanceAlert) -> None:
        """Trigger an alert notification."""
        # Avoid duplicate alerts
        if alert.alert_id in self.active_alerts:
            return

        self.active_alerts[alert.alert_id] = alert
        logger.warning(f"ALERT [{alert.severity.value.upper()}]: {alert.message}")

        # Notify all alert callbacks
        for callback in self.alert_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(alert)
                else:
                    callback(alert)
            except Exception as e:
                logger.error(f"Error in alert callback: {e}")

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an active alert."""
        with self._lock:
            if alert_id in self.active_alerts:
                alert = self.active_alerts[alert_id]
                resolved_alert = PerformanceAlert(
                    alert_id=alert.alert_id,
                    cluster_id=alert.cluster_id,
                    node_id=alert.node_id,
                    severity=alert.severity,
                    metric_name=alert.metric_name,
                    current_value=alert.current_value,
                    threshold_value=alert.threshold_value,
                    timestamp=time.time(),
                    message=f"RESOLVED: {alert.message}",
                    resolved=True,
                )

                del self.active_alerts[alert_id]
                logger.info(f"Alert resolved: {alert_id}")
                return True

        return False

    def get_active_alerts(self) -> list[PerformanceAlert]:
        """Get all currently active alerts."""
        with self._lock:
            return list(self.active_alerts.values())

    def get_performance_summary(self) -> PerformanceSummary:
        """Get a comprehensive performance summary."""
        with self._lock:
            federation_metrics = self.get_federation_metrics()
            active_alerts = self.get_active_alerts()

            # Create cluster performance summaries
            clusters = {
                cluster_id: ClusterPerformanceSummary(
                    cluster_id=cluster_id,
                    health_score=metrics.health_score,
                    avg_latency_ms=metrics.avg_latency_ms,
                    throughput_rps=metrics.throughput_rps,
                    error_rate_percent=metrics.error_rate_percent,
                )
                for cluster_id, metrics in self.cluster_metrics.items()
            }

            # Create alerts summary
            alerts_by_severity = AlertsSummary(
                info=len(
                    [a for a in active_alerts if a.severity == AlertSeverity.INFO]
                ),
                warning=len(
                    [a for a in active_alerts if a.severity == AlertSeverity.WARNING]
                ),
                error=len(
                    [a for a in active_alerts if a.severity == AlertSeverity.ERROR]
                ),
                critical=len(
                    [a for a in active_alerts if a.severity == AlertSeverity.CRITICAL]
                ),
                total=len(active_alerts),
            )

            # Create collection status
            collection_status = CollectionStatus(
                collecting=self.collecting,
                collectors=len(self.collectors),
                collection_interval=self.collection_interval,
            )

            return PerformanceSummary(
                federation_metrics=federation_metrics,
                cluster_count=len(self.cluster_metrics),
                clusters=clusters,
                active_alerts=len(active_alerts),
                alerts_by_severity=alerts_by_severity,
                collection_status=collection_status,
                timestamp=time.time(),
            )


# Helper functions for creating common metric collection scenarios


def create_performance_metrics_service(
    collection_interval: float = 30.0,
    retention_hours: int = 24,
    custom_thresholds: PerformanceThresholds | None = None,
) -> PerformanceMetricsService:
    """Create a performance metrics service with standard configuration."""
    thresholds = custom_thresholds or PerformanceThresholds()

    service = PerformanceMetricsService(
        collection_interval=collection_interval,
        retention_hours=retention_hours,
        thresholds=thresholds,
    )

    logger.info(
        f"Created performance metrics service with {collection_interval}s collection interval"
    )
    return service


def create_production_thresholds() -> PerformanceThresholds:
    """Create production-appropriate performance thresholds."""
    return PerformanceThresholds(
        latency_warning=50.0,
        latency_error=200.0,
        latency_critical=500.0,
        throughput_warning=50.0,
        throughput_error=20.0,
        throughput_critical=5.0,
        error_rate_warning=0.5,
        error_rate_error=2.0,
        error_rate_critical=5.0,
        health_score_warning=85.0,
        health_score_error=70.0,
        health_score_critical=50.0,
    )


def create_development_thresholds() -> PerformanceThresholds:
    """Create development-appropriate performance thresholds."""
    return PerformanceThresholds(
        latency_warning=200.0,
        latency_error=1000.0,
        latency_critical=2000.0,
        throughput_warning=5.0,
        throughput_error=2.0,
        throughput_critical=0.5,
        error_rate_warning=5.0,
        error_rate_error=15.0,
        error_rate_critical=25.0,
        health_score_warning=70.0,
        health_score_error=50.0,
        health_score_critical=30.0,
    )
