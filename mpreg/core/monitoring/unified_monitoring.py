"""
Unified monitoring and observability system for MPREG.

This module provides comprehensive monitoring across all four core MPREG systems
(RPC, Topic Pub/Sub, Message Queue, Cache) plus federation and transport layers.
It leverages the enhanced transport metrics from Phase 1.2.1 to create unified
observability with cross-system correlation tracking.

Key features:
- Cross-system correlation tracking using unified correlation IDs and ULID tracking IDs
- Stable end-to-end event tracking with ULID-based tracking identifiers
- Real-time performance metrics aggregation across all systems
- Transport health integration with enhanced transport monitoring
- Event timeline reconstruction for debugging complex workflows
- Performance trend analysis and anomaly detection
- Self-managing monitoring lifecycle with minimal overhead
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

import ulid
from loguru import logger

from mpreg.core.transport.correlation import CorrelationResult
from mpreg.core.transport.enhanced_health import (
    HealthScore,
    TransportHealthSnapshot,
)
from mpreg.datastructures.type_aliases import CorrelationId

# Type aliases for monitoring semantics
type SystemName = str
type EndpointUrl = str
type MonitoringLatencyMs = float
type EventCount = int
type SystemHealthScore = float  # 0.0 to 1.0, aggregated system health
type TrackingId = (
    str  # ULID-based stable tracking identifier for end-to-end event tracing
)


class SystemType(Enum):
    """Types of MPREG systems being monitored."""

    RPC = "rpc"
    PUBSUB = "pubsub"
    QUEUE = "queue"
    CACHE = "cache"
    FEDERATION = "federation"
    TRANSPORT = "transport"


class EventType(Enum):
    """Types of monitoring events."""

    SYSTEM_START = "system_start"
    SYSTEM_STOP = "system_stop"
    REQUEST_START = "request_start"
    REQUEST_COMPLETE = "request_complete"
    DEPENDENCY_RESOLVED = "dependency_resolved"
    HEALTH_CHECK = "health_check"
    PERFORMANCE_ALERT = "performance_alert"
    CROSS_SYSTEM_CORRELATION = "cross_system_correlation"


class HealthStatus(Enum):
    """System health status categories."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class CrossSystemEvent:
    """Event representing cross-system activity with correlation tracking."""

    tracking_id: TrackingId  # Stable ULID-based tracking ID for end-to-end tracing
    correlation_id: CorrelationId  # May be same as tracking_id or different for correlation grouping
    event_type: EventType
    source_system: SystemType
    target_system: SystemType | None
    timestamp: float
    latency_ms: MonitoringLatencyMs
    success: bool
    metadata: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class SystemPerformanceMetrics:
    """Performance metrics for individual system monitoring."""

    system_type: SystemType
    system_name: SystemName
    requests_per_second: float
    average_latency_ms: MonitoringLatencyMs
    p95_latency_ms: MonitoringLatencyMs
    p99_latency_ms: MonitoringLatencyMs
    error_rate_percent: float
    active_connections: int
    total_operations_last_hour: EventCount
    last_updated: float


@dataclass(frozen=True, slots=True)
class CorrelationMetrics:
    """Cross-system correlation tracking metrics."""

    # Cross-system event correlations
    rpc_to_pubsub_events: EventCount
    rpc_to_queue_events: EventCount
    rpc_to_cache_events: EventCount
    pubsub_to_queue_events: EventCount
    pubsub_to_cache_events: EventCount
    queue_to_cache_events: EventCount

    # Federation metrics
    federation_message_hops: list[float]
    cross_cluster_latency_p95_ms: MonitoringLatencyMs
    federation_success_rate_percent: float

    # Transport metrics from enhanced transport layer
    transport_health_scores: dict[EndpointUrl, HealthScore]
    circuit_breaker_events: EventCount
    correlation_tracking_success_rate: float

    # End-to-end metrics
    end_to_end_latency_p95_ms: MonitoringLatencyMs
    total_cross_system_correlations: EventCount
    correlation_success_rate_percent: float


@dataclass(frozen=True, slots=True)
class UnifiedSystemMetrics:
    """Comprehensive metrics across all MPREG systems."""

    # Individual system metrics
    rpc_metrics: SystemPerformanceMetrics | None
    pubsub_metrics: SystemPerformanceMetrics | None
    queue_metrics: SystemPerformanceMetrics | None
    cache_metrics: SystemPerformanceMetrics | None
    federation_metrics: SystemPerformanceMetrics | None

    # Enhanced transport metrics from Phase 1.2.1
    transport_health_snapshots: dict[EndpointUrl, TransportHealthSnapshot]
    transport_correlation_results: list[CorrelationResult]

    # Cross-system correlation metrics
    correlation_metrics: CorrelationMetrics

    # Overall system health
    overall_health_score: SystemHealthScore
    overall_health_status: HealthStatus
    systems_healthy: int
    systems_degraded: int
    systems_critical: int
    systems_unavailable: int

    # Performance summary
    total_requests_per_second: float
    average_cross_system_latency_ms: MonitoringLatencyMs
    total_active_correlations: EventCount

    # Collection metadata
    collection_timestamp: float
    collection_duration_ms: MonitoringLatencyMs


@dataclass(slots=True)
class MonitoringConfig:
    """Configuration for unified system monitoring."""

    # Collection intervals
    metrics_collection_interval_ms: float = 5000.0  # 5 seconds
    correlation_cleanup_interval_ms: float = 60000.0  # 1 minute
    health_check_interval_ms: float = 10000.0  # 10 seconds

    # History and retention
    max_event_history: int = 50000
    max_correlation_history: int = 100000
    metrics_history_retention_hours: int = 24

    # Performance thresholds
    latency_warning_threshold_ms: float = 1000.0
    latency_critical_threshold_ms: float = 5000.0
    error_rate_warning_threshold: float = 0.05  # 5%
    error_rate_critical_threshold: float = 0.15  # 15%
    health_score_degraded_threshold: float = 0.8
    health_score_critical_threshold: float = 0.6

    # System integration
    enable_transport_monitoring: bool = True
    enable_federation_monitoring: bool = True
    enable_cache_monitoring: bool = True
    enable_cross_system_correlation: bool = True


class MonitoringSystemProtocol(Protocol):
    """Protocol for system-specific monitoring interfaces."""

    async def get_system_metrics(self) -> SystemPerformanceMetrics:
        """Get current performance metrics for this system."""
        ...

    async def get_health_status(self) -> tuple[HealthScore, HealthStatus]:
        """Get current health score and status."""
        ...


@dataclass(slots=True)
class UnifiedSystemMonitor:
    """
    Unified monitoring system for all MPREG components.

    Provides comprehensive monitoring across RPC, Topic Pub/Sub, Message Queue,
    Cache, Federation, and Transport systems with cross-system correlation
    tracking and performance analytics.
    """

    config: MonitoringConfig

    # System monitoring interfaces
    rpc_monitor: MonitoringSystemProtocol | None = None
    pubsub_monitor: MonitoringSystemProtocol | None = None
    queue_monitor: MonitoringSystemProtocol | None = None
    cache_monitor: MonitoringSystemProtocol | None = None
    federation_monitor: MonitoringSystemProtocol | None = None
    transport_monitor: MonitoringSystemProtocol | None = None

    # Event tracking with ULID-based stable tracking IDs
    event_history: deque[CrossSystemEvent] = field(
        default_factory=lambda: deque(maxlen=50000)
    )
    active_correlations: dict[CorrelationId, CrossSystemEvent] = field(
        default_factory=dict
    )
    correlation_chains: dict[CorrelationId, list[CrossSystemEvent]] = field(
        default_factory=dict
    )
    # ULID-based tracking for end-to-end event tracing
    tracking_id_to_events: dict[TrackingId, list[CrossSystemEvent]] = field(
        default_factory=dict
    )
    active_tracking_ids: dict[TrackingId, float] = field(  # tracking_id -> start_time
        default_factory=dict
    )

    # Performance tracking
    system_metrics_history: deque[UnifiedSystemMetrics] = field(
        default_factory=lambda: deque(maxlen=1440)  # 24 hours at 1-minute intervals
    )
    correlation_performance: dict[tuple[SystemType, SystemType], list[float]] = field(
        default_factory=lambda: defaultdict(list)
    )

    # Background tasks
    _monitoring_tasks: list[asyncio.Task[Any]] = field(default_factory=list)
    _running: bool = False

    async def start(self) -> None:
        """Start unified system monitoring."""
        if self._running:
            return

        self._running = True

        # Start background monitoring tasks
        if self.config.metrics_collection_interval_ms > 0:
            task = asyncio.create_task(self._metrics_collection_task())
            self._monitoring_tasks.append(task)

        if self.config.correlation_cleanup_interval_ms > 0:
            task = asyncio.create_task(self._correlation_cleanup_task())
            self._monitoring_tasks.append(task)

        if self.config.health_check_interval_ms > 0:
            task = asyncio.create_task(self._health_monitoring_task())
            self._monitoring_tasks.append(task)

        logger.info(
            f"Unified system monitor started with {len(self._monitoring_tasks)} background tasks"
        )

    async def stop(self) -> None:
        """Stop unified system monitoring."""
        if not self._running:
            return

        self._running = False

        # Cancel and wait for background tasks
        for task in self._monitoring_tasks:
            task.cancel()

        if self._monitoring_tasks:
            await asyncio.gather(*self._monitoring_tasks, return_exceptions=True)

        self._monitoring_tasks.clear()
        logger.info("Unified system monitor stopped")

    def generate_tracking_id(
        self, existing_id: str | None = None, namespace_prefix: str = "mon"
    ) -> TrackingId:
        """
        Generate a stable tracking ID for end-to-end event tracing.

        Uses existing object ID if available (from RPC requests, queue messages, etc.)
        or generates a new namespaced ULID for metrics tracking.

        Args:
            existing_id: Optional existing unique ID from the object being tracked
            namespace_prefix: Prefix for new ULIDs (e.g., "rpc", "msg", "q", "cache")

        Returns:
            TrackingId: Stable ULID-based tracking identifier
        """
        if existing_id and len(existing_id.strip()) > 0:
            # If object already has a unique ID, use it as tracking ID
            return existing_id.strip()
        else:
            # Generate new namespaced ULID for metrics tracking
            return f"{namespace_prefix}-{str(ulid.new())}"

    def generate_system_tracking_id(self, system_type: SystemType) -> TrackingId:
        """
        Generate a system-specific namespaced tracking ID.

        Creates tracking IDs with system-specific prefixes for better log tracking:
        - RPC: "rpc-{ulid}"
        - PubSub: "ps-{ulid}"
        - Queue: "q-{ulid}"
        - Cache: "cache-{ulid}"
        - Federation: "fed-{ulid}"
        - Transport: "tx-{ulid}"

        Args:
            system_type: The system type generating the tracking ID

        Returns:
            TrackingId: System-prefixed ULID tracking identifier
        """
        prefix_map = {
            SystemType.RPC: "rpc",
            SystemType.PUBSUB: "ps",
            SystemType.QUEUE: "q",
            SystemType.CACHE: "cache",
            SystemType.FEDERATION: "fed",
            SystemType.TRANSPORT: "tx",
        }

        prefix = prefix_map.get(system_type, "sys")
        return f"{prefix}-{str(ulid.new())}"

    async def record_cross_system_event(
        self,
        correlation_id: CorrelationId,
        event_type: EventType,
        source_system: SystemType,
        target_system: SystemType | None = None,
        latency_ms: MonitoringLatencyMs = 0.0,
        success: bool = True,
        metadata: dict[str, Any] | None = None,
        error_message: str | None = None,
        tracking_id: TrackingId | None = None,
        existing_object_id: str | None = None,
    ) -> TrackingId:
        """
        Record a cross-system event for correlation tracking with stable tracking ID.

        Args:
            correlation_id: Correlation ID for grouping related events
            event_type: Type of event being recorded
            source_system: System generating the event
            target_system: System receiving the event (if applicable)
            latency_ms: Event processing latency
            success: Whether the event was successful
            metadata: Additional event metadata
            error_message: Error message if unsuccessful
            tracking_id: Existing tracking ID to use (optional)
            existing_object_id: Existing unique ID from the object being tracked

        Returns:
            TrackingId: The tracking ID used for this event (for chaining)
        """
        # Generate or use provided tracking ID
        if tracking_id is None:
            tracking_id = self.generate_tracking_id(existing_object_id)

        event = CrossSystemEvent(
            tracking_id=tracking_id,
            correlation_id=correlation_id,
            event_type=event_type,
            source_system=source_system,
            target_system=target_system,
            timestamp=time.time(),
            latency_ms=latency_ms,
            success=success,
            metadata=metadata or {},
            error_message=error_message,
        )

        # Add to event history
        self.event_history.append(event)

        # Track correlation chains
        if correlation_id not in self.correlation_chains:
            self.correlation_chains[correlation_id] = []
        self.correlation_chains[correlation_id].append(event)

        # Track by ULID tracking ID for end-to-end tracing
        if tracking_id not in self.tracking_id_to_events:
            self.tracking_id_to_events[tracking_id] = []
        self.tracking_id_to_events[tracking_id].append(event)

        # Update active correlations and tracking
        if event_type == EventType.REQUEST_START:
            self.active_correlations[correlation_id] = event
            self.active_tracking_ids[tracking_id] = time.time()
        elif event_type == EventType.REQUEST_COMPLETE:
            self.active_correlations.pop(correlation_id, None)
            self.active_tracking_ids.pop(tracking_id, None)

        # Track cross-system performance
        if target_system and latency_ms > 0:
            key = (source_system, target_system)
            self.correlation_performance[key].append(latency_ms)
            # Keep only recent measurements (last 1000)
            if len(self.correlation_performance[key]) > 1000:
                self.correlation_performance[key] = self.correlation_performance[key][
                    -1000:
                ]

        return tracking_id

    async def get_unified_metrics(self) -> UnifiedSystemMetrics:
        """Get comprehensive metrics across all systems."""
        collection_start = time.time()

        # Collect system-specific metrics
        rpc_metrics = await self._collect_system_metrics(
            self.rpc_monitor, SystemType.RPC
        )
        pubsub_metrics = await self._collect_system_metrics(
            self.pubsub_monitor, SystemType.PUBSUB
        )
        queue_metrics = await self._collect_system_metrics(
            self.queue_monitor, SystemType.QUEUE
        )
        cache_metrics = await self._collect_system_metrics(
            self.cache_monitor, SystemType.CACHE
        )
        federation_metrics = await self._collect_system_metrics(
            self.federation_monitor, SystemType.FEDERATION
        )
        transport_metrics = await self._collect_system_metrics(
            self.transport_monitor, SystemType.TRANSPORT
        )

        # Collect transport metrics (from enhanced transport layer)
        transport_health_snapshots = await self._collect_transport_health_snapshots()
        transport_correlation_results = (
            await self._collect_transport_correlation_results()
        )

        # Calculate correlation metrics
        correlation_metrics = self._calculate_correlation_metrics(
            transport_health_snapshots
        )

        # Calculate overall system health
        all_system_metrics = [
            rpc_metrics,
            pubsub_metrics,
            queue_metrics,
            cache_metrics,
            federation_metrics,
            transport_metrics,
        ]
        overall_health_score, overall_health_status = self._calculate_overall_health(
            all_system_metrics
        )

        # Count system health statuses
        health_counts = self._count_system_health_statuses(all_system_metrics)

        # Calculate performance summary
        total_rps = sum(
            m.requests_per_second for m in all_system_metrics if m is not None
        )

        avg_latencies = [
            m.average_latency_ms for m in all_system_metrics if m is not None
        ]
        average_cross_system_latency = (
            sum(avg_latencies) / len(avg_latencies) if avg_latencies else 0.0
        )

        collection_duration = (time.time() - collection_start) * 1000.0

        return UnifiedSystemMetrics(
            rpc_metrics=rpc_metrics,
            pubsub_metrics=pubsub_metrics,
            queue_metrics=queue_metrics,
            cache_metrics=cache_metrics,
            federation_metrics=federation_metrics,
            transport_health_snapshots=transport_health_snapshots,
            transport_correlation_results=transport_correlation_results,
            correlation_metrics=correlation_metrics,
            overall_health_score=overall_health_score,
            overall_health_status=overall_health_status,
            systems_healthy=health_counts["healthy"],
            systems_degraded=health_counts["degraded"],
            systems_critical=health_counts["critical"],
            systems_unavailable=health_counts["unavailable"],
            total_requests_per_second=total_rps,
            average_cross_system_latency_ms=average_cross_system_latency,
            total_active_correlations=len(self.active_correlations),
            collection_timestamp=time.time(),
            collection_duration_ms=collection_duration,
        )

    def get_correlation_timeline(
        self, correlation_id: CorrelationId
    ) -> list[CrossSystemEvent]:
        """Get complete event timeline for a correlation ID."""
        return self.correlation_chains.get(correlation_id, [])

    def get_tracking_timeline(self, tracking_id: TrackingId) -> list[CrossSystemEvent]:
        """Get complete event timeline for a ULID tracking ID (end-to-end tracing)."""
        return self.tracking_id_to_events.get(tracking_id, [])

    def get_active_tracking_ids(self) -> dict[TrackingId, float]:
        """Get all currently active tracking IDs and their start times."""
        return dict(self.active_tracking_ids)

    def get_recent_events(
        self,
        system_type: SystemType | None = None,
        event_type: EventType | None = None,
        limit: int = 100,
    ) -> list[CrossSystemEvent]:
        """Get recent events with optional filtering."""
        events = list(self.event_history)

        if system_type:
            events = [
                e
                for e in events
                if e.source_system == system_type or e.target_system == system_type
            ]

        if event_type:
            events = [e for e in events if e.event_type == event_type]

        return events[-limit:] if len(events) > limit else events

    def get_cross_system_performance_summary(
        self,
    ) -> dict[tuple[SystemType, SystemType], dict[str, float]]:
        """Get performance summary for cross-system interactions."""
        summary = {}

        for (source, target), latencies in self.correlation_performance.items():
            if latencies:
                sorted_latencies = sorted(latencies)
                n = len(sorted_latencies)
                summary[(source, target)] = {
                    "average_latency_ms": sum(latencies) / n,
                    "p50_latency_ms": sorted_latencies[n // 2],
                    "p95_latency_ms": sorted_latencies[int(n * 0.95)],
                    "p99_latency_ms": sorted_latencies[int(n * 0.99)],
                    "total_interactions": n,
                }

        return summary

    async def _collect_system_metrics(
        self, monitor: MonitoringSystemProtocol | None, system_type: SystemType
    ) -> SystemPerformanceMetrics | None:
        """Collect metrics from a system monitor."""
        if monitor is None:
            return None

        try:
            return await monitor.get_system_metrics()
        except Exception as e:
            logger.warning(f"Failed to collect {system_type.value} metrics: {e}")
            return None

    async def _collect_transport_health_snapshots(
        self,
    ) -> dict[EndpointUrl, TransportHealthSnapshot]:
        """Collect transport health snapshots from enhanced transport layer."""
        # This would integrate with the enhanced transport monitoring
        # For now, return empty dict as placeholder
        return {}

    async def _collect_transport_correlation_results(self) -> list[CorrelationResult]:
        """Collect transport correlation results from enhanced transport layer."""
        # This would integrate with the enhanced transport correlation tracking
        # For now, return empty list as placeholder
        return []

    def _calculate_correlation_metrics(
        self, transport_health: dict[EndpointUrl, TransportHealthSnapshot]
    ) -> CorrelationMetrics:
        """Calculate cross-system correlation metrics."""
        # Count cross-system event types
        rpc_to_pubsub = sum(
            1
            for events in self.correlation_chains.values()
            for event in events
            if event.source_system == SystemType.RPC
            and event.target_system == SystemType.PUBSUB
        )

        rpc_to_queue = sum(
            1
            for events in self.correlation_chains.values()
            for event in events
            if event.source_system == SystemType.RPC
            and event.target_system == SystemType.QUEUE
        )

        rpc_to_cache = sum(
            1
            for events in self.correlation_chains.values()
            for event in events
            if event.source_system == SystemType.RPC
            and event.target_system == SystemType.CACHE
        )

        pubsub_to_queue = sum(
            1
            for events in self.correlation_chains.values()
            for event in events
            if event.source_system == SystemType.PUBSUB
            and event.target_system == SystemType.QUEUE
        )

        pubsub_to_cache = sum(
            1
            for events in self.correlation_chains.values()
            for event in events
            if event.source_system == SystemType.PUBSUB
            and event.target_system == SystemType.CACHE
        )

        queue_to_cache = sum(
            1
            for events in self.correlation_chains.values()
            for event in events
            if event.source_system == SystemType.QUEUE
            and event.target_system == SystemType.CACHE
        )

        # Calculate federation metrics
        federation_events = [
            event
            for event in self.event_history
            if event.source_system == SystemType.FEDERATION
            or event.target_system == SystemType.FEDERATION
        ]

        federation_latencies = [
            event.latency_ms for event in federation_events if event.latency_ms > 0
        ]
        federation_hops = [
            event.metadata.get("hop_count", 1)
            for event in federation_events
            if "hop_count" in event.metadata
        ]

        federation_success_rate = (
            sum(1 for event in federation_events if event.success)
            / len(federation_events)
            * 100.0
            if federation_events
            else 100.0
        )

        # Transport metrics from enhanced transport layer
        transport_health_scores = {
            endpoint: snapshot.overall_health_score
            for endpoint, snapshot in transport_health.items()
        }

        circuit_breaker_events = sum(
            1 for event in self.event_history if "circuit_breaker" in event.metadata
        )

        # End-to-end metrics
        all_latencies = [
            event.latency_ms for event in self.event_history if event.latency_ms > 0
        ]
        sorted_latencies = sorted(all_latencies) if all_latencies else [0.0]

        end_to_end_p95 = (
            sorted_latencies[int(len(sorted_latencies) * 0.95)]
            if sorted_latencies
            else 0.0
        )

        total_correlations = len(self.correlation_chains)
        successful_correlations = sum(
            1
            for events in self.correlation_chains.values()
            if all(event.success for event in events)
        )
        correlation_success_rate = (
            successful_correlations / total_correlations * 100.0
            if total_correlations > 0
            else 100.0
        )

        return CorrelationMetrics(
            rpc_to_pubsub_events=rpc_to_pubsub,
            rpc_to_queue_events=rpc_to_queue,
            rpc_to_cache_events=rpc_to_cache,
            pubsub_to_queue_events=pubsub_to_queue,
            pubsub_to_cache_events=pubsub_to_cache,
            queue_to_cache_events=queue_to_cache,
            federation_message_hops=federation_hops,
            cross_cluster_latency_p95_ms=sorted(federation_latencies)[
                int(len(federation_latencies) * 0.95)
            ]
            if federation_latencies
            else 0.0,
            federation_success_rate_percent=federation_success_rate,
            transport_health_scores=transport_health_scores,
            circuit_breaker_events=circuit_breaker_events,
            correlation_tracking_success_rate=correlation_success_rate,
            end_to_end_latency_p95_ms=end_to_end_p95,
            total_cross_system_correlations=total_correlations,
            correlation_success_rate_percent=correlation_success_rate,
        )

    def _calculate_overall_health(
        self, system_metrics: list[SystemPerformanceMetrics | None]
    ) -> tuple[SystemHealthScore, HealthStatus]:
        """Calculate overall system health score and status."""
        valid_metrics = [m for m in system_metrics if m is not None]

        if not valid_metrics:
            return 0.0, HealthStatus.UNAVAILABLE

        # Calculate health based on error rates and latency
        health_scores = []

        for metrics in valid_metrics:
            # Health decreases with higher error rates and latency
            error_penalty = min(metrics.error_rate_percent / 100.0, 1.0)  # Cap at 100%
            latency_penalty = min(
                metrics.average_latency_ms / self.config.latency_critical_threshold_ms,
                1.0,
            )

            # Simple health calculation: 1.0 - penalties
            health = max(0.0, 1.0 - (error_penalty * 0.6) - (latency_penalty * 0.4))
            health_scores.append(health)

        overall_score = sum(health_scores) / len(health_scores)

        # Determine status based on thresholds
        if overall_score >= self.config.health_score_degraded_threshold:
            status = HealthStatus.HEALTHY
        elif overall_score >= self.config.health_score_critical_threshold:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.CRITICAL

        return overall_score, status

    def _count_system_health_statuses(
        self, system_metrics: list[SystemPerformanceMetrics | None]
    ) -> dict[str, int]:
        """Count systems by health status."""
        counts = {"healthy": 0, "degraded": 0, "critical": 0, "unavailable": 0}

        for metrics in system_metrics:
            if metrics is None:
                counts["unavailable"] += 1
                continue

            # Simple health assessment based on error rate
            if (
                metrics.error_rate_percent
                <= self.config.error_rate_warning_threshold * 100
            ):
                counts["healthy"] += 1
            elif (
                metrics.error_rate_percent
                <= self.config.error_rate_critical_threshold * 100
            ):
                counts["degraded"] += 1
            else:
                counts["critical"] += 1

        return counts

    async def _metrics_collection_task(self) -> None:
        """Background task for periodic metrics collection."""
        try:
            while self._running:
                await asyncio.sleep(self.config.metrics_collection_interval_ms / 1000.0)

                if not self._running:
                    break

                try:
                    metrics = await self.get_unified_metrics()
                    self.system_metrics_history.append(metrics)

                    # Log performance alerts
                    if metrics.overall_health_status != HealthStatus.HEALTHY:
                        logger.warning(
                            f"System health degraded: {metrics.overall_health_status.value} "
                            f"(score: {metrics.overall_health_score:.3f})"
                        )

                except Exception as e:
                    logger.error(f"Error collecting unified metrics: {e}")

        except asyncio.CancelledError:
            logger.info("Metrics collection task cancelled")
        except Exception as e:
            logger.error(f"Metrics collection task error: {e}")

    async def _correlation_cleanup_task(self) -> None:
        """Background task for correlation cleanup."""
        try:
            while self._running:
                await asyncio.sleep(
                    self.config.correlation_cleanup_interval_ms / 1000.0
                )

                if not self._running:
                    break

                try:
                    current_time = time.time()
                    timeout_threshold = current_time - (
                        self.config.correlation_cleanup_interval_ms / 1000.0 * 5
                    )  # 5x cleanup interval

                    # Clean up old active correlations
                    expired_correlations = [
                        correlation_id
                        for correlation_id, event in self.active_correlations.items()
                        if event.timestamp < timeout_threshold
                    ]

                    for correlation_id in expired_correlations:
                        del self.active_correlations[correlation_id]

                    # Clean up old active tracking IDs
                    expired_tracking_ids = [
                        tracking_id
                        for tracking_id, start_time in self.active_tracking_ids.items()
                        if start_time < timeout_threshold
                    ]

                    for tracking_id in expired_tracking_ids:
                        del self.active_tracking_ids[tracking_id]

                    total_expired = len(expired_correlations) + len(
                        expired_tracking_ids
                    )
                    if total_expired > 0:
                        logger.debug(
                            f"Cleaned up {len(expired_correlations)} expired correlations and {len(expired_tracking_ids)} expired tracking IDs"
                        )

                except Exception as e:
                    logger.error(f"Error during correlation cleanup: {e}")

        except asyncio.CancelledError:
            logger.info("Correlation cleanup task cancelled")
        except Exception as e:
            logger.error(f"Correlation cleanup task error: {e}")

    async def _health_monitoring_task(self) -> None:
        """Background task for system health monitoring."""
        try:
            while self._running:
                await asyncio.sleep(self.config.health_check_interval_ms / 1000.0)

                if not self._running:
                    break

                try:
                    # Record health check events for all monitored systems
                    current_time = time.time()

                    for system_type, monitor in [
                        (SystemType.RPC, self.rpc_monitor),
                        (SystemType.PUBSUB, self.pubsub_monitor),
                        (SystemType.QUEUE, self.queue_monitor),
                        (SystemType.CACHE, self.cache_monitor),
                        (SystemType.FEDERATION, self.federation_monitor),
                        (SystemType.TRANSPORT, self.transport_monitor),
                    ]:
                        if monitor is not None:
                            try:
                                (
                                    health_score,
                                    health_status,
                                ) = await monitor.get_health_status()

                                # Generate system-specific tracking ID for health checks
                                health_tracking_id = self.generate_system_tracking_id(
                                    system_type
                                )

                                await self.record_cross_system_event(
                                    correlation_id=f"health_{system_type.value}_{int(current_time)}",
                                    event_type=EventType.HEALTH_CHECK,
                                    source_system=system_type,
                                    success=health_status
                                    in [HealthStatus.HEALTHY, HealthStatus.DEGRADED],
                                    metadata={
                                        "health_score": health_score,
                                        "health_status": health_status.value,
                                    },
                                    tracking_id=health_tracking_id,
                                )

                            except Exception as e:
                                logger.warning(
                                    f"Health check failed for {system_type.value}: {e}"
                                )

                except Exception as e:
                    logger.error(f"Error during health monitoring: {e}")

        except asyncio.CancelledError:
            logger.info("Health monitoring task cancelled")
        except Exception as e:
            logger.error(f"Health monitoring task error: {e}")

    async def __aenter__(self) -> UnifiedSystemMonitor:
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.stop()


# Factory function for creating unified system monitors
def create_unified_system_monitor(
    metrics_collection_interval_ms: float = 5000.0,
    correlation_cleanup_interval_ms: float = 60000.0,
    health_check_interval_ms: float = 10000.0,
    max_event_history: int = 50000,
    max_correlation_history: int = 100000,
    enable_transport_monitoring: bool = True,
    enable_federation_monitoring: bool = True,
    enable_cache_monitoring: bool = True,
    enable_cross_system_correlation: bool = True,
) -> UnifiedSystemMonitor:
    """Create a unified system monitor with specified configuration."""
    config = MonitoringConfig(
        metrics_collection_interval_ms=metrics_collection_interval_ms,
        correlation_cleanup_interval_ms=correlation_cleanup_interval_ms,
        health_check_interval_ms=health_check_interval_ms,
        max_event_history=max_event_history,
        max_correlation_history=max_correlation_history,
        enable_transport_monitoring=enable_transport_monitoring,
        enable_federation_monitoring=enable_federation_monitoring,
        enable_cache_monitoring=enable_cache_monitoring,
        enable_cross_system_correlation=enable_cross_system_correlation,
    )

    return UnifiedSystemMonitor(config=config)
