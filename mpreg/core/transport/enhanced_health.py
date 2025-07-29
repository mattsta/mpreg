"""
Enhanced health scoring and monitoring for MPREG transport connections.

This module provides enhanced health monitoring capabilities that can be
integrated with MPREG's existing MultiProtocolAdapter to add sophisticated
health scoring, connection analytics, and performance monitoring.

Key features:
- 0.0 to 1.0 health scoring for connections
- Connection performance analytics
- Health trend analysis
- Integration with existing transport statistics
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

from .circuit_breaker import CircuitBreakerState

# Type aliases for health monitoring semantics
type HealthScore = float  # 0.0 (unhealthy) to 1.0 (perfect health)
type EndpointUrl = str
type ConnectionId = str
type TransportLatencyMs = float


class HealthTrend(Enum):
    """Health trend indicators."""

    IMPROVING = "improving"
    STABLE = "stable"
    DEGRADING = "degrading"
    CRITICAL = "critical"


@dataclass(frozen=True, slots=True)
class ConnectionHealthMetrics:
    """Comprehensive health metrics for a connection."""

    connection_id: ConnectionId
    endpoint: EndpointUrl
    health_score: HealthScore
    success_rate_percent: float
    average_latency_ms: TransportLatencyMs
    total_operations: int
    recent_failures: int
    last_successful_operation: float | None
    last_failed_operation: float | None
    health_trend: HealthTrend
    circuit_breaker_state: CircuitBreakerState


@dataclass(frozen=True, slots=True)
class TransportHealthSnapshot:
    """Overall health snapshot for transport infrastructure."""

    endpoint: EndpointUrl
    overall_health_score: HealthScore
    active_connections: int
    total_connections: int
    healthy_connections: int
    degraded_connections: int
    failed_connections: int
    average_response_time_ms: TransportLatencyMs
    total_operations_last_hour: int
    error_rate_percent: float
    last_health_check: float


class HealthMonitorProtocol(Protocol):
    """Protocol for health monitoring implementations."""

    def calculate_health_score(self) -> HealthScore:
        """Calculate current health score."""
        ...

    def record_operation(self, latency_ms: float, success: bool) -> None:
        """Record an operation result."""
        ...

    def get_health_metrics(self) -> ConnectionHealthMetrics:
        """Get comprehensive health metrics."""
        ...


@dataclass(slots=True)
class ConnectionHealthMonitor:
    """Health monitor for individual transport connections."""

    connection_id: ConnectionId
    endpoint: EndpointUrl

    # Operation tracking
    total_operations: int = 0
    successful_operations: int = 0
    total_latency_ms: float = 0.0

    # Recent performance tracking (sliding window)
    recent_successes: list[float] = field(default_factory=list)
    recent_failures: list[float] = field(default_factory=list)
    recent_latencies: list[float] = field(default_factory=list)

    # Health analysis
    last_health_calculation: float = 0.0
    cached_health_score: HealthScore = 1.0

    # Configuration
    max_recent_operations: int = 100
    health_calculation_interval_ms: float = 5000.0  # 5 seconds

    def record_operation(self, latency_ms: float, success: bool) -> None:
        """Record an operation and update health metrics."""
        current_time = time.time()

        self.total_operations += 1
        self.total_latency_ms += latency_ms

        # Add to recent tracking with sliding window
        self.recent_latencies.append(latency_ms)
        if len(self.recent_latencies) > self.max_recent_operations:
            self.recent_latencies.pop(0)

        if success:
            self.successful_operations += 1
            self.recent_successes.append(current_time)
            if len(self.recent_successes) > self.max_recent_operations:
                self.recent_successes.pop(0)
        else:
            self.recent_failures.append(current_time)
            if len(self.recent_failures) > self.max_recent_operations:
                self.recent_failures.pop(0)

    def calculate_health_score(self) -> HealthScore:
        """Calculate comprehensive health score (0.0 to 1.0)."""
        current_time = time.time()

        # Use cached score if recently calculated
        if (
            current_time - self.last_health_calculation
        ) * 1000 < self.health_calculation_interval_ms:
            return self.cached_health_score

        if self.total_operations == 0:
            self.cached_health_score = 1.0
            self.last_health_calculation = current_time
            return self.cached_health_score

        # Calculate success rate component (40% of score)
        success_rate = self.successful_operations / self.total_operations
        success_component = success_rate * 0.4

        # Calculate latency component (30% of score)
        if self.recent_latencies:
            avg_latency = sum(self.recent_latencies) / len(self.recent_latencies)
            # Score latency: excellent < 10ms, good < 50ms, acceptable < 200ms
            if avg_latency < 10.0:
                latency_component = 0.3
            elif avg_latency < 50.0:
                latency_component = 0.25
            elif avg_latency < 200.0:
                latency_component = 0.15
            else:
                latency_component = 0.05
        else:
            latency_component = 0.3

        # Calculate recent trend component (20% of score)
        recent_success_rate = self._calculate_recent_success_rate()
        trend_component = recent_success_rate * 0.2

        # Calculate stability component (10% of score)
        stability_component = self._calculate_stability_score() * 0.1

        # Combine components
        health_score = (
            success_component
            + latency_component
            + trend_component
            + stability_component
        )

        # Ensure score is within bounds
        self.cached_health_score = max(0.0, min(1.0, health_score))
        self.last_health_calculation = current_time

        return self.cached_health_score

    def _calculate_recent_success_rate(self) -> float:
        """Calculate success rate for recent operations."""
        current_time = time.time()
        recent_window_seconds = 300.0  # 5 minutes

        recent_successes = [
            t
            for t in self.recent_successes
            if current_time - t <= recent_window_seconds
        ]
        recent_failures = [
            t for t in self.recent_failures if current_time - t <= recent_window_seconds
        ]

        total_recent = len(recent_successes) + len(recent_failures)
        if total_recent == 0:
            return 1.0

        return len(recent_successes) / total_recent

    def _calculate_stability_score(self) -> float:
        """Calculate stability score based on operation consistency."""
        if len(self.recent_latencies) < 10:
            return 1.0

        # Calculate coefficient of variation for latency
        avg_latency = sum(self.recent_latencies) / len(self.recent_latencies)
        if avg_latency == 0:
            return 1.0

        variance = sum((l - avg_latency) ** 2 for l in self.recent_latencies) / len(
            self.recent_latencies
        )
        std_dev = variance**0.5
        cv = std_dev / avg_latency

        # Lower coefficient of variation = higher stability
        # CV < 0.1 = excellent stability, CV > 1.0 = poor stability
        if cv < 0.1:
            return 1.0
        elif cv > 1.0:
            return 0.0
        else:
            return 1.0 - cv

    def get_health_trend(self) -> HealthTrend:
        """Analyze health trend over time."""
        if len(self.recent_failures) == 0:
            return (
                HealthTrend.STABLE
                if self.cached_health_score > 0.8
                else HealthTrend.IMPROVING
            )

        current_time = time.time()
        recent_window = 60.0  # 1 minute
        very_recent_window = 15.0  # 15 seconds

        recent_failures = len(
            [t for t in self.recent_failures if current_time - t <= recent_window]
        )
        very_recent_failures = len(
            [t for t in self.recent_failures if current_time - t <= very_recent_window]
        )

        if very_recent_failures > 3:
            return HealthTrend.CRITICAL
        elif recent_failures > very_recent_failures * 2:
            return HealthTrend.IMPROVING
        elif very_recent_failures > 0:
            return HealthTrend.DEGRADING
        else:
            return HealthTrend.STABLE

    def get_health_metrics(self) -> ConnectionHealthMetrics:
        """Get comprehensive health metrics."""
        health_score = self.calculate_health_score()
        success_rate = (
            (self.successful_operations / self.total_operations * 100.0)
            if self.total_operations > 0
            else 100.0
        )
        avg_latency = (
            (self.total_latency_ms / self.total_operations)
            if self.total_operations > 0
            else 0.0
        )

        last_success = self.recent_successes[-1] if self.recent_successes else None
        last_failure = self.recent_failures[-1] if self.recent_failures else None

        return ConnectionHealthMetrics(
            connection_id=self.connection_id,
            endpoint=self.endpoint,
            health_score=health_score,
            success_rate_percent=success_rate,
            average_latency_ms=avg_latency,
            total_operations=self.total_operations,
            recent_failures=len(self.recent_failures),
            last_successful_operation=last_success,
            last_failed_operation=last_failure,
            health_trend=self.get_health_trend(),
            circuit_breaker_state=CircuitBreakerState.CLOSED,  # Would be provided by circuit breaker
        )


@dataclass(slots=True)
class TransportHealthAggregator:
    """Aggregates health metrics across multiple connections."""

    endpoint: EndpointUrl
    connection_monitors: dict[ConnectionId, ConnectionHealthMonitor] = field(
        default_factory=dict
    )

    def add_connection_monitor(self, monitor: ConnectionHealthMonitor) -> None:
        """Add a connection monitor to track."""
        self.connection_monitors[monitor.connection_id] = monitor

    def remove_connection_monitor(self, connection_id: ConnectionId) -> None:
        """Remove a connection monitor."""
        self.connection_monitors.pop(connection_id, None)

    def get_transport_health_snapshot(self) -> TransportHealthSnapshot:
        """Get aggregated health snapshot for this transport endpoint."""
        if not self.connection_monitors:
            return TransportHealthSnapshot(
                endpoint=self.endpoint,
                overall_health_score=1.0,
                active_connections=0,
                total_connections=0,
                healthy_connections=0,
                degraded_connections=0,
                failed_connections=0,
                average_response_time_ms=0.0,
                total_operations_last_hour=0,
                error_rate_percent=0.0,
                last_health_check=time.time(),
            )

        # Collect metrics from all connections
        health_scores = []
        total_operations = 0
        total_latency = 0.0
        successful_operations = 0
        healthy_count = 0
        degraded_count = 0
        failed_count = 0

        for monitor in self.connection_monitors.values():
            metrics = monitor.get_health_metrics()
            health_scores.append(metrics.health_score)
            total_operations += metrics.total_operations
            total_latency += metrics.average_latency_ms * metrics.total_operations
            successful_operations += int(
                metrics.total_operations * metrics.success_rate_percent / 100.0
            )

            if metrics.health_score >= 0.8:
                healthy_count += 1
            elif metrics.health_score >= 0.5:
                degraded_count += 1
            else:
                failed_count += 1

        # Calculate aggregated metrics
        overall_health = sum(health_scores) / len(health_scores)
        avg_latency = (
            (total_latency / total_operations) if total_operations > 0 else 0.0
        )
        error_rate = (
            ((total_operations - successful_operations) / total_operations * 100.0)
            if total_operations > 0
            else 0.0
        )

        return TransportHealthSnapshot(
            endpoint=self.endpoint,
            overall_health_score=overall_health,
            active_connections=len(self.connection_monitors),
            total_connections=len(self.connection_monitors),
            healthy_connections=healthy_count,
            degraded_connections=degraded_count,
            failed_connections=failed_count,
            average_response_time_ms=avg_latency,
            total_operations_last_hour=total_operations,  # Simplified
            error_rate_percent=error_rate,
            last_health_check=time.time(),
        )


# Factory functions for creating health monitors
def create_connection_health_monitor(
    connection_id: ConnectionId,
    endpoint: EndpointUrl,
    max_recent_operations: int = 100,
) -> ConnectionHealthMonitor:
    """Create a connection health monitor."""
    return ConnectionHealthMonitor(
        connection_id=connection_id,
        endpoint=endpoint,
        max_recent_operations=max_recent_operations,
    )


def create_transport_health_aggregator(
    endpoint: EndpointUrl,
) -> TransportHealthAggregator:
    """Create a transport health aggregator."""
    return TransportHealthAggregator(endpoint=endpoint)
