from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field

from .unified_monitoring import (
    HealthScore,
    HealthStatus,
    SystemPerformanceMetrics,
    SystemType,
)

_RPS_WINDOW_SECONDS = 60.0
_HOUR_WINDOW_SECONDS = 3600.0


def _calculate_percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = int(round((percentile / 100.0) * (len(sorted_values) - 1)))
    index = max(0, min(index, len(sorted_values) - 1))
    return float(sorted_values[index])


def _prune_events(events: deque[float], now: float, window_seconds: float) -> None:
    while events and (now - events[0]) > window_seconds:
        events.popleft()


@dataclass(slots=True)
class ServerMetricsTracker:
    """Tracks server-side RPC and PubSub performance metrics."""

    started_at: float = field(default_factory=time.time)

    rpc_total: int = 0
    rpc_errors: int = 0
    rpc_events: deque[float] = field(default_factory=deque)
    rpc_latencies_ms: deque[float] = field(default_factory=lambda: deque(maxlen=1000))

    pubsub_total: int = 0
    pubsub_errors: int = 0
    pubsub_events: deque[float] = field(default_factory=deque)
    pubsub_latencies_ms: deque[float] = field(
        default_factory=lambda: deque(maxlen=1000)
    )
    pubsub_subscriptions: int = 0
    pubsub_unsubscriptions: int = 0
    pubsub_notifications: int = 0

    def record_rpc(self, latency_ms: float, success: bool) -> None:
        now = time.time()
        self.rpc_total += 1
        if not success:
            self.rpc_errors += 1
        self.rpc_events.append(now)
        _prune_events(self.rpc_events, now, _HOUR_WINDOW_SECONDS)
        self.rpc_latencies_ms.append(latency_ms)

    def record_pubsub(self, latency_ms: float, success: bool) -> None:
        now = time.time()
        self.pubsub_total += 1
        if not success:
            self.pubsub_errors += 1
        self.pubsub_events.append(now)
        _prune_events(self.pubsub_events, now, _HOUR_WINDOW_SECONDS)
        self.pubsub_latencies_ms.append(latency_ms)

    def record_pubsub_subscription(self) -> None:
        self.pubsub_subscriptions += 1

    def record_pubsub_unsubscription(self) -> None:
        self.pubsub_unsubscriptions += 1

    def record_pubsub_notification(self) -> None:
        self.pubsub_notifications += 1

    def rpc_metrics(
        self, system_name: str, active_connections: int
    ) -> SystemPerformanceMetrics:
        now = time.time()
        _prune_events(self.rpc_events, now, _HOUR_WINDOW_SECONDS)
        recent_rps = sum(1 for ts in self.rpc_events if now - ts <= _RPS_WINDOW_SECONDS)
        rps = recent_rps / _RPS_WINDOW_SECONDS if self.rpc_events else 0.0
        latencies = list(self.rpc_latencies_ms)
        average_latency = sum(latencies) / len(latencies) if latencies else 0.0
        error_rate = (
            (self.rpc_errors / self.rpc_total) * 100.0 if self.rpc_total else 0.0
        )
        return SystemPerformanceMetrics(
            system_type=SystemType.RPC,
            system_name=system_name,
            requests_per_second=rps,
            average_latency_ms=average_latency,
            p95_latency_ms=_calculate_percentile(latencies, 95.0),
            p99_latency_ms=_calculate_percentile(latencies, 99.0),
            error_rate_percent=error_rate,
            active_connections=active_connections,
            total_operations_last_hour=len(self.rpc_events),
            last_updated=now,
        )

    def pubsub_metrics(
        self, system_name: str, active_connections: int
    ) -> SystemPerformanceMetrics:
        now = time.time()
        _prune_events(self.pubsub_events, now, _HOUR_WINDOW_SECONDS)
        recent_rps = sum(
            1 for ts in self.pubsub_events if now - ts <= _RPS_WINDOW_SECONDS
        )
        rps = recent_rps / _RPS_WINDOW_SECONDS if self.pubsub_events else 0.0
        latencies = list(self.pubsub_latencies_ms)
        average_latency = sum(latencies) / len(latencies) if latencies else 0.0
        error_rate = (
            (self.pubsub_errors / self.pubsub_total) * 100.0
            if self.pubsub_total
            else 0.0
        )
        return SystemPerformanceMetrics(
            system_type=SystemType.PUBSUB,
            system_name=system_name,
            requests_per_second=rps,
            average_latency_ms=average_latency,
            p95_latency_ms=_calculate_percentile(latencies, 95.0),
            p99_latency_ms=_calculate_percentile(latencies, 99.0),
            error_rate_percent=error_rate,
            active_connections=active_connections,
            total_operations_last_hour=len(self.pubsub_events),
            last_updated=now,
        )


@dataclass(slots=True)
class ServerSystemMonitor:
    """Monitoring adapter for server-level RPC or PubSub metrics."""

    system_type: SystemType
    system_name: str
    tracker: ServerMetricsTracker
    active_connections_provider: Callable[[], int]

    async def get_system_metrics(self) -> SystemPerformanceMetrics:
        if self.system_type == SystemType.RPC:
            return self.tracker.rpc_metrics(
                self.system_name, self.active_connections_provider()
            )
        if self.system_type == SystemType.PUBSUB:
            return self.tracker.pubsub_metrics(
                self.system_name, self.active_connections_provider()
            )
        raise ValueError(
            f"Unsupported system type for server monitor: {self.system_type}"
        )

    async def get_health_status(self) -> tuple[HealthScore, HealthStatus]:
        metrics = await self.get_system_metrics()
        error_penalty = min(metrics.error_rate_percent / 100.0, 1.0)
        latency_penalty = min(metrics.average_latency_ms / 1000.0, 1.0)
        health_score = max(0.0, 1.0 - (0.6 * error_penalty) - (0.4 * latency_penalty))

        if health_score >= 0.8:
            status = HealthStatus.HEALTHY
        elif health_score >= 0.6:
            status = HealthStatus.DEGRADED
        elif health_score > 0.0:
            status = HealthStatus.CRITICAL
        else:
            status = HealthStatus.UNAVAILABLE

        return health_score, status
