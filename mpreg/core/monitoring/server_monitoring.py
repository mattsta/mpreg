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

# Latency histogram bucket upper bounds (ms) for Prometheus-style export.
_LATENCY_BUCKETS_MS: tuple[float, ...] = (
    1.0,
    5.0,
    10.0,
    25.0,
    50.0,
    100.0,
    250.0,
    500.0,
    1000.0,
    2500.0,
    5000.0,
    10000.0,
)

@dataclass(slots=True)
class ServerMetricsTracker:
    """Tracks server-side RPC and PubSub performance metrics."""

    started_at: float = field(default_factory=time.time)

    rpc_total: int = 0
    rpc_errors: int = 0
    rpc_events: deque[float] = field(default_factory=deque)
    rpc_latencies_ms: deque[float] = field(default_factory=lambda: deque(maxlen=1000))
    # Per-error-code counters (stringified MpregErrorCode / wire code).
    rpc_error_codes: dict[str, int] = field(default_factory=dict)
    mgmt_mutations: dict[str, int] = field(default_factory=dict)
    notification_drops: int = 0
    node_draining: int = 0
    rpc_latency_buckets: list[int] = field(
        default_factory=lambda: [0] * (len(_LATENCY_BUCKETS_MS) + 1)
    )
    rpc_latency_sum_ms: float = 0.0

    pubsub_total: int = 0
    pubsub_errors: int = 0
    pubsub_events: deque[float] = field(default_factory=deque)
    pubsub_latencies_ms: deque[float] = field(
        default_factory=lambda: deque(maxlen=1000)
    )
    pubsub_subscriptions: int = 0
    pubsub_unsubscriptions: int = 0
    pubsub_notifications: int = 0
    pubsub_latency_buckets: list[int] = field(
        default_factory=lambda: [0] * (len(_LATENCY_BUCKETS_MS) + 1)
    )
    pubsub_latency_sum_ms: float = 0.0

    def record_rpc(
        self,
        latency_ms: float,
        success: bool,
        *,
        error_code: str | int | None = None,
    ) -> None:
        now = time.time()
        self.rpc_total += 1
        if not success:
            self.rpc_errors += 1
            code_key = str(error_code) if error_code is not None else "unknown"
            self.rpc_error_codes[code_key] = self.rpc_error_codes.get(code_key, 0) + 1
        self.rpc_events.append(now)
        _prune_events(self.rpc_events, now, _HOUR_WINDOW_SECONDS)
        self.rpc_latencies_ms.append(latency_ms)
        self._observe_latency(self.rpc_latency_buckets, latency_ms)
        self.rpc_latency_sum_ms += float(latency_ms)

    def record_pubsub(self, latency_ms: float, success: bool) -> None:
        now = time.time()
        self.pubsub_total += 1
        if not success:
            self.pubsub_errors += 1
        self.pubsub_events.append(now)
        _prune_events(self.pubsub_events, now, _HOUR_WINDOW_SECONDS)
        self.pubsub_latencies_ms.append(latency_ms)
        self._observe_latency(self.pubsub_latency_buckets, latency_ms)
        self.pubsub_latency_sum_ms += float(latency_ms)

    @staticmethod
    def _observe_latency(buckets: list[int], latency_ms: float) -> None:
        placed = False
        for idx, bound in enumerate(_LATENCY_BUCKETS_MS):
            if latency_ms <= bound:
                buckets[idx] += 1
                placed = True
                break
        if not placed:
            buckets[-1] += 1  # +Inf

    def record_mgmt_mutation(self, event: str, *, success: bool = True) -> None:
        key = f"{event}:{'ok' if success else 'err'}"
        self.mgmt_mutations[key] = self.mgmt_mutations.get(key, 0) + 1

    def set_draining(self, draining: bool) -> None:
        self.node_draining = 1 if draining else 0

    def set_notification_drops(self, count: int) -> None:
        self.notification_drops = max(0, int(count))

    def prometheus_lines(self, labels: str) -> list[str]:
        """Emit process-local RPC/pubsub counters and latency histograms."""
        lines: list[str] = []
        lines.append("# HELP mpreg_rpc_requests_total Total RPC requests handled.")
        lines.append("# TYPE mpreg_rpc_requests_total counter")
        lines.append(f"mpreg_rpc_requests_total{{{labels}}} {self.rpc_total}")
        lines.append("# HELP mpreg_rpc_errors_total Total failed RPC requests.")
        lines.append("# TYPE mpreg_rpc_errors_total counter")
        lines.append(f"mpreg_rpc_errors_total{{{labels}}} {self.rpc_errors}")
        lines.append("# HELP mpreg_node_draining 1 if node is draining traffic.")
        lines.append("# TYPE mpreg_node_draining gauge")
        lines.append(f"mpreg_node_draining{{{labels}}} {int(self.node_draining)}")
        lines.append("# HELP mpreg_mgmt_mutations_total Management mutations by event.")
        lines.append("# TYPE mpreg_mgmt_mutations_total counter")
        for key, count in sorted(self.mgmt_mutations.items()):
            safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in key)
            lines.append(
                f'mpreg_mgmt_mutations_total{{{labels},event="{safe}"}} {count}'
            )
        lines.append(
            "# HELP mpreg_client_notification_drops_total "
            "Pubsub notifications dropped (server-observed mirror when set)."
        )
        lines.append("# TYPE mpreg_client_notification_drops_total counter")
        lines.append(
            f"mpreg_client_notification_drops_total{{{labels}}} {self.notification_drops}"
        )
        lines.append(
            "# HELP mpreg_rpc_errors_by_code_total RPC failures labeled by error code."
        )
        lines.append("# TYPE mpreg_rpc_errors_by_code_total counter")
        for code, count in sorted(self.rpc_error_codes.items()):
            safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in code)
            lines.append(
                f'mpreg_rpc_errors_by_code_total{{{labels},code="{safe}"}} {count}'
            )
        lines.extend(
            self._histogram_lines(
                "mpreg_rpc_latency_ms",
                labels,
                self.rpc_latency_buckets,
                self.rpc_latency_sum_ms,
                self.rpc_total,
            )
        )
        lines.append("# HELP mpreg_pubsub_requests_total Total pubsub ops handled.")
        lines.append("# TYPE mpreg_pubsub_requests_total counter")
        lines.append(f"mpreg_pubsub_requests_total{{{labels}}} {self.pubsub_total}")
        lines.append("# HELP mpreg_pubsub_errors_total Total failed pubsub ops.")
        lines.append("# TYPE mpreg_pubsub_errors_total counter")
        lines.append(f"mpreg_pubsub_errors_total{{{labels}}} {self.pubsub_errors}")
        lines.extend(
            self._histogram_lines(
                "mpreg_pubsub_latency_ms",
                labels,
                self.pubsub_latency_buckets,
                self.pubsub_latency_sum_ms,
                self.pubsub_total,
            )
        )
        return lines

    @staticmethod
    def _histogram_lines(
        name: str,
        labels: str,
        buckets: list[int],
        sum_ms: float,
        count: int,
    ) -> list[str]:
        lines = [
            f"# HELP {name} Request latency histogram in milliseconds.",
            f"# TYPE {name} histogram",
        ]
        cumulative = 0
        for idx, bound in enumerate(_LATENCY_BUCKETS_MS):
            cumulative += buckets[idx]
            # le label uses plain number; +Inf last
            lines.append(
                f'{name}_bucket{{{labels},le="{bound:g}"}} {cumulative}'
            )
        cumulative += buckets[-1]
        lines.append(f'{name}_bucket{{{labels},le="+Inf"}} {cumulative}')
        lines.append(f"{name}_sum{{{labels}}} {sum_ms:.6f}")
        lines.append(f"{name}_count{{{labels}}} {count}")
        return lines

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
