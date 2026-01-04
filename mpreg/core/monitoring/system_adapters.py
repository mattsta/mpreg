from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from .unified_monitoring import (
    HealthScore,
    HealthStatus,
    SystemPerformanceMetrics,
    SystemType,
)

_HOUR_WINDOW_SECONDS = 3600.0


def _calculate_percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = int(round((percentile / 100.0) * (len(sorted_values) - 1)))
    index = max(0, min(index, len(sorted_values) - 1))
    return float(sorted_values[index])


def _prune_samples(samples: deque[tuple[float, int]], now: float) -> None:
    while samples and (now - samples[0][0]) > _HOUR_WINDOW_SECONDS:
        samples.popleft()


def _calculate_health(
    average_latency_ms: float, error_rate_percent: float
) -> tuple[HealthScore, HealthStatus]:
    error_penalty = min(error_rate_percent / 100.0, 1.0)
    latency_penalty = min(average_latency_ms / 1000.0, 1.0)
    health_score = max(0.0, 1.0 - (0.6 * error_penalty) - (0.4 * latency_penalty))

    if health_score >= 0.8:
        return health_score, HealthStatus.HEALTHY
    if health_score >= 0.6:
        return health_score, HealthStatus.DEGRADED
    if health_score > 0.0:
        return health_score, HealthStatus.CRITICAL
    return health_score, HealthStatus.UNAVAILABLE


@dataclass(slots=True)
class CacheSystemMonitor:
    cache_manager: Any
    system_name: str
    _samples: deque[tuple[float, int]] = field(default_factory=deque)

    async def get_system_metrics(self) -> SystemPerformanceMetrics:
        stats = self.cache_manager.get_statistics()
        op_stats = stats.get("operation_stats", {})
        hits = sum(
            op_stats.get(key, 0) for key in ("l1_hits", "l2_hits", "l3_hits", "l4_hits")
        )
        misses = op_stats.get("misses", 0)
        gets = hits + misses
        total_ops = gets + op_stats.get("puts", 0) + op_stats.get("deletes", 0)
        total_ops += op_stats.get("invalidations", 0)

        now = time.time()
        self._samples.append((now, total_ops))
        _prune_samples(self._samples, now)
        first_time, first_count = self._samples[0]
        elapsed = max(now - first_time, 1.0)
        rps = (total_ops - first_count) / elapsed

        perf_stats = stats.get("performance_metrics", {})
        latencies = [
            level_stats.get("avg_time_ms", 0.0)
            for level_stats in perf_stats.values()
            if isinstance(level_stats, dict)
        ]
        average_latency = sum(latencies) / len(latencies) if latencies else 0.0
        error_rate = (misses / gets) * 100.0 if gets > 0 else 0.0

        active_connections = 0
        cache_protocol = getattr(self.cache_manager, "cache_protocol", None)
        if cache_protocol and getattr(cache_protocol, "peer_ids", None):
            active_connections = len(cache_protocol.peer_ids())

        return SystemPerformanceMetrics(
            system_type=SystemType.CACHE,
            system_name=self.system_name,
            requests_per_second=max(rps, 0.0),
            average_latency_ms=average_latency,
            p95_latency_ms=_calculate_percentile(latencies, 95.0),
            p99_latency_ms=_calculate_percentile(latencies, 99.0),
            error_rate_percent=error_rate,
            active_connections=active_connections,
            total_operations_last_hour=total_ops,
            last_updated=now,
        )

    async def get_health_status(self) -> tuple[HealthScore, HealthStatus]:
        metrics = await self.get_system_metrics()
        return _calculate_health(metrics.average_latency_ms, metrics.error_rate_percent)


@dataclass(slots=True)
class QueueSystemMonitor:
    queue_manager: Any
    system_name: str
    _samples: deque[tuple[float, int]] = field(default_factory=deque)

    async def get_system_metrics(self) -> SystemPerformanceMetrics:
        stats = self.queue_manager.get_global_statistics()
        total_sent = stats.total_messages_sent
        total_processed = (
            stats.total_messages_acknowledged + stats.total_messages_failed
        )

        now = time.time()
        self._samples.append((now, total_sent))
        _prune_samples(self._samples, now)
        first_time, first_count = self._samples[0]
        elapsed = max(now - first_time, 1.0)
        rps = (total_sent - first_count) / elapsed

        average_latency_ms = 0.0
        queue_names = self.queue_manager.list_queues()
        if queue_names:
            latencies = []
            for queue_name in queue_names:
                queue_stats = self.queue_manager.get_queue_statistics(queue_name)
                if queue_stats:
                    latencies.append(
                        queue_stats.average_processing_time_seconds * 1000.0
                    )
            if latencies:
                average_latency_ms = sum(latencies) / len(latencies)

        error_rate = (
            (stats.total_messages_failed / total_processed) * 100.0
            if total_processed > 0
            else 0.0
        )

        return SystemPerformanceMetrics(
            system_type=SystemType.QUEUE,
            system_name=self.system_name,
            requests_per_second=max(rps, 0.0),
            average_latency_ms=average_latency_ms,
            p95_latency_ms=average_latency_ms,
            p99_latency_ms=average_latency_ms,
            error_rate_percent=error_rate,
            active_connections=stats.active_subscriptions,
            total_operations_last_hour=total_sent,
            last_updated=now,
        )

    async def get_health_status(self) -> tuple[HealthScore, HealthStatus]:
        metrics = await self.get_system_metrics()
        return _calculate_health(metrics.average_latency_ms, metrics.error_rate_percent)


@dataclass(slots=True)
class FederationSystemMonitor:
    federation_manager: Any
    system_name: str
    _samples: deque[tuple[float, int]] = field(default_factory=deque)

    async def get_system_metrics(self) -> SystemPerformanceMetrics:
        metrics = self.federation_manager.get_federation_metrics()
        total_requests = int(metrics.get("total_cross_federation_requests", 0))
        success_rate = float(metrics.get("cross_federation_success_rate", 1.0))

        now = time.time()
        self._samples.append((now, total_requests))
        _prune_samples(self._samples, now)
        first_time, first_count = self._samples[0]
        elapsed = max(now - first_time, 1.0)
        rps = (total_requests - first_count) / elapsed

        error_rate = (1.0 - success_rate) * 100.0
        active_connections = int(metrics.get("total_active_connections", 0))

        return SystemPerformanceMetrics(
            system_type=SystemType.FEDERATION,
            system_name=self.system_name,
            requests_per_second=max(rps, 0.0),
            average_latency_ms=0.0,
            p95_latency_ms=0.0,
            p99_latency_ms=0.0,
            error_rate_percent=error_rate,
            active_connections=active_connections,
            total_operations_last_hour=total_requests,
            last_updated=now,
        )

    async def get_health_status(self) -> tuple[HealthScore, HealthStatus]:
        metrics = await self.get_system_metrics()
        return _calculate_health(metrics.average_latency_ms, metrics.error_rate_percent)
