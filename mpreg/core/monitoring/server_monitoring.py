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
# Cap in-memory event timestamps used for RPS windows (OBS-08).
_EVENT_DEQUE_MAXLEN = 50
_ERROR_CODE_LABEL_MAX = 64
_MGMT_EVENT_LABEL_MAX = 32_000


def _calculate_percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = round((percentile / 100.0) * (len(sorted_values) - 1))
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
    # OBS-08: bound event deques (prune still runs; maxlen caps multi-kRPS memory).
    rpc_events: deque[float] = field(
        default_factory=lambda: deque(maxlen=_EVENT_DEQUE_MAXLEN)
    )
    rpc_latencies_ms: deque[float] = field(default_factory=lambda: deque(maxlen=1000))
    # Per-error-code counters (stringified MpregErrorCode / wire code).
    rpc_error_codes: dict[str, int] = field(default_factory=dict)
    mgmt_mutations: dict[str, int] = field(default_factory=dict)
    notification_drops: int = 0
    gossip_pending_drops: int = 0
    replication_drops: int = 0
    cache_pubsub_drops: int = 0
    drain_refusals: int = 0
    accept_rejects: int = 0
    peer_accept_rejects: int = 0
    queue_dlq_total: int = 0
    federation_in_flight_drops: int = 0
    catalog_dedup_skips: int = 0
    raft_snapshot_installs: int = 0
    raft_snapshot_chunk_aborts: int = 0
    raft_snapshot_chunk_bytes: int = 0
    # OBS-T14-01: bridge from ProductionRaft.metrics (gauges; last scrape wins)
    raft_term: int = 0
    raft_commit_index: int = 0
    raft_last_applied: int = 0
    raft_log_size: int = 0
    raft_elections_started: int = 0
    raft_elections_won: int = 0
    raft_append_entries_success: int = 0
    raft_append_entries_failure: int = 0
    raft_commands_applied: int = 0
    raft_state: str = "unknown"
    node_draining: int = 0
    node_ready: int = 1
    # OBS-T11-02: wall-clock RPS from monotonic counters (not maxlen deques)
    _rpc_window_start: float = field(default_factory=time.time)
    _rpc_window_count: int = 0
    _rpc_rps_ewma: float = 0.0
    _pubsub_window_start: float = field(default_factory=time.time)
    _pubsub_window_count: int = 0
    _pubsub_rps_ewma: float = 0.0
    rpc_latency_buckets: list[int] = field(
        default_factory=lambda: [0] * (len(_LATENCY_BUCKETS_MS) + 1)
    )
    rpc_latency_sum_ms: float = 0.0

    pubsub_total: int = 0
    pubsub_errors: int = 0
    pubsub_events: deque[float] = field(
        default_factory=lambda: deque(maxlen=_EVENT_DEQUE_MAXLEN)
    )
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
        self._rpc_window_count += 1
        elapsed = now - self._rpc_window_start
        if elapsed >= 1.0:
            inst = self._rpc_window_count / max(elapsed, 1e-6)
            self._rpc_rps_ewma = (
                inst
                if self._rpc_rps_ewma <= 0
                else (0.3 * inst + 0.7 * self._rpc_rps_ewma)
            )
            self._rpc_window_start = now
            self._rpc_window_count = 0
        if not success:
            self.rpc_errors += 1
            code_key = str(error_code) if error_code is not None else "unknown"
            if (
                code_key not in self.rpc_error_codes
                and len(self.rpc_error_codes) >= _ERROR_CODE_LABEL_MAX
            ):
                code_key = "_other"
            self.rpc_error_codes[code_key] = self.rpc_error_codes.get(code_key, 0) + 1
        self.rpc_events.append(now)
        _prune_events(self.rpc_events, now, _HOUR_WINDOW_SECONDS)
        self.rpc_latencies_ms.append(latency_ms)
        self._observe_latency(self.rpc_latency_buckets, latency_ms)
        self.rpc_latency_sum_ms += float(latency_ms)

    def record_pubsub(self, latency_ms: float, success: bool) -> None:
        now = time.time()
        self.pubsub_total += 1
        self._pubsub_window_count += 1
        elapsed = now - self._pubsub_window_start
        if elapsed >= 1.0:
            inst = self._pubsub_window_count / max(elapsed, 1e-6)
            self._pubsub_rps_ewma = (
                inst
                if self._pubsub_rps_ewma <= 0
                else (0.3 * inst + 0.7 * self._pubsub_rps_ewma)
            )
            self._pubsub_window_start = now
            self._pubsub_window_count = 0
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
        if (
            key not in self.mgmt_mutations
            and len(self.mgmt_mutations) >= _MGMT_EVENT_LABEL_MAX
        ):
            key = "_other"
        self.mgmt_mutations[key] = self.mgmt_mutations.get(key, 0) + 1

    def set_draining(self, draining: bool) -> None:
        """OBS-T13-01: drain flag; also clear ready when entering drain."""
        self.node_draining = 1 if draining else 0
        if draining:
            self.node_ready = 0

    def set_notification_drops(self, count: int) -> None:
        self.notification_drops = max(0, int(count))

    def record_notification_drop(self, n: int = 1) -> None:
        """Increment server-observed notification delivery drops (OBS-01)."""
        self.notification_drops = max(0, int(self.notification_drops) + max(0, int(n)))

    def record_replication_drop(self, n: int = 1) -> None:
        """Increment cache replication enqueue drops (OBS-04)."""
        self.replication_drops = max(0, int(self.replication_drops) + max(0, int(n)))

    def record_cache_pubsub_drop(self, n: int = 1) -> None:
        """Increment cache-pubsub notification queue drops (OBS-04)."""
        self.cache_pubsub_drops = max(0, int(self.cache_pubsub_drops) + max(0, int(n)))

    def record_gossip_pending_drop(self, n: int = 1) -> None:
        """OBS-T10-01 / PERF-T10-05: gossip pending overflow drops."""
        self.gossip_pending_drops = max(
            0, int(self.gossip_pending_drops) + max(0, int(n))
        )

    def record_accept_reject(self, n: int = 1) -> None:
        """PERF-T11-01: inbound client connection rejected at cap."""
        self.accept_rejects = max(
            0, int(getattr(self, "accept_rejects", 0)) + max(0, int(n))
        )

    def record_peer_accept_reject(self, n: int = 1) -> None:
        """OBS-T13-03 / COR-T13-06: peer mesh connection refused at cap."""
        self.peer_accept_rejects = max(
            0, int(getattr(self, "peer_accept_rejects", 0)) + max(0, int(n))
        )

    def record_raft_snapshot_chunk_abort(self, n: int = 1) -> None:
        """OBS-T13-02: partial InstallSnapshot buffer dropped (TTL/bound)."""
        self.raft_snapshot_chunk_aborts = max(
            0, int(getattr(self, "raft_snapshot_chunk_aborts", 0)) + max(0, int(n))
        )

    def set_raft_snapshot_chunk_bytes(self, n: int) -> None:
        """OBS-T13-02: current buffered snapshot chunk bytes."""
        self.raft_snapshot_chunk_bytes = max(0, int(n))

    def set_raft_bridge(
        self,
        *,
        term: int = 0,
        commit_index: int = 0,
        last_applied: int = 0,
        log_size: int = 0,
        elections_started: int = 0,
        elections_won: int = 0,
        append_entries_success: int = 0,
        append_entries_failure: int = 0,
        commands_applied: int = 0,
        state: str = "unknown",
    ) -> None:
        """OBS-T14-01: snapshot internal Raft metrics onto unified Prom path."""
        self.raft_term = max(0, int(term))
        self.raft_commit_index = max(0, int(commit_index))
        self.raft_last_applied = max(0, int(last_applied))
        self.raft_log_size = max(0, int(log_size))
        self.raft_elections_started = max(0, int(elections_started))
        self.raft_elections_won = max(0, int(elections_won))
        self.raft_append_entries_success = max(0, int(append_entries_success))
        self.raft_append_entries_failure = max(0, int(append_entries_failure))
        self.raft_commands_applied = max(0, int(commands_applied))
        self.raft_state = str(state or "unknown")

    def record_drain_refusal(self, role: str = "unknown", n: int = 1) -> None:
        """OBS-T11-01: data-plane messages refused while draining."""
        self.drain_refusals = max(0, int(self.drain_refusals) + max(0, int(n)))

    def record_queue_dlq(self, n: int = 1) -> None:
        """OBS-T11-01: messages moved to DLQ (e.g. no-subscriber)."""
        self.queue_dlq_total = max(0, int(self.queue_dlq_total) + max(0, int(n)))

    def record_federation_in_flight_drop(self, n: int = 1) -> None:
        """OBS-T11-01: federation in_flight admission refusals."""
        self.federation_in_flight_drops = max(
            0, int(self.federation_in_flight_drops) + max(0, int(n))
        )

    def record_catalog_dedup_skip(self, n: int = 1) -> None:
        """OBS-T11-01: catalog update_id dedup skips."""
        self.catalog_dedup_skips = max(
            0, int(self.catalog_dedup_skips) + max(0, int(n))
        )

    def record_raft_snapshot_install(self, n: int = 1) -> None:
        self.raft_snapshot_installs = max(
            0, int(self.raft_snapshot_installs) + max(0, int(n))
        )

    def set_ready(self, ready: bool) -> None:
        """OBS-T11-03: mirror /ready predicate for Prom."""
        self.node_ready = 1 if ready else 0

    def set_replication_drops(self, count: int) -> None:
        self.replication_drops = max(0, int(count))

    def set_cache_pubsub_drops(self, count: int) -> None:
        self.cache_pubsub_drops = max(0, int(count))

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
        # OBS-T10-04: canonical name is server-side; keep legacy client_* alias.
        lines.append(
            "# HELP mpreg_server_notification_drops_total "
            "Pubsub notifications dropped on the server send path."
        )
        lines.append("# TYPE mpreg_server_notification_drops_total counter")
        lines.append(
            f"mpreg_server_notification_drops_total{{{labels}}} {self.notification_drops}"
        )
        lines.append(
            "# HELP mpreg_client_notification_drops_total "
            "DEPRECATED alias of mpreg_server_notification_drops_total "
            "(server-observed delivery failures, not client-local drops)."
        )
        lines.append("# TYPE mpreg_client_notification_drops_total counter")
        lines.append(
            f"mpreg_client_notification_drops_total{{{labels}}} {self.notification_drops}"
        )
        lines.append(
            "# HELP mpreg_gossip_pending_drops_total "
            "Gossip pending-queue overflow drops (storm loss signal)."
        )
        lines.append("# TYPE mpreg_gossip_pending_drops_total counter")
        lines.append(
            f"mpreg_gossip_pending_drops_total{{{labels}}} {self.gossip_pending_drops}"
        )
        lines.append(
            "# HELP mpreg_drain_refusals_total Data-plane messages refused while draining."
        )
        lines.append("# TYPE mpreg_drain_refusals_total counter")
        lines.append(f"mpreg_drain_refusals_total{{{labels}}} {self.drain_refusals}")
        lines.append(
            "# HELP mpreg_accept_rejects_total Inbound connections rejected at cap."
        )
        lines.append("# TYPE mpreg_accept_rejects_total counter")
        lines.append(
            f"mpreg_accept_rejects_total{{{labels}}} {getattr(self, 'accept_rejects', 0)}"
        )
        lines.append(
            "# HELP mpreg_peer_accept_rejects_total "
            "Peer mesh connections refused at max_peer_connections."
        )
        lines.append("# TYPE mpreg_peer_accept_rejects_total counter")
        lines.append(
            f"mpreg_peer_accept_rejects_total{{{labels}}} "
            f"{getattr(self, 'peer_accept_rejects', 0)}"
        )
        lines.append(
            "# HELP mpreg_raft_snapshot_chunk_aborts_total "
            "Partial InstallSnapshot buffers dropped (TTL/bound/error)."
        )
        lines.append("# TYPE mpreg_raft_snapshot_chunk_aborts_total counter")
        lines.append(
            f"mpreg_raft_snapshot_chunk_aborts_total{{{labels}}} "
            f"{getattr(self, 'raft_snapshot_chunk_aborts', 0)}"
        )
        lines.append(
            "# HELP mpreg_raft_snapshot_chunk_bytes "
            "Bytes currently buffered for in-progress InstallSnapshot."
        )
        lines.append("# TYPE mpreg_raft_snapshot_chunk_bytes gauge")
        lines.append(
            f"mpreg_raft_snapshot_chunk_bytes{{{labels}}} "
            f"{getattr(self, 'raft_snapshot_chunk_bytes', 0)}"
        )
        lines.append(
            "# HELP mpreg_queue_dlq_total Messages moved to dead-letter queue."
        )
        lines.append("# TYPE mpreg_queue_dlq_total counter")
        lines.append(f"mpreg_queue_dlq_total{{{labels}}} {self.queue_dlq_total}")
        lines.append(
            "# HELP mpreg_queue_federation_in_flight_drops_total "
            "Federation in_flight admission refusals when full."
        )
        lines.append("# TYPE mpreg_queue_federation_in_flight_drops_total counter")
        lines.append(
            f"mpreg_queue_federation_in_flight_drops_total{{{labels}}} "
            f"{self.federation_in_flight_drops}"
        )
        lines.append(
            "# HELP mpreg_catalog_dedup_skips_total Catalog update_id dedup skips."
        )
        lines.append("# TYPE mpreg_catalog_dedup_skips_total counter")
        lines.append(
            f"mpreg_catalog_dedup_skips_total{{{labels}}} {self.catalog_dedup_skips}"
        )
        lines.append(
            "# HELP mpreg_raft_snapshot_installs_total Successful InstallSnapshot applies."
        )
        lines.append("# TYPE mpreg_raft_snapshot_installs_total counter")
        lines.append(
            f"mpreg_raft_snapshot_installs_total{{{labels}}} {self.raft_snapshot_installs}"
        )
        lines.append("# HELP mpreg_node_ready 1 if node would pass /ready admission.")
        lines.append("# TYPE mpreg_node_ready gauge")
        lines.append(f"mpreg_node_ready{{{labels}}} {int(self.node_ready)}")
        # OBS-T14-01: Raft internal gauges (bridged from ProductionRaft)
        lines.append("# HELP mpreg_raft_term Current Raft term (bridged).")
        lines.append("# TYPE mpreg_raft_term gauge")
        lines.append(
            f"mpreg_raft_term{{{labels}}} {int(getattr(self, 'raft_term', 0))}"
        )
        lines.append("# HELP mpreg_raft_commit_index Raft commit_index (bridged).")
        lines.append("# TYPE mpreg_raft_commit_index gauge")
        lines.append(
            f"mpreg_raft_commit_index{{{labels}}} {int(getattr(self, 'raft_commit_index', 0))}"
        )
        lines.append("# HELP mpreg_raft_last_applied Raft last_applied (bridged).")
        lines.append("# TYPE mpreg_raft_last_applied gauge")
        lines.append(
            f"mpreg_raft_last_applied{{{labels}}} {int(getattr(self, 'raft_last_applied', 0))}"
        )
        lines.append("# HELP mpreg_raft_log_size Raft log entry count (bridged).")
        lines.append("# TYPE mpreg_raft_log_size gauge")
        lines.append(
            f"mpreg_raft_log_size{{{labels}}} {int(getattr(self, 'raft_log_size', 0))}"
        )
        lines.append(
            "# HELP mpreg_raft_elections_started_total Elections started (bridged)."
        )
        lines.append("# TYPE mpreg_raft_elections_started_total counter")
        lines.append(
            f"mpreg_raft_elections_started_total{{{labels}}} "
            f"{int(getattr(self, 'raft_elections_started', 0))}"
        )
        lines.append("# HELP mpreg_raft_elections_won_total Elections won (bridged).")
        lines.append("# TYPE mpreg_raft_elections_won_total counter")
        lines.append(
            f"mpreg_raft_elections_won_total{{{labels}}} "
            f"{int(getattr(self, 'raft_elections_won', 0))}"
        )
        lines.append(
            "# HELP mpreg_raft_append_entries_success_total AE success (bridged)."
        )
        lines.append("# TYPE mpreg_raft_append_entries_success_total counter")
        lines.append(
            f"mpreg_raft_append_entries_success_total{{{labels}}} "
            f"{int(getattr(self, 'raft_append_entries_success', 0))}"
        )
        lines.append(
            "# HELP mpreg_raft_append_entries_failure_total AE failure (bridged)."
        )
        lines.append("# TYPE mpreg_raft_append_entries_failure_total counter")
        lines.append(
            f"mpreg_raft_append_entries_failure_total{{{labels}}} "
            f"{int(getattr(self, 'raft_append_entries_failure', 0))}"
        )
        lines.append(
            "# HELP mpreg_raft_commands_applied_total SM commands applied (bridged)."
        )
        lines.append("# TYPE mpreg_raft_commands_applied_total counter")
        lines.append(
            f"mpreg_raft_commands_applied_total{{{labels}}} "
            f"{int(getattr(self, 'raft_commands_applied', 0))}"
        )
        lines.append(
            "# HELP mpreg_cache_replication_drops_total "
            "Cache replication operations dropped due to backpressure."
        )
        lines.append("# TYPE mpreg_cache_replication_drops_total counter")
        lines.append(
            f"mpreg_cache_replication_drops_total{{{labels}}} {self.replication_drops}"
        )
        lines.append(
            "# HELP mpreg_cache_pubsub_notification_drops_total "
            "Cache-pubsub integration notification queue drops."
        )
        lines.append("# TYPE mpreg_cache_pubsub_notification_drops_total counter")
        lines.append(
            f"mpreg_cache_pubsub_notification_drops_total{{{labels}}} "
            f"{self.cache_pubsub_drops}"
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
            lines.append(f'{name}_bucket{{{labels},le="{bound:g}"}} {cumulative}')
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

    def _fabric_route_snapshot(self) -> dict[str, float | int]:
        """Hop / reachability stats from the process route-decision log (Phase Q)."""
        try:
            from mpreg.fabric.route_decision_log import get_default_route_decision_log

            log = get_default_route_decision_log()
            st = log.stats()
            hops: list[int] = []
            try:
                for rec in log.recent(limit=256):
                    hops.append(int(getattr(rec, "hops_required", 0) or 0))
            except Exception:
                hops = []
            avg_hops = (sum(hops) / len(hops)) if hops else 0.0
            max_hops = max(hops) if hops else 0
            return {
                "decisions_total": int(st.get("total_recorded", 0) or 0),
                "decisions_buffered": int(st.get("size", 0) or 0),
                "blackhole_count": int(st.get("blackhole_count", 0) or 0),
                "reachable_ratio": round(
                    float(st.get("reachable_ratio", 1.0) or 1.0), 4
                ),
                "hop_samples": len(hops),
                "avg_hops": round(float(avg_hops), 3),
                "max_hops": int(max_hops),
            }
        except Exception:
            return {
                "decisions_total": 0,
                "decisions_buffered": 0,
                "blackhole_count": 0,
                "reachable_ratio": 1.0,
                "hop_samples": 0,
                "avg_hops": 0.0,
                "max_hops": 0,
            }

    def snapshot(self) -> dict[str, float | int | dict[str, int]]:
        """In-process metrics snapshot for curriculum / operator probes (PG7).

        Does not require HTTP scrape. Suitable for example apps asserting
        latency and throughput after live RPC/pubsub traffic.
        """
        now = time.time()
        rpc_lat = list(self.rpc_latencies_ms)
        pub_lat = list(self.pubsub_latencies_ms)
        uptime = max(now - self.started_at, 1e-6)
        rpc_rps = (
            float(self._rpc_rps_ewma)
            if self._rpc_rps_ewma > 0
            else float(self.rpc_total) / uptime
        )
        pub_rps = (
            float(self._pubsub_rps_ewma)
            if self._pubsub_rps_ewma > 0
            else float(self.pubsub_total) / uptime
        )
        return {
            "uptime_s": round(uptime, 4),
            "rpc": {
                "total": int(self.rpc_total),
                "errors": int(self.rpc_errors),
                "samples": len(rpc_lat),
                "avg_ms": round((sum(rpc_lat) / len(rpc_lat)) if rpc_lat else 0.0, 3),
                "p50_ms": round(_calculate_percentile(rpc_lat, 50.0), 3),
                "p95_ms": round(_calculate_percentile(rpc_lat, 95.0), 3),
                "p99_ms": round(_calculate_percentile(rpc_lat, 99.0), 3),
                "min_ms": round(min(rpc_lat), 3) if rpc_lat else 0.0,
                "max_ms": round(max(rpc_lat), 3) if rpc_lat else 0.0,
                "rps": round(rpc_rps, 3),
                "error_codes": dict(self.rpc_error_codes),
            },
            "pubsub": {
                "total": int(self.pubsub_total),
                "errors": int(self.pubsub_errors),
                "samples": len(pub_lat),
                "avg_ms": round((sum(pub_lat) / len(pub_lat)) if pub_lat else 0.0, 3),
                "p50_ms": round(_calculate_percentile(pub_lat, 50.0), 3),
                "p95_ms": round(_calculate_percentile(pub_lat, 95.0), 3),
                "p99_ms": round(_calculate_percentile(pub_lat, 99.0), 3),
                "min_ms": round(min(pub_lat), 3) if pub_lat else 0.0,
                "max_ms": round(max(pub_lat), 3) if pub_lat else 0.0,
                "rps": round(pub_rps, 3),
                "notifications": int(self.pubsub_notifications),
            },
            "fabric": self._fabric_route_snapshot(),
        }

    def rpc_metrics(
        self, system_name: str, active_connections: int
    ) -> SystemPerformanceMetrics:
        now = time.time()
        _prune_events(self.rpc_events, now, _HOUR_WINDOW_SECONDS)
        # OBS-T11-02: prefer EWMA from counter windows; fall back to lifetime rate
        if self._rpc_rps_ewma > 0:
            rps = float(self._rpc_rps_ewma)
        else:
            uptime = max(now - self.started_at, 1e-6)
            rps = float(self.rpc_total) / uptime
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
            total_operations_last_hour=int(self.rpc_total),
            last_updated=now,
        )

    def pubsub_metrics(
        self, system_name: str, active_connections: int
    ) -> SystemPerformanceMetrics:
        now = time.time()
        _prune_events(self.pubsub_events, now, _HOUR_WINDOW_SECONDS)
        if self._pubsub_rps_ewma > 0:
            rps = float(self._pubsub_rps_ewma)
        else:
            uptime = max(now - self.started_at, 1e-6)
            rps = float(self.pubsub_total) / uptime
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
            total_operations_last_hour=int(self.pubsub_total),
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
