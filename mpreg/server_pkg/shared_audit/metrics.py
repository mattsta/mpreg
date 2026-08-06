"""Lightweight counters for shared audit (optional Prometheus later)."""

from __future__ import annotations

from dataclasses import dataclass

@dataclass(slots=True)
class SharedAuditMetrics:
    deltas_sent: int = 0
    deltas_recv: int = 0
    digests_sent: int = 0
    pulls_sent: int = 0
    pulls_recv: int = 0
    merge_conflicts: int = 0
    publish_dropped: int = 0
    persist_fail: int = 0
    rejected_cross_cluster: int = 0
    rejected_below_watermark: int = 0

    def snapshot(self) -> dict[str, int]:
        return {
            "deltas_sent": self.deltas_sent,
            "deltas_recv": self.deltas_recv,
            "digests_sent": self.digests_sent,
            "pulls_sent": self.pulls_sent,
            "pulls_recv": self.pulls_recv,
            "merge_conflicts": self.merge_conflicts,
            "publish_dropped": self.publish_dropped,
            "persist_fail": self.persist_fail,
            "rejected_cross_cluster": self.rejected_cross_cluster,
            "rejected_below_watermark": self.rejected_below_watermark,
        }

_GLOBAL: SharedAuditMetrics = SharedAuditMetrics()

def get_shared_audit_metrics() -> SharedAuditMetrics:
    return _GLOBAL
