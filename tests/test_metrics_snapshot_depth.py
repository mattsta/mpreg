"""Phase P: ServerMetricsTracker.snapshot includes samples/min/max."""

from mpreg.core.monitoring.server_monitoring import ServerMetricsTracker

def test_snapshot_rpc_depth_keys() -> None:
    t = ServerMetricsTracker()
    for i in range(5):
        t.record_rpc(1.0 + i, True)
    t.record_rpc(10.0, False, error_code="timeout")
    s = t.snapshot()
    rpc = s["rpc"]
    assert rpc["total"] == 6
    assert rpc["errors"] == 1
    assert rpc["samples"] >= 1
    assert "min_ms" in rpc and "max_ms" in rpc
    assert rpc["min_ms"] <= rpc["avg_ms"] <= rpc["max_ms"] + 1e-9
    assert "p50_ms" in rpc and "p95_ms" in rpc and "p99_ms" in rpc

def test_snapshot_includes_fabric_hop_stats() -> None:
    from mpreg.core.monitoring.server_monitoring import ServerMetricsTracker
    from mpreg.fabric.route_decision_log import (
        RouteDecisionRecord,
        get_default_route_decision_log,
    )

    log = get_default_route_decision_log()
    log.record(
        RouteDecisionRecord(
            timestamp=0.0,
            message_id="m1",
            correlation_id="c1",
            topic="t",
            message_type="rpc",
            reason="ok",
            cached=False,
            targets=("n2",),
            routing_path=("n1", "n2"),
            hops_required=2,
        )
    )
    t = ServerMetricsTracker()
    s = t.snapshot()
    assert "fabric" in s
    fab = s["fabric"]
    assert "avg_hops" in fab and "max_hops" in fab
    assert "decisions_total" in fab
    assert int(fab["decisions_total"]) >= 1
    assert int(fab["max_hops"]) >= 2

