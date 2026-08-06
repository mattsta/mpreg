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
