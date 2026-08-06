"""Phase T: format_server_snapshot prints fabric hop block."""

from __future__ import annotations

import io
from contextlib import redirect_stdout

from mpreg.examples.apps._shared.obs import format_server_snapshot

def test_format_server_snapshot_prints_fabric_hops() -> None:
    snap = {
        "rpc": {
            "total": 2,
            "errors": 0,
            "samples": 2,
            "avg_ms": 1.0,
            "p50_ms": 1.0,
            "p95_ms": 1.0,
            "min_ms": 0.5,
            "max_ms": 1.5,
            "rps": 0.1,
        },
        "pubsub": {"total": 0, "errors": 0, "samples": 0, "avg_ms": 0, "p95_ms": 0, "rps": 0, "notifications": 0},
        "fabric": {
            "decisions_total": 4,
            "decisions_buffered": 1,
            "blackhole_count": 0,
            "reachable_ratio": 1.0,
            "avg_hops": 1.25,
            "max_hops": 3,
        },
    }
    buf = io.StringIO()
    with redirect_stdout(buf):
        format_server_snapshot(snap, prefix="t")
    out = buf.getvalue()
    assert "fabric decisions=4" in out
    assert "avg_hops=1.25" in out
    assert "max_hops=3" in out
    assert "buffered=1" in out
    assert "rpc total=2" in out

def test_format_server_snapshot_skips_empty_fabric() -> None:
    buf = io.StringIO()
    with redirect_stdout(buf):
        format_server_snapshot({"rpc": {}, "pubsub": {}, "fabric": {}}, prefix="t")
    out = buf.getvalue()
    assert "rpc total=" in out
    assert "fabric" not in out
