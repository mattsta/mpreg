"""DistLab SLI helpers and soak budget (lab bounds, not WAN SLA)."""

from __future__ import annotations

import pytest

from mpreg.testing.distlab.sli import (
    DEFAULT_STRONG_SOAK_BUDGET,
    SliBudget,
    WallTimer,
    percentile,
    summarize_latencies_ms,
)

def test_percentile_empty_and_edges() -> None:
    assert percentile([], 50) == 0.0
    assert percentile([10.0], 50) == 10.0
    assert percentile([1.0, 2.0, 3.0, 4.0], 0) == 1.0
    assert percentile([1.0, 2.0, 3.0, 4.0], 100) == 4.0
    assert percentile([1.0, 2.0, 3.0, 4.0], 50) >= 2.0

def test_summarize_latencies() -> None:
    s = summarize_latencies_ms([10.0, 20.0, 30.0, 40.0, 100.0])
    assert s["sample_count"] == 5
    assert float(s["max_ms"]) == 100.0
    assert float(s["avg_ms"]) == pytest.approx(40.0)

def test_wall_timer() -> None:
    t = WallTimer(name="x")
    with t.measure():
        pass
    assert len(t.samples_ms) == 1
    assert t.summary()["sample_count"] == 1

def test_sli_budget_ok_and_fail() -> None:
    b = SliBudget(max_p99_ms=50.0, max_avg_ms=40.0, max_duration_s=1.0)
    ok = b.check(latency_ms=[1.0, 2.0, 3.0], duration_s=0.1, success=3, total=3)
    assert ok["ok"] is True
    bad = b.check(latency_ms=[1.0, 100.0], duration_s=0.1, success=2, total=2)
    assert bad["ok"] is False
    assert bad["violations"]

def test_default_soak_budget_exists() -> None:
    assert DEFAULT_STRONG_SOAK_BUDGET.max_p99_ms > 0
