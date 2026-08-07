"""T40 residual closeout: live e2e retry_abort surface + OPERATE honesty."""

from __future__ import annotations

from pathlib import Path

def test_t40_live_e2e_source_asserts_retry_prom() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "testing"
        / "test_distlab_live.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "mpreg_strong_retry_abort_calls_total" in text
    assert "strong_retry_abort" in text
    assert "retry_abort_calls" in text
    assert "last_abort_fail_peers" in text

def test_t40_operate_retry_prom() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "examples-curriculum"
        / "OPERATE.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "retry_abort" in text
    assert "mpreg_strong_retry_abort" in text
    assert "not auto-heal" in text.lower() or "ops-driven" in text.lower()

def test_t40_residual_honesty_phase_28() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 28" in text
    assert "retry_abort" in text.lower()
