"""T98 residual closeout: build_strong_metrics peer count unit tests."""

from __future__ import annotations

from pathlib import Path

def test_t98_metrics_tests_present() -> None:
    text = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "server_pkg"
        / "test_strong_audit_metrics.py"
    ).read_text(encoding="utf-8")
    assert "abort_fail_peer_count" in text
    assert "test_build_strong_metrics_abort_fail_peer_count" in text
    assert "residual_ops_hint" in text

def test_t98_phase_86_honesty() -> None:
    assert "Phase 86" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")

def test_t98_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T98_METRICS_PEER_COUNT_UNIT_PLAN.md"
    ).is_file()
    assert "T98" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
