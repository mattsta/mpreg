"""T84 residual closeout: Hypothesis count_abort_fail_peers."""

from __future__ import annotations

from pathlib import Path

def test_t84_hypothesis_present() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "test_cli_strong_audit_monitor.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "test_count_abort_fail_peers_hypothesis" in text
    assert "count_abort_fail_peers" in text

def test_t84_phase_72_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    assert "Phase 72" in path.read_text(encoding="utf-8")

def test_t84_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T84_HYPOTHESIS_PEER_COUNT_PLAN.md"
    ).is_file()
    assert "T84" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
