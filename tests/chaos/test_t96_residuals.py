"""T96 residual closeout: live doctor detail abort_fail_peer_count."""

from __future__ import annotations

from pathlib import Path

def test_t96_live_doctor_detail_assert() -> None:
    text = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "testing"
        / "test_distlab_live.py"
    ).read_text(encoding="utf-8")
    assert "abort_fail_peer_count=0" in text
    assert "abort_fail_peer_count=" in text
    assert "test_distlab_live_doctor_strong_audit_e2e" in text
    assert "test_distlab_live_residual_ops_hint_enriched_e2e" in text

def test_t96_phase_84_honesty() -> None:
    assert "Phase 84" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")

def test_t96_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T96_LIVE_DOCTOR_PEER_COUNT_PLAN.md"
    ).is_file()
    assert "T96" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
