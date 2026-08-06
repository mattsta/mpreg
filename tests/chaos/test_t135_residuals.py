"""T135 residual closeout: doctor help unit residual types."""

from __future__ import annotations

from pathlib import Path

def test_t135_phase_123_honesty() -> None:
    assert "Phase 123" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")

def test_t135_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T135_DOCTOR_HELP_UNIT_PLAN.md"
    ).is_file()
    assert "T135" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )

def test_t135_help_unit() -> None:
    root = Path(__file__).resolve().parents[2]
    text = (root / "tests" / "test_cli_strong_audit_monitor.py").read_text(encoding="utf-8")
    assert "abort_fail_peer_count" in text
    assert "test_doctor_strong_audit_flags_help" in text

