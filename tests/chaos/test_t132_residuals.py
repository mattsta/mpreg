"""T132 residual closeout: doctor --strong help residual types."""

from __future__ import annotations

from pathlib import Path

def test_t132_phase_120_honesty() -> None:
    assert "Phase 120" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")

def test_t132_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T132_DOCTOR_HELP_RESIDUAL_TYPES_PLAN.md"
    ).is_file()
    assert "T132" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )

def test_t132_doctor_help_source() -> None:
    root = Path(__file__).resolve().parents[2]
    text = (root / "mpreg" / "cli" / "main.py").read_text(encoding="utf-8")
    assert "abort_fail_peer_count (int)" in text or "abort_fail_peer_count" in text
    assert "not auto-heal" in text

