"""T99 residual closeout: OPERATE doctor/monitor peer count."""

from __future__ import annotations

from pathlib import Path


def test_t99_operate() -> None:
    text = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "examples-curriculum"
        / "OPERATE.md"
    ).read_text(encoding="utf-8")
    assert "abort_fail_peer_count" in text
    assert "doctor" in text.lower()
    assert "monitor" in text.lower() or "Doctor detail" in text


def test_t99_phase_87_honesty() -> None:
    assert "Phase 87" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")


def test_t99_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T99_OPERATE_DOCTOR_PEER_COUNT_PLAN.md"
    ).is_file()
    assert "T99" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
