"""T112 residual closeout: Curriculum doctor JSON op_id."""

from __future__ import annotations

from pathlib import Path

def test_t112_phase_100_honesty() -> None:
    assert "Phase 100" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")

def test_t112_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T112_CURRICULUM_DOCTOR_OP_ID_PLAN.md"
    ).is_file()
    assert "T112" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )

def test_t112_curriculum_op_id() -> None:
    root = Path(__file__).resolve().parents[2]
    text = (
        root / "mpreg" / "examples" / "apps" / "02_moderate" / "ops_cli_tour" / "run.py"
    ).read_text(encoding="utf-8")
    assert "last_abort_fail_op_id" in text
    assert 'isinstance(row.get("last_abort_fail_op_id"), str)' in text

