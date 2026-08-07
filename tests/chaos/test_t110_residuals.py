"""T110 residual closeout: Doctor JSON last_abort_fail_op_id."""

from __future__ import annotations

from pathlib import Path


def test_t110_phase_98_honesty() -> None:
    assert "Phase 98" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")


def test_t110_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T110_DOCTOR_JSON_OP_ID_PLAN.md"
    ).is_file()
    assert "T110" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )


def test_t110_op_id_in_helper() -> None:
    from mpreg.cli.main import strong_doctor_json_residual_fields

    f = strong_doctor_json_residual_fields(
        {"last_abort_fail_peers": ["p"], "last_abort_fail_op_id": "oid-x"}
    )
    assert f["last_abort_fail_op_id"] == "oid-x"
    assert isinstance(f["last_abort_fail_op_id"], str)
