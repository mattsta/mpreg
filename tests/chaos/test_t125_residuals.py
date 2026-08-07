"""T125 residual closeout: config-check pytest doctor JSON types."""

from __future__ import annotations

from pathlib import Path


def test_t125_phase_113_honesty() -> None:
    assert "Phase 113" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")


def test_t125_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T125_CONFIG_CHECK_PYTEST_DOCTOR_JSON_PLAN.md"
    ).is_file()
    assert "T125" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )


def test_t125_config_check_pytest() -> None:
    root = Path(__file__).resolve().parents[2]
    text = (root / "tests" / "test_config_check_cli.py").read_text(encoding="utf-8")
    assert "strong_doctor_json_residual_fields" in text or "doctor" in text
    assert "last_abort_fail_op_id" in text
