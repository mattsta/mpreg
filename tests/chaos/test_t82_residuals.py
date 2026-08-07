"""T82 residual closeout: ops_cli_tour doctor JSON residual_ops_hint."""

from __future__ import annotations

from pathlib import Path


def test_t82_ops_cli_doctor_json_assert() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "examples"
        / "apps"
        / "02_moderate"
        / "ops_cli_tour"
        / "run.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "residual_ops_hint" in text
    assert "--format" in text and "json" in text
    assert "metrics_strong" in text
    assert "mgmt_strong" in text


def test_t82_phase_70_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    assert "Phase 70" in path.read_text(encoding="utf-8")


def test_t82_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T82_OPS_CLI_DOCTOR_JSON_HINT_PLAN.md"
    ).is_file()
    assert "T82" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
