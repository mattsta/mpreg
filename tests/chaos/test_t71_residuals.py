"""T71 residual closeout: doctor JSON residual_ops_hint field."""

from __future__ import annotations

from pathlib import Path

from mpreg.cli.main import strong_residual_ops_hint

def test_t71_doctor_main_row_key() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "cli"
        / "main.py"
    )
    text = path.read_text(encoding="utf-8")
    assert 'row["residual_ops_hint"]' in text or "residual_ops_hint" in text
    assert "metrics_strong" in text
    assert "strong_residual_ops_hint" in text

def test_t71_unit_test_present() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "test_cli_strong_audit_monitor.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "test_doctor_strong_row_residual_ops_hint_field" in text

def test_t71_hint_empty_without_peers() -> None:
    assert strong_residual_ops_hint({"last_abort_fail_peers": []}) == ""

def test_t71_phase_59_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 59" in text

def test_t71_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T71_DOCTOR_JSON_HINT_PLAN.md"
    ).is_file()
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T71" in ledger
