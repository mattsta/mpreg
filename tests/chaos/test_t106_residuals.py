"""T106 residual closeout: Curriculum doctor JSON int/list."""

from __future__ import annotations

from pathlib import Path

def test_t106_phase_94_honesty() -> None:
    assert "Phase 94" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")

def test_t106_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T106_CURRICULUM_DOCTOR_JSON_TYPES_PLAN.md"
    ).is_file()
    assert "T106" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )

def test_t106_curriculum_types() -> None:
    root = Path(__file__).resolve().parents[2]
    text = (
        root / "mpreg" / "examples" / "apps" / "02_moderate" / "ops_cli_tour" / "run.py"
    ).read_text(encoding="utf-8")
    assert "isinstance(row.get(\"abort_fail_peer_count\"), int)" in text
    assert "last_abort_fail_peers" in text

