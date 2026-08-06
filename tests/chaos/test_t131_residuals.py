"""T131 residual closeout: Curriculum monitor strong JSON."""

from __future__ import annotations

from pathlib import Path

def test_t131_phase_119_honesty() -> None:
    assert "Phase 119" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")

def test_t131_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T131_CURRICULUM_MONITOR_JSON_PLAN.md"
    ).is_file()
    assert "T131" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )

def test_t131_curriculum_monitor_json() -> None:
    root = Path(__file__).resolve().parents[2]
    text = (
        root / "mpreg" / "examples" / "apps" / "02_moderate" / "ops_cli_tour" / "run.py"
    ).read_text(encoding="utf-8")
    assert "monitor" in text and "strong" in text and "--format" in text
    assert "abort_fail_peer_count" in text
    assert "monitor strong json" in text.lower() or "strong_j" in text
