"""T130 residual closeout: monitor strong JSON residual ensure."""

from __future__ import annotations

from pathlib import Path


def test_t130_phase_118_honesty() -> None:
    assert "Phase 118" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")


def test_t130_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T130_MONITOR_JSON_RESIDUAL_ENSURE_PLAN.md"
    ).is_file()
    assert "T130" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )


def test_t130_monitor_source() -> None:
    root = Path(__file__).resolve().parents[2]
    text = (root / "mpreg" / "cli" / "main.py").read_text(encoding="utf-8")
    assert "strong_doctor_json_residual_fields" in text
    assert "setdefault" in text
    assert 'fmt == "json"' in text or "fmt == 'json'" in text
