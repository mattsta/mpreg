"""T120 residual closeout: config-check explain doctor JSON types."""

from __future__ import annotations

from pathlib import Path

def test_t120_phase_108_honesty() -> None:
    assert "Phase 108" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")

def test_t120_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T120_CONFIG_CHECK_DOCTOR_JSON_TYPES_PLAN.md"
    ).is_file()
    assert "T120" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )

def test_t120_config_check_source() -> None:
    root = Path(__file__).resolve().parents[2]
    text = (root / "mpreg" / "cli" / "main.py").read_text(encoding="utf-8")
    assert "strong_doctor_json_residual_fields" in text
    assert "doctor" in text.lower() and "abort_fail_peer_count int" in text

