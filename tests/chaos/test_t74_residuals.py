"""T74 residual closeout: Hypothesis doctor residual hint + dishonest caps."""

from __future__ import annotations

from pathlib import Path


def test_t74_hypothesis_tests_present() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "test_cli_strong_audit_monitor.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "test_doctor_residual_hint_hypothesis" in text
    assert "test_doctor_dishonest_caps_hypothesis" in text
    assert "cache-strong-retry-abort" in text


def test_t74_phase_62_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 62" in text


def test_t74_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T74_HYPOTHESIS_DOCTOR_HINT_PLAN.md"
    ).is_file()
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T74" in ledger
