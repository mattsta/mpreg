"""T66 residual closeout: curriculum config-check residual_ops_hint assert."""

from __future__ import annotations

from pathlib import Path


def test_t66_ops_cli_tour_explain_residual_assert() -> None:
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
    assert "cache-strong-retry-abort" in text
    assert "auto-heal" in text.lower() or "ops-driven" in text.lower()
    assert "config-check" in text
    assert "--explain" in text or "explain" in text


def test_t66_phase_54_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 54" in text


def test_t66_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T66_CURRICULUM_EXPLAIN_ASSERT_PLAN.md"
    ).is_file()
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T66" in ledger
