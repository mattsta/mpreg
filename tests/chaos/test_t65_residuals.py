"""T65 residual closeout: config-check explain residual ops loop."""

from __future__ import annotations

from pathlib import Path

def test_t65_config_check_explain_strong_cache() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "cli"
        / "main.py"
    )
    text = path.read_text(encoding="utf-8")
    # strong_cache explain guide mentions residual_ops_hint ops loop
    assert "residual_ops_hint" in text
    assert "cache-strong-retry-abort" in text
    assert "not" in text and "auto-heal" in text

def test_t65_ops_cli_tour_config_check() -> None:
    """Curriculum still exercises config-check --explain."""
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
    assert "config-check" in text
    assert "--explain" in text or "explain" in text

def test_t65_phase_53_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 53" in text

def test_t65_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T65_CONFIG_CHECK_HINT_PLAN.md"
    ).is_file()
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T65" in ledger
