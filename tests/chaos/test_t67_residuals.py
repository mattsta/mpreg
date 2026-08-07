"""T67 residual closeout: design-doc residual_ops_hint polish."""

from __future__ import annotations

from pathlib import Path


def test_t67_design_doc_residual_ops_hint() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "residual_ops_hint" in text
    assert "format_residual_ops_hint" in text
    assert "cft_residual_ops_hint_enriched" in text
    assert "recent_abort_fails" in text
    assert "not" in text.lower() and (
        "auto" in text.lower() or "ops-driven" in text.lower()
    )


def test_t67_phase_55_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 55" in text


def test_t67_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T67_DESIGN_HINT_POLISH_PLAN.md"
    ).is_file()
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T67" in ledger
