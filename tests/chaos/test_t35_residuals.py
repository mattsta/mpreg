"""T35 residual closeout: design doc CFT residual honesty."""

from __future__ import annotations

from pathlib import Path

def test_t35_design_doc_qualifies_residual_free_cft() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md"
    )
    text = path.read_text(encoding="utf-8")
    lower = text.lower()
    assert "cft" in lower or "best-effort" in lower
    assert "lost abort" in lower or "abort is lost" in lower
    assert "not residual-free" in lower or "not claimed residual-free" in lower
    assert "pending ttl" in lower and "residual" in lower
    # Must not leave the old absolute claim without CFT qualification nearby
    assert "Failed put residual-free invariant (cluster-visible, CFT best-effort)" in text
    assert "CFT exception" in text or "cft exception" in lower

def test_t35_design_doc_goals_mention_abort_best_effort() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md"
    )
    text = path.read_text(encoding="utf-8")
    # Goals section should not promise absolute peer residual-free without CFT
    assert "delivered" in text.lower() or "best-effort" in text.lower()
