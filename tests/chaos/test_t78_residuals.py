"""T78 residual closeout: cache_strong_quorum residual_ops_hint assert."""

from __future__ import annotations

from pathlib import Path

def test_t78_curriculum_hint_assert() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "examples"
        / "apps"
        / "02_moderate"
        / "cache_strong_quorum"
        / "run.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "residual_ops_hint" in text
    assert "cache-strong-retry-abort" in text
    assert "not auto-heal" in text
    assert "gcm_hint" in text or "strong_status" in text

def test_t78_phase_66_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    assert "Phase 66" in path.read_text(encoding="utf-8")

def test_t78_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T78_CURRICULUM_HINT_ASSERT_PLAN.md"
    ).is_file()
    assert "T78" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
