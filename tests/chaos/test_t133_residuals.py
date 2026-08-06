"""T133 residual closeout: GETTING_STARTED residual ops."""

from __future__ import annotations

from pathlib import Path

def test_t133_phase_121_honesty() -> None:
    assert "Phase 121" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")

def test_t133_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T133_GETTING_STARTED_RESIDUAL_PLAN.md"
    ).is_file()
    assert "T133" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )

def test_t133_getting_started() -> None:
    root = Path(__file__).resolve().parents[2]
    text = (root / "docs" / "GETTING_STARTED.md").read_text(encoding="utf-8")
    assert "doctor --strong" in text
    assert "abort_fail_peer_count" in text

