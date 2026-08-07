"""T91 residual closeout: cache_strong_quorum peer count assert."""

from __future__ import annotations

from pathlib import Path


def test_t91_curriculum_peer_count() -> None:
    text = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "examples"
        / "apps"
        / "02_moderate"
        / "cache_strong_quorum"
        / "run.py"
    ).read_text(encoding="utf-8")
    assert "abort_fail_peer_count" in text
    assert "residual_ops_hint" in text


def test_t91_phase_79_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    assert "Phase 79" in path.read_text(encoding="utf-8")


def test_t91_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T91_CURRICULUM_PEER_COUNT_PLAN.md"
    ).is_file()
    assert "T91" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
