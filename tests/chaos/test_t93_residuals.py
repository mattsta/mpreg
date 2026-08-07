"""T93 residual closeout: runbook abort_fail_peer_count."""

from __future__ import annotations

from pathlib import Path


def test_t93_runbook() -> None:
    text = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "ops"
        / "STRONG_AND_SHARED_AUDIT_RUNBOOK.md"
    ).read_text(encoding="utf-8")
    assert "abort_fail_peer_count" in text
    assert "count_abort_fail_peers" in text
    assert "mpreg_strong_abort_fail_peers" in text


def test_t93_phase_81_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    assert "Phase 81" in path.read_text(encoding="utf-8")


def test_t93_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T93_RUNBOOK_PEER_COUNT_PLAN.md"
    ).is_file()
    assert "T93" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
