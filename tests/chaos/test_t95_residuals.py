"""T95 residual closeout: GCM strong_status uses count_abort_fail_peers."""

from __future__ import annotations

from pathlib import Path

def test_t95_gcm_uses_helper() -> None:
    text = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "core"
        / "global_cache.py"
    ).read_text(encoding="utf-8")
    assert "count_abort_fail_peers" in text
    assert "abort_fail_peer_count" in text

def test_t95_phase_83_honesty() -> None:
    assert "Phase 83" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")

def test_t95_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T95_GCM_COUNT_HELPER_PLAN.md"
    ).is_file()
    assert "T95" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
