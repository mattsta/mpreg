"""T83 residual closeout: client guide abort_fail_peer_count / doctor JSON."""

from __future__ import annotations

from pathlib import Path

def test_t83_client_guide() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "MPREG_CLIENT_GUIDE.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "mpreg_strong_abort_fail_peers" in text
    assert "abort_fail_peer_count" in text
    assert "doctor" in text.lower() and "residual_ops_hint" in text

def test_t83_phase_71_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    assert "Phase 71" in path.read_text(encoding="utf-8")

def test_t83_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T83_CLIENT_GUIDE_PEER_COUNT_PLAN.md"
    ).is_file()
    assert "T83" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
