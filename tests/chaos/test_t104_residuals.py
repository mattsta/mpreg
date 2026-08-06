"""T104 residual closeout: claims.yaml T66-T109 + non_claims."""

from __future__ import annotations

from pathlib import Path

def test_t104_phase_92_honesty() -> None:
    assert "Phase 92" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")

def test_t104_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T104_CLAIMS_PEER_COUNT_CLOSEOUT_PLAN.md"
    ).is_file()
    assert "T104" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )

def test_t104_claims() -> None:
    root = Path(__file__).resolve().parents[2]
    claims = (root / "tests" / "invariants" / "claims.yaml").read_text(encoding="utf-8")
    assert "test_t100_residuals" in claims or "test_t109_residuals" in claims
    assert "doctor JSON" in claims.lower() or "abort_fail_peer_count" in claims

