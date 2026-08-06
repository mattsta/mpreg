"""T127 residual closeout: claims.yaml T120–T129."""

from __future__ import annotations

from pathlib import Path

def test_t127_phase_115_honesty() -> None:
    assert "Phase 115" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")

def test_t127_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T127_CLAIMS_T120_T129_PLAN.md"
    ).is_file()
    assert "T127" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )

def test_t127_claims() -> None:
    root = Path(__file__).resolve().parents[2]
    c = (root / "tests" / "invariants" / "claims.yaml").read_text(encoding="utf-8")
    assert "test_t120_residuals" in c or "test_t129_residuals" in c

