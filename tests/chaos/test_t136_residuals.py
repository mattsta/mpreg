"""T136 residual closeout: claims.yaml T130–T139."""

from __future__ import annotations

from pathlib import Path


def test_t136_phase_124_honesty() -> None:
    assert "Phase 124" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")


def test_t136_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (root / "docs" / "plans" / "DISTLAB_T136_CLAIMS_T130_T139_PLAN.md").is_file()
    assert "T136" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )


def test_t136_claims() -> None:
    root = Path(__file__).resolve().parents[2]
    c = (root / "tests" / "invariants" / "claims.yaml").read_text(encoding="utf-8")
    assert "test_t130_residuals" in c or "test_t139_residuals" in c
