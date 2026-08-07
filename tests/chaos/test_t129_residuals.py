"""T129 residual closeout: Gate T120–T129."""

from __future__ import annotations

from pathlib import Path


def test_t129_phase_117_honesty() -> None:
    assert "Phase 117" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")


def test_t129_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (root / "docs" / "plans" / "DISTLAB_T129_GATE_T120_T129_PLAN.md").is_file()
    assert "T129" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )


def test_t129_ledger_gate() -> None:
    root = Path(__file__).resolve().parents[2]
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T120" in ledger and "T129" in ledger
    assert "test_t129_residuals" in ledger
