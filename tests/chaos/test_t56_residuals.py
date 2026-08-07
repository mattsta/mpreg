"""T56 residual closeout: strong-core membership for retry_abort ops surface."""

from __future__ import annotations

from pathlib import Path

from mpreg.testing.distlab.builtins import ensure_builtins
from mpreg.testing.distlab.registry import resolve_preset

def test_t56_strong_core_retry_abort_membership() -> None:
    ensure_builtins()
    required = {
        "strong.cft_retry_abort_clears_residual",
        "strong.cft_retry_abort_self_target",
        "strong.cft_gcm_retry_abort_clears_residual",
    }
    for preset in ("strong-core", "ci-core"):
        names = set(resolve_preset(preset))
        missing = required - names
        assert not missing, f"{preset} missing {missing}"

def test_t56_phase_44_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 44" in text
    assert "strong-core" in text

def test_t56_ledger_and_plan() -> None:
    root = Path(__file__).resolve().parents[2]
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T56" in ledger
    assert "T55" in ledger
    assert (
        root / "docs" / "plans" / "DISTLAB_T56_STRONG_CORE_MEMBERSHIP_PLAN.md"
    ).is_file()

def test_t56_claims() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "invariants"
        / "claims.yaml"
    )
    text = path.read_text(encoding="utf-8")
    assert "strong-core" in text or "T56" in text or "membership" in text
