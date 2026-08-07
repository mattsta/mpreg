"""T134 residual closeout: README residual doctor JSON."""

from __future__ import annotations

from pathlib import Path


def test_t134_phase_122_honesty() -> None:
    assert "Phase 122" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")


def test_t134_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T134_README_RESIDUAL_DOCTOR_PLAN.md"
    ).is_file()
    assert "T134" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )


def test_t134_readme() -> None:
    root = Path(__file__).resolve().parents[2]
    text = (root / "README.md").read_text(encoding="utf-8")
    assert "abort_fail_peer_count" in text
    assert "not" in text.lower() and "auto-heal" in text.lower()
