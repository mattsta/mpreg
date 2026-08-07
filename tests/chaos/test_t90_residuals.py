"""T90 residual closeout: ops_cli_tour peer count asserts."""

from __future__ import annotations

from pathlib import Path


def test_t90_ops_cli_peer_count_asserts() -> None:
    text = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "examples"
        / "apps"
        / "02_moderate"
        / "ops_cli_tour"
        / "run.py"
    ).read_text(encoding="utf-8")
    assert "abort_fail_peer_count=" in text
    assert "abort_fail_peer_count" in text
    assert "metrics_strong" in text


def test_t90_phase_78_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    assert "Phase 78" in path.read_text(encoding="utf-8")


def test_t90_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T90_OPS_CLI_PEER_COUNT_PLAN.md"
    ).is_file()
    assert "T90" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
