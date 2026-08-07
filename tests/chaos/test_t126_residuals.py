"""T126 residual closeout: OpenAPI residual examples unit."""

from __future__ import annotations

from pathlib import Path


def test_t126_phase_114_honesty() -> None:
    assert "Phase 114" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")


def test_t126_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T126_OPENAPI_RESIDUAL_EXAMPLES_UNIT_PLAN.md"
    ).is_file()
    assert "T126" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )


def test_t126_openapi_unit() -> None:
    root = Path(__file__).resolve().parents[2]
    text = (root / "tests" / "test_cli_strong_audit_monitor.py").read_text(
        encoding="utf-8"
    )
    assert "test_openapi_abort_fail_peer_count_example" in text
    assert "last_abort_fail_op_id" in text
    assert "last_abort_fail_peers" in text
