"""T123 residual closeout: OpenAPI last_abort_fail_op_id example."""

from __future__ import annotations

from pathlib import Path


def test_t123_phase_111_honesty() -> None:
    assert "Phase 111" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")


def test_t123_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T123_OPENAPI_OP_ID_EXAMPLE_PLAN.md"
    ).is_file()
    assert "T123" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )


def test_t123_openapi_op_id() -> None:
    import json

    from mpreg.server_pkg.openapi_surface import build_monitoring_openapi

    blob = json.dumps(build_monitoring_openapi())
    assert "last_abort_fail_op_id" in blob
    assert "op-abc123" in blob
