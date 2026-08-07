"""T102 residual closeout: OpenAPI abort_fail_peer_count example."""

from __future__ import annotations

from pathlib import Path


def test_t102_phase_90_honesty() -> None:
    assert "Phase 90" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")


def test_t102_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T102_OPENAPI_PEER_COUNT_EXAMPLE_PLAN.md"
    ).is_file()
    assert "T102" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )


def test_t102_openapi_example() -> None:
    import json

    from mpreg.server_pkg.openapi_surface import build_monitoring_openapi

    blob = json.dumps(build_monitoring_openapi())
    assert "abort_fail_peer_count" in blob
    assert '"example": 1' in blob or '"example":1' in blob
