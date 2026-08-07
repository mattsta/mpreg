"""T124 residual closeout: OpenAPI last_abort_fail_peers example."""

from __future__ import annotations

from pathlib import Path


def test_t124_phase_112_honesty() -> None:
    assert "Phase 112" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")


def test_t124_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T124_OPENAPI_PEERS_EXAMPLE_PLAN.md"
    ).is_file()
    assert "T124" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )


def test_t124_openapi_peers() -> None:
    import json

    from mpreg.server_pkg.openapi_surface import build_monitoring_openapi

    blob = json.dumps(build_monitoring_openapi())
    assert "last_abort_fail_peers" in blob
    assert "9001" in blob or "ws://" in blob
