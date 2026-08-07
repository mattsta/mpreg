"""T76 residual closeout: OpenAPI documents prom residual gauge."""

from __future__ import annotations

from pathlib import Path

from mpreg.server_pkg.openapi_surface import build_monitoring_openapi


def test_t76_openapi_strong_mentions_abort_fail_peers_gauge() -> None:
    doc = build_monitoring_openapi()
    strong = (doc.get("paths") or {}).get("/metrics/strong") or {}
    desc = str((strong.get("get") or {}).get("description") or "").lower()
    assert "abort_fail_peers" in desc or "mpreg_strong_abort_fail_peers" in desc
    assert "residual_ops_hint" in desc


def test_t76_phase_64_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    assert "Phase 64" in path.read_text(encoding="utf-8")


def test_t76_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T76_OPENAPI_PROM_RESIDUAL_PLAN.md"
    ).is_file()
    assert "T76" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
