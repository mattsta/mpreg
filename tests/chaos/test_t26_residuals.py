"""T26 residual closeout: coexistence honesty contract still documented."""

from __future__ import annotations

from pathlib import Path

from mpreg.server_pkg.openapi_surface import (
    build_monitoring_openapi,
    openapi_path_set,
    route_table_path_set,
)
from mpreg.testing.distlab.registry import resolve_preset


def test_t26_openapi_both_schemas_present() -> None:
    doc = build_monitoring_openapi()
    schemas = (doc.get("components") or {}).get("schemas") or {}
    assert "StrongMetricsResponse" in schemas
    assert "SharedAuditMetricsResponse" in schemas
    strong_caps = (
        (
            (schemas["StrongMetricsResponse"].get("properties") or {}).get("strong")
            or {}
        ).get("properties")
        or {}
    ).get("capabilities") or {}
    audit_caps = (
        (
            (schemas["SharedAuditMetricsResponse"].get("properties") or {}).get(
                "shared_audit"
            )
            or {}
        ).get("properties")
        or {}
    ).get("capabilities") or {}
    assert (strong_caps.get("properties") or {}).get("get_quorum", {}).get("enum") == [
        False
    ]
    assert (audit_caps.get("properties") or {}).get("siem", {}).get("enum") == [False]


def test_t26_openapi_matches_route_table() -> None:
    assert openapi_path_set() == route_table_path_set()


def test_t26_ci_core_still_covers_both_tracks() -> None:
    names = resolve_preset("ci-core")
    assert any(n.startswith("strong.") for n in names)
    assert any(n.startswith("audit.") for n in names)
    assert "strong.refuse_get_delete" in names


def test_t26_operate_mentions_ci_core_and_caps() -> None:
    root = Path(__file__).resolve().parents[2]
    operate = (root / "docs/examples-curriculum/OPERATE.md").read_text(encoding="utf-8")
    assert "ci-core" in operate
    assert "mpreg_strong_cap_" in operate or "mpreg_shared_audit_cap_" in operate
    assert "SharedAuditMetricsResponse" in operate
