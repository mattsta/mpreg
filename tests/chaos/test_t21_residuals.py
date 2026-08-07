"""T21 residual closeout: OpenAPI STRONG/audit honesty schemas."""

from __future__ import annotations

from mpreg.server_pkg.openapi_surface import (
    build_monitoring_openapi,
    openapi_path_set,
    route_table_path_set,
)

def test_t21_openapi_strong_schema_honesty() -> None:
    """OpenAPI documents put-only capabilities and refuse counters."""
    doc = build_monitoring_openapi()
    schemas = (doc.get("components") or {}).get("schemas") or {}
    assert "StrongMetricsResponse" in schemas
    assert "SharedAuditMetricsResponse" in schemas

    strong = schemas["StrongMetricsResponse"]
    # Walk to capabilities.get_quorum enum [false]
    props = strong.get("properties") or {}
    strong_body = (props.get("strong") or {}).get("properties") or {}
    caps = (strong_body.get("capabilities") or {}).get("properties") or {}
    assert "get_quorum" in caps
    assert caps["get_quorum"].get("enum") == [False]
    assert caps["delete_quorum"].get("enum") == [False]
    counters_desc = str((strong_body.get("counters") or {}).get("description") or "")
    assert "gets_refused" in counters_desc
    assert "deletes_refused" in counters_desc

    # Paths reference schemas
    paths = doc.get("paths") or {}
    for p in ("/metrics/strong", "/mgmt/v1/strong"):
        resp = paths[p]["get"]["responses"]["200"]
        ref = resp["content"]["application/json"]["schema"]["$ref"]
        assert ref.endswith("StrongMetricsResponse")
    audit_ref = paths["/metrics/shared-audit"]["get"]["responses"]["200"][
        "content"
    ]["application/json"]["schema"]["$ref"]
    assert audit_ref.endswith("SharedAuditMetricsResponse")

    # Description honesty
    desc = paths["/metrics/strong"]["get"]["description"]
    assert "WAN" in desc or "not a WAN" in desc.lower() or "Not a WAN" in desc
    assert "1012" in desc or "v1.1" in desc

    tags = {t["name"]: t for t in (doc.get("tags") or []) if isinstance(t, dict)}
    assert "strong" in tags
    assert "audit" in tags
    assert "v1.1" in tags["strong"]["description"] or "get" in tags["strong"][
        "description"
    ].lower()

def test_t21_openapi_still_matches_route_table() -> None:
    assert openapi_path_set() == route_table_path_set()
    assert "/metrics/strong" in openapi_path_set()
    assert "/metrics/shared-audit" in openapi_path_set()
