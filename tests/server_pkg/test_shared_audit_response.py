"""build_audit_response scope contract (PR-A4)."""

from __future__ import annotations

from mpreg.server_pkg.shared_audit import (
    SharedAuditStore,
    build_audit_response,
    record_from_mgmt_entry,
)

def test_default_local_scope() -> None:
    local = [{"event": "drain", "timestamp": 1.0, "success": True}]
    out = build_audit_response(
        store=None,
        local_entries=local,
        scope="local",
        limit=50,
        self_node="n1",
        shared_enabled=False,
    )
    assert out["scope"] == "local"
    assert out["mutation_count"] == 1
    assert out["shared_enabled"] is False

def test_cluster_requires_shared() -> None:
    out = build_audit_response(
        store=None,
        local_entries=[],
        scope="cluster",
        shared_enabled=False,
    )
    assert out["error"] == "shared_audit_disabled"
    assert out["mutations"] == []

def test_cluster_merged_view() -> None:
    store = SharedAuditStore(cluster_id="c1", local_node="n1")
    r = record_from_mgmt_entry(
        event="detach",
        timestamp=1.0,
        actor="a",
        success=True,
        detail={},
        cluster_id="c1",
        origin_node="n2",
    )
    store.insert(r)
    out = build_audit_response(
        store=store,
        local_entries=[],
        scope="cluster",
        shared_enabled=True,
        self_node="n1",
    )
    assert out["scope"] == "cluster"
    assert out["mutation_count"] == 1
    assert out["mutations"][0]["entry_id"] == r.entry_id
    assert "not a SIEM" in out["non_claims"][0]
