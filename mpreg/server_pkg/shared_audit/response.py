"""Single builder for GET /mgmt/v1/audit responses."""

from __future__ import annotations

from typing import Any, Literal

from mpreg.server_pkg.shared_audit.replicator import SharedAuditHealth
from mpreg.server_pkg.shared_audit.store import SharedAuditStore

def build_audit_response(
    *,
    store: SharedAuditStore | None,
    local_entries: list[dict[str, Any]],
    route_records: list[Any] | None = None,
    scope: Literal["local", "cluster"] = "local",
    limit: int = 50,
    origin_node_filter: str | None = None,
    self_node: str = "",
    shared_enabled: bool = False,
    health: SharedAuditHealth | None = None,
) -> dict[str, Any]:
    """Build the canonical audit HTTP/CLI payload.

    Default scope is **local** (process ring / origin mirror). Cluster scope
    requires shared audit enabled and returns the merged G-Set window.
    """
    scope_norm: Literal["local", "cluster"] = (
        "cluster" if str(scope).lower() == "cluster" else "local"
    )
    non_claims = [
        "not a SIEM / infinite retention store",
        "not BFT; G-Set eventual under partition",
        "bounded per-origin watermark window",
        "route decisions remain node-local",
    ]

    if scope_norm == "cluster":
        if not shared_enabled or store is None:
            return {
                "audit_kind": "mgmt_mutations",
                "scope": "cluster",
                "shared_enabled": False,
                "error": "shared_audit_disabled",
                "error_message": (
                    "scope=cluster requires mgmt_audit_shared_enabled "
                    "(SharedAuditStore not active on this node)"
                ),
                "mutations": [],
                "mutation_count": 0,
                "recent_route_decisions": [],
                "non_claims": non_claims,
            }
        records = store.snapshot(
            limit=limit if limit >= 0 else None,  # store handles limit==0
            origin_node=origin_node_filter,
        )
        mutations = [r.to_dict() for r in records]
        return {
            "audit_kind": "mgmt_mutations",
            "scope": "cluster",
            "shared_enabled": True,
            "mutations": mutations,
            "mutation_count": len(mutations),
            "recent_route_decisions": _route_dicts(route_records, limit),
            "health": health.to_dict() if health is not None else None,
            "non_claims": non_claims,
            "self_node": self_node,
        }

    # local scope
    mutations = list(local_entries)
    if origin_node_filter:
        mutations = [
            m
            for m in mutations
            if str(m.get("origin_node") or self_node) == origin_node_filter
        ]
    # Note: mutations[-0:] is the full list in Python — handle limit==0 explicitly.
    if limit == 0:
        mutations = []
    elif limit > 0 and len(mutations) > limit:
        mutations = mutations[-limit:]
    return {
        "audit_kind": "mgmt_mutations",
        "scope": "local",
        "shared_enabled": shared_enabled,
        "mutations": mutations,
        "mutation_count": len(mutations),
        "recent_route_decisions": _route_dicts(route_records, limit),
        "health": health.to_dict() if health is not None else None,
        "non_claims": non_claims if shared_enabled else [],
        "self_node": self_node,
    }

def _route_dicts(route_records: list[Any] | None, limit: int) -> list[dict[str, Any]]:
    if not route_records:
        return []
    out: list[dict[str, Any]] = []
    for r in route_records[: min(limit, 20) if limit >= 0 else 20]:
        if hasattr(r, "to_dict"):
            out.append(r.to_dict())
        elif isinstance(r, dict):
            out.append(r)
    return out
