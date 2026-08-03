"""Management mutation plane: drain, detach, policy apply, and audit log.

HTTP handlers live on FederationMonitoringSystem; this module holds the
server-side mutation logic and a durable-in-process audit ring buffer.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from threading import RLock
from typing import Any

@dataclass(frozen=True, slots=True)
class MgmtAuditEntry:
    """One admin mutation audit record."""

    event: str
    timestamp: float
    actor: str | None
    success: bool
    detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event": self.event,
            "timestamp": self.timestamp,
            "actor": self.actor,
            "success": self.success,
            "detail": dict(self.detail),
        }

@dataclass(slots=True)
class MgmtAuditLog:
    """Process-local ring buffer of management mutations."""

    max_entries: int = 500
    _entries: deque[MgmtAuditEntry] = field(default_factory=deque)
    _lock: RLock = field(default_factory=RLock)

    def record(self, entry: MgmtAuditEntry) -> None:
        with self._lock:
            self._entries.append(entry)
            while len(self._entries) > self.max_entries:
                self._entries.popleft()

    def snapshot(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            entries = list(self._entries)
        if limit is not None and limit >= 0:
            entries = entries[-limit:]
        return [e.to_dict() for e in entries]

def apply_node_drain(
    server: Any,
    *,
    draining: bool = True,
    actor: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    """Mark the local node as draining (or clear drain). Affects /ready."""
    server._mgmt_draining = bool(draining)
    detail = {
        "draining": bool(draining),
        "node": str(getattr(server.settings, "name", "")),
        "cluster_id": str(getattr(server.settings, "cluster_id", "")),
        "reason": reason or ("enter_drain" if draining else "clear_drain"),
    }
    _audit(
        server,
        event="node_drain",
        actor=actor,
        success=True,
        detail=detail,
    )
    return {"applied": True, "draining": bool(draining), "detail": detail}

async def apply_peer_detach(
    server: Any,
    *,
    peer_url: str,
    actor: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    """Detach a peer connection and remove it from the local peer directory."""
    peer_url = str(peer_url or "").strip()
    if not peer_url:
        _audit(
            server,
            event="peer_detach",
            actor=actor,
            success=False,
            detail={"error": "peer_url_required"},
        )
        return {
            "applied": False,
            "error": "peer_url_required",
            "message": "Body must include non-empty peer_url",
        }

    local_url = getattr(getattr(server, "cluster", None), "local_url", None)
    if local_url and peer_url == local_url:
        _audit(
            server,
            event="peer_detach",
            actor=actor,
            success=False,
            detail={"error": "cannot_detach_self", "peer_url": peer_url},
        )
        return {
            "applied": False,
            "error": "cannot_detach_self",
            "peer_url": peer_url,
        }

    closed = False
    if hasattr(server, "_close_peer_connection"):
        await server._close_peer_connection(peer_url)
        closed = True

    directory_removed = False
    peer_directory = getattr(server, "_peer_directory", None)
    if peer_directory is not None and hasattr(peer_directory, "remove_node_by_id"):
        try:
            peer_directory.remove_node_by_id(peer_url)
            directory_removed = True
        except Exception:  # noqa: BLE001 - detach best-effort on directory
            directory_removed = False

    # Dial state so we do not immediately re-dial.
    dial_state = getattr(server, "_peer_dial_state", None)
    if isinstance(dial_state, dict):
        dial_state.pop(peer_url, None)

    detail = {
        "peer_url": peer_url,
        "connection_closed": closed,
        "directory_removed": directory_removed,
        "reason": reason or "mgmt_detach",
    }
    _audit(server, event="peer_detach", actor=actor, success=True, detail=detail)
    return {"applied": True, "detail": detail}

def apply_namespace_policy(
    server: Any,
    body: dict[str, Any],
    *,
    actor: str | None = None,
) -> dict[str, Any]:
    """Apply namespace policy via the same path as the RPC command."""
    if not hasattr(server, "_namespace_policy_apply"):
        _audit(
            server,
            event="policy_apply",
            actor=actor,
            success=False,
            detail={"error": "namespace_policy_unavailable"},
        )
        return {
            "applied": False,
            "error": "namespace_policy_unavailable",
        }

    payload = dict(body)
    if actor and "actor" not in payload:
        payload["actor"] = actor
    result = server._namespace_policy_apply(payload)
    if not isinstance(result, dict):
        result = {"result": result}
    success = bool(result.get("applied", False))
    _audit(
        server,
        event="policy_apply",
        actor=actor or payload.get("actor"),
        success=success,
        detail={
            "rule_count": result.get("rule_count"),
            "valid": result.get("valid"),
            "errors": result.get("errors"),
            "applied": success,
        },
    )
    # Re-bind data planes after apply
    if success and hasattr(server, "_bind_namespace_policy_to_data_planes"):
        server._bind_namespace_policy_to_data_planes()
    return result

def policy_dry_run(server: Any, body: dict[str, Any]) -> dict[str, Any]:
    """Evaluate namespace visibility without mutating policy state."""
    from mpreg.core.namespace_policy import NamespacePolicyEngine, NamespacePolicyRule

    namespace = str(body.get("namespace", "") or "")
    action = str(body.get("action", "query") or "query")
    viewer_cluster = body.get("viewer_cluster_id") or body.get("cluster_id")
    viewer_tenant = body.get("viewer_tenant_id") or body.get("tenant_id")
    write = action.lower() in {"write", "put", "publish", "send", "apply"}

    engine = getattr(server, "_namespace_policy_engine", None)
    if engine is None:
        # Ephemeral engine from settings for dry-run when disabled at runtime
        settings = server.settings
        rules = getattr(settings, "discovery_policy_rules", ()) or ()
        engine = NamespacePolicyEngine(
            enabled=bool(getattr(settings, "discovery_policy_enabled", False)),
            default_allow=bool(
                getattr(settings, "discovery_policy_default_allow", True)
            ),
            rules=tuple(
                r
                if isinstance(r, NamespacePolicyRule)
                else NamespacePolicyRule.from_dict(r)
                for r in rules
            ),
        )

    if not namespace:
        return {
            "dry_run": True,
            "allowed": False,
            "reason": "namespace_required",
            "action": action,
        }

    if write or viewer_tenant or action.lower() in {"data", "data_access"}:
        decision = engine.allows_data_access(
            namespace,
            actor_cluster=str(viewer_cluster) if viewer_cluster else None,
            actor_tenant_id=str(viewer_tenant) if viewer_tenant else None,
            write=write,
        )
    else:
        decision = engine.allows_viewer(
            namespace,
            str(viewer_cluster or getattr(server.settings, "cluster_id", "")),
            viewer_tenant_id=str(viewer_tenant) if viewer_tenant else None,
        )

    return {
        "dry_run": True,
        "namespace": namespace,
        "action": action,
        "allowed": bool(decision.allowed),
        "reason": decision.reason,
        "viewer_cluster_id": viewer_cluster,
        "viewer_tenant_id": viewer_tenant,
        "write": write,
    }

def _audit(
    server: Any,
    *,
    event: str,
    actor: str | None,
    success: bool,
    detail: dict[str, Any],
) -> None:
    log = getattr(server, "_mgmt_audit_log", None)
    if log is None:
        log = MgmtAuditLog()
        server._mgmt_audit_log = log
    log.record(
        MgmtAuditEntry(
            event=event,
            timestamp=time.time(),
            actor=actor,
            success=success,
            detail=detail,
        )
    )
