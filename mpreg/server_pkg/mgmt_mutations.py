"""Management mutation plane: drain, detach, policy apply, and audit log.

HTTP handlers live on FederationMonitoringSystem; this module holds the
server-side mutation logic and a durable-in-process audit ring buffer.
Optional JSONL append path (``persist_path``) survives process restart for
ops forensics; the in-memory ring remains the primary /mgmt/v1/audit source.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any

from mpreg.core.native_codec import JSONDecodeError, dumps_text, loads_text


@dataclass(frozen=True, slots=True)
class MgmtAuditEntry:
    """One admin mutation audit record."""

    event: str
    timestamp: float
    actor: str | None
    success: bool
    detail: dict[str, Any] = field(default_factory=dict)
    # Additive optional fields for shared-audit schema alignment
    entry_id: str | None = None
    origin_node: str | None = None
    cluster_id: str | None = None
    schema_version: int | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "event": self.event,
            "timestamp": self.timestamp,
            "actor": self.actor,
            "success": self.success,
            "detail": dict(self.detail),
        }
        if self.entry_id is not None:
            out["entry_id"] = self.entry_id
        if self.origin_node is not None:
            out["origin_node"] = self.origin_node
        if self.cluster_id is not None:
            out["cluster_id"] = self.cluster_id
        if self.schema_version is not None:
            out["schema_version"] = self.schema_version
        return out


@dataclass(slots=True)
class MgmtAuditLog:
    """Process-local ring buffer of management mutations (+ optional JSONL)."""

    max_entries: int = 500
    persist_path: str | None = None
    _entries: deque[MgmtAuditEntry] = field(default_factory=deque)
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        if self.persist_path:
            self._load_from_disk()

    def _load_from_disk(self) -> None:
        path = Path(self.persist_path) if self.persist_path else None
        if path is None or not path.is_file():
            return
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return
        loaded: list[MgmtAuditEntry] = []
        for line in lines[-self.max_entries :]:
            line = line.strip()
            if not line:
                continue
            try:
                raw = loads_text(line)
            except JSONDecodeError:
                continue
            if not isinstance(raw, dict):
                continue
            loaded.append(
                MgmtAuditEntry(
                    event=str(raw.get("event") or "unknown"),
                    timestamp=float(raw.get("timestamp") or 0.0),
                    actor=raw.get("actor"),
                    success=bool(raw.get("success", False)),
                    detail=dict(raw.get("detail") or {}),
                )
            )
        with self._lock:
            self._entries.extend(loaded)
            while len(self._entries) > self.max_entries:
                self._entries.popleft()

    def _append_disk(self, entry: MgmtAuditEntry) -> None:
        if not self.persist_path:
            return
        path = Path(self.persist_path)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(dumps_text(entry.to_dict()) + "\n")
        except OSError:
            pass

    def record(self, entry: MgmtAuditEntry) -> None:
        with self._lock:
            self._entries.append(entry)
            while len(self._entries) > self.max_entries:
                self._entries.popleft()
        self._append_disk(entry)

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
    ts = time.time()
    entry_id: str | None = None
    origin_node: str | None = None
    cluster_id: str | None = None
    schema_version: int | None = None

    # Shared audit path (opt-in): SharedAuditStore is authority; local ring mirrors origin.
    shared_store = getattr(server, "_shared_audit_store", None)
    shared_rep = getattr(server, "_shared_audit_replicator", None)
    if shared_store is not None:
        from mpreg.server_pkg.shared_audit.models import record_from_mgmt_entry

        settings = getattr(server, "settings", None)
        cluster_id = str(getattr(settings, "cluster_id", "") or "default-cluster")
        origin_url = ""
        try:
            origin_url = str(server.cluster.local_url)
        except Exception:  # noqa: BLE001
            origin_url = ""
        # Prefer stable peer URL as origin_node for watermark/peer identity.
        origin_node = origin_url or str(getattr(settings, "name", "") or "local")
        rec = record_from_mgmt_entry(
            event=event,
            timestamp=ts,
            actor=actor,
            success=success,
            detail=detail,
            cluster_id=cluster_id,
            origin_node=origin_node,
            origin_url=origin_url,
        )
        stored = shared_store.insert_and_persist(rec)
        if stored is not None:
            entry_id = stored.entry_id
            schema_version = stored.schema_version
            if shared_rep is not None:
                shared_rep.publish(stored)

    log.record(
        MgmtAuditEntry(
            event=event,
            timestamp=ts,
            actor=actor,
            success=success,
            detail=detail,
            entry_id=entry_id,
            origin_node=origin_node,
            cluster_id=cluster_id,
            schema_version=schema_version,
        )
    )
    tracker = getattr(server, "_metrics_tracker", None)
    if tracker is not None and hasattr(tracker, "record_mgmt_mutation"):
        tracker.record_mgmt_mutation(event, success=success)
    if (
        event == "node_drain"
        and tracker is not None
        and hasattr(tracker, "set_draining")
    ):
        tracker.set_draining(bool(detail.get("draining", False)))
