"""Peer-side STRONG prepare/commit/abort handlers bound to a local backend."""

from __future__ import annotations

from typing import Any

from mpreg.core.cache_models import CacheMetadata, GlobalCacheKey
from mpreg.core.cache_protocol import CacheKeyMessage
from mpreg.core.cache_strong import (
    CommitAck,
    PrepareAck,
    StrongLocalBackend,
    StrongVersion,
)

def key_from_payload(raw: dict[str, Any] | None) -> GlobalCacheKey | None:
    if not isinstance(raw, dict):
        return None
    try:
        return CacheKeyMessage.from_dict(raw).to_global_cache_key()
    except Exception:  # noqa: BLE001
        try:
            return GlobalCacheKey(
                namespace=str(raw.get("namespace") or ""),
                identifier=str(raw.get("identifier") or raw.get("key") or ""),
                version=str(raw.get("version") or "v1.0.0"),
            )
        except Exception:  # noqa: BLE001
            return None

def key_to_payload(key: GlobalCacheKey) -> dict[str, Any]:
    return CacheKeyMessage.from_global_cache_key(key).to_dict()

class StrongPeerHandler:
    """Dispatches STRONG_* request payloads against a :class:`StrongLocalBackend`."""

    def __init__(
        self,
        backend: StrongLocalBackend,
        *,
        cluster_id: str = "",
        pending_ttl_s: float = 30.0,
    ) -> None:
        self.backend = backend
        self.cluster_id = cluster_id
        self.pending_ttl_s = pending_ttl_s

    async def handle_prepare(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.cluster_id and str(payload.get("cluster_id") or "") not in (
            "",
            self.cluster_id,
        ):
            return _prep_ack(self.backend.node_id, False, "cluster_mismatch")
        key = key_from_payload(payload.get("key") if isinstance(payload.get("key"), dict) else None)
        if key is None:
            return _prep_ack(self.backend.node_id, False, "bad_key")
        sv = StrongVersion.from_dict(payload.get("strong_version"))
        if sv is None:
            return _prep_ack(self.backend.node_id, False, "bad_version")
        replica_set = tuple(str(x) for x in (payload.get("replica_set") or ()))
        quorum = int(payload.get("quorum") or 0)
        meta_raw = payload.get("metadata")
        metadata = CacheMetadata()
        if isinstance(meta_raw, dict):
            metadata = CacheMetadata(
                access_patterns=dict(meta_raw.get("access_patterns") or {}),
                created_by=str(meta_raw.get("created_by") or ""),
                ttl_seconds=meta_raw.get("ttl_seconds"),
            )
        ack = await self.backend.prepare(
            key=key,
            value=payload.get("value"),
            metadata=metadata,
            strong_version=sv,
            replica_set=replica_set,
            quorum=quorum,
            ttl_s=float(payload.get("pending_ttl_s") or self.pending_ttl_s),
        )
        return {
            "node_id": ack.node_id,
            "ok": ack.ok,
            "reason": ack.reason,
            "request_id": payload.get("request_id", ""),
        }

    async def handle_commit(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.cluster_id and str(payload.get("cluster_id") or "") not in (
            "",
            self.cluster_id,
        ):
            return _commit_ack(self.backend.node_id, False, False, "cluster_mismatch")
        key = key_from_payload(payload.get("key") if isinstance(payload.get("key"), dict) else None)
        op_id = str(payload.get("op_id") or "")
        if key is None or not op_id:
            return _commit_ack(self.backend.node_id, False, False, "bad_request")
        ack = await self.backend.commit(op_id=op_id, key=key)
        return {
            "node_id": ack.node_id,
            "ok": ack.ok,
            "applied": ack.applied,
            "reason": ack.reason,
            "request_id": payload.get("request_id", ""),
        }

    async def handle_abort(self, payload: dict[str, Any]) -> dict[str, Any]:
        key = key_from_payload(payload.get("key") if isinstance(payload.get("key"), dict) else None)
        op_id = str(payload.get("op_id") or "")
        if key is None or not op_id:
            return {
                "node_id": self.backend.node_id,
                "ok": False,
                "request_id": payload.get("request_id", ""),
            }
        ok = await self.backend.abort(op_id=op_id, key=key)
        return {
            "node_id": self.backend.node_id,
            "ok": ok,
            "request_id": payload.get("request_id", ""),
        }

def _prep_ack(node_id: str, ok: bool, reason: str = "") -> dict[str, Any]:
    return {"node_id": node_id, "ok": ok, "reason": reason}

def _commit_ack(
    node_id: str, ok: bool, applied: bool, reason: str = ""
) -> dict[str, Any]:
    return {"node_id": node_id, "ok": ok, "applied": applied, "reason": reason}

def prepare_ack_from_dict(raw: dict[str, Any]) -> PrepareAck:
    return PrepareAck(
        node_id=str(raw.get("node_id") or ""),
        ok=bool(raw.get("ok")),
        reason=str(raw.get("reason") or ""),
    )

def commit_ack_from_dict(raw: dict[str, Any]) -> CommitAck:
    return CommitAck(
        node_id=str(raw.get("node_id") or ""),
        ok=bool(raw.get("ok")),
        applied=bool(raw.get("applied")),
        reason=str(raw.get("reason") or ""),
    )
