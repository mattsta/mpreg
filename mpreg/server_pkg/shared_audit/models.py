"""Shared audit record model and deterministic merge helpers."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

import orjson
import ulid


def stable_canonical_json(obj: Any) -> str:
    """Deterministic JSON for merge conflict resolution (bytewise-min wins)."""
    return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS).decode("utf-8")


def mint_entry_id() -> str:
    """Mint a new ULID string for an origin audit record."""
    return str(ulid.new())


def entry_sort_key(timestamp: float, entry_id: str) -> tuple[float, str]:
    """Total order: timestamp ascending, then entry_id lexicographic."""
    return (float(timestamp), str(entry_id))


@dataclass(frozen=True, slots=True)
class SharedAuditRecord:
    """Cluster-replicated audit record. Identity is (cluster_id, entry_id)."""

    schema_version: int
    entry_id: str
    cluster_id: str
    origin_node: str
    origin_url: str
    event: str
    timestamp: float
    actor: str | None
    success: bool
    detail: dict[str, Any] = field(default_factory=dict)
    gossip_eligible: bool = True

    def identity(self) -> tuple[str, str]:
        return (self.cluster_id, self.entry_id)

    def sort_key(self) -> tuple[float, str]:
        return entry_sort_key(self.timestamp, self.entry_id)

    def payload_for_merge(self) -> dict[str, Any]:
        """Canonical payload fields used for conflict comparison."""
        return {
            "schema_version": self.schema_version,
            "entry_id": self.entry_id,
            "cluster_id": self.cluster_id,
            "origin_node": self.origin_node,
            "origin_url": self.origin_url,
            "event": self.event,
            "timestamp": self.timestamp,
            "actor": self.actor,
            "success": self.success,
            "detail": self.detail,
            "gossip_eligible": self.gossip_eligible,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "entry_id": self.entry_id,
            "cluster_id": self.cluster_id,
            "origin_node": self.origin_node,
            "origin_url": self.origin_url,
            "event": self.event,
            "timestamp": self.timestamp,
            "actor": self.actor,
            "success": self.success,
            "detail": dict(self.detail),
            "gossip_eligible": self.gossip_eligible,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> SharedAuditRecord:
        return cls(
            schema_version=int(raw.get("schema_version") or 1),
            entry_id=str(raw.get("entry_id") or ""),
            cluster_id=str(raw.get("cluster_id") or ""),
            origin_node=str(raw.get("origin_node") or ""),
            origin_url=str(raw.get("origin_url") or ""),
            event=str(raw.get("event") or "unknown"),
            timestamp=float(raw.get("timestamp") or 0.0),
            actor=raw.get("actor"),
            success=bool(raw.get("success", False)),
            detail=dict(raw.get("detail") or {}),
            gossip_eligible=bool(raw.get("gossip_eligible", True)),
        )


def merge_records(
    a: SharedAuditRecord, b: SharedAuditRecord
) -> tuple[SharedAuditRecord, bool]:
    """Merge same-identity records.

    Returns ``(winner, conflict)`` where ``conflict`` is True when payloads
    differed and bytewise-min of canonical JSON selected the winner.
    """
    if a.identity() != b.identity():
        raise ValueError(
            f"cannot merge distinct identities {a.identity()!r} vs {b.identity()!r}"
        )
    if a.cluster_id != b.cluster_id:
        raise ValueError("cross-cluster_id merge rejected")
    ca = stable_canonical_json(a.payload_for_merge())
    cb = stable_canonical_json(b.payload_for_merge())
    if ca == cb:
        return a, False
    winner = a if ca < cb else b
    return winner, True


def record_from_mgmt_entry(
    *,
    event: str,
    timestamp: float,
    actor: str | None,
    success: bool,
    detail: dict[str, Any],
    cluster_id: str,
    origin_node: str,
    origin_url: str = "",
    entry_id: str | None = None,
    schema_version: int = 1,
    gossip_eligible: bool = True,
) -> SharedAuditRecord:
    """Build a :class:`SharedAuditRecord` from local mutation fields."""
    return SharedAuditRecord(
        schema_version=schema_version,
        entry_id=entry_id or mint_entry_id(),
        cluster_id=cluster_id,
        origin_node=origin_node,
        origin_url=origin_url,
        event=event,
        timestamp=timestamp,
        actor=actor,
        success=success,
        detail=dict(detail),
        gossip_eligible=gossip_eligible,
    )


def legacy_synthetic_id(legacy_fields: dict[str, Any]) -> str:
    """Stable non-gossip id for pre-schema JSONL lines."""
    digest = hashlib.sha256(
        stable_canonical_json(legacy_fields).encode("utf-8")
    ).hexdigest()[:32]
    return f"legacy:{digest}"
