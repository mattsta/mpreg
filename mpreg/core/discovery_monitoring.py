from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from threading import RLock

from mpreg.core.payloads import (
    PAYLOAD_FLOAT,
    PAYLOAD_INT,
    PAYLOAD_KEEP_EMPTY,
    PAYLOAD_LIST,
    Payload,
    PayloadMapping,
    payload_from_dataclass,
)
from mpreg.datastructures.type_aliases import (
    ClusterId,
    NamespaceName,
    TenantId,
    Timestamp,
)

from .namespace_policy import NamespacePolicyAuditEntry, NamespacePolicyRule


@dataclass(frozen=True, slots=True)
class DiscoveryAccessAuditEntry:
    event: str
    timestamp: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    viewer_cluster_id: ClusterId
    viewer_tenant_id: TenantId | None
    namespace: NamespaceName | None
    allowed: bool
    reason: str

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> DiscoveryAccessAuditEntry:
        return cls(
            event=str(payload.get("event", "")),
            timestamp=float(payload.get("timestamp", 0.0) or 0.0),
            viewer_cluster_id=str(payload.get("viewer_cluster_id", "")),
            viewer_tenant_id=(
                str(payload.get("viewer_tenant_id"))
                if payload.get("viewer_tenant_id") is not None
                else None
            ),
            namespace=(
                str(payload.get("namespace"))
                if payload.get("namespace") is not None
                else None
            ),
            allowed=bool(payload.get("allowed", False)),
            reason=str(payload.get("reason", "")),
        )


@dataclass(frozen=True, slots=True)
class DiscoveryAccessAuditRequest:
    limit: int | None = field(default=None, metadata={PAYLOAD_INT: True})

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> DiscoveryAccessAuditRequest:
        data = payload or {}
        return cls(
            limit=int(data.get("limit")) if data.get("limit") is not None else None
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(slots=True)
class DiscoveryAccessAuditLog:
    max_entries: int = 500
    _entries: deque[DiscoveryAccessAuditEntry] = field(default_factory=deque)
    _lock: RLock = field(default_factory=RLock)

    def record(self, entry: DiscoveryAccessAuditEntry) -> None:
        with self._lock:
            self._entries.append(entry)
            while len(self._entries) > self.max_entries:
                self._entries.popleft()

    def snapshot(
        self, *, limit: int | None = None
    ) -> tuple[DiscoveryAccessAuditEntry, ...]:
        with self._lock:
            entries = list(self._entries)
        if limit is not None and limit >= 0:
            entries = entries[-limit:]
        return tuple(entries)


@dataclass(frozen=True, slots=True)
class DiscoveryPolicyStatus:
    enabled: bool
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    default_allow: bool
    rule_count: int = field(metadata={PAYLOAD_INT: True})
    rules: tuple[NamespacePolicyRule, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    audit_entries: tuple[NamespacePolicyAuditEntry, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    audit_total: int = field(metadata={PAYLOAD_INT: True})
    access_entries: tuple[DiscoveryAccessAuditEntry, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    access_total: int = field(metadata={PAYLOAD_INT: True})

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class DiscoveryAccessAuditResponse:
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    entries: tuple[DiscoveryAccessAuditEntry, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> DiscoveryAccessAuditResponse:
        entries_payload = payload.get("entries", []) or []
        entries = tuple(
            DiscoveryAccessAuditEntry.from_dict(entry)
            for entry in entries_payload
            if isinstance(entry, dict)
        )
        return cls(
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            entries=entries,
        )


@dataclass(frozen=True, slots=True)
class DiscoveryLagStatus:
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    resolver_enabled: bool
    summary_export_enabled: bool
    last_delta_at: Timestamp | None = field(
        default=None, metadata={PAYLOAD_FLOAT: True}
    )
    delta_lag_seconds: float | None = field(
        default=None, metadata={PAYLOAD_FLOAT: True}
    )
    last_seed_at: Timestamp | None = field(default=None, metadata={PAYLOAD_FLOAT: True})
    last_summary_export_at: Timestamp | None = field(
        default=None, metadata={PAYLOAD_FLOAT: True}
    )
    summary_export_lag_seconds: float | None = field(
        default=None, metadata={PAYLOAD_FLOAT: True}
    )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)
