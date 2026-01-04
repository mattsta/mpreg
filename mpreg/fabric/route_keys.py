"""Key registry for route announcement signature verification."""

from __future__ import annotations

import hashlib
import inspect
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field, replace
from typing import Protocol

from mpreg.datastructures.blockchain_types import PublicKey
from mpreg.datastructures.type_aliases import (
    ClusterId,
    DurationSeconds,
    RouteKeyId,
    Timestamp,
)


def derive_route_key_id(public_key: PublicKey) -> RouteKeyId:
    """Derive a stable key id for a public key."""
    digest = hashlib.sha256(public_key).hexdigest()
    return f"route_key_{digest[:16]}"


def _as_float(value: object, default: float) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float, str)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    return default


def _as_key_records(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


@dataclass(frozen=True, slots=True)
class RouteKeyRecord:
    """Track a public key and its validity window."""

    key_id: RouteKeyId
    public_key: PublicKey
    added_at: Timestamp
    expires_at: Timestamp | None = None

    def is_active(self, now: Timestamp | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return self.expires_at is None or timestamp < self.expires_at

    def to_dict(self) -> dict[str, object]:
        return {
            "key_id": self.key_id,
            "public_key": self.public_key.hex(),
            "added_at": float(self.added_at),
            "expires_at": float(self.expires_at)
            if self.expires_at is not None
            else None,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RouteKeyRecord:
        public_key_hex = str(payload.get("public_key", ""))
        try:
            public_key = bytes.fromhex(public_key_hex) if public_key_hex else b""
        except ValueError:
            public_key = b""
        expires_at = payload.get("expires_at")
        return cls(
            key_id=str(payload.get("key_id", "")),
            public_key=public_key,
            added_at=_as_float(payload.get("added_at"), time.time()),
            expires_at=_as_float(expires_at, time.time())
            if expires_at is not None
            else None,
        )


@dataclass(frozen=True, slots=True)
class RouteKeyAnnouncement:
    """Gossip payload for distributing route verification keys."""

    cluster_id: ClusterId
    keys: tuple[RouteKeyRecord, ...] = field(default_factory=tuple)
    primary_key_id: RouteKeyId | None = None
    advertised_at: Timestamp = field(default_factory=time.time)
    ttl_seconds: DurationSeconds = 120.0

    def is_expired(self, now: Timestamp | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.advertised_at + self.ttl_seconds)

    def to_dict(self) -> dict[str, object]:
        return {
            "cluster_id": self.cluster_id,
            "keys": [record.to_dict() for record in self.keys],
            "primary_key_id": self.primary_key_id,
            "advertised_at": float(self.advertised_at),
            "ttl_seconds": float(self.ttl_seconds),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RouteKeyAnnouncement:
        keys_payload = _as_key_records(payload.get("keys", []))
        return cls(
            cluster_id=str(payload.get("cluster_id", "")),
            keys=tuple(RouteKeyRecord.from_dict(item) for item in keys_payload),
            primary_key_id=(
                str(payload.get("primary_key_id"))
                if payload.get("primary_key_id") is not None
                else None
            ),
            advertised_at=_as_float(payload.get("advertised_at"), time.time()),
            ttl_seconds=_as_float(payload.get("ttl_seconds"), 120.0),
        )


@dataclass(slots=True)
class RouteKeySet:
    """Manage active and rotated keys for a single cluster."""

    keys: dict[RouteKeyId, RouteKeyRecord] = field(default_factory=dict)
    primary_key_id: RouteKeyId | None = None

    def register_key(
        self,
        *,
        public_key: PublicKey,
        key_id: RouteKeyId | None = None,
        make_primary: bool = True,
        expires_at: Timestamp | None = None,
        now: Timestamp | None = None,
    ) -> RouteKeyRecord:
        timestamp = now if now is not None else time.time()
        resolved_key_id = key_id or derive_route_key_id(public_key)
        record = RouteKeyRecord(
            key_id=resolved_key_id,
            public_key=public_key,
            added_at=timestamp,
            expires_at=expires_at,
        )
        self.keys[resolved_key_id] = record
        if make_primary:
            self.primary_key_id = resolved_key_id
        return record

    def rotate_key(
        self,
        *,
        public_key: PublicKey,
        overlap_seconds: DurationSeconds = 60.0,
        now: Timestamp | None = None,
    ) -> RouteKeyRecord:
        timestamp = now if now is not None else time.time()
        if self.primary_key_id and self.primary_key_id in self.keys:
            existing = self.keys[self.primary_key_id]
            self.keys[self.primary_key_id] = replace(
                existing, expires_at=timestamp + overlap_seconds
            )
        return self.register_key(
            public_key=public_key,
            make_primary=True,
            now=timestamp,
        )

    def purge_expired(self, *, now: Timestamp | None = None) -> None:
        timestamp = now if now is not None else time.time()
        expired = [
            key_id
            for key_id, record in self.keys.items()
            if not record.is_active(timestamp)
        ]
        for key_id in expired:
            self.keys.pop(key_id, None)
            if key_id == self.primary_key_id:
                self.primary_key_id = None

    def active_records(
        self, *, now: Timestamp | None = None
    ) -> tuple[RouteKeyRecord, ...]:
        timestamp = now if now is not None else time.time()
        active = [
            record for record in self.keys.values() if record.is_active(timestamp)
        ]
        active.sort(
            key=lambda record: (record.key_id != self.primary_key_id, record.key_id)
        )
        return tuple(active)

    def resolve_keys(self, *, now: Timestamp | None = None) -> tuple[PublicKey, ...]:
        return tuple(record.public_key for record in self.active_records(now=now))


@dataclass(slots=True)
class RouteKeyRegistry:
    """Registry for cluster route keys with rotation support."""

    key_sets: dict[ClusterId, RouteKeySet] = field(default_factory=dict)

    def register_key(
        self,
        *,
        cluster_id: ClusterId,
        public_key: PublicKey,
        key_id: RouteKeyId | None = None,
        make_primary: bool = True,
        expires_at: Timestamp | None = None,
        now: Timestamp | None = None,
    ) -> RouteKeyRecord:
        key_set = self.key_sets.setdefault(cluster_id, RouteKeySet())
        return key_set.register_key(
            public_key=public_key,
            key_id=key_id,
            make_primary=make_primary,
            expires_at=expires_at,
            now=now,
        )

    def rotate_key(
        self,
        *,
        cluster_id: ClusterId,
        public_key: PublicKey,
        overlap_seconds: DurationSeconds = 60.0,
        now: Timestamp | None = None,
    ) -> RouteKeyRecord:
        key_set = self.key_sets.setdefault(cluster_id, RouteKeySet())
        return key_set.rotate_key(
            public_key=public_key,
            overlap_seconds=overlap_seconds,
            now=now,
        )

    def resolve_public_keys(
        self, cluster_id: ClusterId, *, now: Timestamp | None = None
    ) -> tuple[PublicKey, ...]:
        key_set = self.key_sets.get(cluster_id)
        if not key_set:
            return ()
        return key_set.resolve_keys(now=now)

    def apply_announcement(
        self, announcement: RouteKeyAnnouncement, *, now: Timestamp | None = None
    ) -> int:
        """Apply a route key announcement to the registry.

        Returns the number of keys updated.
        """
        timestamp = now if now is not None else time.time()
        if announcement.is_expired(timestamp):
            return 0
        key_set = self.key_sets.setdefault(announcement.cluster_id, RouteKeySet())
        updated = 0
        for record in announcement.keys:
            key_set.keys[record.key_id] = record
            updated += 1
        if announcement.primary_key_id in key_set.keys:
            key_set.primary_key_id = announcement.primary_key_id
        key_set.purge_expired(now=timestamp)
        return updated

    def purge_expired(self, *, now: Timestamp | None = None) -> None:
        timestamp = now if now is not None else time.time()
        for cluster_id, key_set in list(self.key_sets.items()):
            key_set.purge_expired(now=timestamp)
            if not key_set.keys:
                self.key_sets.pop(cluster_id, None)

    def resolver(
        self, *, now: Timestamp | None = None
    ) -> Callable[[ClusterId], tuple[PublicKey, ...]]:
        def _resolver(cluster_id: ClusterId) -> tuple[PublicKey, ...]:
            return self.resolve_public_keys(cluster_id, now=now)

        return _resolver

    def to_dict(self, *, now: Timestamp | None = None) -> dict[str, object]:
        timestamp = now if now is not None else time.time()
        clusters: dict[str, dict[str, object]] = {}
        for cluster_id, key_set in self.key_sets.items():
            key_set.purge_expired(now=timestamp)
            if not key_set.keys:
                continue
            clusters[str(cluster_id)] = {
                "primary_key_id": key_set.primary_key_id,
                "keys": [record.to_dict() for record in key_set.keys.values()],
            }
        return {"generated_at": float(timestamp), "clusters": clusters}

    def load_from_dict(
        self, payload: dict[str, object], *, now: Timestamp | None = None
    ) -> int:
        timestamp = now if now is not None else time.time()
        clusters_payload = payload.get("clusters", {})
        self.key_sets.clear()
        updated = 0
        if isinstance(clusters_payload, dict):
            for cluster_id, cluster_data in clusters_payload.items():
                if not isinstance(cluster_data, dict):
                    continue
                key_set = RouteKeySet()
                keys_payload = cluster_data.get("keys", [])
                if isinstance(keys_payload, list):
                    for item in keys_payload:
                        if not isinstance(item, dict):
                            continue
                        record = RouteKeyRecord.from_dict(item)
                        if record.is_active(timestamp):
                            key_set.keys[record.key_id] = record
                            updated += 1
                primary_key_id = cluster_data.get("primary_key_id")
                if primary_key_id in key_set.keys:
                    key_set.primary_key_id = str(primary_key_id)
                if key_set.keys:
                    self.key_sets[str(cluster_id)] = key_set
        return updated

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RouteKeyRegistry:
        registry = cls()
        registry.load_from_dict(payload)
        return registry


class RouteKeyProvider(Protocol):
    """Provider for refreshing route key registries."""

    def refresh(self, registry: RouteKeyRegistry) -> Awaitable[None] | None: ...


async def refresh_route_keys(
    provider: RouteKeyProvider, registry: RouteKeyRegistry
) -> None:
    """Refresh route keys using a provider that may be sync or async."""
    result = provider.refresh(registry)
    if inspect.isawaitable(result):
        await result


def normalize_public_keys(
    keys: PublicKey | Iterable[PublicKey] | None,
) -> tuple[PublicKey, ...]:
    if keys is None:
        return ()
    if isinstance(keys, (bytes, bytearray)):
        return (bytes(keys),)
    normalized: list[PublicKey] = []
    for key in keys:
        if key:
            normalized.append(bytes(key))
    return tuple(normalized)
