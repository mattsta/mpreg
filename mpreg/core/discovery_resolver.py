from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from threading import RLock

from mpreg.core.cluster_map import ClusterNodeSnapshot
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
    DurationSeconds,
    EntryCount,
    HitCount,
    MissCount,
    NodeId,
    Timestamp,
)
from mpreg.fabric.catalog import RoutingCatalog
from mpreg.fabric.catalog_delta import RoutingCatalogApplier, RoutingCatalogDelta
from mpreg.fabric.index import RoutingIndex

from .discovery_events import CatalogDeltaCounts


@dataclass(frozen=True, slots=True)
class CatalogEntryCounts:
    functions: EntryCount = field(default=0, metadata={PAYLOAD_INT: True})
    topics: EntryCount = field(default=0, metadata={PAYLOAD_INT: True})
    queues: EntryCount = field(default=0, metadata={PAYLOAD_INT: True})
    services: EntryCount = field(default=0, metadata={PAYLOAD_INT: True})
    caches: EntryCount = field(default=0, metadata={PAYLOAD_INT: True})
    cache_profiles: EntryCount = field(default=0, metadata={PAYLOAD_INT: True})
    nodes: EntryCount = field(default=0, metadata={PAYLOAD_INT: True})

    @classmethod
    def from_catalog(cls, catalog: RoutingCatalog) -> CatalogEntryCounts:
        return cls(
            functions=catalog.functions.entry_count(),
            topics=catalog.topics.entry_count(),
            queues=catalog.queues.entry_count(),
            services=catalog.services.entry_count(),
            caches=catalog.caches.entry_count(),
            cache_profiles=catalog.cache_profiles.entry_count(),
            nodes=catalog.nodes.entry_count(),
        )

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> CatalogEntryCounts:
        return cls(
            functions=int(payload.get("functions", 0) or 0),
            topics=int(payload.get("topics", 0) or 0),
            queues=int(payload.get("queues", 0) or 0),
            services=int(payload.get("services", 0) or 0),
            caches=int(payload.get("caches", 0) or 0),
            cache_profiles=int(payload.get("cache_profiles", 0) or 0),
            nodes=int(payload.get("nodes", 0) or 0),
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


class QueryCacheState(Enum):
    HIT = "hit"
    STALE = "stale"
    MISS = "miss"


@dataclass(frozen=True, slots=True)
class DiscoveryResolverQueryCacheConfig:
    enabled: bool = True
    ttl_seconds: DurationSeconds = 10.0
    stale_while_revalidate_seconds: DurationSeconds = 20.0
    negative_ttl_seconds: DurationSeconds = 5.0
    max_entries: int = 1000


@dataclass(frozen=True, slots=True)
class CatalogQueryCacheKey:
    entry_type: str
    scope: str | None
    namespace: str | None
    viewer_cluster_id: ClusterId | None
    capabilities: tuple[str, ...]
    resources: tuple[str, ...]
    cluster_id: ClusterId | None
    node_id: NodeId | None
    function_name: str | None
    function_id: str | None
    version_constraint: str | None
    queue_name: str | None
    service_name: str | None
    service_protocol: str | None
    service_port: int | None
    topic: str | None
    tags: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ClusterMapQueryCacheKey:
    scope: str | None
    namespace_filter: str | None
    capabilities: tuple[str, ...]
    resources: tuple[str, ...]
    cluster_id: ClusterId | None


@dataclass(frozen=True, slots=True)
class CatalogQueryCacheEntry:
    items: tuple[Payload, ...]
    generated_at: Timestamp
    expires_at: Timestamp
    stale_until: Timestamp
    negative: bool
    generation: int


@dataclass(frozen=True, slots=True)
class ClusterMapQueryCacheEntry:
    nodes: tuple[ClusterNodeSnapshot, ...]
    generated_at: Timestamp
    expires_at: Timestamp
    stale_until: Timestamp
    negative: bool
    generation: int


@dataclass(slots=True)
class DiscoveryResolverQueryCacheStats:
    catalog_hits: HitCount = 0
    catalog_misses: MissCount = 0
    catalog_stale_serves: HitCount = 0
    catalog_negative_hits: HitCount = 0
    catalog_refreshes: int = 0
    cluster_map_hits: HitCount = 0
    cluster_map_misses: MissCount = 0
    cluster_map_stale_serves: HitCount = 0
    cluster_map_negative_hits: HitCount = 0
    cluster_map_refreshes: int = 0


@dataclass(frozen=True, slots=True)
class DiscoveryResolverQueryCacheStatsSnapshot:
    catalog_entries: EntryCount = field(metadata={PAYLOAD_INT: True})
    cluster_map_entries: EntryCount = field(metadata={PAYLOAD_INT: True})
    catalog_hits: HitCount = field(metadata={PAYLOAD_INT: True})
    catalog_misses: MissCount = field(metadata={PAYLOAD_INT: True})
    catalog_stale_serves: HitCount = field(metadata={PAYLOAD_INT: True})
    catalog_negative_hits: HitCount = field(metadata={PAYLOAD_INT: True})
    catalog_refreshes: int = field(metadata={PAYLOAD_INT: True})
    cluster_map_hits: HitCount = field(metadata={PAYLOAD_INT: True})
    cluster_map_misses: MissCount = field(metadata={PAYLOAD_INT: True})
    cluster_map_stale_serves: HitCount = field(metadata={PAYLOAD_INT: True})
    cluster_map_negative_hits: HitCount = field(metadata={PAYLOAD_INT: True})
    cluster_map_refreshes: int = field(metadata={PAYLOAD_INT: True})

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(
        cls, payload: PayloadMapping
    ) -> DiscoveryResolverQueryCacheStatsSnapshot:
        return cls(
            catalog_entries=int(payload.get("catalog_entries", 0) or 0),
            cluster_map_entries=int(payload.get("cluster_map_entries", 0) or 0),
            catalog_hits=int(payload.get("catalog_hits", 0) or 0),
            catalog_misses=int(payload.get("catalog_misses", 0) or 0),
            catalog_stale_serves=int(payload.get("catalog_stale_serves", 0) or 0),
            catalog_negative_hits=int(payload.get("catalog_negative_hits", 0) or 0),
            catalog_refreshes=int(payload.get("catalog_refreshes", 0) or 0),
            cluster_map_hits=int(payload.get("cluster_map_hits", 0) or 0),
            cluster_map_misses=int(payload.get("cluster_map_misses", 0) or 0),
            cluster_map_stale_serves=int(
                payload.get("cluster_map_stale_serves", 0) or 0
            ),
            cluster_map_negative_hits=int(
                payload.get("cluster_map_negative_hits", 0) or 0
            ),
            cluster_map_refreshes=int(payload.get("cluster_map_refreshes", 0) or 0),
        )


@dataclass(slots=True)
class DiscoveryResolverQueryCache:
    config: DiscoveryResolverQueryCacheConfig
    stats: DiscoveryResolverQueryCacheStats = field(
        default_factory=DiscoveryResolverQueryCacheStats
    )
    _catalog_entries: dict[CatalogQueryCacheKey, CatalogQueryCacheEntry] = field(
        default_factory=dict
    )
    _cluster_map_entries: dict[ClusterMapQueryCacheKey, ClusterMapQueryCacheEntry] = (
        field(default_factory=dict)
    )
    _generation: int = 0

    def bump_generation(self) -> None:
        self._generation += 1

    def _stale_until(self, *, now: Timestamp, ttl: DurationSeconds) -> Timestamp:
        stale_window = max(0.0, float(self.config.stale_while_revalidate_seconds))
        return now + float(ttl) + stale_window

    def catalog_lookup(
        self, key: CatalogQueryCacheKey, *, now: Timestamp
    ) -> tuple[QueryCacheState, CatalogQueryCacheEntry | None]:
        entry = self._catalog_entries.get(key)
        if entry is None:
            self.stats.catalog_misses += 1
            return QueryCacheState.MISS, None
        if entry.generation == self._generation and now <= entry.expires_at:
            self.stats.catalog_hits += 1
            if entry.negative:
                self.stats.catalog_negative_hits += 1
            return QueryCacheState.HIT, entry
        if now <= entry.stale_until:
            self.stats.catalog_stale_serves += 1
            if entry.negative:
                self.stats.catalog_negative_hits += 1
            return QueryCacheState.STALE, entry
        self._catalog_entries.pop(key, None)
        self.stats.catalog_misses += 1
        return QueryCacheState.MISS, None

    def catalog_store(
        self,
        key: CatalogQueryCacheKey,
        items: tuple[Payload, ...],
        *,
        now: Timestamp,
        negative: bool,
    ) -> CatalogQueryCacheEntry:
        ttl = self.config.negative_ttl_seconds if negative else self.config.ttl_seconds
        ttl = max(0.0, float(ttl))
        entry = CatalogQueryCacheEntry(
            items=items,
            generated_at=now,
            expires_at=now + ttl,
            stale_until=self._stale_until(now=now, ttl=ttl),
            negative=negative,
            generation=self._generation,
        )
        self._catalog_entries[key] = entry
        self._prune()
        return entry

    def catalog_refresh_recorded(self) -> None:
        self.stats.catalog_refreshes += 1

    def cluster_map_lookup(
        self, key: ClusterMapQueryCacheKey, *, now: Timestamp
    ) -> tuple[QueryCacheState, ClusterMapQueryCacheEntry | None]:
        entry = self._cluster_map_entries.get(key)
        if entry is None:
            self.stats.cluster_map_misses += 1
            return QueryCacheState.MISS, None
        if entry.generation == self._generation and now <= entry.expires_at:
            self.stats.cluster_map_hits += 1
            if entry.negative:
                self.stats.cluster_map_negative_hits += 1
            return QueryCacheState.HIT, entry
        if now <= entry.stale_until:
            self.stats.cluster_map_stale_serves += 1
            if entry.negative:
                self.stats.cluster_map_negative_hits += 1
            return QueryCacheState.STALE, entry
        self._cluster_map_entries.pop(key, None)
        self.stats.cluster_map_misses += 1
        return QueryCacheState.MISS, None

    def cluster_map_store(
        self,
        key: ClusterMapQueryCacheKey,
        nodes: tuple[ClusterNodeSnapshot, ...],
        *,
        now: Timestamp,
        negative: bool,
    ) -> ClusterMapQueryCacheEntry:
        ttl = self.config.negative_ttl_seconds if negative else self.config.ttl_seconds
        ttl = max(0.0, float(ttl))
        entry = ClusterMapQueryCacheEntry(
            nodes=nodes,
            generated_at=now,
            expires_at=now + ttl,
            stale_until=self._stale_until(now=now, ttl=ttl),
            negative=negative,
            generation=self._generation,
        )
        self._cluster_map_entries[key] = entry
        self._prune()
        return entry

    def cluster_map_refresh_recorded(self) -> None:
        self.stats.cluster_map_refreshes += 1

    def snapshot(self) -> DiscoveryResolverQueryCacheStatsSnapshot:
        return DiscoveryResolverQueryCacheStatsSnapshot(
            catalog_entries=len(self._catalog_entries),
            cluster_map_entries=len(self._cluster_map_entries),
            catalog_hits=self.stats.catalog_hits,
            catalog_misses=self.stats.catalog_misses,
            catalog_stale_serves=self.stats.catalog_stale_serves,
            catalog_negative_hits=self.stats.catalog_negative_hits,
            catalog_refreshes=self.stats.catalog_refreshes,
            cluster_map_hits=self.stats.cluster_map_hits,
            cluster_map_misses=self.stats.cluster_map_misses,
            cluster_map_stale_serves=self.stats.cluster_map_stale_serves,
            cluster_map_negative_hits=self.stats.cluster_map_negative_hits,
            cluster_map_refreshes=self.stats.cluster_map_refreshes,
        )

    def prune_expired(self, *, now: Timestamp) -> None:
        expired_catalog = [
            key
            for key, entry in self._catalog_entries.items()
            if entry.stale_until <= now
        ]
        for key in expired_catalog:
            self._catalog_entries.pop(key, None)
        expired_cluster_map = [
            key
            for key, entry in self._cluster_map_entries.items()
            if entry.stale_until <= now
        ]
        for key in expired_cluster_map:
            self._cluster_map_entries.pop(key, None)
        self._prune()

    def _prune(self) -> None:
        max_entries = max(0, int(self.config.max_entries))
        if max_entries <= 0:
            self._catalog_entries.clear()
            self._cluster_map_entries.clear()
            return
        total = len(self._catalog_entries) + len(self._cluster_map_entries)
        if total <= max_entries:
            return
        target = max(0, total - max_entries)
        if target <= 0:
            return
        all_entries: list[tuple[Timestamp, str, object]] = []
        for key, entry in self._catalog_entries.items():
            all_entries.append((entry.generated_at, "catalog", key))
        for key, entry in self._cluster_map_entries.items():
            all_entries.append((entry.generated_at, "cluster_map", key))
        all_entries.sort(key=lambda item: item[0])
        for _, kind, key in all_entries[:target]:
            if kind == "catalog":
                self._catalog_entries.pop(key, None)
            else:
                self._cluster_map_entries.pop(key, None)


@dataclass(slots=True)
class DiscoveryResolverStats:
    deltas_applied: int = 0
    deltas_skipped: int = 0
    deltas_invalid: int = 0
    last_delta_id: str | None = None
    last_delta_cluster: ClusterId | None = None
    last_delta_at: Timestamp | None = None
    last_delta_counts: CatalogDeltaCounts | None = None
    last_seed_at: Timestamp | None = None
    last_seed_counts: CatalogEntryCounts | None = None
    last_prune_at: Timestamp | None = None
    last_prune_counts: CatalogEntryCounts | None = None


@dataclass(frozen=True, slots=True)
class DiscoveryResolverStatsSnapshot:
    deltas_applied: int = field(metadata={PAYLOAD_INT: True})
    deltas_skipped: int = field(metadata={PAYLOAD_INT: True})
    deltas_invalid: int = field(metadata={PAYLOAD_INT: True})
    last_delta_id: str | None = field(default=None, metadata={PAYLOAD_KEEP_EMPTY: True})
    last_delta_cluster: ClusterId | None = field(
        default=None, metadata={PAYLOAD_KEEP_EMPTY: True}
    )
    last_delta_at: Timestamp | None = field(
        default=None, metadata={PAYLOAD_FLOAT: True, PAYLOAD_KEEP_EMPTY: True}
    )
    last_delta_counts: CatalogDeltaCounts | None = None
    last_seed_at: Timestamp | None = field(
        default=None, metadata={PAYLOAD_FLOAT: True, PAYLOAD_KEEP_EMPTY: True}
    )
    last_seed_counts: CatalogEntryCounts | None = None
    last_prune_at: Timestamp | None = field(
        default=None, metadata={PAYLOAD_FLOAT: True, PAYLOAD_KEEP_EMPTY: True}
    )
    last_prune_counts: CatalogEntryCounts | None = None

    @classmethod
    def from_stats(
        cls, stats: DiscoveryResolverStats
    ) -> DiscoveryResolverStatsSnapshot:
        return cls(
            deltas_applied=stats.deltas_applied,
            deltas_skipped=stats.deltas_skipped,
            deltas_invalid=stats.deltas_invalid,
            last_delta_id=stats.last_delta_id,
            last_delta_cluster=stats.last_delta_cluster,
            last_delta_at=stats.last_delta_at,
            last_delta_counts=stats.last_delta_counts,
            last_seed_at=stats.last_seed_at,
            last_seed_counts=stats.last_seed_counts,
            last_prune_at=stats.last_prune_at,
            last_prune_counts=stats.last_prune_counts,
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> DiscoveryResolverStatsSnapshot:
        delta_counts_payload = payload.get("last_delta_counts")
        seed_counts_payload = payload.get("last_seed_counts")
        prune_counts_payload = payload.get("last_prune_counts")
        return cls(
            deltas_applied=int(payload.get("deltas_applied", 0) or 0),
            deltas_skipped=int(payload.get("deltas_skipped", 0) or 0),
            deltas_invalid=int(payload.get("deltas_invalid", 0) or 0),
            last_delta_id=(
                str(payload.get("last_delta_id"))
                if payload.get("last_delta_id") is not None
                else None
            ),
            last_delta_cluster=(
                str(payload.get("last_delta_cluster"))
                if payload.get("last_delta_cluster") is not None
                else None
            ),
            last_delta_at=(
                float(payload.get("last_delta_at"))
                if payload.get("last_delta_at") is not None
                else None
            ),
            last_delta_counts=(
                CatalogDeltaCounts.from_dict(delta_counts_payload)
                if isinstance(delta_counts_payload, dict)
                else None
            ),
            last_seed_at=(
                float(payload.get("last_seed_at"))
                if payload.get("last_seed_at") is not None
                else None
            ),
            last_seed_counts=(
                CatalogEntryCounts.from_dict(seed_counts_payload)
                if isinstance(seed_counts_payload, dict)
                else None
            ),
            last_prune_at=(
                float(payload.get("last_prune_at"))
                if payload.get("last_prune_at") is not None
                else None
            ),
            last_prune_counts=(
                CatalogEntryCounts.from_dict(prune_counts_payload)
                if isinstance(prune_counts_payload, dict)
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class DiscoveryResolverCacheStatsResponse:
    enabled: bool
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    namespaces: tuple[str, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    entry_counts: CatalogEntryCounts
    stats: DiscoveryResolverStatsSnapshot | None
    query_cache: DiscoveryResolverQueryCacheStatsSnapshot | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> DiscoveryResolverCacheStatsResponse:
        namespaces_payload = payload.get("namespaces", []) or []
        entry_counts_payload = payload.get("entry_counts")
        stats_payload = payload.get("stats")
        query_cache_payload = payload.get("query_cache")
        return cls(
            enabled=bool(payload.get("enabled", False)),
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            namespaces=tuple(
                str(item) for item in namespaces_payload if item is not None
            ),
            entry_counts=CatalogEntryCounts.from_dict(entry_counts_payload)
            if isinstance(entry_counts_payload, dict)
            else CatalogEntryCounts(),
            stats=(
                DiscoveryResolverStatsSnapshot.from_dict(stats_payload)
                if isinstance(stats_payload, dict)
                else None
            ),
            query_cache=(
                DiscoveryResolverQueryCacheStatsSnapshot.from_dict(query_cache_payload)
                if isinstance(query_cache_payload, dict)
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class DiscoveryResolverResyncResponse:
    enabled: bool
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    resynced: bool
    entry_counts: CatalogEntryCounts | None
    stats: DiscoveryResolverStatsSnapshot | None
    error: str | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> DiscoveryResolverResyncResponse:
        entry_counts_payload = payload.get("entry_counts")
        stats_payload = payload.get("stats")
        return cls(
            enabled=bool(payload.get("enabled", False)),
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            resynced=bool(payload.get("resynced", False)),
            entry_counts=(
                CatalogEntryCounts.from_dict(entry_counts_payload)
                if isinstance(entry_counts_payload, dict)
                else None
            ),
            stats=(
                DiscoveryResolverStatsSnapshot.from_dict(stats_payload)
                if isinstance(stats_payload, dict)
                else None
            ),
            error=(
                str(payload.get("error")) if payload.get("error") is not None else None
            ),
        )


@dataclass(slots=True)
class DiscoveryResolverCache:
    catalog: RoutingCatalog = field(default_factory=RoutingCatalog)
    applier: RoutingCatalogApplier = field(init=False)
    index: RoutingIndex = field(init=False)
    stats: DiscoveryResolverStats = field(default_factory=DiscoveryResolverStats)
    query_cache_config: DiscoveryResolverQueryCacheConfig = field(
        default_factory=DiscoveryResolverQueryCacheConfig
    )
    query_cache: DiscoveryResolverQueryCache = field(init=False)
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        self.applier = RoutingCatalogApplier(self.catalog)
        self.index = RoutingIndex(catalog=self.catalog)
        self.query_cache = DiscoveryResolverQueryCache(self.query_cache_config)

    @contextmanager
    def locked(self) -> Iterator[None]:
        with self._lock:
            yield

    def apply_delta(
        self, delta: RoutingCatalogDelta, *, now: Timestamp | None = None
    ) -> CatalogDeltaCounts:
        timestamp = now if now is not None else time.time()
        with self._lock:
            applied = self.applier.apply(delta, now=timestamp)
            counts = CatalogDeltaCounts.from_dict(applied)
            self.stats.deltas_applied += 1
            self.stats.last_delta_id = delta.update_id
            self.stats.last_delta_cluster = delta.cluster_id
            self.stats.last_delta_at = timestamp
            self.stats.last_delta_counts = counts
            if self.query_cache_config.enabled:
                self.query_cache.bump_generation()
            return counts

    def seed_from_catalog(
        self, catalog: RoutingCatalog, *, now: Timestamp | None = None
    ) -> CatalogEntryCounts:
        timestamp = now if now is not None else time.time()
        with self._lock:
            self.catalog = RoutingCatalog()
            self.applier = RoutingCatalogApplier(self.catalog)
            self.index = RoutingIndex(catalog=self.catalog)
            for entry in catalog.functions.entries(now=timestamp):
                self.catalog.functions.register(entry, now=timestamp)
            for entry in catalog.topics.entries(now=timestamp):
                self.catalog.topics.register(entry, now=timestamp)
            for entry in catalog.queues.entries(now=timestamp):
                self.catalog.queues.register(entry, now=timestamp)
            for entry in catalog.services.entries(now=timestamp):
                self.catalog.services.register(entry, now=timestamp)
            for entry in catalog.caches.entries(now=timestamp):
                self.catalog.caches.register(entry, now=timestamp)
            for entry in catalog.cache_profiles.entries(now=timestamp):
                self.catalog.cache_profiles.register(entry, now=timestamp)
            for entry in catalog.nodes.entries(now=timestamp):
                self.catalog.nodes.register(entry, now=timestamp)
            counts = CatalogEntryCounts.from_catalog(self.catalog)
            self.stats.last_seed_at = timestamp
            self.stats.last_seed_counts = counts
            if self.query_cache_config.enabled:
                self.query_cache.bump_generation()
            return counts

    def prune_expired(self, *, now: Timestamp | None = None) -> CatalogEntryCounts:
        timestamp = now if now is not None else time.time()
        with self._lock:
            pruned = self.catalog.prune_expired(timestamp)
            counts = CatalogEntryCounts.from_dict(pruned)
            self.stats.last_prune_at = timestamp
            self.stats.last_prune_counts = counts
            if self.query_cache_config.enabled:
                self.query_cache.prune_expired(now=timestamp)
            return counts

    def entry_counts(self) -> CatalogEntryCounts:
        with self._lock:
            return CatalogEntryCounts.from_catalog(self.catalog)

    def stats_snapshot(self) -> DiscoveryResolverStatsSnapshot:
        with self._lock:
            return DiscoveryResolverStatsSnapshot.from_stats(self.stats)

    def query_cache_snapshot(
        self,
    ) -> DiscoveryResolverQueryCacheStatsSnapshot | None:
        if not self.query_cache_config.enabled:
            return None
        return self.query_cache.snapshot()

    def record_skipped(self) -> None:
        with self._lock:
            self.stats.deltas_skipped += 1

    def record_invalid(self) -> None:
        with self._lock:
            self.stats.deltas_invalid += 1
