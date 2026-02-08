from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from threading import RLock

from mpreg.core.payloads import (
    PAYLOAD_FLOAT,
    PAYLOAD_INT,
    PAYLOAD_KEEP_EMPTY,
    PAYLOAD_LIST,
    Payload,
    payload_from_dataclass,
)
from mpreg.datastructures.type_aliases import (
    ClusterId,
    EndpointScope,
    EntryCount,
    NamespaceName,
    NodeId,
    Timestamp,
)

from .discovery_summary import DiscoverySummaryMessage, ServiceSummary

type SummaryCacheKey = tuple[NamespaceName, str, ClusterId, EndpointScope | None]


@dataclass(frozen=True, slots=True)
class SummaryCacheEntry:
    summary: ServiceSummary
    source_cluster: ClusterId
    source_node: NodeId
    published_at: Timestamp
    expires_at: Timestamp
    scope: EndpointScope | None


@dataclass(frozen=True, slots=True)
class SummaryCacheEntryCounts:
    summaries: EntryCount = field(default=0, metadata={PAYLOAD_INT: True})

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(slots=True)
class DiscoverySummaryCacheStats:
    messages_applied: int = 0
    messages_skipped: int = 0
    messages_invalid: int = 0
    summaries_applied: int = 0
    summaries_expired: int = 0
    last_message_at: Timestamp | None = None
    last_source_cluster: ClusterId | None = None
    last_source_node: NodeId | None = None
    last_prune_at: Timestamp | None = None


@dataclass(frozen=True, slots=True)
class DiscoverySummaryCacheStatsSnapshot:
    messages_applied: int = field(metadata={PAYLOAD_INT: True})
    messages_skipped: int = field(metadata={PAYLOAD_INT: True})
    messages_invalid: int = field(metadata={PAYLOAD_INT: True})
    summaries_applied: int = field(metadata={PAYLOAD_INT: True})
    summaries_expired: int = field(metadata={PAYLOAD_INT: True})
    last_message_at: Timestamp | None = field(
        default=None, metadata={PAYLOAD_FLOAT: True}
    )
    last_source_cluster: ClusterId | None = None
    last_source_node: NodeId | None = None
    last_prune_at: Timestamp | None = field(
        default=None, metadata={PAYLOAD_FLOAT: True}
    )

    @classmethod
    def from_stats(
        cls, stats: DiscoverySummaryCacheStats
    ) -> DiscoverySummaryCacheStatsSnapshot:
        return cls(
            messages_applied=int(stats.messages_applied),
            messages_skipped=int(stats.messages_skipped),
            messages_invalid=int(stats.messages_invalid),
            summaries_applied=int(stats.summaries_applied),
            summaries_expired=int(stats.summaries_expired),
            last_message_at=stats.last_message_at,
            last_source_cluster=stats.last_source_cluster,
            last_source_node=stats.last_source_node,
            last_prune_at=stats.last_prune_at,
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class DiscoverySummaryCacheStatsResponse:
    enabled: bool
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    namespaces: tuple[str, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    entry_counts: SummaryCacheEntryCounts
    stats: DiscoverySummaryCacheStatsSnapshot | None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(slots=True)
class DiscoverySummaryCache:
    stats: DiscoverySummaryCacheStats = field(
        default_factory=DiscoverySummaryCacheStats
    )
    _entries: dict[SummaryCacheKey, SummaryCacheEntry] = field(default_factory=dict)
    _lock: RLock = field(default_factory=RLock)

    def apply_message(
        self, message: DiscoverySummaryMessage, *, now: Timestamp | None = None
    ) -> int:
        timestamp = now if now is not None else time.time()
        applied = 0
        with self._lock:
            for summary in message.summaries:
                if summary.ttl_seconds <= 0:
                    continue
                if summary.source_cluster is None:
                    summary = replace(summary, source_cluster=message.source_cluster)
                scope = summary.scope or message.scope
                if scope is not None and summary.scope != scope:
                    summary = replace(summary, scope=scope)
                expires_at = summary.generated_at + summary.ttl_seconds
                key = (
                    summary.namespace,
                    summary.service_id,
                    message.source_cluster,
                    scope,
                )
                existing = self._entries.get(key)
                if existing and summary.generated_at <= existing.summary.generated_at:
                    continue
                self._entries[key] = SummaryCacheEntry(
                    summary=summary,
                    source_cluster=message.source_cluster,
                    source_node=message.source_node,
                    published_at=message.published_at,
                    expires_at=expires_at,
                    scope=scope,
                )
                applied += 1
            self.stats.messages_applied += 1
            self.stats.summaries_applied += applied
            self.stats.last_message_at = timestamp
            self.stats.last_source_cluster = message.source_cluster
            self.stats.last_source_node = message.source_node
        return applied

    def prune_expired(self, *, now: Timestamp | None = None) -> int:
        timestamp = now if now is not None else time.time()
        removed = 0
        with self._lock:
            expired_keys = [
                key
                for key, entry in self._entries.items()
                if entry.expires_at <= timestamp
            ]
            for key in expired_keys:
                del self._entries[key]
            removed = len(expired_keys)
            if removed:
                self.stats.summaries_expired += removed
            self.stats.last_prune_at = timestamp
        return removed

    def entry_counts(self) -> SummaryCacheEntryCounts:
        with self._lock:
            return SummaryCacheEntryCounts(summaries=len(self._entries))

    def stats_snapshot(self) -> DiscoverySummaryCacheStatsSnapshot:
        with self._lock:
            return DiscoverySummaryCacheStatsSnapshot.from_stats(self.stats)

    def record_skipped(self) -> None:
        with self._lock:
            self.stats.messages_skipped += 1

    def record_invalid(self) -> None:
        with self._lock:
            self.stats.messages_invalid += 1

    def query(
        self,
        *,
        namespace: NamespaceName | None,
        service_id: str | None,
        scope: EndpointScope | None,
        now: Timestamp | None = None,
    ) -> tuple[ServiceSummary, ...]:
        timestamp = now if now is not None else time.time()
        entries: list[SummaryCacheEntry] = []
        with self._lock:
            expired_keys = [
                key
                for key, entry in self._entries.items()
                if entry.expires_at <= timestamp
            ]
            for key in expired_keys:
                del self._entries[key]
            if expired_keys:
                self.stats.summaries_expired += len(expired_keys)
                self.stats.last_prune_at = timestamp
            for entry in self._entries.values():
                summary = entry.summary
                if namespace and not summary.namespace.startswith(namespace):
                    continue
                if service_id and summary.service_id != service_id:
                    continue
                if scope is not None and entry.scope not in {None, scope}:
                    continue
                entries.append(entry)
        entries.sort(
            key=lambda entry: (
                entry.summary.namespace,
                entry.summary.service_id,
                entry.source_cluster,
                entry.summary.generated_at,
            )
        )
        return tuple(entry.summary for entry in entries)
