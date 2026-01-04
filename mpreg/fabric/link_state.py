"""Optional link-state control plane for fabric routing."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from loguru import logger

from mpreg.datastructures.type_aliases import (
    AreaId,
    BandwidthMbps,
    ClusterId,
    DurationSeconds,
    NetworkLatencyMs,
    ReliabilityScore,
    RouteCostScore,
    Timestamp,
)
from mpreg.fabric.federation_graph import (
    FederationGraphEdge,
    FederationGraphNode,
    GeographicCoordinate,
    GraphBasedFederationRouter,
    NodeType,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mpreg.fabric.gossip import GossipMessage, GossipProtocol
    from mpreg.fabric.peer_directory import PeerNeighbor
    from mpreg.fabric.route_announcer import RouteLinkMetrics


class LinkStateMode(str, Enum):
    """Link-state routing mode selection."""

    DISABLED = "disabled"
    PREFER = "prefer"
    ONLY = "only"


@dataclass(frozen=True, slots=True)
class LinkStateNeighbor:
    """Neighbor cluster information for link-state updates."""

    cluster_id: ClusterId
    latency_ms: NetworkLatencyMs = 0.0
    bandwidth_mbps: BandwidthMbps | None = None
    reliability_score: ReliabilityScore = 1.0
    cost_score: RouteCostScore = 0.0

    def to_dict(self) -> dict[str, object]:
        return {
            "cluster_id": self.cluster_id,
            "latency_ms": float(self.latency_ms),
            "bandwidth_mbps": self.bandwidth_mbps,
            "reliability_score": float(self.reliability_score),
            "cost_score": float(self.cost_score),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> LinkStateNeighbor:
        return cls(
            cluster_id=str(payload.get("cluster_id", "")),
            latency_ms=float(payload.get("latency_ms", 0.0)),
            bandwidth_mbps=payload.get("bandwidth_mbps"),  # type: ignore[assignment]
            reliability_score=float(payload.get("reliability_score", 1.0)),
            cost_score=float(payload.get("cost_score", 0.0)),
        )


@dataclass(frozen=True, slots=True)
class LinkStateUpdate:
    """Link-state advertisement for a cluster's direct neighbors."""

    origin: ClusterId
    neighbors: tuple[LinkStateNeighbor, ...]
    area: AreaId | None = None
    advertised_at: Timestamp = field(default_factory=time.time)
    ttl_seconds: DurationSeconds = 30.0
    sequence: int = 0

    def is_expired(self, now: Timestamp | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.advertised_at + self.ttl_seconds)

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "origin": self.origin,
            "neighbors": [neighbor.to_dict() for neighbor in self.neighbors],
            "advertised_at": float(self.advertised_at),
            "ttl_seconds": float(self.ttl_seconds),
            "sequence": int(self.sequence),
        }
        if self.area is not None:
            payload["area"] = self.area
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> LinkStateUpdate:
        neighbors_payload = payload.get("neighbors", []) or []
        neighbors = tuple(
            LinkStateNeighbor.from_dict(entry)
            for entry in neighbors_payload
            if isinstance(entry, dict)
        )
        area_value = payload.get("area")
        return cls(
            origin=str(payload.get("origin", "")),
            neighbors=neighbors,
            area=str(area_value) if area_value is not None else None,
            advertised_at=float(payload.get("advertised_at", time.time())),
            ttl_seconds=float(payload.get("ttl_seconds", 30.0)),
            sequence=int(payload.get("sequence", 0)),
        )


@dataclass(frozen=True, slots=True)
class LinkStateEntryKey:
    """Key for a link-state entry scoped by origin + area."""

    origin: ClusterId
    area: AreaId | None


@dataclass(frozen=True, slots=True)
class LinkStateEdgeKey:
    """Key for tracking link-state edge membership."""

    source: ClusterId
    target: ClusterId


@dataclass(frozen=True, slots=True)
class LinkStateAreaPair:
    """Pair of source and target areas for summary export rules."""

    source_area: AreaId
    target_area: AreaId


@dataclass(frozen=True, slots=True)
class LinkStateSummaryFilter:
    """Summary export filter for inter-area announcements."""

    allowed_neighbors: frozenset[ClusterId] | None = None
    denied_neighbors: frozenset[ClusterId] | None = None

    def allows(self, cluster_id: ClusterId) -> bool:
        if (
            self.allowed_neighbors is not None
            and cluster_id not in self.allowed_neighbors
        ):
            return False
        return not (
            self.denied_neighbors is not None and cluster_id in self.denied_neighbors
        )


@dataclass(frozen=True, slots=True)
class LinkStateAreaPolicy:
    """Policy for scoping link-state announcements by area."""

    local_areas: tuple[AreaId, ...] = ()
    neighbor_areas: dict[ClusterId, tuple[AreaId, ...]] = field(default_factory=dict)
    area_neighbor_filters: dict[AreaId, frozenset[ClusterId]] = field(
        default_factory=dict
    )
    area_hierarchy: dict[AreaId, AreaId] = field(default_factory=dict)
    summary_filters: dict[LinkStateAreaPair, LinkStateSummaryFilter] = field(
        default_factory=dict
    )
    default_area: AreaId | None = None
    allow_unmapped_neighbors: bool = True

    def allowed_areas(self) -> frozenset[AreaId] | None:
        if self.local_areas:
            return frozenset(self.local_areas)
        if self.default_area is not None:
            return frozenset({self.default_area})
        return None

    def is_local_area(self, area: AreaId | None) -> bool:
        if area is None:
            return False
        if self.local_areas:
            return area in self.local_areas
        if self.default_area is not None:
            return area == self.default_area
        return False

    def group_neighbors(
        self, neighbors: list[PeerNeighbor]
    ) -> dict[AreaId | None, list[PeerNeighbor]]:
        grouped: dict[AreaId | None, list[PeerNeighbor]] = {}
        for peer in neighbors:
            areas = self.neighbor_areas.get(peer.cluster_id)
            if areas is None:
                if not self.allow_unmapped_neighbors:
                    continue
                areas = (None,) if self.default_area is None else (self.default_area,)
            for area in areas:
                if (
                    area is not None
                    and self.local_areas
                    and area not in self.local_areas
                ):
                    continue
                if area in self.area_neighbor_filters:
                    allowed = self.area_neighbor_filters[area]
                    if peer.cluster_id not in allowed:
                        continue
                grouped.setdefault(area, []).append(peer)
        for area, peers in grouped.items():
            grouped[area] = sorted(peers)
        return grouped

    def summarize_neighbors(
        self, source_area: AreaId | None, neighbors: list[PeerNeighbor]
    ) -> dict[AreaId, list[PeerNeighbor]]:
        if source_area is None:
            return {}
        summary: dict[AreaId, list[PeerNeighbor]] = {}
        targets: set[AreaId] = set()

        parent_area = self.area_hierarchy.get(source_area)
        if parent_area is not None:
            targets.add(parent_area)
        for pair in self.summary_filters:
            if pair.source_area == source_area:
                targets.add(pair.target_area)

        for target_area in targets:
            if not self.is_local_area(target_area):
                continue
            filter_key = LinkStateAreaPair(
                source_area=source_area, target_area=target_area
            )
            summary_filter = self.summary_filters.get(filter_key)
            if summary_filter is None:
                continue
            allowed_neighbors = self.area_neighbor_filters.get(target_area)
            selected: list[PeerNeighbor] = []
            for peer in neighbors:
                if (
                    allowed_neighbors is not None
                    and peer.cluster_id not in allowed_neighbors
                ):
                    continue
                if not summary_filter.allows(peer.cluster_id):
                    continue
                selected.append(peer)
            if selected:
                summary[target_area] = sorted(selected)
        return summary


@dataclass(slots=True)
class LinkStateEntry:
    """Stored link-state update for a cluster."""

    origin: ClusterId
    neighbors: tuple[LinkStateNeighbor, ...]
    area: AreaId | None
    advertised_at: Timestamp
    ttl_seconds: DurationSeconds
    sequence: int

    def is_expired(self, now: Timestamp | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.advertised_at + self.ttl_seconds)


@dataclass(slots=True)
class LinkStateStats:
    """Counters for link-state processing."""

    updates_received: int = 0
    updates_applied: int = 0
    updates_rejected: int = 0
    updates_filtered: int = 0
    entries_expired: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "updates_received": self.updates_received,
            "updates_applied": self.updates_applied,
            "updates_rejected": self.updates_rejected,
            "updates_filtered": self.updates_filtered,
            "entries_expired": self.entries_expired,
        }


@dataclass(slots=True)
class LinkStateTable:
    """In-memory link-state database keyed by origin cluster."""

    local_cluster: ClusterId
    entries: dict[LinkStateEntryKey, LinkStateEntry] = field(default_factory=dict)
    stats: LinkStateStats = field(default_factory=LinkStateStats)

    def neighbors_for(
        self, origin: ClusterId, area: AreaId | None
    ) -> tuple[LinkStateNeighbor, ...]:
        entry = self.entries.get(LinkStateEntryKey(origin=origin, area=area))
        return entry.neighbors if entry else ()

    def apply_update(
        self, update: LinkStateUpdate, *, now: Timestamp | None = None
    ) -> bool:
        timestamp = now if now is not None else time.time()
        self.stats.updates_received += 1
        if update.is_expired(timestamp):
            self.stats.updates_rejected += 1
            logger.debug(
                "Link-state update rejected: expired origin={}",
                update.origin,
            )
            return False
        key = LinkStateEntryKey(origin=update.origin, area=update.area)
        existing = self.entries.get(key)
        if existing:
            if update.sequence < existing.sequence:
                self.stats.updates_rejected += 1
                logger.debug(
                    "Link-state update rejected: stale sequence origin={} seq={} prev={}",
                    update.origin,
                    update.sequence,
                    existing.sequence,
                )
                return False
            if (
                update.sequence == existing.sequence
                and update.advertised_at <= existing.advertised_at
            ):
                self.stats.updates_rejected += 1
                logger.debug(
                    "Link-state update rejected: stale timestamp origin={} seq={} advertised_at={}",
                    update.origin,
                    update.sequence,
                    update.advertised_at,
                )
                return False
        self.entries[key] = LinkStateEntry(
            origin=update.origin,
            neighbors=update.neighbors,
            area=update.area,
            advertised_at=update.advertised_at,
            ttl_seconds=update.ttl_seconds,
            sequence=update.sequence,
        )
        self.stats.updates_applied += 1
        logger.debug(
            "Link-state update applied: origin={} neighbors={}",
            update.origin,
            len(update.neighbors),
        )
        return True

    def purge_expired(
        self, *, now: Timestamp | None = None
    ) -> tuple[LinkStateEntry, ...]:
        timestamp = now if now is not None else time.time()
        expired: list[LinkStateEntry] = []
        for key, entry in list(self.entries.items()):
            if entry.is_expired(timestamp):
                expired.append(entry)
                self.entries.pop(key, None)
        if expired:
            self.stats.entries_expired += len(expired)
        return tuple(expired)

    def has_origin(self, origin: ClusterId) -> bool:
        return any(key.origin == origin for key in self.entries)

    def entries_for_origin(self, origin: ClusterId) -> tuple[LinkStateEntry, ...]:
        return tuple(
            entry for key, entry in self.entries.items() if key.origin == origin
        )

    def neighbor_clusters(self, origin: ClusterId) -> frozenset[ClusterId]:
        """Return the unique neighbor clusters advertised for an origin."""
        neighbors: set[ClusterId] = set()
        for entry in self.entries_for_origin(origin):
            for neighbor in entry.neighbors:
                neighbors.add(neighbor.cluster_id)
        return frozenset(neighbors)


@dataclass(slots=True)
class LinkStatePublisher:
    """Publish link-state updates through the gossip protocol."""

    gossip: GossipProtocol
    ttl: int = 10
    max_hops: int = 5

    async def publish(self, update: LinkStateUpdate) -> GossipMessage:
        from mpreg.fabric.gossip import GossipMessage, GossipMessageType

        message = GossipMessage(
            message_id=f"{self.gossip.node_id}:{update.origin}:{update.sequence}",
            message_type=GossipMessageType.LINK_STATE_UPDATE,
            sender_id=self.gossip.node_id,
            payload=update.to_dict(),
            vector_clock=self.gossip.vector_clock.copy(),
            sequence_number=self.gossip.protocol_stats.messages_created,
            ttl=self.ttl,
            max_hops=self.max_hops,
        )
        await self.gossip.add_message(message)
        self.gossip.protocol_stats.messages_created += 1
        return message


@dataclass(slots=True)
class LinkStateProcessor:
    """Apply link-state updates and update a graph router."""

    local_cluster: ClusterId
    table: LinkStateTable
    router: GraphBasedFederationRouter
    sender_cluster_resolver: Callable[[str], ClusterId | None] | None = None
    allowed_areas: frozenset[AreaId] | None = None
    edge_membership: dict[LinkStateEdgeKey, set[LinkStateEntryKey]] = field(
        default_factory=dict
    )

    async def handle_update(self, update: LinkStateUpdate, *, sender_id: str) -> bool:
        timestamp = time.time()
        self._purge_expired(timestamp)
        if not self._is_area_allowed(update.area):
            self.table.stats.updates_filtered += 1
            logger.debug(
                "Link-state update rejected: area mismatch origin={} area={}",
                update.origin,
                update.area,
            )
            return False
        entry_key = LinkStateEntryKey(origin=update.origin, area=update.area)
        previous = {
            neighbor.cluster_id: neighbor
            for neighbor in self.table.neighbors_for(update.origin, update.area)
        }
        if not self.table.apply_update(update, now=timestamp):
            return False
        current = {
            neighbor.cluster_id: neighbor
            for neighbor in self.table.neighbors_for(update.origin, update.area)
        }

        removed = set(previous) - set(current)
        for neighbor_id in removed:
            self._release_edge(entry_key, update.origin, neighbor_id)
        for neighbor in current.values():
            self._retain_edge(entry_key, update.origin, neighbor, update.area)
        return True

    def prune_expired(self, *, now: Timestamp | None = None) -> int:
        timestamp = now if now is not None else time.time()
        expired = self.table.purge_expired(now=timestamp)
        for entry in expired:
            self._remove_entry(entry)
        return len(expired)

    def _purge_expired(self, now: Timestamp) -> None:
        expired = self.table.purge_expired(now=now)
        for entry in expired:
            self._remove_entry(entry)

    def _ensure_node(
        self, cluster_id: ClusterId, *, area: AreaId | None = None
    ) -> None:
        if self.router.graph.get_node(cluster_id) is not None:
            return
        node = FederationGraphNode(
            node_id=cluster_id,
            node_type=NodeType.CLUSTER,
            region=area or "link-state",
            coordinates=GeographicCoordinate(0.0, 0.0),
            max_capacity=1000,
            current_load=0.0,
            health_score=1.0,
            processing_latency_ms=1.0,
            bandwidth_mbps=1000,
            reliability_score=1.0,
        )
        self.router.add_node(node)

    def _remove_entry(self, entry: LinkStateEntry) -> None:
        entry_key = LinkStateEntryKey(origin=entry.origin, area=entry.area)
        for neighbor in entry.neighbors:
            self._release_edge(entry_key, entry.origin, neighbor.cluster_id)
        if not self.table.has_origin(entry.origin):
            self.router.remove_node(entry.origin)
        logger.debug(
            "Link-state entry expired: origin={} neighbors={}",
            entry.origin,
            len(entry.neighbors),
        )

    def _remove_edge(self, origin: ClusterId, neighbor_id: ClusterId) -> None:
        removed = self.router.remove_edge(origin, neighbor_id)
        if removed:
            logger.debug(
                "Link-state edge removed: origin={} neighbor={}",
                origin,
                neighbor_id,
            )

    def _retain_edge(
        self,
        entry_key: LinkStateEntryKey,
        origin: ClusterId,
        neighbor: LinkStateNeighbor,
        area: AreaId | None,
    ) -> None:
        edge_key = LinkStateEdgeKey(source=origin, target=neighbor.cluster_id)
        membership = self.edge_membership.setdefault(edge_key, set())
        if entry_key in membership:
            self._upsert_edge(origin, neighbor, area)
            return
        membership.add(entry_key)
        if len(membership) == 1:
            self._upsert_edge(origin, neighbor, area)
        else:
            logger.debug(
                "Link-state edge retained: origin={} neighbor={} area={}",
                origin,
                neighbor.cluster_id,
                area,
            )

    def _release_edge(
        self, entry_key: LinkStateEntryKey, origin: ClusterId, neighbor_id: ClusterId
    ) -> None:
        edge_key = LinkStateEdgeKey(source=origin, target=neighbor_id)
        membership = self.edge_membership.get(edge_key)
        if not membership:
            return
        membership.discard(entry_key)
        if membership:
            return
        self.edge_membership.pop(edge_key, None)
        self._remove_edge(origin, neighbor_id)

    def _upsert_edge(
        self,
        origin: ClusterId,
        neighbor: LinkStateNeighbor,
        area: AreaId | None,
    ) -> None:
        self._ensure_node(origin, area=area)
        self._ensure_node(neighbor.cluster_id, area=area)
        bandwidth = (
            1000 if neighbor.bandwidth_mbps is None else int(neighbor.bandwidth_mbps)
        )
        cost_factor = 1.0 + max(0.0, float(neighbor.cost_score)) / 10.0
        edge = FederationGraphEdge(
            source_id=origin,
            target_id=neighbor.cluster_id,
            latency_ms=float(neighbor.latency_ms),
            bandwidth_mbps=bandwidth,
            reliability_score=float(neighbor.reliability_score),
            cost_factor=cost_factor,
        )
        self.router.add_edge(edge)
        logger.debug(
            "Link-state edge upserted: origin={} neighbor={} latency_ms={}",
            origin,
            neighbor.cluster_id,
            neighbor.latency_ms,
        )

    def _is_area_allowed(self, area: AreaId | None) -> bool:
        if self.allowed_areas is None:
            return True
        if area is None:
            return False
        return area in self.allowed_areas


@dataclass(slots=True)
class LinkStateAnnouncer:
    """Periodic broadcaster for link-state updates."""

    local_cluster: ClusterId
    neighbor_locator: Callable[[], list[PeerNeighbor]]
    publisher: LinkStatePublisher
    area: AreaId | None = None
    area_policy: LinkStateAreaPolicy | None = None
    ttl_seconds: DurationSeconds = 30.0
    interval_seconds: DurationSeconds = 10.0
    link_metrics_resolver: Callable[[ClusterId], RouteLinkMetrics] | None = None
    processor: LinkStateProcessor | None = None
    _task: asyncio.Task[None] | None = field(init=False, default=None)
    _area_sequences: dict[AreaId | None, int] = field(default_factory=dict, init=False)
    _announced_areas: set[AreaId | None] = field(default_factory=set, init=False)

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._announce_loop())

    async def stop(self) -> None:
        if not self._task:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def announce_once(self, *, now: Timestamp | None = None) -> None:
        timestamp = now if now is not None else time.time()
        if self.processor:
            self.processor.prune_expired(now=timestamp)
        updates = self._build_updates(timestamp)
        for update in updates:
            await self.publisher.publish(update)

    async def _announce_loop(self) -> None:
        while True:
            await self.announce_once()
            await asyncio.sleep(self.interval_seconds)

    def _build_updates(self, timestamp: Timestamp) -> tuple[LinkStateUpdate, ...]:
        peers = list(self.neighbor_locator())
        if self.area_policy:
            grouped = self.area_policy.group_neighbors(peers)
            summary_exports: dict[AreaId, list[PeerNeighbor]] = {}
            for area, area_peers in grouped.items():
                if area is None:
                    continue
                summaries = self.area_policy.summarize_neighbors(area, area_peers)
                for target_area, summary_peers in summaries.items():
                    summary_exports.setdefault(target_area, []).extend(summary_peers)
            for target_area, summary_peers in summary_exports.items():
                grouped.setdefault(target_area, []).extend(summary_peers)
            for area, area_peers in grouped.items():
                grouped[area] = self._dedupe_peers(area_peers)
            announce_areas = set(grouped.keys())
            if self.area_policy.local_areas:
                announce_areas.update(self.area_policy.local_areas)
            if self.area_policy.default_area is not None:
                announce_areas.add(self.area_policy.default_area)
        else:
            grouped = {self.area: peers}
            announce_areas = {self.area}
        announce_areas.update(self._announced_areas)

        updates: list[LinkStateUpdate] = []
        for area in sorted(
            announce_areas, key=lambda value: "" if value is None else value
        ):
            area_peers = grouped.get(area, [])
            neighbors = self._build_link_neighbors(area_peers)
            sequence = self._next_sequence(area)
            updates.append(
                LinkStateUpdate(
                    origin=self.local_cluster,
                    neighbors=neighbors,
                    area=area,
                    advertised_at=timestamp,
                    ttl_seconds=self.ttl_seconds,
                    sequence=sequence,
                )
            )
        self._announced_areas = set(announce_areas)
        return tuple(updates)

    def _build_link_neighbors(
        self, peers: list[PeerNeighbor]
    ) -> tuple[LinkStateNeighbor, ...]:
        neighbors: list[LinkStateNeighbor] = []
        for peer in sorted(peers):
            metrics = self._link_metrics(peer.cluster_id)
            neighbors.append(
                LinkStateNeighbor(
                    cluster_id=peer.cluster_id,
                    latency_ms=metrics.latency_ms,
                    bandwidth_mbps=metrics.bandwidth_mbps,
                    reliability_score=metrics.reliability_score,
                    cost_score=metrics.cost_score,
                )
            )
        return tuple(neighbors)

    def _dedupe_peers(self, peers: list[PeerNeighbor]) -> list[PeerNeighbor]:
        seen: set[ClusterId] = set()
        deduped: list[PeerNeighbor] = []
        for peer in sorted(peers):
            if peer.cluster_id in seen:
                continue
            seen.add(peer.cluster_id)
            deduped.append(peer)
        return deduped

    def _next_sequence(self, area: AreaId | None) -> int:
        current = self._area_sequences.get(area, 0) + 1
        self._area_sequences[area] = current
        return current

    def _link_metrics(self, cluster_id: ClusterId) -> RouteLinkMetrics:
        from mpreg.fabric.route_announcer import RouteLinkMetrics

        if not self.link_metrics_resolver:
            return RouteLinkMetrics()
        return self.link_metrics_resolver(cluster_id)
