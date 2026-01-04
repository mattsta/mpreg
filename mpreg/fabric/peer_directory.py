"""Peer directory for tracking discovered nodes and connection state."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from loguru import logger

from mpreg.core.connection_events import ConnectionEvent, ConnectionEventType
from mpreg.datastructures.type_aliases import ClusterId, NodeId, Timestamp

from .catalog import NodeDescriptor, NodeKey
from .catalog_delta import RoutingCatalogDelta


@dataclass(frozen=True, slots=True)
class PeerDirectoryDelta:
    nodes_added: int
    nodes_updated: int
    nodes_removed: int
    nodes_ignored: int


@dataclass(frozen=True, slots=True, order=True)
class PeerNeighbor:
    """Typed peer neighbor reference for discovery/routing."""

    cluster_id: ClusterId
    node_id: NodeId


@dataclass(eq=False, slots=True)
class PeerDirectory:
    """Maintain a fast lookup of known nodes and connection state."""

    local_node_id: NodeId
    local_cluster_id: ClusterId
    _nodes: dict[NodeKey, NodeDescriptor] = field(default_factory=dict)
    _nodes_by_id: dict[NodeId, NodeDescriptor] = field(default_factory=dict)
    _cluster_index: dict[ClusterId, set[NodeId]] = field(default_factory=dict)
    _connected: set[NodeId] = field(default_factory=set)

    def apply_delta(
        self, delta: RoutingCatalogDelta, *, now: Timestamp | None = None
    ) -> PeerDirectoryDelta:
        timestamp = now if now is not None else time.time()
        added = 0
        updated = 0
        removed = 0
        ignored = 0

        for key in delta.node_removals:
            if self.remove_node(key):
                removed += 1

        for node in delta.nodes:
            if node.is_expired(timestamp):
                ignored += 1
                continue
            was_added = self.register_node(node)
            if was_added:
                added += 1
            else:
                updated += 1

        return PeerDirectoryDelta(
            nodes_added=added,
            nodes_updated=updated,
            nodes_removed=removed,
            nodes_ignored=ignored,
        )

    def register_node(self, node: NodeDescriptor) -> bool:
        """Register or update a node descriptor.

        Returns True if this was a new node, False if it was an update.
        """
        existing = self._nodes_by_id.get(node.node_id)
        is_new = existing is None
        if existing and existing.key() != node.key():
            self._remove_by_id(node.node_id)
        self._nodes[node.key()] = node
        self._nodes_by_id[node.node_id] = node
        cluster_nodes = self._cluster_index.setdefault(node.cluster_id, set())
        cluster_nodes.add(node.node_id)
        if is_new:
            logger.debug(
                "[{}] Registered node: node_id={} cluster_id={}",
                self.local_node_id,
                node.node_id,
                node.cluster_id,
            )
        return is_new

    def remove_node(self, key: NodeKey) -> bool:
        node = self._nodes.pop(key, None)
        if node is None:
            return False
        current = self._nodes_by_id.get(node.node_id)
        if current and current.key() == key:
            self._nodes_by_id.pop(node.node_id, None)
        cluster_nodes = self._cluster_index.get(node.cluster_id)
        if cluster_nodes:
            cluster_nodes.discard(node.node_id)
            if not cluster_nodes:
                self._cluster_index.pop(node.cluster_id, None)
        logger.debug(
            "[{}] Removed node: node_id={} cluster_id={}",
            self.local_node_id,
            node.node_id,
            node.cluster_id,
        )
        return True

    def _remove_by_id(self, node_id: NodeId) -> None:
        node = self._nodes_by_id.pop(node_id, None)
        if node is None:
            return
        self._nodes.pop(node.key(), None)
        cluster_nodes = self._cluster_index.get(node.cluster_id)
        if cluster_nodes:
            cluster_nodes.discard(node_id)
            if not cluster_nodes:
                self._cluster_index.pop(node.cluster_id, None)

    def remove_node_by_id(self, node_id: NodeId) -> bool:
        if node_id not in self._nodes_by_id:
            return False
        self._remove_by_id(node_id)
        logger.debug(
            "Removed node by id: node_id={}",
            node_id,
        )
        return True

    def node_for_id(self, node_id: NodeId) -> NodeDescriptor | None:
        return self._nodes_by_id.get(node_id)

    def cluster_id_for_node(self, node_id: NodeId) -> ClusterId | None:
        node = self._nodes_by_id.get(node_id)
        return node.cluster_id if node else None

    def peers_for_cluster(
        self, cluster_id: ClusterId, *, connected_only: bool = False
    ) -> list[NodeId]:
        node_ids = sorted(self._cluster_index.get(cluster_id, set()))
        peers = [node_id for node_id in node_ids if node_id != self.local_node_id]
        if connected_only:
            peers = [node_id for node_id in peers if node_id in self._connected]
        return peers

    def neighbors(self, *, connected_only: bool = False) -> list[PeerNeighbor]:
        neighbors: list[PeerNeighbor] = []
        for cluster_id, node_ids in self._cluster_index.items():
            for node_id in node_ids:
                if node_id == self.local_node_id:
                    continue
                if connected_only and node_id not in self._connected:
                    continue
                neighbors.append(PeerNeighbor(cluster_id=cluster_id, node_id=node_id))
        return sorted(neighbors)

    def mark_connected(self, node_id: NodeId) -> None:
        self._connected.add(node_id)

    def mark_disconnected(self, node_id: NodeId) -> None:
        self._connected.discard(node_id)

    def connected_peers(self) -> set[NodeId]:
        return set(self._connected)

    def nodes(self) -> tuple[NodeDescriptor, ...]:
        return tuple(self._nodes.values())

    def on_catalog_delta(
        self, delta: RoutingCatalogDelta, counts: dict[str, int]
    ) -> None:
        self.apply_delta(delta, now=delta.sent_at)

    def on_connection_established(self, event: ConnectionEvent) -> None:
        if event.event_type is ConnectionEventType.ESTABLISHED:
            self.mark_connected(event.node_url)

    def on_connection_lost(self, event: ConnectionEvent) -> None:
        if event.event_type is ConnectionEventType.LOST:
            self.mark_disconnected(event.node_url)
