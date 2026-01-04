"""Routing index helpers for the unified fabric."""

from __future__ import annotations

from dataclasses import dataclass, field

from mpreg.datastructures.function_identity import FunctionSelector
from mpreg.datastructures.type_aliases import (
    ClusterId,
    NodeId,
    QueueName,
    Timestamp,
)

from .catalog import (
    CacheNodeProfile,
    CacheRole,
    CacheRoleEntry,
    FunctionEndpoint,
    NodeDescriptor,
    QueueEndpoint,
    RoutingCatalog,
    TopicSubscription,
)


@dataclass(frozen=True, slots=True)
class FunctionQuery:
    selector: FunctionSelector
    resources: frozenset[str] = field(default_factory=frozenset)
    cluster_id: ClusterId | None = None
    node_id: NodeId | None = None


@dataclass(frozen=True, slots=True)
class TopicQuery:
    topic: str


@dataclass(frozen=True, slots=True)
class QueueQuery:
    queue_name: QueueName
    cluster_id: ClusterId | None = None


@dataclass(frozen=True, slots=True)
class CacheQuery:
    role: CacheRole
    cluster_id: ClusterId | None = None


@dataclass(frozen=True, slots=True)
class CacheProfileQuery:
    cluster_id: ClusterId | None = None
    node_id: NodeId | None = None


@dataclass(frozen=True, slots=True)
class NodeQuery:
    cluster_id: ClusterId | None = None
    node_id: NodeId | None = None
    resources: frozenset[str] | None = None
    capabilities: frozenset[str] | None = None


@dataclass(slots=True)
class RoutingIndex:
    """Read-optimized query interface over the routing catalog."""

    catalog: RoutingCatalog = field(default_factory=RoutingCatalog)

    def find_functions(
        self, query: FunctionQuery, *, now: Timestamp | None = None
    ) -> list[FunctionEndpoint]:
        resources = query.resources if query.resources else None
        return self.catalog.functions.find(
            query.selector,
            resources=resources,
            cluster_id=query.cluster_id,
            node_id=query.node_id,
            now=now,
        )

    def match_topics(
        self, query: TopicQuery, *, now: Timestamp | None = None
    ) -> list[TopicSubscription]:
        return self.catalog.topics.match(query.topic, now=now)

    def find_queues(
        self, query: QueueQuery, *, now: Timestamp | None = None
    ) -> list[QueueEndpoint]:
        return self.catalog.queues.find(
            query.queue_name, cluster_id=query.cluster_id, now=now
        )

    def find_cache_roles(
        self, query: CacheQuery, *, now: Timestamp | None = None
    ) -> list[CacheRoleEntry]:
        return self.catalog.caches.find(
            query.role, cluster_id=query.cluster_id, now=now
        )

    def find_cache_profiles(
        self, query: CacheProfileQuery, *, now: Timestamp | None = None
    ) -> list[CacheNodeProfile]:
        return self.catalog.cache_profiles.find(
            cluster_id=query.cluster_id, node_id=query.node_id, now=now
        )

    def find_nodes(
        self, query: NodeQuery, *, now: Timestamp | None = None
    ) -> list[NodeDescriptor]:
        nodes = self.catalog.nodes.find(
            cluster_id=query.cluster_id, node_id=query.node_id, now=now
        )
        if query.resources:
            nodes = [node for node in nodes if query.resources.issubset(node.resources)]
        if query.capabilities:
            nodes = [
                node for node in nodes if query.capabilities.issubset(node.capabilities)
            ]
        return nodes

    def prune_expired(self, now: Timestamp | None = None) -> dict[str, int]:
        return self.catalog.prune_expired(now)
