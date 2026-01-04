"""Catalog delta updates for unified gossip ingestion."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from mpreg.datastructures.type_aliases import ClusterId, SubscriptionId, Timestamp

from .catalog import (
    CacheNodeKey,
    CacheNodeProfile,
    CacheRoleEntry,
    CacheRoleKey,
    FunctionEndpoint,
    FunctionKey,
    NodeDescriptor,
    NodeKey,
    QueueEndpoint,
    QueueKey,
    RoutingCatalog,
    TopicSubscription,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .catalog_policy import CatalogFilterPolicy


class CatalogDeltaObserver(Protocol):
    def on_catalog_delta(
        self, delta: RoutingCatalogDelta, counts: dict[str, int]
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class RoutingCatalogDelta:
    update_id: str
    cluster_id: ClusterId
    sent_at: Timestamp = field(default_factory=time.time)
    functions: tuple[FunctionEndpoint, ...] = ()
    function_removals: tuple[FunctionKey, ...] = ()
    topics: tuple[TopicSubscription, ...] = ()
    topic_removals: tuple[SubscriptionId, ...] = ()
    queues: tuple[QueueEndpoint, ...] = ()
    queue_removals: tuple[QueueKey, ...] = ()
    caches: tuple[CacheRoleEntry, ...] = ()
    cache_removals: tuple[CacheRoleKey, ...] = ()
    cache_profiles: tuple[CacheNodeProfile, ...] = ()
    cache_profile_removals: tuple[CacheNodeKey, ...] = ()
    nodes: tuple[NodeDescriptor, ...] = ()
    node_removals: tuple[NodeKey, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "update_id": self.update_id,
            "cluster_id": self.cluster_id,
            "sent_at": float(self.sent_at),
            "functions": [entry.to_dict() for entry in self.functions],
            "function_removals": [key.to_dict() for key in self.function_removals],
            "topics": [entry.to_dict() for entry in self.topics],
            "topic_removals": list(self.topic_removals),
            "queues": [entry.to_dict() for entry in self.queues],
            "queue_removals": [key.to_dict() for key in self.queue_removals],
            "caches": [entry.to_dict() for entry in self.caches],
            "cache_removals": [key.to_dict() for key in self.cache_removals],
            "cache_profiles": [entry.to_dict() for entry in self.cache_profiles],
            "cache_profile_removals": [
                key.to_dict() for key in self.cache_profile_removals
            ],
            "nodes": [entry.to_dict() for entry in self.nodes],
            "node_removals": [key.to_dict() for key in self.node_removals],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RoutingCatalogDelta:
        return cls(
            update_id=str(payload.get("update_id", "")),
            cluster_id=str(payload.get("cluster_id", "")),
            sent_at=float(payload.get("sent_at", time.time())),
            functions=tuple(
                FunctionEndpoint.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("functions", [])
            ),
            function_removals=tuple(
                FunctionKey.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("function_removals", [])
            ),
            topics=tuple(
                TopicSubscription.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("topics", [])
            ),
            topic_removals=tuple(payload.get("topic_removals", [])),
            queues=tuple(
                QueueEndpoint.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("queues", [])
            ),
            queue_removals=tuple(
                QueueKey.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("queue_removals", [])
            ),
            caches=tuple(
                CacheRoleEntry.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("caches", [])
            ),
            cache_removals=tuple(
                CacheRoleKey.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("cache_removals", [])
            ),
            cache_profiles=tuple(
                CacheNodeProfile.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("cache_profiles", [])
            ),
            cache_profile_removals=tuple(
                CacheNodeKey.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("cache_profile_removals", [])
            ),
            nodes=tuple(
                NodeDescriptor.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("nodes", [])
            ),
            node_removals=tuple(
                NodeKey.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("node_removals", [])
            ),
        )


@dataclass(slots=True)
class RoutingCatalogApplier:
    catalog: RoutingCatalog
    policy: CatalogFilterPolicy | None = None
    observers: tuple[CatalogDeltaObserver, ...] = field(default_factory=tuple)

    def apply(
        self, delta: RoutingCatalogDelta, *, now: Timestamp | None = None
    ) -> dict[str, int]:
        counts = {
            "functions_added": 0,
            "functions_removed": 0,
            "topics_added": 0,
            "topics_removed": 0,
            "queues_added": 0,
            "queues_removed": 0,
            "caches_added": 0,
            "caches_removed": 0,
            "cache_profiles_added": 0,
            "cache_profiles_removed": 0,
            "nodes_added": 0,
            "nodes_removed": 0,
        }
        policy = self.policy
        function_removals = [
            key
            for key in delta.function_removals
            if not policy or policy.allows_cluster(key.cluster_id)
        ]
        queue_removals = [
            key
            for key in delta.queue_removals
            if not policy or policy.allows_cluster(key.cluster_id)
        ]
        cache_removals = [
            key
            for key in delta.cache_removals
            if not policy or policy.allows_cluster(key.cluster_id)
        ]
        cache_profile_removals = [
            key
            for key in delta.cache_profile_removals
            if not policy or policy.allows_cluster(key.cluster_id)
        ]
        node_removals = [
            key
            for key in delta.node_removals
            if not policy or policy.allows_cluster(key.cluster_id)
        ]
        functions = [
            endpoint
            for endpoint in delta.functions
            if not policy or policy.allows_function(endpoint)
        ]
        topics = [
            subscription
            for subscription in delta.topics
            if not policy or policy.allows_topic(subscription)
        ]
        queues = [
            endpoint
            for endpoint in delta.queues
            if not policy or policy.allows_queue(endpoint)
        ]
        caches = [
            entry for entry in delta.caches if not policy or policy.allows_cache(entry)
        ]
        cache_profiles = [
            profile
            for profile in delta.cache_profiles
            if not policy or policy.allows_cache_profile(profile)
        ]
        nodes = [node for node in delta.nodes if not policy or policy.allows_node(node)]

        for key in function_removals:
            if self.catalog.functions.remove(key):
                counts["functions_removed"] += 1
        for subscription_id in delta.topic_removals:
            if self.catalog.topics.remove(subscription_id):
                counts["topics_removed"] += 1
        for key in queue_removals:
            if self.catalog.queues.remove(key):
                counts["queues_removed"] += 1
        for key in cache_removals:
            if self.catalog.caches.remove(key):
                counts["caches_removed"] += 1
        for key in cache_profile_removals:
            if self.catalog.cache_profiles.remove(key):
                counts["cache_profiles_removed"] += 1
        for key in node_removals:
            if self.catalog.nodes.remove(key):
                counts["nodes_removed"] += 1

        for endpoint in functions:
            try:
                if self.catalog.functions.register(endpoint, now=now):
                    counts["functions_added"] += 1
            except ValueError:
                continue
        for subscription in topics:
            if self.catalog.topics.register(subscription, now=now):
                counts["topics_added"] += 1
        for endpoint in queues:
            if self.catalog.queues.register(endpoint, now=now):
                counts["queues_added"] += 1
        for entry in caches:
            if self.catalog.caches.register(entry, now=now):
                counts["caches_added"] += 1
        for profile in cache_profiles:
            if self.catalog.cache_profiles.register(profile, now=now):
                counts["cache_profiles_added"] += 1
        for node in nodes:
            if self.catalog.nodes.register(node, now=now):
                counts["nodes_added"] += 1

        filtered_delta = RoutingCatalogDelta(
            update_id=delta.update_id,
            cluster_id=delta.cluster_id,
            sent_at=delta.sent_at,
            functions=tuple(functions),
            function_removals=tuple(function_removals),
            topics=tuple(topics),
            topic_removals=delta.topic_removals,
            queues=tuple(queues),
            queue_removals=tuple(queue_removals),
            caches=tuple(caches),
            cache_removals=tuple(cache_removals),
            cache_profiles=tuple(cache_profiles),
            cache_profile_removals=tuple(cache_profile_removals),
            nodes=tuple(nodes),
            node_removals=tuple(node_removals),
        )

        for observer in self.observers:
            observer.on_catalog_delta(filtered_delta, counts)

        return counts
