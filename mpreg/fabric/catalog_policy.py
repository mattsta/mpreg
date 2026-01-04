"""Catalog filtering policy for fabric routing updates."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from mpreg.datastructures.type_aliases import ClusterId, FunctionName, NodeId

from .catalog import (
    CacheNodeProfile,
    CacheRoleEntry,
    FunctionEndpoint,
    NodeDescriptor,
    QueueEndpoint,
    TopicSubscription,
)


@dataclass(frozen=True, slots=True)
class CatalogFilterPolicy:
    """Filter policy for routing catalog updates."""

    local_cluster: ClusterId
    allowed_clusters: frozenset[ClusterId] | None = None
    allowed_functions: frozenset[FunctionName] = field(default_factory=frozenset)
    blocked_functions: frozenset[FunctionName] = field(default_factory=frozenset)
    node_filter: Callable[[NodeId, ClusterId], bool] | None = None

    def _allows_node_id(self, node_id: NodeId, cluster_id: ClusterId) -> bool:
        if not self.allows_cluster(cluster_id):
            return False
        if self.node_filter is None:
            return True
        return self.node_filter(node_id, cluster_id)

    def allows_cluster(self, cluster_id: ClusterId) -> bool:
        if cluster_id == self.local_cluster:
            return True
        if self.allowed_clusters is None:
            return True
        return cluster_id in self.allowed_clusters

    def allows_function(self, endpoint: FunctionEndpoint) -> bool:
        if not self._allows_node_id(endpoint.node_id, endpoint.cluster_id):
            return False
        if endpoint.cluster_id == self.local_cluster:
            return True
        if self.blocked_functions and endpoint.identity.name in self.blocked_functions:
            return False
        if self.allowed_functions:
            return endpoint.identity.name in self.allowed_functions
        return True

    def allows_node(self, node: NodeDescriptor) -> bool:
        return self._allows_node_id(node.node_id, node.cluster_id)

    def allows_queue(self, endpoint: QueueEndpoint) -> bool:
        return self._allows_node_id(endpoint.node_id, endpoint.cluster_id)

    def allows_cache(self, entry: CacheRoleEntry) -> bool:
        return self._allows_node_id(entry.node_id, entry.cluster_id)

    def allows_cache_profile(self, profile: CacheNodeProfile) -> bool:
        return self._allows_node_id(profile.node_id, profile.cluster_id)

    def allows_topic(self, subscription: TopicSubscription) -> bool:
        return self._allows_node_id(subscription.node_id, subscription.cluster_id)
