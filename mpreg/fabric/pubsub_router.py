"""Topic-based routing planner for pub/sub messages."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from mpreg.datastructures.type_aliases import ClusterId, NodeId, Timestamp

from .catalog import TopicSubscription
from .index import RoutingIndex, TopicQuery


class PubSubRouteReason(Enum):
    NO_SUBSCRIBERS = "no_subscribers"
    LOCAL_ONLY = "local_only"
    REMOTE_ONLY = "remote_only"
    LOCAL_AND_REMOTE = "local_and_remote"


@dataclass(frozen=True, slots=True)
class PubSubRoutingPlan:
    topic: str
    local_subscriptions: tuple[TopicSubscription, ...]
    remote_subscriptions: tuple[TopicSubscription, ...]
    target_nodes: tuple[NodeId, ...]
    reason: PubSubRouteReason


@dataclass(slots=True)
class PubSubRoutingPlanner:
    routing_index: RoutingIndex
    local_node_id: NodeId
    allowed_clusters: frozenset[ClusterId] | None = None

    def plan(self, topic: str, *, now: Timestamp | None = None) -> PubSubRoutingPlan:
        matches = self.routing_index.match_topics(TopicQuery(topic=topic), now=now)
        if self.allowed_clusters is not None:
            matches = [
                sub for sub in matches if sub.cluster_id in self.allowed_clusters
            ]

        local = tuple(sub for sub in matches if sub.node_id == self.local_node_id)
        remote = tuple(sub for sub in matches if sub.node_id != self.local_node_id)
        target_nodes = tuple(sorted({sub.node_id for sub in remote}))
        if not matches:
            reason = PubSubRouteReason.NO_SUBSCRIBERS
        elif local and remote:
            reason = PubSubRouteReason.LOCAL_AND_REMOTE
        elif local:
            reason = PubSubRouteReason.LOCAL_ONLY
        else:
            reason = PubSubRouteReason.REMOTE_ONLY
        return PubSubRoutingPlan(
            topic=topic,
            local_subscriptions=local,
            remote_subscriptions=remote,
            target_nodes=target_nodes,
            reason=reason,
        )
