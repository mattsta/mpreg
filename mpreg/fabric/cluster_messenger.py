"""Cluster-aware messenger for fabric message forwarding."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from mpreg.datastructures.type_aliases import ClusterId, HopCount, NodeId
from mpreg.fabric.federation_planner import FabricForwardingPlan

from .message import MessageHeaders, UnifiedMessage


class ClusterHopPlanner(Protocol):
    def plan_next_hop(
        self,
        *,
        target_cluster: ClusterId | None,
        visited_clusters: tuple[ClusterId, ...] = (),
        remaining_hops: HopCount | None = None,
    ) -> FabricForwardingPlan: ...


class FabricClusterTransport(Protocol):
    async def send_message(self, peer_id: str, message: UnifiedMessage) -> bool: ...


@dataclass(slots=True)
class ClusterMessenger:
    """Route fabric messages to a target cluster with hop/path tracking."""

    cluster_id: ClusterId
    node_id: NodeId
    transport: FabricClusterTransport
    peer_locator: Callable[[ClusterId], Sequence[str]]
    hop_planner: ClusterHopPlanner | None = None
    max_hops: int = 5

    def next_headers(
        self,
        correlation_id: str,
        headers: MessageHeaders | None,
        *,
        target_cluster: ClusterId | None,
    ) -> MessageHeaders | None:
        if headers is None:
            federation_path = (self.cluster_id,)
            return MessageHeaders(
                correlation_id=correlation_id,
                source_cluster=self.cluster_id,
                target_cluster=target_cluster,
                routing_path=(self.node_id,),
                federation_path=federation_path,
                hop_budget=self.max_hops,
            )

        if self.node_id in headers.routing_path:
            return None

        hop_budget = headers.hop_budget
        if hop_budget is None:
            hop_budget = self.max_hops
        else:
            hop_budget = min(hop_budget, self.max_hops)

        routing_path = headers.routing_path
        if not routing_path or routing_path[-1] != self.node_id:
            routing_path = (*routing_path, self.node_id)

        federation_path = headers.federation_path
        if not federation_path or federation_path[-1] != self.cluster_id:
            federation_path = (*federation_path, self.cluster_id)

        hop_count = max(0, len(routing_path) - 1)
        if hop_budget is not None and hop_count > hop_budget:
            return None

        return MessageHeaders(
            correlation_id=headers.correlation_id or correlation_id,
            source_cluster=headers.source_cluster or self.cluster_id,
            target_cluster=target_cluster or headers.target_cluster,
            routing_path=routing_path,
            federation_path=federation_path,
            hop_budget=hop_budget,
            priority=headers.priority,
            metadata=dict(headers.metadata),
        )

    async def send_to_cluster(
        self,
        message: UnifiedMessage,
        target_cluster: ClusterId,
        source_peer_url: str | None,
    ) -> bool:
        next_hop = self._select_next_hop(
            target_cluster=target_cluster,
            headers=message.headers,
        )
        if next_hop is None:
            return False
        if next_hop == source_peer_url:
            return False
        return await self.transport.send_message(next_hop, message)

    def _select_next_hop(
        self, *, target_cluster: ClusterId, headers: MessageHeaders
    ) -> str | None:
        if self.hop_planner:
            remaining = headers.hop_budget
            plan = self.hop_planner.plan_next_hop(
                target_cluster=target_cluster,
                visited_clusters=headers.federation_path,
                remaining_hops=remaining,
            )
            if plan.can_forward and plan.next_peer_url:
                return plan.next_peer_url

        peers = list(self.peer_locator(target_cluster))
        return peers[0] if peers else None
