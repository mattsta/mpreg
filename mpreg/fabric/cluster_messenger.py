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
    ) -> MessageHeaders:
        """Advance headers for the next hop (fail-closed).

        Raises:
            MpregError (ROUTE_LOOP): local node already on ``routing_path``.
            MpregError (HOP_BUDGET_EXCEEDED): hop count would exceed budget.
        """
        from mpreg.fabric.hop_headers import advance_fabric_headers

        return advance_fabric_headers(
            correlation_id=correlation_id,
            headers=headers,
            node_id=self.node_id,
            cluster_id=self.cluster_id,
            max_hops=self.max_hops,
            target_cluster=target_cluster,
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
