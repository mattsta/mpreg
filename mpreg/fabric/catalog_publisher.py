"""Publish routing catalog deltas through the gossip protocol."""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass

from mpreg.fabric.gossip import (
    GossipMessage,
    GossipMessageType,
    GossipProtocol,
)

from .catalog_delta import RoutingCatalogDelta


@dataclass(slots=True)
class CatalogDeltaPublisher:
    gossip: GossipProtocol
    ttl: int = 10
    max_hops: int = 5

    def _adaptive_max_hops(self) -> int:
        peer_count = len(self.gossip.transport.peer_ids(exclude=self.gossip.node_id))
        if peer_count <= 0:
            return self.max_hops
        required_depth = int(math.ceil(math.log2(peer_count + 1)))
        return max(self.max_hops, min(12, required_depth + 2))

    async def publish(self, delta: RoutingCatalogDelta) -> GossipMessage:
        sequence_number = self.gossip.next_sequence_number()
        adaptive_max_hops = self._adaptive_max_hops()
        message = GossipMessage(
            message_id=f"{self.gossip.node_id}:{uuid.uuid4()}",
            message_type=GossipMessageType.CATALOG_UPDATE,
            sender_id=self.gossip.node_id,
            payload=delta.to_dict(),
            vector_clock=self.gossip.vector_clock.copy(),
            sequence_number=sequence_number,
            ttl=self.ttl,
            max_hops=adaptive_max_hops,
        )
        await self.gossip.add_message(message)
        return message
