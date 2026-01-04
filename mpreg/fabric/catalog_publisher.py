"""Publish routing catalog deltas through the gossip protocol."""

from __future__ import annotations

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

    async def publish(self, delta: RoutingCatalogDelta) -> GossipMessage:
        message = GossipMessage(
            message_id=f"{self.gossip.node_id}:{uuid.uuid4()}",
            message_type=GossipMessageType.CATALOG_UPDATE,
            sender_id=self.gossip.node_id,
            payload=delta.to_dict(),
            vector_clock=self.gossip.vector_clock.copy(),
            sequence_number=self.gossip.protocol_stats.messages_created,
            ttl=self.ttl,
            max_hops=self.max_hops,
        )
        await self.gossip.add_message(message)
        self.gossip.protocol_stats.messages_created += 1
        return message
