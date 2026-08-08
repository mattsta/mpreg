"""Server-backed transport for fabric gossip messages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from mpreg.core.model import FabricGossipEnvelope
from mpreg.core.server_envelope_transport import ServerEnvelopeTransport
from mpreg.datastructures.type_aliases import NodeId

from .gossip_signatures import sign_gossip_payload
from .gossip_transport import GossipTransport

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .gossip import GossipMessage, GossipProtocol


@dataclass(slots=True)
class ServerGossipTransport(ServerEnvelopeTransport, GossipTransport):
    """Transport that delivers gossip messages over MPREG server connections."""

    _protocol: GossipProtocol | None = None
    require_hmac: bool = False
    hmac_secret: str | None = None

    def register(self, protocol: GossipProtocol) -> None:
        self._protocol = protocol

    def unregister(self, node_id: NodeId) -> None:
        if self._protocol and self._protocol.node_id == node_id:
            self._protocol = None

    async def send_message(self, peer_id: NodeId, message: GossipMessage) -> bool:
        payload = message.to_dict()
        if self.require_hmac and self.hmac_secret:
            payload = sign_gossip_payload(payload, self.hmac_secret)
        envelope = FabricGossipEnvelope(payload=payload)
        return await self.send_envelope(peer_id, envelope)

    async def send_preencoded(self, peer_id: NodeId, data: bytes) -> bool:
        """Fan-out identical pre-encoded gossip envelope bytes to one peer."""
        return await self.send_bytes(peer_id, data)
