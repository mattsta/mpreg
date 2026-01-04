"""Server-backed transport for unified fabric messages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from mpreg.core.model import FabricMessageEnvelope
from mpreg.core.server_envelope_transport import ServerEnvelopeTransport
from mpreg.datastructures.type_aliases import NodeId

from .message import UnifiedMessage
from .message_codec import unified_message_to_dict

if TYPE_CHECKING:  # pragma: no cover - typing only
    pass


@dataclass(slots=True)
class ServerFabricTransport(ServerEnvelopeTransport):
    """Transport that delivers unified fabric messages over MPREG connections."""

    async def send_message(self, peer_id: NodeId, message: UnifiedMessage) -> bool:
        envelope = FabricMessageEnvelope(payload=unified_message_to_dict(message))
        return await self.send_envelope(peer_id, envelope)

    async def broadcast(
        self,
        message: UnifiedMessage,
        *,
        exclude: NodeId | None = None,
    ) -> int:
        peers = self.peer_ids(exclude=exclude)
        if not peers:
            return 0
        sent = 0
        for peer_id in peers:
            if await self.send_message(peer_id, message):
                sent += 1
        return sent
