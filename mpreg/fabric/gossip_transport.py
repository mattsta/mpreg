"""Transport abstraction for fabric gossip delivery."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from mpreg.datastructures.type_aliases import NodeId

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .gossip import GossipMessage, GossipProtocol


class GossipTransport(Protocol):
    def register(self, protocol: GossipProtocol) -> None: ...

    def unregister(self, node_id: NodeId) -> None: ...

    def peer_ids(self, *, exclude: NodeId | None = None) -> tuple[NodeId, ...]: ...

    async def send_message(self, peer_id: NodeId, message: GossipMessage) -> bool: ...


@dataclass(slots=True)
class InProcessGossipTransport:
    """In-process transport for fabric gossip (test/local use)."""

    _peers: dict[NodeId, GossipProtocol] = field(default_factory=dict)

    def register(self, protocol: GossipProtocol) -> None:
        self._peers[protocol.node_id] = protocol

    def unregister(self, node_id: NodeId) -> None:
        self._peers.pop(node_id, None)

    def peer_ids(self, *, exclude: NodeId | None = None) -> tuple[NodeId, ...]:
        peers = [peer_id for peer_id in self._peers if peer_id != exclude]
        return tuple(sorted(peers))

    async def send_message(self, peer_id: NodeId, message: GossipMessage) -> bool:
        protocol = self._peers.get(peer_id)
        if protocol is None:
            return False
        await protocol.handle_received_message(message)
        return True
