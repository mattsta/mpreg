"""Fabric gossip adapter for SharedAuditReplicator."""

from __future__ import annotations

import random
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from mpreg.fabric.gossip import GossipMessage, GossipMessageType

_TYPE_MAP = {
    "mgmt_audit_delta": GossipMessageType.MGMT_AUDIT_DELTA,
    "mgmt_audit_digest": GossipMessageType.MGMT_AUDIT_DIGEST,
    "mgmt_audit_pull": GossipMessageType.MGMT_AUDIT_PULL,
    "mgmt_audit_pull_resp": GossipMessageType.MGMT_AUDIT_PULL_RESP,
}

@dataclass(slots=True)
class FabricSharedAuditTransport:
    """Sends MGMT_AUDIT_* via ServerGossipTransport / peer send_message."""

    send_message: Callable[[str, GossipMessage], Any]
    local_node_id: str
    peer_list: Callable[[], Sequence[str]]
    gossip_targets: int = 3
    cluster_id: str = ""

    async def send_epidemic(self, message_type: str, payload: dict[str, Any]) -> int:
        peers = [p for p in self.peer_list() if p and p != self.local_node_id]
        if not peers:
            return 0
        if len(peers) <= self.gossip_targets:
            targets = list(peers)
        else:
            targets = random.sample(peers, self.gossip_targets)
        n = 0
        for peer in targets:
            if await self._send(peer, message_type, payload):
                n += 1
        return n

    async def send_unicast(
        self, peer_id: str, message_type: str, payload: dict[str, Any]
    ) -> bool:
        return await self._send(peer_id, message_type, payload)

    async def _send(
        self, peer_id: str, message_type: str, payload: dict[str, Any]
    ) -> bool:
        gtype = _TYPE_MAP.get(message_type)
        if gtype is None:
            return False
        msg = GossipMessage(
            message_id=str(uuid.uuid4()),
            message_type=gtype,
            sender_id=self.local_node_id,
            payload=payload,
            ttl=5,
            max_hops=3,
        )
        try:
            result = self.send_message(peer_id, msg)
            if hasattr(result, "__await__"):
                return bool(await result)
            return bool(result)
        except Exception as exc:  # noqa: BLE001
            logger.debug("shared_audit send failed peer={} err={}", peer_id, exc)
            return False
