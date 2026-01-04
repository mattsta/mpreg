from __future__ import annotations

import pytest

from mpreg.fabric.gossip import GossipMessage, GossipMessageType, GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport


@pytest.mark.asyncio
async def test_inprocess_gossip_transport_delivers_message() -> None:
    transport = InProcessGossipTransport()
    protocol = GossipProtocol(node_id="node-a", transport=transport)
    await protocol.start()

    message = GossipMessage(
        message_id="msg-1",
        message_type=GossipMessageType.STATE_UPDATE,
        sender_id="node-b",
        payload={"key": "alpha", "value": "beta"},
    )

    delivered = await transport.send_message("node-a", message)

    assert delivered
    assert "msg-1" in protocol.recent_messages

    await protocol.stop()


def test_gossip_message_round_trip() -> None:
    message = GossipMessage(
        message_id="msg-2",
        message_type=GossipMessageType.CONFIG_UPDATE,
        sender_id="node-a",
        payload={"config_key": "alpha", "config_value": "beta"},
    )

    payload = message.to_dict()
    restored = GossipMessage.from_dict(payload)

    assert restored.message_id == "msg-2"
    assert restored.message_type is GossipMessageType.CONFIG_UPDATE
    assert restored.sender_id == "node-a"
    assert hasattr(restored.payload, "config_key")
    assert restored.payload.config_key == "alpha"
    assert restored.payload.config_value == "beta"
