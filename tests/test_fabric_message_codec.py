from mpreg.fabric.message import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    RoutingPriority,
    UnifiedMessage,
)
from mpreg.fabric.message_codec import (
    unified_message_from_dict,
    unified_message_to_dict,
)


def test_unified_message_roundtrip() -> None:
    headers = MessageHeaders(
        correlation_id="corr-1",
        source_cluster="cluster-a",
        target_cluster="cluster-b",
        routing_path=("node-a", "node-b"),
        federation_path=("cluster-a", "cluster-b"),
        hop_budget=2,
        priority=RoutingPriority.HIGH,
        metadata={"trace": "abc"},
    )
    message = UnifiedMessage(
        message_id="msg-1",
        topic="topic.events",
        message_type=MessageType.PUBSUB,
        delivery=DeliveryGuarantee.BROADCAST,
        payload={"hello": "world"},
        headers=headers,
        timestamp=123.45,
    )

    encoded = unified_message_to_dict(message)
    decoded = unified_message_from_dict(encoded)

    assert decoded == message
