from mpreg.fabric.message import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    RoutingPriority,
    UnifiedMessage,
)


def test_unified_message_fields() -> None:
    headers = MessageHeaders(
        correlation_id="corr-1",
        source_cluster="cluster-a",
        target_cluster="cluster-b",
        routing_path=("node-1",),
        federation_path=("cluster-a",),
        hop_budget=3,
        priority=RoutingPriority.HIGH,
        metadata={"trace_id": "trace-1"},
    )
    message = UnifiedMessage(
        message_id="msg-1",
        topic="mpreg.rpc.echo",
        message_type=MessageType.RPC,
        delivery=DeliveryGuarantee.AT_LEAST_ONCE,
        payload={"value": 1},
        headers=headers,
        timestamp=123.0,
    )

    assert message.message_id == "msg-1"
    assert message.topic == "mpreg.rpc.echo"
    assert message.message_type is MessageType.RPC
    assert message.delivery is DeliveryGuarantee.AT_LEAST_ONCE
    assert message.payload == {"value": 1}
    assert message.headers.correlation_id == "corr-1"
    assert message.headers.routing_path == ("node-1",)
    assert message.headers.metadata["trace_id"] == "trace-1"
    assert message.timestamp == 123.0
