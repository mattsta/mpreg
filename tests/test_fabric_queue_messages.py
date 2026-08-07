import time

import pytest

from mpreg.core.message_queue import DeliveryGuarantee as QueueDeliveryGuarantee
from mpreg.fabric.queue_messages import (
    QueueFederationAck,
    QueueFederationKind,
    QueueFederationRequest,
    QueueFederationSubscription,
    QueueMessageOptions,
    queue_message_from_dict,
)


def test_queue_message_options_round_trip() -> None:
    options = QueueMessageOptions(
        priority=2,
        delay_seconds=1.5,
        visibility_timeout_seconds=12.0,
        max_retries=3,
        acknowledgment_timeout_seconds=45.0,
        required_acknowledgments=2,
        headers={"trace_id": "abc"},
    )

    payload = options.to_dict()
    restored = QueueMessageOptions.from_dict(payload)

    assert restored == options
    assert restored.headers["trace_id"] == "abc"


def test_queue_federation_request_round_trip() -> None:
    now = time.time()
    options = QueueMessageOptions(priority=1, headers={"x": "y"})
    request = QueueFederationRequest(
        request_id="req-1",
        queue_name="jobs",
        topic="jobs.created",
        payload={"job": "run"},
        delivery_guarantee=QueueDeliveryGuarantee.AT_LEAST_ONCE,
        source_cluster="cluster-a",
        target_cluster="cluster-b",
        subscription_id="sub-1",
        subscriber_id="worker-1",
        ack_token="ack-1",
        required_cluster_acks=2,
        options=options,
        created_at=now,
    )

    payload = request.to_dict()
    restored = QueueFederationRequest.from_dict(payload)

    assert restored.request_id == "req-1"
    assert restored.queue_name == "jobs"
    assert restored.topic == "jobs.created"
    assert restored.payload == {"job": "run"}
    assert restored.delivery_guarantee is QueueDeliveryGuarantee.AT_LEAST_ONCE
    assert restored.source_cluster == "cluster-a"
    assert restored.target_cluster == "cluster-b"
    assert restored.subscription_id == "sub-1"
    assert restored.subscriber_id == "worker-1"
    assert restored.ack_token == "ack-1"
    assert restored.required_cluster_acks == 2
    assert restored.options.headers["x"] == "y"
    assert restored.created_at == now


def test_queue_federation_ack_round_trip() -> None:
    now = time.time()
    ack = QueueFederationAck(
        ack_token="ack-1",
        message_id="msg-1",
        acknowledging_cluster="cluster-b",
        acknowledging_subscriber="sub-1",
        success=False,
        error_message="boom",
        ack_timestamp=now,
    )

    payload = ack.to_dict()
    restored = QueueFederationAck.from_dict(payload)

    assert restored.ack_token == "ack-1"
    assert restored.message_id == "msg-1"
    assert restored.acknowledging_cluster == "cluster-b"
    assert restored.acknowledging_subscriber == "sub-1"
    assert restored.success is False
    assert restored.error_message == "boom"
    assert restored.ack_timestamp == now


def test_queue_federation_subscription_round_trip() -> None:
    now = time.time()
    subscription = QueueFederationSubscription(
        subscription_id="sub-1",
        subscriber_id="worker-1",
        queue_pattern="jobs-*",
        topic_pattern="jobs.*",
        delivery_guarantee=QueueDeliveryGuarantee.AT_LEAST_ONCE,
        auto_acknowledge=False,
        source_cluster="cluster-a",
        target_cluster="cluster-b",
        created_at=now,
    )

    payload = subscription.to_dict(kind=QueueFederationKind.SUBSCRIBE)
    restored = QueueFederationSubscription.from_dict(payload)

    assert restored.subscription_id == "sub-1"
    assert restored.subscriber_id == "worker-1"
    assert restored.queue_pattern == "jobs-*"
    assert restored.topic_pattern == "jobs.*"
    assert restored.delivery_guarantee is QueueDeliveryGuarantee.AT_LEAST_ONCE
    assert restored.auto_acknowledge is False
    assert restored.source_cluster == "cluster-a"
    assert restored.target_cluster == "cluster-b"
    assert restored.created_at == now


def test_queue_message_from_dict() -> None:
    request = QueueFederationRequest(
        request_id="req-1",
        queue_name="jobs",
        topic="jobs.created",
        payload="payload",
        delivery_guarantee=QueueDeliveryGuarantee.AT_LEAST_ONCE,
        source_cluster="cluster-a",
        target_cluster="cluster-b",
    )
    ack = QueueFederationAck(
        ack_token="ack-1",
        message_id="msg-1",
        acknowledging_cluster="cluster-b",
        acknowledging_subscriber="sub-1",
    )

    parsed_request = queue_message_from_dict(request.to_dict())
    parsed_ack = queue_message_from_dict(ack.to_dict())

    assert isinstance(parsed_request, QueueFederationRequest)
    assert isinstance(parsed_ack, QueueFederationAck)

    unknown = {"kind": "queue-unknown"}
    with pytest.raises(ValueError, match="Unknown queue federation message kind"):
        queue_message_from_dict(unknown)
