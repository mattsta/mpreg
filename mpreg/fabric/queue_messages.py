"""Fabric queue federation message formats and serialization helpers."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from mpreg.core.message_queue import DeliveryGuarantee as QueueDeliveryGuarantee
from mpreg.datastructures.type_aliases import (
    ClusterId,
    MessageIdString,
    QueueName,
    SubscriberId,
    Timestamp,
    TopicName,
)


class QueueFederationKind(Enum):
    """Supported fabric queue federation message kinds."""

    REQUEST = "queue_request"
    ACK = "queue_ack"
    SUBSCRIBE = "queue_subscribe"
    UNSUBSCRIBE = "queue_unsubscribe"


@dataclass(frozen=True, slots=True)
class QueueMessageOptions:
    """Structured queue message options for federation routing."""

    priority: int = 0
    delay_seconds: float = 0.0
    visibility_timeout_seconds: float | None = None
    max_retries: int | None = None
    acknowledgment_timeout_seconds: float | None = None
    required_acknowledgments: int | None = None
    headers: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "priority": int(self.priority),
            "delay_seconds": float(self.delay_seconds),
            "visibility_timeout_seconds": self.visibility_timeout_seconds,
            "max_retries": self.max_retries,
            "acknowledgment_timeout_seconds": self.acknowledgment_timeout_seconds,
            "required_acknowledgments": self.required_acknowledgments,
            "headers": dict(self.headers),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> QueueMessageOptions:
        return cls(
            priority=int(payload.get("priority", 0)),
            delay_seconds=float(payload.get("delay_seconds", 0.0)),
            visibility_timeout_seconds=payload.get("visibility_timeout_seconds"),
            max_retries=payload.get("max_retries"),
            acknowledgment_timeout_seconds=payload.get(
                "acknowledgment_timeout_seconds"
            ),
            required_acknowledgments=payload.get("required_acknowledgments"),
            headers=dict(payload.get("headers", {})),
        )


@dataclass(frozen=True, slots=True)
class QueueFederationRequest:
    """Queue delivery request routed through the fabric."""

    request_id: MessageIdString
    queue_name: QueueName
    topic: TopicName
    payload: Any
    delivery_guarantee: QueueDeliveryGuarantee
    source_cluster: ClusterId
    target_cluster: ClusterId
    subscription_id: str | None = None
    subscriber_id: SubscriberId | None = None
    ack_token: str | None = None
    required_cluster_acks: int = 1
    options: QueueMessageOptions = field(default_factory=QueueMessageOptions)
    created_at: Timestamp = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": QueueFederationKind.REQUEST.value,
            "request_id": self.request_id,
            "queue_name": self.queue_name,
            "topic": self.topic,
            "payload": self.payload,
            "delivery_guarantee": self.delivery_guarantee.value,
            "source_cluster": self.source_cluster,
            "target_cluster": self.target_cluster,
            "subscription_id": self.subscription_id,
            "subscriber_id": self.subscriber_id,
            "ack_token": self.ack_token,
            "required_cluster_acks": int(self.required_cluster_acks),
            "options": self.options.to_dict(),
            "created_at": float(self.created_at),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> QueueFederationRequest:
        return cls(
            request_id=str(payload.get("request_id", "")),
            queue_name=str(payload.get("queue_name", "")),
            topic=str(payload.get("topic", "")),
            payload=payload.get("payload"),
            delivery_guarantee=QueueDeliveryGuarantee(
                str(
                    payload.get(
                        "delivery_guarantee", QueueDeliveryGuarantee.AT_LEAST_ONCE.value
                    )
                )
            ),
            source_cluster=str(payload.get("source_cluster", "")),
            target_cluster=str(payload.get("target_cluster", "")),
            subscription_id=payload.get("subscription_id"),
            subscriber_id=payload.get("subscriber_id"),
            ack_token=payload.get("ack_token"),
            required_cluster_acks=int(payload.get("required_cluster_acks", 1)),
            options=QueueMessageOptions.from_dict(dict(payload.get("options", {}))),
            created_at=float(payload.get("created_at", time.time())),
        )


@dataclass(frozen=True, slots=True)
class QueueFederationAck:
    """Acknowledgment routed through the fabric for queue deliveries."""

    ack_token: str
    message_id: MessageIdString
    acknowledging_cluster: ClusterId
    acknowledging_subscriber: SubscriberId
    success: bool = True
    error_message: str | None = None
    ack_timestamp: Timestamp = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": QueueFederationKind.ACK.value,
            "ack_token": self.ack_token,
            "message_id": self.message_id,
            "acknowledging_cluster": self.acknowledging_cluster,
            "acknowledging_subscriber": self.acknowledging_subscriber,
            "success": bool(self.success),
            "error_message": self.error_message,
            "ack_timestamp": float(self.ack_timestamp),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> QueueFederationAck:
        return cls(
            ack_token=str(payload.get("ack_token", "")),
            message_id=str(payload.get("message_id", "")),
            acknowledging_cluster=str(payload.get("acknowledging_cluster", "")),
            acknowledging_subscriber=str(payload.get("acknowledging_subscriber", "")),
            success=bool(payload.get("success", True)),
            error_message=payload.get("error_message"),
            ack_timestamp=float(payload.get("ack_timestamp", time.time())),
        )


@dataclass(frozen=True, slots=True)
class QueueFederationSubscription:
    """Subscription control message for federated queues."""

    subscription_id: str
    subscriber_id: SubscriberId
    queue_pattern: str
    topic_pattern: str
    delivery_guarantee: QueueDeliveryGuarantee
    auto_acknowledge: bool = True
    source_cluster: ClusterId = ""
    target_cluster: ClusterId = ""
    created_at: Timestamp = field(default_factory=time.time)

    def to_dict(self, *, kind: QueueFederationKind) -> dict[str, Any]:
        return {
            "kind": kind.value,
            "subscription_id": self.subscription_id,
            "subscriber_id": self.subscriber_id,
            "queue_pattern": self.queue_pattern,
            "topic_pattern": self.topic_pattern,
            "delivery_guarantee": self.delivery_guarantee.value,
            "auto_acknowledge": bool(self.auto_acknowledge),
            "source_cluster": self.source_cluster,
            "target_cluster": self.target_cluster,
            "created_at": float(self.created_at),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> QueueFederationSubscription:
        return cls(
            subscription_id=str(payload.get("subscription_id", "")),
            subscriber_id=str(payload.get("subscriber_id", "")),
            queue_pattern=str(payload.get("queue_pattern", "")),
            topic_pattern=str(payload.get("topic_pattern", "")),
            delivery_guarantee=QueueDeliveryGuarantee(
                str(
                    payload.get(
                        "delivery_guarantee", QueueDeliveryGuarantee.AT_LEAST_ONCE.value
                    )
                )
            ),
            auto_acknowledge=bool(payload.get("auto_acknowledge", True)),
            source_cluster=str(payload.get("source_cluster", "")),
            target_cluster=str(payload.get("target_cluster", "")),
            created_at=float(payload.get("created_at", time.time())),
        )


QueueFederationMessage = (
    QueueFederationRequest | QueueFederationAck | QueueFederationSubscription
)


def queue_message_from_dict(payload: dict[str, Any]) -> QueueFederationMessage:
    kind = payload.get("kind")
    if kind == QueueFederationKind.REQUEST.value:
        return QueueFederationRequest.from_dict(payload)
    if kind == QueueFederationKind.ACK.value:
        return QueueFederationAck.from_dict(payload)
    if kind in (
        QueueFederationKind.SUBSCRIBE.value,
        QueueFederationKind.UNSUBSCRIBE.value,
    ):
        return QueueFederationSubscription.from_dict(payload)
    raise ValueError(f"Unknown queue federation message kind: {kind}")
