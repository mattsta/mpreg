"""Serialization helpers for UnifiedMessage."""

from __future__ import annotations

import time
from typing import Any

from .message import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    RoutingPriority,
    UnifiedMessage,
)


def message_headers_to_dict(headers: MessageHeaders) -> dict[str, Any]:
    return {
        "correlation_id": headers.correlation_id,
        "source_cluster": headers.source_cluster,
        "target_cluster": headers.target_cluster,
        "routing_path": list(headers.routing_path),
        "federation_path": list(headers.federation_path),
        "hop_budget": headers.hop_budget,
        "priority": headers.priority.value,
        "metadata": headers.metadata,
    }


def message_headers_from_dict(payload: dict[str, Any]) -> MessageHeaders:
    correlation_id = str(payload.get("correlation_id", ""))
    if not correlation_id:
        raise ValueError("UnifiedMessage headers missing correlation_id")
    routing_path_raw = payload.get("routing_path", ())
    federation_path_raw = payload.get("federation_path", ())
    routing_path = (
        tuple(str(node) for node in routing_path_raw)
        if isinstance(routing_path_raw, (list, tuple))
        else ()
    )
    federation_path = (
        tuple(str(cluster) for cluster in federation_path_raw)
        if isinstance(federation_path_raw, (list, tuple))
        else ()
    )
    priority_raw = payload.get("priority", RoutingPriority.NORMAL.value)
    try:
        priority = RoutingPriority(str(priority_raw))
    except ValueError:
        priority = RoutingPriority.NORMAL
    hop_budget_raw = payload.get("hop_budget")
    hop_budget = None
    if hop_budget_raw is not None:
        try:
            hop_budget = int(hop_budget_raw)
        except (TypeError, ValueError):
            hop_budget = None
    metadata = payload.get("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
    return MessageHeaders(
        correlation_id=correlation_id,
        source_cluster=payload.get("source_cluster"),
        target_cluster=payload.get("target_cluster"),
        routing_path=routing_path,
        federation_path=federation_path,
        hop_budget=hop_budget,
        priority=priority,
        metadata=metadata,
    )


def unified_message_to_dict(message: UnifiedMessage) -> dict[str, Any]:
    return {
        "message_id": message.message_id,
        "topic": message.topic,
        "message_type": message.message_type.value,
        "delivery": message.delivery.value,
        "payload": message.payload,
        "headers": message_headers_to_dict(message.headers),
        "timestamp": message.timestamp,
    }


def unified_message_from_dict(payload: dict[str, Any]) -> UnifiedMessage:
    message_id = str(payload.get("message_id", ""))
    topic = str(payload.get("topic", ""))
    if not message_id or not topic:
        raise ValueError("UnifiedMessage missing message_id or topic")
    message_type_raw = payload.get("message_type", MessageType.DATA.value)
    delivery_raw = payload.get("delivery", DeliveryGuarantee.FIRE_AND_FORGET.value)
    try:
        message_type = MessageType(str(message_type_raw))
    except ValueError:
        message_type = MessageType.DATA
    try:
        delivery = DeliveryGuarantee(str(delivery_raw))
    except ValueError:
        delivery = DeliveryGuarantee.FIRE_AND_FORGET
    headers_payload = payload.get("headers", {})
    if not isinstance(headers_payload, dict):
        headers_payload = {}
    headers = message_headers_from_dict(headers_payload)
    timestamp_raw = payload.get("timestamp")
    try:
        timestamp = float(timestamp_raw) if timestamp_raw is not None else None
    except (TypeError, ValueError):
        timestamp = None
    return UnifiedMessage(
        message_id=message_id,
        topic=topic,
        message_type=message_type,
        delivery=delivery,
        payload=payload.get("payload"),
        headers=headers,
        timestamp=timestamp if timestamp is not None else time.time(),
    )
