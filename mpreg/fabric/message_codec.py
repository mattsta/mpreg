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
    payload = {
        "correlation_id": headers.correlation_id,
        "source_cluster": headers.source_cluster,
        "target_cluster": headers.target_cluster,
        "routing_path": list(headers.routing_path),
        "federation_path": list(headers.federation_path),
        "hop_budget": headers.hop_budget,
        "priority": headers.priority.value,
        "metadata": headers.metadata,
    }
    if headers.deadline_remaining_ms is not None:
        payload["deadline_remaining_ms"] = float(headers.deadline_remaining_ms)
    return payload

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
        except TypeError, ValueError:
            hop_budget = None
    metadata = payload.get("metadata", {})
    metadata = {} if not isinstance(metadata, dict) else dict(metadata)
    # Accept top-level traceparent for interop; store in metadata.
    if payload.get("traceparent") and "traceparent" not in metadata:
        metadata["traceparent"] = payload["traceparent"]
    if payload.get("tracestate") and "tracestate" not in metadata:
        metadata["tracestate"] = payload["tracestate"]
    deadline_raw = payload.get("deadline_remaining_ms")
    deadline_remaining_ms = None
    if deadline_raw is None and isinstance(metadata, dict):
        deadline_raw = metadata.get("mpreg.deadline_remaining_ms")
    if deadline_raw is not None:
        try:
            deadline_remaining_ms = float(deadline_raw)
        except TypeError, ValueError:
            deadline_remaining_ms = None
    return MessageHeaders(
        correlation_id=correlation_id,
        source_cluster=payload.get("source_cluster"),
        target_cluster=payload.get("target_cluster"),
        routing_path=routing_path,
        federation_path=federation_path,
        hop_budget=hop_budget,
        priority=priority,
        metadata=metadata,
        deadline_remaining_ms=deadline_remaining_ms,
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
    except TypeError, ValueError:
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
