"""Unified message envelope for the routing fabric."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from mpreg.datastructures.type_aliases import (
    ClusterId,
    CorrelationId,
    HopCount,
    MessageIdString,
    NodeId,
    Timestamp,
    TopicName,
)


class MessageType(Enum):
    """Canonical message types for the routing fabric."""

    RPC = "rpc"
    PUBSUB = "pubsub"
    QUEUE = "queue"
    CACHE = "cache"
    CONTROL = "control"
    DATA = "data"


class DeliveryGuarantee(Enum):
    """Delivery guarantees for fabric messages."""

    FIRE_AND_FORGET = "fire_and_forget"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"
    BROADCAST = "broadcast"
    QUORUM = "quorum"


class RoutingPriority(Enum):
    """Routing priority levels."""

    CRITICAL = "critical"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"
    BULK = "bulk"


@dataclass(frozen=True, slots=True)
class MessageHeaders:
    """Common headers for all fabric messages."""

    correlation_id: CorrelationId
    source_cluster: ClusterId | None = None
    target_cluster: ClusterId | None = None
    routing_path: tuple[NodeId, ...] = field(default_factory=tuple)
    federation_path: tuple[ClusterId, ...] = field(default_factory=tuple)
    hop_budget: HopCount | None = None
    priority: RoutingPriority = RoutingPriority.NORMAL
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class UnifiedMessage:
    """Canonical message envelope for the routing fabric."""

    message_id: MessageIdString
    topic: TopicName
    message_type: MessageType
    delivery: DeliveryGuarantee
    payload: Any
    headers: MessageHeaders
    timestamp: Timestamp = field(default_factory=time.time)
