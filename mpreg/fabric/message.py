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

    def with_trace_context(
        self,
        *,
        traceparent: str | None = None,
        tracestate: str | None = None,
    ) -> MessageHeaders:
        """Return headers with W3C trace context injected into metadata."""
        from mpreg.core.observability.trace_context import inject_trace_metadata

        meta = inject_trace_metadata(
            dict(self.metadata),
            traceparent=traceparent,
            tracestate=tracestate,
        )
        return MessageHeaders(
            correlation_id=self.correlation_id,
            source_cluster=self.source_cluster,
            target_cluster=self.target_cluster,
            routing_path=self.routing_path,
            federation_path=self.federation_path,
            hop_budget=self.hop_budget,
            priority=self.priority,
            metadata=meta,
        )

    @property
    def traceparent(self) -> str | None:
        from mpreg.core.observability.trace_context import extract_traceparent

        return extract_traceparent(self.metadata)

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
