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
    """Delivery guarantees for fabric messages.

    Queue local storage uses ``mpreg.core.message_queue.DeliveryGuarantee``.
    Shared wire values: fire_and_forget, at_least_once, exactly_once, broadcast,
    quorum. Convert via ``DeliveryGuarantee(other.value)`` at plane boundaries.
    """

    FIRE_AND_FORGET = "fire_and_forget"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"
    BROADCAST = "broadcast"
    QUORUM = "quorum"

    @classmethod
    def from_queue_value(cls, value: str | DeliveryGuarantee) -> DeliveryGuarantee:
        """Map a queue-plane or wire string onto the fabric enum."""
        if isinstance(value, DeliveryGuarantee):
            return value
        return cls(str(value))

    @classmethod
    def from_wire(cls, value: str | DeliveryGuarantee) -> DeliveryGuarantee:
        """Parse a wire/string guarantee onto the fabric enum (COR-14)."""
        if isinstance(value, cls):
            return value
        raw = getattr(value, "value", value)
        return cls(str(raw))

    def to_queue_guarantee(self) -> Any:
        """Map fabric guarantee onto the queue-plane enum.

        Raises:
            ValueError: EXACTLY_ONCE has no queue member (COR-14).
        """
        from mpreg.core.message_queue import DeliveryGuarantee as QueueDG

        if self is DeliveryGuarantee.EXACTLY_ONCE:
            raise ValueError(
                "unsupported_delivery_guarantee:exactly_once "
                "(queue plane has no EXACTLY_ONCE member)"
            )
        return QueueDG(self.value)

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
    # Soft-RT remaining budget in milliseconds (INV-P2/C1). None = no deadline.
    deadline_remaining_ms: float | None = None

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
            deadline_remaining_ms=self.deadline_remaining_ms,
        )

    @property
    def traceparent(self) -> str | None:
        from mpreg.core.observability.trace_context import extract_traceparent

        return extract_traceparent(self.metadata)

    def with_deadline_remaining_ms(self, remaining_ms: float | None) -> MessageHeaders:
        """Return headers with updated soft-RT deadline budget."""
        return MessageHeaders(
            correlation_id=self.correlation_id,
            source_cluster=self.source_cluster,
            target_cluster=self.target_cluster,
            routing_path=self.routing_path,
            federation_path=self.federation_path,
            hop_budget=self.hop_budget,
            priority=self.priority,
            metadata=dict(self.metadata),
            deadline_remaining_ms=remaining_ms,
        )

    def deadline_exhausted(self) -> bool:
        """True when a deadline was set and remaining budget is <= 0."""
        if self.deadline_remaining_ms is None:
            return False
        return float(self.deadline_remaining_ms) <= 0.0

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
