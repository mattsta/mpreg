"""
Unified routing system for MPREG's three core communication systems.

This module provides a unified interface and routing coordination layer that extends
MPREG's existing sophisticated federation and routing infrastructure. It enables
consistent routing patterns across RPC, Topic Pub/Sub, and Message Queue systems
while leveraging the existing graph-based routing algorithms and federation bridge.

Key features:
- Unified message routing interface across all three systems
- Integration with existing GraphBasedFederationRouter
- Type-safe routing with comprehensive semantic type aliases
- Policy-based routing decisions with performance awareness
- Cross-system message correlation and monitoring
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from mpreg.datastructures.type_aliases import (
    ClusterId,
    HopCount,
    NodeId,
    PathLatencyMs,
    RouteCostScore,
)

# New unified routing type aliases following MPREG conventions
type UnifiedRouteId = str
type SystemMessageType = str
type TopicPattern = str
type RouteHandlerId = str  # Handler identifier for routing targets
type RoutingPolicyId = str
type MessageCorrelationId = str


class MessageType(Enum):
    """Core message types in the unified routing system."""

    RPC = "rpc"
    PUBSUB = "pubsub"
    QUEUE = "queue"
    CACHE = "cache"  # Cache coordination and management
    FEDERATION = "federation"
    CONTROL_PLANE = "control_plane"  # Internal system coordination
    DATA_PLANE = "data_plane"  # User-controlled data


class DeliveryGuarantee(Enum):
    """Delivery guarantee levels for unified routing."""

    FIRE_AND_FORGET = "fire_and_forget"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"
    BROADCAST = "broadcast"
    QUORUM = "quorum"


class RoutingPriority(Enum):
    """Message routing priority levels."""

    CRITICAL = "critical"  # System control messages
    HIGH = "high"  # Time-sensitive operations
    NORMAL = "normal"  # Standard operations
    LOW = "low"  # Background/batch operations
    BULK = "bulk"  # Large data transfers


@dataclass(frozen=True, slots=True)
class MessageHeaders:
    """Unified message headers for cross-system correlation."""

    correlation_id: MessageCorrelationId
    source_system: MessageType
    target_system: MessageType | None = None
    priority: RoutingPriority = RoutingPriority.NORMAL
    federation_hop: HopCount | None = None
    source_cluster: ClusterId | None = None
    target_cluster: ClusterId | None = None
    routing_metadata: dict[str, Any] = field(default_factory=dict)

    def with_federation_hop(self, hop_count: HopCount) -> MessageHeaders:
        """Create new headers with federation hop count."""
        return MessageHeaders(
            correlation_id=self.correlation_id,
            source_system=self.source_system,
            target_system=self.target_system,
            priority=self.priority,
            federation_hop=hop_count,
            source_cluster=self.source_cluster,
            target_cluster=self.target_cluster,
            routing_metadata=self.routing_metadata,
        )


@dataclass(frozen=True, slots=True)
class UnifiedMessage:
    """Core unified message structure for cross-system routing."""

    topic: TopicPattern
    routing_type: MessageType
    delivery_guarantee: DeliveryGuarantee
    payload: Any
    headers: MessageHeaders
    timestamp: float = field(default_factory=time.time)

    @property
    def is_federation_message(self) -> bool:
        """Check if this message requires federation routing."""
        return (
            self.headers.federation_hop is not None
            or self.headers.target_cluster is not None
            or self.topic.startswith("mpreg.federation.")
        )

    @property
    def is_control_plane_message(self) -> bool:
        """Check if this is an internal control plane message."""
        return (
            self.topic.startswith("mpreg.")
            and self.routing_type == MessageType.CONTROL_PLANE
        )

    def with_routing_metadata(self, key: str, value: Any) -> UnifiedMessage:
        """Create new message with additional routing metadata."""
        new_headers = MessageHeaders(
            correlation_id=self.headers.correlation_id,
            source_system=self.headers.source_system,
            target_system=self.headers.target_system,
            priority=self.headers.priority,
            federation_hop=self.headers.federation_hop,
            source_cluster=self.headers.source_cluster,
            target_cluster=self.headers.target_cluster,
            routing_metadata={**self.headers.routing_metadata, key: value},
        )
        return UnifiedMessage(
            topic=self.topic,
            routing_type=self.routing_type,
            delivery_guarantee=self.delivery_guarantee,
            payload=self.payload,
            headers=new_headers,
            timestamp=self.timestamp,
        )


@dataclass(frozen=True, slots=True)
class RouteTarget:
    """Target destination for routing decisions."""

    system_type: MessageType
    target_id: str  # Queue name, RPC function, or subscription ID
    node_id: NodeId | None = None
    cluster_id: ClusterId | None = None
    priority_weight: float = 1.0


@dataclass(frozen=True, slots=True)
class RouteResult:
    """Result of unified routing decision."""

    route_id: UnifiedRouteId
    targets: list[RouteTarget]
    routing_path: list[NodeId]
    federation_path: list[ClusterId]
    estimated_latency_ms: PathLatencyMs
    route_cost: RouteCostScore
    federation_required: bool
    hops_required: HopCount

    @property
    def is_local_route(self) -> bool:
        """Check if route is entirely local (no federation required)."""
        return not self.federation_required and len(self.federation_path) <= 1

    @property
    def is_multi_target(self) -> bool:
        """Check if route delivers to multiple targets (broadcast/fanout)."""
        return len(self.targets) > 1


@dataclass(frozen=True, slots=True)
class RoutingPolicy:
    """Policy configuration for routing decisions."""

    policy_id: RoutingPolicyId
    message_type_filter: set[MessageType] | None = None
    topic_pattern_filter: str | None = None
    priority_filter: set[RoutingPriority] | None = None

    # Routing preferences
    prefer_local_routing: bool = True
    max_federation_hops: HopCount = 5
    enable_load_balancing: bool = True
    enable_geographic_optimization: bool = True

    # Performance thresholds
    max_latency_ms: PathLatencyMs = 10000.0
    max_route_cost: RouteCostScore = 100.0
    min_reliability_threshold: float = 0.95

    def matches_message(self, message: UnifiedMessage) -> bool:
        """Check if this policy applies to the given message."""
        if (
            self.message_type_filter
            and message.routing_type not in self.message_type_filter
        ):
            return False

        if (
            self.priority_filter
            and message.headers.priority not in self.priority_filter
        ):
            return False

        if self.topic_pattern_filter:
            # Simple pattern matching - could be enhanced with trie matching
            import fnmatch

            if not fnmatch.fnmatch(message.topic, self.topic_pattern_filter):
                return False

        return True


@dataclass(slots=True)
class UnifiedRoutingConfig:
    """Configuration for the unified routing system."""

    # Feature flags
    enable_topic_rpc: bool = True
    enable_queue_topics: bool = True
    enable_cross_system_correlation: bool = True
    enable_federation_optimization: bool = True

    # Performance settings
    federation_timeout_ms: float = 5000.0
    max_routing_hops: HopCount = 10
    routing_cache_ttl_ms: float = 30000.0
    max_cached_routes: int = 10000

    # Policy settings
    default_policy: RoutingPolicy = field(
        default_factory=lambda: RoutingPolicy("default")
    )
    policies: list[RoutingPolicy] = field(default_factory=list)

    # Integration settings
    use_existing_graph_router: bool = True
    use_existing_federation_bridge: bool = True
    enable_gossip_coordination: bool = True

    def get_policy_for_message(self, message: UnifiedMessage) -> RoutingPolicy:
        """Get the most specific policy that matches the message."""
        for policy in self.policies:
            if policy.matches_message(message):
                return policy
        return self.default_policy


@dataclass(frozen=True, slots=True)
class RoutingStatisticsSnapshot:
    """Snapshot of unified routing statistics."""

    total_routes_computed: int
    local_routes: int
    federation_routes: int
    multi_target_routes: int

    average_route_computation_ms: float
    average_local_latency_ms: PathLatencyMs
    average_federation_latency_ms: PathLatencyMs

    cache_hit_ratio: float
    policy_application_rate: float

    # Cross-system routing breakdown
    rpc_routes: int
    pubsub_routes: int
    queue_routes: int
    cache_routes: int
    control_plane_routes: int

    @property
    def federation_ratio(self) -> float:
        """Percentage of routes requiring federation."""
        total = self.total_routes_computed
        return (self.federation_routes / total) if total > 0 else 0.0

    @property
    def local_efficiency(self) -> float:
        """Efficiency metric for local routing."""
        return self.cache_hit_ratio * (1.0 - self.federation_ratio)


class RouteHandler(Protocol):
    """Protocol for route handling implementations."""

    async def handle_route(self, message: UnifiedMessage, route: RouteResult) -> bool:
        """Handle routing of a unified message to its targets."""
        ...

    async def get_handler_statistics(self) -> dict[str, Any]:
        """Get statistics about route handling performance."""
        ...


class UnifiedRouter(Protocol):
    """Protocol defining the unified routing interface."""

    async def route_message(self, message: UnifiedMessage) -> RouteResult:
        """Route a unified message and return the routing decision."""
        ...

    async def register_route_pattern(
        self, pattern: TopicPattern, handler: RouteHandlerId
    ) -> None:
        """Register a route pattern with a specific handler."""
        ...

    async def register_routing_policy(self, policy: RoutingPolicy) -> None:
        """Register a routing policy for specific message patterns."""
        ...

    async def get_routing_statistics(self) -> RoutingStatisticsSnapshot:
        """Get comprehensive routing statistics."""
        ...

    async def invalidate_routing_cache(
        self, pattern: TopicPattern | None = None
    ) -> None:
        """Invalidate routing cache for specific patterns or all routes."""
        ...


class SystemRouteRegistrar(Protocol):
    """Protocol for system-specific route registration."""

    async def register_rpc_routes(self, router: UnifiedRouter) -> None:
        """Register RPC-specific routing patterns."""
        ...

    async def register_pubsub_routes(self, router: UnifiedRouter) -> None:
        """Register pub/sub routing patterns."""
        ...

    async def register_queue_routes(self, router: UnifiedRouter) -> None:
        """Register message queue routing patterns."""
        ...

    async def register_federation_routes(self, router: UnifiedRouter) -> None:
        """Register federation routing patterns."""
        ...


# Utility functions for routing


def create_correlation_id() -> MessageCorrelationId:
    """Create a unique correlation ID for message tracking."""
    import uuid

    return f"corr_{uuid.uuid4().hex[:16]}"


def create_route_id(message: UnifiedMessage) -> UnifiedRouteId:
    """Create a unique route ID based on message characteristics."""
    import hashlib

    route_key = (
        f"{message.topic}:{message.routing_type.value}:{message.headers.correlation_id}"
    )
    return f"route_{hashlib.sha256(route_key.encode()).hexdigest()[:16]}"


def is_internal_topic(topic: TopicPattern) -> bool:
    """Check if a topic is in the internal mpreg.* namespace."""
    return topic.startswith("mpreg.")


def extract_system_from_topic(topic: TopicPattern) -> MessageType | None:
    """Extract the target system type from a topic pattern."""
    if topic.startswith("mpreg.rpc."):
        return MessageType.RPC
    elif topic.startswith("mpreg.queue."):
        return MessageType.QUEUE
    elif topic.startswith("mpreg.pubsub."):
        return MessageType.PUBSUB
    elif topic.startswith("mpreg.cache."):
        return MessageType.CACHE
    elif topic.startswith("mpreg.federation."):
        return MessageType.FEDERATION
    elif topic.startswith("mpreg."):
        return MessageType.CONTROL_PLANE
    else:
        return MessageType.DATA_PLANE
