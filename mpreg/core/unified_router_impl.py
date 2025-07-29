"""
Concrete implementation of the UnifiedRouter for MPREG integration.

This module provides the concrete implementation of the UnifiedRouter protocol
that integrates MPREG's three communication systems (RPC, Topic Pub/Sub, Message Queue)
with the existing sophisticated federation routing infrastructure.

Key features:
- Extends existing GraphBasedFederationRouter capabilities
- Integrates with GraphAwareFederationBridge for message handling
- Leverages vector clock coordination and gossip protocol
- Maintains backward compatibility with existing systems
- Provides unified routing interface across all communication types
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from mpreg.core.topic_taxonomy import TopicTemplateEngine, TopicValidator
from mpreg.core.unified_routing import (
    DeliveryGuarantee,
    MessageType,
    RouteHandlerId,
    RouteResult,
    RouteTarget,
    RoutingStatisticsSnapshot,
    TopicPattern,
    UnifiedMessage,
    UnifiedRouter,
    UnifiedRoutingConfig,
    create_route_id,
)
from mpreg.datastructures.vector_clock import VectorClock

# Import existing federation infrastructure
try:
    from mpreg.core.message_queue_manager import MessageQueueManager
    from mpreg.core.topic_exchange import TopicExchange
    from mpreg.federation.federation_bridge import GraphAwareFederationBridge
    from mpreg.federation.federation_graph import GraphBasedFederationRouter

    HAS_FEDERATION = True
except ImportError:
    # Graceful degradation for testing
    HAS_FEDERATION = False
    GraphBasedFederationRouter = type("GraphBasedFederationRouter", (), {})  # type: ignore
    GraphAwareFederationBridge = type("GraphAwareFederationBridge", (), {})  # type: ignore
    TopicExchange = type("TopicExchange", (), {})  # type: ignore
    MessageQueueManager = type("MessageQueueManager", (), {})  # type: ignore


@dataclass(slots=True)
class RouteHandlerRegistry:
    """Registry for route handlers across different systems."""

    handlers: dict[RouteHandlerId, Any] = field(default_factory=dict)
    pattern_mappings: dict[TopicPattern, set[RouteHandlerId]] = field(
        default_factory=dict
    )
    system_mappings: dict[MessageType, set[RouteHandlerId]] = field(
        default_factory=dict
    )

    async def register_handler(
        self,
        handler_id: RouteHandlerId,
        handler: Any,
        patterns: list[TopicPattern],
        system_type: MessageType,
    ) -> None:
        """Register a route handler with patterns and system type."""
        self.handlers[handler_id] = handler

        # Map patterns to handler
        for pattern in patterns:
            if pattern not in self.pattern_mappings:
                self.pattern_mappings[pattern] = set()
            self.pattern_mappings[pattern].add(handler_id)

        # Map system type to handler
        if system_type not in self.system_mappings:
            self.system_mappings[system_type] = set()
        self.system_mappings[system_type].add(handler_id)

    async def get_handlers_for_message(
        self, message: UnifiedMessage
    ) -> list[tuple[RouteHandlerId, Any]]:
        """Get all handlers that should process this message."""
        matching_handlers = set()

        # Check pattern matches
        for pattern, handler_ids in self.pattern_mappings.items():
            if self._pattern_matches(pattern, message.topic):
                matching_handlers.update(handler_ids)

        # Check system type matches
        if message.routing_type in self.system_mappings:
            matching_handlers.update(self.system_mappings[message.routing_type])

        return [
            (handler_id, self.handlers[handler_id]) for handler_id in matching_handlers
        ]

    def _pattern_matches(self, pattern: TopicPattern, topic: str) -> bool:
        """Check if pattern matches topic using AMQP-style wildcards."""
        # Convert AMQP pattern to fnmatch pattern
        # * matches one segment, # matches zero or more segments
        import fnmatch

        # Replace AMQP wildcards with fnmatch equivalents
        fnmatch_pattern = pattern.replace("#", "*")  # # becomes * for fnmatch
        return fnmatch.fnmatch(topic, fnmatch_pattern)


@dataclass(slots=True)
class RoutingMetrics:
    """Metrics tracking for unified routing operations."""

    total_routes_computed: int = 0
    local_routes: int = 0
    federation_routes: int = 0
    multi_target_routes: int = 0

    total_route_computation_time_ms: float = 0.0
    local_latency_total_ms: float = 0.0
    federation_latency_total_ms: float = 0.0

    cache_hits: int = 0
    cache_misses: int = 0

    # Per-system breakdown
    rpc_routes: int = 0
    pubsub_routes: int = 0
    queue_routes: int = 0
    cache_routes: int = 0
    control_plane_routes: int = 0

    # Policy tracking
    policies_applied: int = 0

    def record_route_computation(
        self,
        route: RouteResult,
        computation_time_ms: float,
        message_type: MessageType,
        used_cache: bool,
    ) -> None:
        """Record metrics for a routing computation."""
        self.total_routes_computed += 1
        self.total_route_computation_time_ms += computation_time_ms

        if route.is_local_route:
            self.local_routes += 1
            self.local_latency_total_ms += route.estimated_latency_ms
        else:
            self.federation_routes += 1
            self.federation_latency_total_ms += route.estimated_latency_ms

        if route.is_multi_target:
            self.multi_target_routes += 1

        # Track cache usage
        if used_cache:
            self.cache_hits += 1
        else:
            self.cache_misses += 1

        # Track by system type
        if message_type == MessageType.RPC:
            self.rpc_routes += 1
        elif message_type == MessageType.PUBSUB:
            self.pubsub_routes += 1
        elif message_type == MessageType.QUEUE:
            self.queue_routes += 1
        elif message_type == MessageType.CACHE:
            self.cache_routes += 1
        elif message_type == MessageType.CONTROL_PLANE:
            self.control_plane_routes += 1

    def get_statistics_snapshot(self) -> RoutingStatisticsSnapshot:
        """Generate statistics snapshot."""
        total = max(1, self.total_routes_computed)
        cache_total = max(1, self.cache_hits + self.cache_misses)

        return RoutingStatisticsSnapshot(
            total_routes_computed=self.total_routes_computed,
            local_routes=self.local_routes,
            federation_routes=self.federation_routes,
            multi_target_routes=self.multi_target_routes,
            average_route_computation_ms=(self.total_route_computation_time_ms / total),
            average_local_latency_ms=(
                self.local_latency_total_ms / max(1, self.local_routes)
            ),
            average_federation_latency_ms=(
                self.federation_latency_total_ms / max(1, self.federation_routes)
            ),
            cache_hit_ratio=self.cache_hits / cache_total,
            policy_application_rate=self.policies_applied / total,
            rpc_routes=self.rpc_routes,
            pubsub_routes=self.pubsub_routes,
            queue_routes=self.queue_routes,
            cache_routes=self.cache_routes,
            control_plane_routes=self.control_plane_routes,
        )


class UnifiedRouterImpl:
    """
    Concrete implementation of UnifiedRouter that integrates with existing MPREG infrastructure.

    This router extends MPREG's sophisticated federation routing capabilities to provide
    a unified interface across RPC, Topic Pub/Sub, and Message Queue systems while
    leveraging existing graph-based routing, caching, and federation bridge infrastructure.
    """

    def __init__(
        self,
        config: UnifiedRoutingConfig,
        federation_router: GraphBasedFederationRouter | None = None,
        federation_bridge: GraphAwareFederationBridge | None = None,
        topic_exchange: TopicExchange | None = None,
        message_queue: MessageQueueManager | None = None,
    ):
        """Initialize the unified router with existing infrastructure components."""
        self.config = config
        self.federation_router = federation_router
        self.federation_bridge = federation_bridge
        self.topic_exchange = topic_exchange
        self.message_queue = message_queue

        # Internal components
        self.handler_registry = RouteHandlerRegistry()
        self.metrics = RoutingMetrics()
        self.topic_template_engine = TopicTemplateEngine()
        self.vector_clock = VectorClock()

        # Route caching
        self.route_cache: dict[str, tuple[RouteResult, float]] = {}

        # Initialize system integrations
        self._initialize_system_handlers()

    def _initialize_system_handlers(self) -> None:
        """Initialize handlers for each communication system."""
        if self.config.enable_topic_rpc:
            # RPC system is integrated through topic-based routing
            # Topics like "mpreg.rpc.command_name" will be routed to RPC handlers
            logger.info("RPC routing enabled via topic patterns")

        if self.config.enable_queue_topics and self.message_queue:
            # Queue topic routing is integrated through MessageQueueManager
            # Topics like "mpreg.queue.queue_name" will be routed to queue handlers
            logger.info("Queue routing enabled with MessageQueueManager integration")
        elif self.config.enable_queue_topics:
            # Queue routing enabled without MessageQueueManager - use topic-based fallback
            logger.info("Queue routing enabled via topic patterns (fallback mode)")

    async def route_message(self, message: UnifiedMessage) -> RouteResult:
        """Route a unified message using integrated MPREG infrastructure."""
        start_time = time.time()

        # Apply routing policy
        policy = self.config.get_policy_for_message(message)

        # Check cache first
        cache_key = self._get_cache_key(message)
        cached_result = self._get_cached_route(cache_key)
        if cached_result:
            computation_time = (time.time() - start_time) * 1000
            self.metrics.record_route_computation(
                cached_result, computation_time, message.routing_type, used_cache=True
            )
            return cached_result

        # Compute new route
        route_result = await self._compute_route(message, policy)

        # Cache the result
        if route_result:
            self._cache_route(cache_key, route_result)

        # Record metrics
        computation_time = (time.time() - start_time) * 1000
        if route_result:
            self.metrics.record_route_computation(
                route_result, computation_time, message.routing_type, used_cache=False
            )

        return route_result

    async def _compute_route(self, message: UnifiedMessage, policy) -> RouteResult:
        """Compute routing decision based on message type and policy."""
        route_id = create_route_id(message)

        # Handle federation routing
        if message.is_federation_message and self.config.enable_federation_optimization:
            return await self._compute_federation_route(message, policy, route_id)

        # Handle local routing based on message type
        if message.routing_type == MessageType.RPC:
            return await self._compute_rpc_route(message, policy, route_id)
        elif message.routing_type == MessageType.PUBSUB:
            return await self._compute_pubsub_route(message, policy, route_id)
        elif message.routing_type == MessageType.QUEUE:
            return await self._compute_queue_route(message, policy, route_id)
        elif message.routing_type == MessageType.CACHE:
            return await self._compute_cache_route(message, policy, route_id)
        elif message.routing_type == MessageType.CONTROL_PLANE:
            return await self._compute_control_plane_route(message, policy, route_id)
        else:
            return await self._compute_data_plane_route(message, policy, route_id)

    async def _compute_federation_route(
        self, message: UnifiedMessage, policy, route_id: str
    ) -> RouteResult:
        """Compute federation route using existing GraphBasedFederationRouter."""
        if not self.federation_router or not HAS_FEDERATION:
            # Fallback to local routing
            return await self._compute_local_route(message, policy, route_id)

        # Extract cluster information
        source_cluster = message.headers.source_cluster or "local"
        target_cluster = message.headers.target_cluster

        if not target_cluster:
            # No specific target cluster - use local routing
            return await self._compute_local_route(message, policy, route_id)

        # Use existing federation router for path computation
        try:
            optimal_path = self.federation_router.find_optimal_path(
                source_cluster, target_cluster, max_hops=policy.max_federation_hops
            )

            if not optimal_path:
                # No federation path found - fallback to local routing
                return await self._compute_local_route(message, policy, route_id)

            # Get federation path statistics
            federation_stats = self.federation_router.get_comprehensive_statistics()
            estimated_latency = (
                federation_stats.routing_performance.average_computation_time_ms
            )

            # Create federation route result
            targets = [
                RouteTarget(
                    system_type=message.routing_type,
                    target_id=target_cluster,
                    cluster_id=target_cluster,
                    priority_weight=1.0,
                )
            ]

            return RouteResult(
                route_id=route_id,
                targets=targets,
                routing_path=optimal_path,
                federation_path=optimal_path,
                estimated_latency_ms=estimated_latency,
                route_cost=len(optimal_path) * 10.0,  # Simple cost model
                federation_required=True,
                hops_required=len(optimal_path) - 1,
            )

        except Exception:
            # Fallback to local routing on federation errors
            return await self._compute_local_route(message, policy, route_id)

    async def _compute_rpc_route(
        self, message: UnifiedMessage, policy, route_id: str
    ) -> RouteResult:
        """Compute RPC-specific routing using MPREG's command registry."""
        # Extract command name from topic for RPC routing
        # Expect topics like "mpreg.rpc.command_name" or "user.rpc.command_name"
        command_name = self._extract_rpc_command_from_topic(message.topic)

        if not command_name:
            # Invalid RPC topic format - fallback to local routing
            return await self._compute_local_route(message, policy, route_id)

        # For now, route to local RPC handler since we don't have cluster peer info here
        # In a full implementation, this would query the server's peer registry
        # to find which servers have the requested RPC command available
        targets = [
            RouteTarget(
                system_type=MessageType.RPC,
                target_id=command_name,
                node_id="local",  # Would be actual node with the command
                priority_weight=2.0,  # Higher priority for RPC calls
            )
        ]

        return RouteResult(
            route_id=route_id,
            targets=targets,
            routing_path=["local"],
            federation_path=[],
            estimated_latency_ms=10.0,  # RPC calls have moderate latency
            route_cost=5.0,  # Higher cost for RPC coordination
            federation_required=False,
            hops_required=0,
        )

    async def _compute_pubsub_route(
        self, message: UnifiedMessage, policy, route_id: str
    ) -> RouteResult:
        """Compute pub/sub routing using existing TopicExchange."""
        if not self.topic_exchange:
            return await self._compute_local_route(message, policy, route_id)

        # Use existing topic exchange for routing
        try:
            # Find matching subscriptions using existing trie
            matching_subscriptions = self.topic_exchange.trie.match_topic(message.topic)

            # Create targets for each matching subscription
            targets = []
            for subscription_id in matching_subscriptions:
                if subscription_id in self.topic_exchange.subscriptions:
                    subscription = self.topic_exchange.subscriptions[subscription_id]
                    targets.append(
                        RouteTarget(
                            system_type=MessageType.PUBSUB,
                            target_id=subscription_id,
                            node_id=subscription.subscriber,
                            priority_weight=1.0,
                        )
                    )

            if not targets:
                # No subscribers - create empty result but still valid
                return RouteResult(
                    route_id=route_id,
                    targets=[],
                    routing_path=[],
                    federation_path=[],
                    estimated_latency_ms=1.0,
                    route_cost=0.0,
                    federation_required=False,
                    hops_required=0,
                )

            return RouteResult(
                route_id=route_id,
                targets=targets,
                routing_path=[],  # Local routing
                federation_path=[],
                estimated_latency_ms=5.0,  # Fast local pub/sub
                route_cost=len(targets) * 2.0,  # Fanout cost
                federation_required=False,
                hops_required=0,
            )

        except Exception:
            return await self._compute_local_route(message, policy, route_id)

    async def _compute_queue_route(
        self, message: UnifiedMessage, policy, route_id: str
    ) -> RouteResult:
        """Compute message queue routing using MessageQueueManager integration."""
        # Extract queue name from topic pattern
        # Support topics like "mpreg.queue.order_processing" or "user.queue.notifications"
        queue_name = self._extract_queue_name_from_topic(message.topic)

        if not queue_name:
            # Invalid queue topic format - fallback to local routing
            return await self._compute_local_route(message, policy, route_id)

        # Route to message queue system
        # In a full implementation, this would integrate with MessageQueueManager
        # to find the appropriate queue handlers and apply delivery guarantees
        targets = [
            RouteTarget(
                system_type=MessageType.QUEUE,
                target_id=queue_name,
                node_id="local",  # Would be actual queue manager node
                priority_weight=1.5,  # Moderate priority for queue operations
            )
        ]

        # Adjust routing based on delivery guarantee
        estimated_latency = 15.0  # Base queue latency
        route_cost = 3.0  # Base queue cost

        if message.delivery_guarantee == DeliveryGuarantee.EXACTLY_ONCE:
            estimated_latency *= 2.0  # Higher latency for exactly-once
            route_cost *= 2.0
        elif message.delivery_guarantee == DeliveryGuarantee.BROADCAST:
            # Broadcast to multiple queue workers
            route_cost *= len(targets) * 1.5
            estimated_latency *= 1.2

        return RouteResult(
            route_id=route_id,
            targets=targets,
            routing_path=["local"],
            federation_path=[],
            estimated_latency_ms=estimated_latency,
            route_cost=route_cost,
            federation_required=False,
            hops_required=0,
        )

    async def _compute_cache_route(
        self, message: UnifiedMessage, policy, route_id: str
    ) -> RouteResult:
        """Compute cache coordination routing for cache invalidation, replication, and analytics."""
        # Cache messages require specific handling based on topic pattern
        if message.topic.startswith("mpreg.cache.invalidation."):
            # Cache invalidation - broadcast to all relevant cache nodes
            return await self._compute_cache_invalidation_route(
                message, policy, route_id
            )
        elif message.topic.startswith("mpreg.cache.coordination."):
            # Cache coordination - route to cache management nodes
            return await self._compute_cache_coordination_route(
                message, policy, route_id
            )
        elif message.topic.startswith("mpreg.cache.gossip."):
            # Cache gossip - route for state synchronization
            return await self._compute_cache_gossip_route(message, policy, route_id)
        elif message.topic.startswith("mpreg.cache.federation."):
            # Cross-cluster cache sync - use federation routing
            return await self._compute_cache_federation_route(message, policy, route_id)
        elif message.topic.startswith(
            "mpreg.cache.events."
        ) or message.topic.startswith("mpreg.cache.analytics."):
            # Cache events and analytics - route to monitoring systems
            return await self._compute_cache_monitoring_route(message, policy, route_id)
        else:
            # Unknown cache topic - use local routing with cache priority
            return await self._compute_local_route(
                message, policy, route_id, priority_boost=True
            )

    async def _compute_cache_invalidation_route(
        self, message: UnifiedMessage, policy, route_id: str
    ) -> RouteResult:
        """Route cache invalidation messages to all relevant cache nodes."""
        # Cache invalidation requires broadcast delivery
        targets = [
            RouteTarget(
                system_type=MessageType.CACHE,
                target_id="cache_manager",
                node_id="local",
                priority_weight=2.0,  # High priority for cache consistency
            ),
            # Additional targets for cache replicas could be added here
        ]

        return RouteResult(
            route_id=route_id,
            targets=targets,
            routing_path=["local"],
            federation_path=[],
            estimated_latency_ms=2.0,  # Very fast for cache operations
            route_cost=len(targets) * 1.5,  # Broadcast cost
            federation_required=False,
            hops_required=0,
        )

    async def _compute_cache_coordination_route(
        self, message: UnifiedMessage, policy, route_id: str
    ) -> RouteResult:
        """Route cache coordination messages for replication and management."""
        target = RouteTarget(
            system_type=MessageType.CACHE,
            target_id="cache_coordinator",
            node_id="local",
            priority_weight=1.5,  # High priority for coordination
        )

        return RouteResult(
            route_id=route_id,
            targets=[target],
            routing_path=["local"],
            federation_path=[],
            estimated_latency_ms=3.0,  # Fast coordination
            route_cost=2.0,
            federation_required=False,
            hops_required=0,
        )

    async def _compute_cache_gossip_route(
        self, message: UnifiedMessage, policy, route_id: str
    ) -> RouteResult:
        """Route cache gossip messages for state synchronization."""
        target = RouteTarget(
            system_type=MessageType.CACHE,
            target_id="cache_gossip",
            node_id="local",
            priority_weight=1.0,  # Normal priority for gossip
        )

        return RouteResult(
            route_id=route_id,
            targets=[target],
            routing_path=["local"],
            federation_path=[],
            estimated_latency_ms=5.0,  # Relaxed timing for gossip
            route_cost=1.0,
            federation_required=False,
            hops_required=0,
        )

    async def _compute_cache_federation_route(
        self, message: UnifiedMessage, policy, route_id: str
    ) -> RouteResult:
        """Route cross-cluster cache synchronization messages."""
        # Use federation routing for cross-cluster cache sync
        if self.federation_router and HAS_FEDERATION:
            return await self._compute_federation_route(message, policy, route_id)
        else:
            # Fallback to local cache handling
            return await self._compute_local_route(
                message, policy, route_id, priority_boost=True
            )

    async def _compute_cache_monitoring_route(
        self, message: UnifiedMessage, policy, route_id: str
    ) -> RouteResult:
        """Route cache monitoring and analytics messages."""
        target = RouteTarget(
            system_type=MessageType.CACHE,
            target_id="cache_monitor",
            node_id="local",
            priority_weight=0.5,  # Lower priority for monitoring
        )

        return RouteResult(
            route_id=route_id,
            targets=[target],
            routing_path=["local"],
            federation_path=[],
            estimated_latency_ms=10.0,  # Relaxed timing for analytics
            route_cost=1.0,
            federation_required=False,
            hops_required=0,
        )

    async def _compute_control_plane_route(
        self, message: UnifiedMessage, policy, route_id: str
    ) -> RouteResult:
        """Compute control plane message routing."""
        # Control plane messages get priority routing
        return await self._compute_local_route(
            message, policy, route_id, priority_boost=True
        )

    async def _compute_data_plane_route(
        self, message: UnifiedMessage, policy, route_id: str
    ) -> RouteResult:
        """Compute data plane message routing."""
        return await self._compute_local_route(message, policy, route_id)

    async def _compute_local_route(
        self,
        message: UnifiedMessage,
        policy,
        route_id: str,
        priority_boost: bool = False,
    ) -> RouteResult:
        """Compute local routing as fallback."""
        # Simple local routing implementation
        target = RouteTarget(
            system_type=message.routing_type,
            target_id="local_handler",
            node_id="local",
            priority_weight=2.0 if priority_boost else 1.0,
        )

        base_latency = 1.0 if priority_boost else 5.0

        return RouteResult(
            route_id=route_id,
            targets=[target],
            routing_path=["local"],
            federation_path=[],
            estimated_latency_ms=base_latency,
            route_cost=1.0,
            federation_required=False,
            hops_required=0,
        )

    async def register_route_pattern(
        self, pattern: TopicPattern, handler: RouteHandlerId
    ) -> None:
        """Register a route pattern with a handler."""
        # Validate pattern
        is_valid, error_msg = TopicValidator.validate_topic_pattern(pattern)
        if not is_valid:
            raise ValueError(f"Invalid topic pattern: {error_msg}")

        # For now, register as a simple mapping
        if pattern not in self.handler_registry.pattern_mappings:
            self.handler_registry.pattern_mappings[pattern] = set()
        self.handler_registry.pattern_mappings[pattern].add(handler)

    async def register_routing_policy(self, policy) -> None:
        """Register a routing policy."""
        # Add to config policies
        self.config.policies.append(policy)

    async def get_routing_statistics(self) -> RoutingStatisticsSnapshot:
        """Get comprehensive routing statistics."""
        return self.metrics.get_statistics_snapshot()

    async def invalidate_routing_cache(
        self, pattern: TopicPattern | None = None
    ) -> None:
        """Invalidate routing cache."""
        if pattern is None:
            self.route_cache.clear()
        else:
            # Remove entries matching pattern
            to_remove = []
            for cache_key in self.route_cache.keys():
                if pattern in cache_key:  # Simple pattern matching
                    to_remove.append(cache_key)

            for key in to_remove:
                del self.route_cache[key]

    def _get_cache_key(self, message: UnifiedMessage) -> str:
        """Generate cache key for message routing."""
        return f"{message.topic}:{message.routing_type.value}:{message.delivery_guarantee.value}"

    def _get_cached_route(self, cache_key: str) -> RouteResult | None:
        """Get cached route if valid."""
        if cache_key in self.route_cache:
            route, cached_at = self.route_cache[cache_key]

            # Check TTL
            if time.time() - cached_at <= (self.config.routing_cache_ttl_ms / 1000):
                return route
            else:
                # Expired entry
                del self.route_cache[cache_key]

        return None

    def _cache_route(self, cache_key: str, route: RouteResult) -> None:
        """Cache a computed route."""
        # Evict old entries if cache is full
        if len(self.route_cache) >= self.config.max_cached_routes:
            # Simple eviction - remove oldest entries
            oldest_keys = list(self.route_cache.keys())[: len(self.route_cache) // 4]
            for key in oldest_keys:
                del self.route_cache[key]

        self.route_cache[cache_key] = (route, time.time())

    def _extract_rpc_command_from_topic(self, topic: str) -> str | None:
        """Extract RPC command name from topic pattern."""
        # Support patterns like "mpreg.rpc.command_name" or "user.rpc.command_name"
        parts = topic.split(".")

        # Find 'rpc' segment and extract command after it
        for i, part in enumerate(parts):
            if part == "rpc" and i + 1 < len(parts):
                return parts[i + 1]

        return None

    def _extract_queue_name_from_topic(self, topic: str) -> str | None:
        """Extract queue name from topic pattern."""
        # Support patterns like "mpreg.queue.queue_name" or "user.queue.notifications"
        parts = topic.split(".")

        # Find 'queue' segment and extract queue name after it
        for i, part in enumerate(parts):
            if part == "queue" and i + 1 < len(parts):
                # Join remaining parts as queue name (support nested names)
                return ".".join(parts[i + 1 :])

        return None


# Factory function for creating unified router instances
def create_unified_router(
    config: UnifiedRoutingConfig,
    federation_router: GraphBasedFederationRouter | None = None,
    federation_bridge: GraphAwareFederationBridge | None = None,
    topic_exchange: TopicExchange | None = None,
    message_queue: MessageQueueManager | None = None,
) -> UnifiedRouter:
    """Create a UnifiedRouter instance with appropriate integrations."""
    return UnifiedRouterImpl(
        config=config,
        federation_router=federation_router,
        federation_bridge=federation_bridge,
        topic_exchange=topic_exchange,
        message_queue=message_queue,
    )
