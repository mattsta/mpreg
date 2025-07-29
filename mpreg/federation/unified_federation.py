"""
Unified Federation Protocol for MPREG.

This module implements a topic-based federation system that unifies RPC, Topic Pub/Sub,
and Message Queue federation under a single protocol. It builds on MPREG's existing
federation infrastructure while adding topic-driven routing and cross-system coordination.

Key features:
- Topic-pattern-based federation routing
- Unified message model for all three core systems
- Integration with existing GraphBasedFederationRouter
- Type-safe federation with comprehensive semantic types
- Intelligent routing rules and caching
- Cross-system federation coordination
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from ..core.unified_routing import RoutingPriority
from ..datastructures.trie import TopicTrie
from ..datastructures.type_aliases import (
    CorrelationId,
    HopCount,
    PathLatencyMs,
    RouteCostScore,
    Timestamp,
)

# Federation-specific type aliases following MPREG conventions
type FederationMessageId = str
type FederationClusterId = str
type FederationTopicPattern = str
type FederationRuleId = str
type FederationHopPath = list[FederationClusterId]
type FederationCacheKey = str


class SystemMessageType(Enum):
    """Message types for unified federation across core systems."""

    RPC = "rpc"
    PUBSUB = "pubsub"
    QUEUE = "queue"
    CACHE = "cache"
    FEDERATION_CONTROL = "federation_control"


class FederationHopStrategy(Enum):
    """Strategies for federation message hopping."""

    SHORTEST_PATH = "shortest_path"
    LOWEST_LATENCY = "lowest_latency"
    LOAD_BALANCED = "load_balanced"
    REDUNDANT_PATHS = "redundant_paths"


@dataclass(frozen=True, slots=True)
class UnifiedFederationMessage:
    """
    Unified message format for federation across all MPREG systems.

    This message format enables any of the three core systems (RPC, Topic Pub/Sub,
    Message Queue) to be federated using topic-pattern-based routing.
    """

    message_id: FederationMessageId
    source_cluster: FederationClusterId
    target_pattern: FederationTopicPattern  # Topic pattern for routing
    message_type: SystemMessageType
    original_topic: str
    payload: Any
    federation_path: FederationHopPath = field(default_factory=list)
    hop_count: HopCount = 0
    max_hops: HopCount = 10
    correlation_id: CorrelationId | None = None
    priority: RoutingPriority = RoutingPriority.NORMAL
    created_at: Timestamp = field(default_factory=time.time)
    expires_at: Timestamp | None = None

    @property
    def is_expired(self) -> bool:
        """Check if message has expired."""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

    @property
    def has_exceeded_max_hops(self) -> bool:
        """Check if message has exceeded maximum hop count."""
        return self.hop_count >= self.max_hops

    @property
    def remaining_hops(self) -> HopCount:
        """Get remaining hop count."""
        return max(0, self.max_hops - self.hop_count)

    def with_hop_added(
        self, cluster_id: FederationClusterId
    ) -> UnifiedFederationMessage:
        """Create new message with an additional hop."""
        new_path = self.federation_path + [cluster_id]
        return UnifiedFederationMessage(
            message_id=self.message_id,
            source_cluster=self.source_cluster,
            target_pattern=self.target_pattern,
            message_type=self.message_type,
            original_topic=self.original_topic,
            payload=self.payload,
            federation_path=new_path,
            hop_count=self.hop_count + 1,
            max_hops=self.max_hops,
            correlation_id=self.correlation_id,
            priority=self.priority,
            created_at=self.created_at,
            expires_at=self.expires_at,
        )


@dataclass(frozen=True, slots=True)
class FederationRoutingRule:
    """
    Topic-pattern-based routing rule for federation.

    Rules define how messages matching certain patterns should be routed
    across clusters, with fine-grained control over message types and priorities.
    """

    rule_id: FederationRuleId
    source_pattern: FederationTopicPattern
    target_clusters: set[FederationClusterId] | str  # Specific clusters or "all"
    message_types: set[SystemMessageType]
    priority: int = 100  # Lower numbers = higher priority
    enabled: bool = True
    hop_strategy: FederationHopStrategy = FederationHopStrategy.SHORTEST_PATH
    max_hops_override: HopCount | None = None
    cache_ttl_ms: float = 60000.0  # How long to cache routing decisions
    created_at: Timestamp = field(default_factory=time.time)

    @property
    def applies_to_all_clusters(self) -> bool:
        """Check if rule applies to all clusters."""
        return isinstance(self.target_clusters, str) and self.target_clusters == "all"

    def matches_message(self, message: UnifiedFederationMessage) -> bool:
        """Check if this rule matches a federation message."""
        return (
            self.enabled
            and message.message_type in self.message_types
            and self._matches_topic_pattern(message.original_topic, self.source_pattern)
        )

    def _matches_topic_pattern(self, topic: str, pattern: str) -> bool:
        """Check if topic matches pattern (simple wildcard matching)."""
        import re

        # Convert pattern to regex-style matching
        if pattern == "*":
            return True

        # Convert simple wildcard pattern to regex
        # Replace * with .* for regex matching
        regex_pattern = pattern.replace("*", ".*")
        # Anchor the pattern to match the entire string
        regex_pattern = f"^{regex_pattern}$"

        try:
            return bool(re.match(regex_pattern, topic))
        except re.error:
            # If regex is invalid, fall back to exact match
            return topic == pattern


@dataclass(slots=True)
class UnifiedFederationConfig:
    """Configuration for the unified federation system."""

    enable_topic_federation: bool = True
    max_federation_hops: HopCount = 5
    federation_timeout_ms: float = 10000.0
    routing_cache_ttl_ms: float = 60000.0
    enable_route_caching: bool = True
    enable_federation_analytics: bool = True
    default_hop_strategy: FederationHopStrategy = FederationHopStrategy.SHORTEST_PATH
    federation_queue_max_size: int = 10000
    enable_circuit_breaker: bool = True
    circuit_breaker_threshold: int = 5  # Failures before opening
    cluster_id: FederationClusterId = "default_cluster"


@dataclass(frozen=True, slots=True)
class FederationRoutingMetadata:
    """Metadata for federation routing decisions."""

    rule_priority: int = 0
    estimated_hops: HopCount = 1
    selected_strategy: str = ""


@dataclass(frozen=True, slots=True)
class FederationRouteResult:
    """Result of federation routing decision."""

    target_clusters: list[FederationClusterId]
    route_cost: RouteCostScore
    estimated_latency_ms: PathLatencyMs
    hop_strategy_used: FederationHopStrategy
    cache_hit: bool = False
    rule_id: FederationRuleId | None = None
    routing_metadata: FederationRoutingMetadata = field(
        default_factory=FederationRoutingMetadata
    )


@dataclass(frozen=True, slots=True)
class FederationAnalyticsEvent:
    """Analytics event for federation operations."""

    event_type: str  # "message_routed", "rule_matched", "routing_failure", etc.
    message_id: FederationMessageId
    source_cluster: FederationClusterId
    target_clusters: list[FederationClusterId]
    message_type: SystemMessageType
    processing_time_ms: float
    hop_count: HopCount
    rule_id: FederationRuleId | None = None
    error_message: str | None = None
    timestamp: Timestamp = field(default_factory=time.time)


@dataclass(frozen=True, slots=True)
class FederationRouteDistribution:
    """Distribution of routing decisions by cluster count."""

    zero_clusters: int = 0
    one_cluster: int = 0
    two_clusters: int = 0
    three_plus_clusters: int = 0

    @property
    def total_routes(self) -> int:
        """Total number of routing decisions."""
        return (
            self.zero_clusters
            + self.one_cluster
            + self.two_clusters
            + self.three_plus_clusters
        )


@dataclass(frozen=True, slots=True)
class FederationStatistics:
    """Comprehensive federation statistics dataclass."""

    total_rules: int
    cache_entries: int
    cache_hit_rate_percent: float
    total_events: int
    recent_events: list[FederationAnalyticsEvent]
    route_distribution: FederationRouteDistribution
    average_processing_time_ms: float
    cluster_id: FederationClusterId


class FederationRouterProtocol(Protocol):
    """Protocol for federation routing implementations."""

    async def route_message(
        self, message: UnifiedFederationMessage
    ) -> FederationRouteResult:
        """Route a federation message to target clusters."""
        ...

    async def register_routing_rule(self, rule: FederationRoutingRule) -> None:
        """Register a new routing rule."""
        ...

    async def get_cluster_health(
        self, cluster_id: FederationClusterId
    ) -> FederationStatistics:
        """Get health status of a cluster."""
        ...


class FederationBridgeProtocol(Protocol):
    """Protocol for federation bridge implementations."""

    async def forward_to_federation(
        self, topic: str, message: Any, message_type: SystemMessageType
    ) -> None:
        """Forward local message to federation."""
        ...

    async def receive_from_federation(
        self, fed_message: UnifiedFederationMessage
    ) -> None:
        """Receive and route federation message to appropriate local system."""
        ...

    async def get_federation_statistics(self) -> FederationStatistics:
        """Get federation performance statistics."""
        ...

    def get_known_clusters(self) -> set[FederationClusterId]:
        """Get set of known clusters in the federation."""
        ...


# Factory functions for creating federation components
def create_federation_message(
    source_cluster: FederationClusterId,
    topic: str,
    message_type: SystemMessageType,
    payload: Any,
    target_pattern: FederationTopicPattern | None = None,
    **kwargs: Any,
) -> UnifiedFederationMessage:
    """
    Factory function for creating federation messages.

    Args:
        source_cluster: Source cluster ID
        topic: Original topic
        message_type: Type of message (RPC, PUBSUB, QUEUE, etc.)
        payload: Message payload
        target_pattern: Target topic pattern (defaults to topic)
        **kwargs: Additional message parameters

    Returns:
        Configured UnifiedFederationMessage
    """
    import uuid

    return UnifiedFederationMessage(
        message_id=str(uuid.uuid4()),
        source_cluster=source_cluster,
        target_pattern=target_pattern or topic,
        message_type=message_type,
        original_topic=topic,
        payload=payload,
        **kwargs,
    )


def create_federation_rule(
    rule_id: FederationRuleId,
    source_pattern: FederationTopicPattern,
    target_clusters: set[FederationClusterId] | str,
    message_types: set[SystemMessageType] | None = None,
    **kwargs: Any,
) -> FederationRoutingRule:
    """
    Factory function for creating federation routing rules.

    Args:
        rule_id: Unique identifier for the rule
        source_pattern: Topic pattern to match
        target_clusters: Target clusters or "all"
        message_types: Message types to apply rule to (defaults to all)
        **kwargs: Additional rule parameters

    Returns:
        Configured FederationRoutingRule
    """
    if message_types is None:
        message_types = {
            SystemMessageType.RPC,
            SystemMessageType.PUBSUB,
            SystemMessageType.QUEUE,
        }

    return FederationRoutingRule(
        rule_id=rule_id,
        source_pattern=source_pattern,
        target_clusters=target_clusters,
        message_types=message_types,
        **kwargs,
    )


@dataclass(slots=True)
class UnifiedFederationRouter:
    """
    Topic-driven federation router that routes messages across clusters.

    This router uses topic patterns and intelligent caching to efficiently
    route federation messages. It integrates with MPREG's existing graph-based
    federation infrastructure while adding topic-aware routing capabilities.
    """

    config: UnifiedFederationConfig
    graph_router: Any | None = None  # GraphBasedFederationRouter from existing system
    federation_rules: list[FederationRoutingRule] = field(default_factory=list)
    federation_trie: TopicTrie = field(default_factory=TopicTrie)
    route_cache: dict[FederationCacheKey, FederationRouteResult] = field(
        default_factory=dict
    )
    cache_timestamps: dict[FederationCacheKey, Timestamp] = field(default_factory=dict)
    analytics_events: list[FederationAnalyticsEvent] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Initialize router components."""
        # Initialize federation trie with existing rules
        for rule in self.federation_rules:
            self._add_rule_to_trie(rule)

    async def register_routing_rule(self, rule: FederationRoutingRule) -> None:
        """Register a new federation routing rule."""
        self.federation_rules.append(rule)
        self._add_rule_to_trie(rule)

        # Clear related cache entries
        self._invalidate_cache_for_pattern(rule.source_pattern)

        # Record analytics
        if self.config.enable_federation_analytics:
            event = FederationAnalyticsEvent(
                event_type="rule_registered",
                message_id="",  # No specific message
                source_cluster=self.config.cluster_id,
                target_clusters=[],
                message_type=SystemMessageType.FEDERATION_CONTROL,
                processing_time_ms=0.0,
                hop_count=0,
                rule_id=rule.rule_id,
            )
            self.analytics_events.append(event)

    async def route_message(
        self, message: UnifiedFederationMessage
    ) -> FederationRouteResult:
        """
        Route a federation message to target clusters.

        Uses topic patterns, caching, and intelligent routing to determine
        the optimal target clusters for the message.
        """
        start_time = time.time()

        # Check if message is valid for routing
        if message.is_expired or message.has_exceeded_max_hops:
            raise ValueError(
                f"Message {message.message_id} cannot be routed: expired or max hops exceeded"
            )

        # Check cache first
        cache_key = self._create_cache_key(message)
        cached_result = self._get_cached_route(cache_key)
        if cached_result is not None:
            processing_time = (time.time() - start_time) * 1000
            self._record_analytics_event(
                "route_cache_hit",
                message,
                cached_result.target_clusters,
                processing_time,
            )
            return cached_result

        # Find matching rules
        matching_rules = self._find_matching_rules(message)
        if not matching_rules:
            # No rules match, use default behavior
            result = FederationRouteResult(
                target_clusters=[],
                route_cost=1.0,
                estimated_latency_ms=0.0,
                hop_strategy_used=self.config.default_hop_strategy,
                cache_hit=False,
            )
        else:
            # Use highest priority rule (lowest priority number)
            best_rule = min(matching_rules, key=lambda r: r.priority)
            result = await self._execute_routing_rule(message, best_rule)

        # Cache the result
        if self.config.enable_route_caching:
            self._cache_route_result(cache_key, result)

        processing_time = (time.time() - start_time) * 1000
        self._record_analytics_event(
            "message_routed",
            message,
            result.target_clusters,
            processing_time,
            best_rule.rule_id if matching_rules else None,
        )

        return result

    async def handle_incoming_federation_message(
        self, message: UnifiedFederationMessage
    ) -> None:
        """
        Process incoming federation message and route to local systems.

        This method handles messages received from other clusters and routes
        them to the appropriate local system (RPC, PubSub, Queue, etc.).
        """
        start_time = time.time()

        try:
            # Update message with current cluster in path
            updated_message = message.with_hop_added(self.config.cluster_id)

            # Route to local system based on message type
            await self._route_to_local_system(updated_message)

            processing_time = (time.time() - start_time) * 1000
            self._record_analytics_event(
                "incoming_message_processed",
                updated_message,
                [self.config.cluster_id],
                processing_time,
            )

        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            self._record_analytics_event(
                "incoming_message_error", message, [], processing_time, error=str(e)
            )
            raise

    async def get_federation_statistics(self) -> FederationStatistics:
        """Get comprehensive federation statistics."""
        return FederationStatistics(
            total_rules=len(self.federation_rules),
            cache_entries=len(self.route_cache),
            cache_hit_rate_percent=self._calculate_cache_hit_rate(),
            total_events=len(self.analytics_events),
            recent_events=self.analytics_events[-10:] if self.analytics_events else [],
            route_distribution=self._calculate_route_distribution(),
            average_processing_time_ms=self._calculate_average_processing_time(),
            cluster_id=self.config.cluster_id,
        )

    def _add_rule_to_trie(self, rule: FederationRoutingRule) -> None:
        """Add routing rule to topic trie for efficient pattern matching."""
        # Store rule ID in trie for the pattern
        self.federation_trie.add_pattern(rule.source_pattern, rule.rule_id)

    def _create_cache_key(
        self, message: UnifiedFederationMessage
    ) -> FederationCacheKey:
        """Create cache key for routing result."""
        return f"{message.message_type.value}:{message.original_topic}:{message.priority.value}"

    def _get_cached_route(
        self, cache_key: FederationCacheKey
    ) -> FederationRouteResult | None:
        """Get cached routing result if still valid."""
        if not self.config.enable_route_caching or cache_key not in self.route_cache:
            return None

        cache_time = self.cache_timestamps.get(cache_key, 0.0)
        if time.time() - cache_time > (self.config.routing_cache_ttl_ms / 1000.0):
            # Cache expired
            del self.route_cache[cache_key]
            del self.cache_timestamps[cache_key]
            return None

        result = self.route_cache[cache_key]
        # Return with cache_hit flag set
        return FederationRouteResult(
            target_clusters=result.target_clusters,
            route_cost=result.route_cost,
            estimated_latency_ms=result.estimated_latency_ms,
            hop_strategy_used=result.hop_strategy_used,
            cache_hit=True,
            rule_id=result.rule_id,
            routing_metadata=result.routing_metadata,
        )

    def _cache_route_result(
        self, cache_key: FederationCacheKey, result: FederationRouteResult
    ) -> None:
        """Cache routing result."""
        self.route_cache[cache_key] = result
        self.cache_timestamps[cache_key] = time.time()

    def _find_matching_rules(
        self, message: UnifiedFederationMessage
    ) -> list[FederationRoutingRule]:
        """Find all routing rules that match the message."""
        matching_rules = []
        for rule in self.federation_rules:
            if rule.matches_message(message):
                matching_rules.append(rule)
        return matching_rules

    async def _execute_routing_rule(
        self, message: UnifiedFederationMessage, rule: FederationRoutingRule
    ) -> FederationRouteResult:
        """Execute a specific routing rule to get target clusters."""
        if rule.applies_to_all_clusters:
            # Route to all available clusters (would need cluster discovery)
            target_clusters = await self._get_all_available_clusters()
        else:
            target_clusters = (
                list(rule.target_clusters)
                if isinstance(rule.target_clusters, set)
                else [rule.target_clusters]
            )

        # Filter out source cluster to avoid loops
        target_clusters = [c for c in target_clusters if c != message.source_cluster]

        # Estimate routing cost and latency based on hop strategy
        route_cost, estimated_latency = await self._estimate_route_metrics(
            target_clusters, rule.hop_strategy
        )

        return FederationRouteResult(
            target_clusters=target_clusters,
            route_cost=route_cost,
            estimated_latency_ms=estimated_latency,
            hop_strategy_used=rule.hop_strategy,
            cache_hit=False,
            rule_id=rule.rule_id,
            routing_metadata=FederationRoutingMetadata(
                rule_priority=rule.priority,
                estimated_hops=len(target_clusters),
                selected_strategy=rule.hop_strategy.value,
            ),
        )

    async def _route_to_local_system(self, message: UnifiedFederationMessage) -> None:
        """Route federation message to appropriate local system."""
        # This would integrate with the actual system bridges
        # For now, we'll just log the routing decision
        pass

    async def _get_all_available_clusters(self) -> list[FederationClusterId]:
        """Get list of all available clusters for routing."""
        # This would integrate with cluster discovery
        # For now, return empty list
        return []

    async def _estimate_route_metrics(
        self,
        target_clusters: list[FederationClusterId],
        hop_strategy: FederationHopStrategy,
    ) -> tuple[RouteCostScore, PathLatencyMs]:
        """Estimate routing cost and latency for target clusters."""
        # Basic estimation - would integrate with actual network metrics
        cluster_count = len(target_clusters)
        base_cost = 1.0
        base_latency = 10.0

        if hop_strategy == FederationHopStrategy.SHORTEST_PATH:
            cost = base_cost * cluster_count
            latency = base_latency * cluster_count
        elif hop_strategy == FederationHopStrategy.LOWEST_LATENCY:
            cost = (
                base_cost * cluster_count * 1.2
            )  # Higher cost for latency optimization
            latency = base_latency * cluster_count * 0.8  # Lower latency
        elif hop_strategy == FederationHopStrategy.LOAD_BALANCED:
            cost = base_cost * cluster_count * 1.1
            latency = base_latency * cluster_count * 0.9
        else:  # REDUNDANT_PATHS
            cost = base_cost * cluster_count * 1.5  # Higher cost for redundancy
            latency = base_latency * cluster_count

        return cost, latency

    def _invalidate_cache_for_pattern(self, pattern: str) -> None:
        """Invalidate cache entries that might be affected by a pattern."""
        keys_to_remove = []
        for key in self.route_cache.keys():
            # Simple pattern matching - could be more sophisticated
            if pattern in key or pattern == "*":
                keys_to_remove.append(key)

        for key in keys_to_remove:
            del self.route_cache[key]
            if key in self.cache_timestamps:
                del self.cache_timestamps[key]

    def _record_analytics_event(
        self,
        event_type: str,
        message: UnifiedFederationMessage,
        target_clusters: list[FederationClusterId],
        processing_time: float,
        rule_id: FederationRuleId | None = None,
        error: str | None = None,
    ) -> None:
        """Record analytics event for federation operation."""
        if not self.config.enable_federation_analytics:
            return

        event = FederationAnalyticsEvent(
            event_type=event_type,
            message_id=message.message_id,
            source_cluster=message.source_cluster,
            target_clusters=target_clusters,
            message_type=message.message_type,
            processing_time_ms=processing_time,
            hop_count=message.hop_count,
            rule_id=rule_id,
            error_message=error,
        )
        self.analytics_events.append(event)

        # Keep only recent events to prevent memory growth
        if len(self.analytics_events) > 1000:
            self.analytics_events = self.analytics_events[-500:]

    def _calculate_cache_hit_rate(self) -> float:
        """Calculate cache hit rate from recent events."""
        if not self.analytics_events:
            return 0.0

        recent_events = self.analytics_events[-100:]  # Last 100 events
        cache_hits = sum(
            1 for event in recent_events if event.event_type == "route_cache_hit"
        )
        route_requests = sum(
            1
            for event in recent_events
            if event.event_type in ["route_cache_hit", "message_routed"]
        )

        return (cache_hits / route_requests * 100.0) if route_requests > 0 else 0.0

    def _calculate_route_distribution(self) -> FederationRouteDistribution:
        """Calculate distribution of routing decisions."""
        zero_clusters = 0
        one_cluster = 0
        two_clusters = 0
        three_plus_clusters = 0

        for event in self.analytics_events:
            if event.event_type == "message_routed":
                cluster_count = len(event.target_clusters)
                if cluster_count == 0:
                    zero_clusters += 1
                elif cluster_count == 1:
                    one_cluster += 1
                elif cluster_count == 2:
                    two_clusters += 1
                else:
                    three_plus_clusters += 1

        return FederationRouteDistribution(
            zero_clusters=zero_clusters,
            one_cluster=one_cluster,
            two_clusters=two_clusters,
            three_plus_clusters=three_plus_clusters,
        )

    def _calculate_average_processing_time(self) -> float:
        """Calculate average processing time for routing operations."""
        routing_events = [
            e
            for e in self.analytics_events
            if e.event_type in ["message_routed", "route_cache_hit"]
        ]
        if not routing_events:
            return 0.0

        total_time = sum(event.processing_time_ms for event in routing_events)
        return total_time / len(routing_events)


# Factory function for creating federation router
def create_federation_router(
    config: UnifiedFederationConfig | None = None, graph_router: Any | None = None
) -> UnifiedFederationRouter:
    """
    Factory function for creating unified federation router.

    Args:
        config: Federation configuration (uses defaults if None)
        graph_router: Existing graph-based router to integrate with

    Returns:
        Configured UnifiedFederationRouter
    """
    if config is None:
        config = UnifiedFederationConfig()

    return UnifiedFederationRouter(config=config, graph_router=graph_router)
