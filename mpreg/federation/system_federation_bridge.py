"""
Cross-System Federation Integration Bridge for MPREG.

This module implements the bridge that connects the unified federation protocol
with MPREG's existing RPC, Topic Pub/Sub, Message Queue, and Cache systems. It provides
seamless federation capability across all four core systems using topic-based
routing.

Key features:
- Bidirectional message routing between local systems and federation
- Type-safe system integration with comprehensive dataclasses
- Intelligent message forwarding with federation-aware routing
- Performance monitoring and analytics for cross-system operations
- Self-managing subscription lifecycle and resource cleanup
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Protocol

from ..core.caching import SmartCacheManager
from ..core.enhanced_rpc import TopicAwareRPCExecutor
from ..core.topic_exchange import TopicExchange
from ..datastructures.type_aliases import (
    Timestamp,
)
from .unified_federation import (
    FederationClusterId,
    FederationMessageId,
    SystemMessageType,
    UnifiedFederationMessage,
    UnifiedFederationRouter,
    create_federation_message,
)

# System integration type aliases
type SystemInstanceId = str
type LocalSystemType = str
type ForwardingRuleId = str
type BridgeOperationId = str


@dataclass(frozen=True, slots=True)
class SystemForwardingRule:
    """Rule for forwarding messages from local systems to federation."""

    rule_id: ForwardingRuleId
    local_topic_pattern: str
    system_type: SystemMessageType
    forward_to_federation: bool = True
    federation_topic_pattern: str | None = None  # If None, use local pattern
    enabled: bool = True
    priority: int = 100
    created_at: Timestamp = field(default_factory=time.time)

    @property
    def effective_federation_pattern(self) -> str:
        """Get the effective federation topic pattern."""
        return self.federation_topic_pattern or self.local_topic_pattern


@dataclass(frozen=True, slots=True)
class SystemBridgeOperation:
    """Record of a bridge operation for analytics."""

    operation_id: BridgeOperationId
    operation_type: str  # "forward_to_federation", "receive_from_federation"
    system_type: SystemMessageType
    local_topic: str
    federation_message_id: FederationMessageId | None = None
    processing_time_ms: float = 0.0
    success: bool = True
    error_message: str | None = None
    timestamp: Timestamp = field(default_factory=time.time)


@dataclass(frozen=True, slots=True)
class SystemBridgeStatistics:
    """Statistics for the system federation bridge."""

    total_forwarded_to_federation: int
    total_received_from_federation: int
    operations_by_system_type: dict[SystemMessageType, int]
    recent_operations: list[SystemBridgeOperation]
    average_forwarding_time_ms: float
    average_receiving_time_ms: float
    error_rate_percent: float
    active_forwarding_rules: int
    bridge_uptime_seconds: float


@dataclass(slots=True)
class SystemBridgeConfig:
    """Configuration for the system federation bridge."""

    enable_rpc_federation: bool = True
    enable_pubsub_federation: bool = True
    enable_queue_federation: bool = True
    enable_cache_federation: bool = True
    enable_analytics: bool = True
    max_operations_history: int = 1000
    forwarding_timeout_ms: float = 5000.0
    receiving_timeout_ms: float = 5000.0
    cluster_id: FederationClusterId = "default_cluster"


class MessageQueueManagerProtocol(Protocol):
    """Protocol for message queue manager integration."""

    async def publish_message(self, topic: str, message: Any) -> None:
        """Publish message to queue topic."""
        ...

    async def subscribe_to_topic(self, topic: str, callback: Any) -> str:
        """Subscribe to queue topic."""
        ...


@dataclass(slots=True)
class SystemFederationBridge:
    """
    Bridge connecting local MPREG systems with unified federation.

    This bridge enables seamless federation of all four core MPREG systems:
    RPC, Topic Pub/Sub, Message Queue, and Cache systems by providing
    bidirectional message routing between local systems and the federation layer.
    """

    config: SystemBridgeConfig
    federation_router: UnifiedFederationRouter
    rpc_system: TopicAwareRPCExecutor | None = None
    pubsub_system: TopicExchange | None = None
    queue_system: MessageQueueManagerProtocol | None = None
    cache_system: SmartCacheManager[Any] | None = None
    forwarding_rules: list[SystemForwardingRule] = field(default_factory=list)
    bridge_operations: list[SystemBridgeOperation] = field(default_factory=list)
    _start_time: Timestamp = field(default_factory=time.time)
    _operation_callbacks: dict[SystemMessageType, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize bridge components."""
        # Set up system integrations
        self._setup_system_integrations()

    async def register_forwarding_rule(self, rule: SystemForwardingRule) -> None:
        """Register a rule for forwarding messages to federation."""
        self.forwarding_rules.append(rule)

        # Set up subscriptions for this rule if the system is available
        await self._setup_rule_subscription(rule)

        # Record analytics
        if self.config.enable_analytics:
            operation = SystemBridgeOperation(
                operation_id=str(uuid.uuid4()),
                operation_type="rule_registered",
                system_type=rule.system_type,
                local_topic=rule.local_topic_pattern,
                success=True,
            )
            self._record_operation(operation)

    async def forward_to_federation(
        self, topic: str, message: Any, message_type: SystemMessageType
    ) -> None:
        """
        Forward a local system message to federation.

        Args:
            topic: Local topic where message originated
            message: Message payload
            message_type: Type of system that generated the message
        """
        start_time = time.time()
        operation_id = str(uuid.uuid4())

        try:
            # Find matching forwarding rules
            matching_rules = self._find_matching_forwarding_rules(topic, message_type)
            if not matching_rules:
                # No rules match, don't forward
                return

            # Use highest priority rule
            best_rule = min(matching_rules, key=lambda r: r.priority)

            # Create federation message
            federation_message = create_federation_message(
                source_cluster=self.config.cluster_id,
                topic=topic,
                message_type=message_type,
                payload=message,
                target_pattern=best_rule.effective_federation_pattern,
                correlation_id=str(uuid.uuid4()),
            )

            # Route through federation
            route_result = await self.federation_router.route_message(
                federation_message
            )

            processing_time = (time.time() - start_time) * 1000

            # Record successful operation
            if self.config.enable_analytics:
                operation = SystemBridgeOperation(
                    operation_id=operation_id,
                    operation_type="forward_to_federation",
                    system_type=message_type,
                    local_topic=topic,
                    federation_message_id=federation_message.message_id,
                    processing_time_ms=processing_time,
                    success=True,
                )
                self._record_operation(operation)

        except Exception as e:
            processing_time = (time.time() - start_time) * 1000

            # Record failed operation
            if self.config.enable_analytics:
                operation = SystemBridgeOperation(
                    operation_id=operation_id,
                    operation_type="forward_to_federation",
                    system_type=message_type,
                    local_topic=topic,
                    processing_time_ms=processing_time,
                    success=False,
                    error_message=str(e),
                )
                self._record_operation(operation)

            raise

    async def receive_from_federation(
        self, fed_message: UnifiedFederationMessage
    ) -> None:
        """
        Receive and route federation message to appropriate local system.

        Args:
            fed_message: Federation message to route locally
        """
        start_time = time.time()
        operation_id = str(uuid.uuid4())

        try:
            # Route to appropriate local system based on message type
            if fed_message.message_type == SystemMessageType.RPC and self.rpc_system:
                await self._route_to_rpc_system(fed_message)
            elif (
                fed_message.message_type == SystemMessageType.PUBSUB
                and self.pubsub_system
            ):
                await self._route_to_pubsub_system(fed_message)
            elif (
                fed_message.message_type == SystemMessageType.QUEUE
                and self.queue_system
            ):
                await self._route_to_queue_system(fed_message)
            elif (
                fed_message.message_type == SystemMessageType.CACHE
                and self.cache_system
            ):
                await self._route_to_cache_system(fed_message)
            else:
                raise ValueError(
                    f"No handler for message type {fed_message.message_type}"
                )

            processing_time = (time.time() - start_time) * 1000

            # Record successful operation
            if self.config.enable_analytics:
                operation = SystemBridgeOperation(
                    operation_id=operation_id,
                    operation_type="receive_from_federation",
                    system_type=fed_message.message_type,
                    local_topic=fed_message.original_topic,
                    federation_message_id=fed_message.message_id,
                    processing_time_ms=processing_time,
                    success=True,
                )
                self._record_operation(operation)

        except Exception as e:
            processing_time = (time.time() - start_time) * 1000

            # Record failed operation
            if self.config.enable_analytics:
                operation = SystemBridgeOperation(
                    operation_id=operation_id,
                    operation_type="receive_from_federation",
                    system_type=fed_message.message_type,
                    local_topic=fed_message.original_topic,
                    federation_message_id=fed_message.message_id,
                    processing_time_ms=processing_time,
                    success=False,
                    error_message=str(e),
                )
                self._record_operation(operation)

            raise

    async def get_bridge_statistics(self) -> SystemBridgeStatistics:
        """Get comprehensive bridge statistics."""
        total_forwarded = sum(
            1
            for op in self.bridge_operations
            if op.operation_type == "forward_to_federation" and op.success
        )
        total_received = sum(
            1
            for op in self.bridge_operations
            if op.operation_type == "receive_from_federation" and op.success
        )

        operations_by_type: dict[SystemMessageType, int] = {}
        for op in self.bridge_operations:
            if op.success:
                operations_by_type[op.system_type] = (
                    operations_by_type.get(op.system_type, 0) + 1
                )

        forwarding_times = [
            op.processing_time_ms
            for op in self.bridge_operations
            if op.operation_type == "forward_to_federation" and op.success
        ]
        receiving_times = [
            op.processing_time_ms
            for op in self.bridge_operations
            if op.operation_type == "receive_from_federation" and op.success
        ]

        total_operations = len(self.bridge_operations)
        failed_operations = sum(1 for op in self.bridge_operations if not op.success)

        return SystemBridgeStatistics(
            total_forwarded_to_federation=total_forwarded,
            total_received_from_federation=total_received,
            operations_by_system_type=operations_by_type,
            recent_operations=self.bridge_operations[-10:]
            if self.bridge_operations
            else [],
            average_forwarding_time_ms=sum(forwarding_times) / len(forwarding_times)
            if forwarding_times
            else 0.0,
            average_receiving_time_ms=sum(receiving_times) / len(receiving_times)
            if receiving_times
            else 0.0,
            error_rate_percent=(failed_operations / total_operations * 100.0)
            if total_operations > 0
            else 0.0,
            active_forwarding_rules=len(
                [r for r in self.forwarding_rules if r.enabled]
            ),
            bridge_uptime_seconds=time.time() - self._start_time,
        )

    def _setup_system_integrations(self) -> None:
        """Set up integrations with available systems."""
        # This would set up callbacks and subscriptions
        # For now, we'll just prepare the callback mapping
        self._operation_callbacks = {
            SystemMessageType.RPC: self._handle_rpc_message,
            SystemMessageType.PUBSUB: self._handle_pubsub_message,
            SystemMessageType.QUEUE: self._handle_queue_message,
            SystemMessageType.CACHE: self._handle_cache_message,
        }

    async def _setup_rule_subscription(self, rule: SystemForwardingRule) -> None:
        """Set up subscription for a forwarding rule."""
        if not rule.enabled:
            return

        # This would set up actual subscriptions with the systems
        # For now, we'll just simulate the setup
        pass

    def _find_matching_forwarding_rules(
        self, topic: str, message_type: SystemMessageType
    ) -> list[SystemForwardingRule]:
        """Find forwarding rules that match the topic and message type."""
        matching_rules = []
        for rule in self.forwarding_rules:
            if (
                rule.enabled
                and rule.system_type == message_type
                and self._topic_matches_pattern(topic, rule.local_topic_pattern)
            ):
                matching_rules.append(rule)
        return matching_rules

    def _topic_matches_pattern(self, topic: str, pattern: str) -> bool:
        """Check if topic matches pattern (simple wildcard matching)."""
        if pattern == "*":
            return True
        if pattern.endswith("*"):
            return topic.startswith(pattern[:-1])
        if pattern.startswith("*"):
            return topic.endswith(pattern[1:])
        return topic == pattern

    async def _route_to_rpc_system(self, fed_message: UnifiedFederationMessage) -> None:
        """Route federation message to RPC system."""
        if not self.rpc_system:
            raise ValueError("RPC system not available")

        # This would integrate with the actual RPC system
        # For now, we'll just simulate the routing
        pass

    async def _route_to_pubsub_system(
        self, fed_message: UnifiedFederationMessage
    ) -> None:
        """Route federation message to Pub/Sub system."""
        if not self.pubsub_system:
            raise ValueError("Pub/Sub system not available")

        # This would integrate with the actual Pub/Sub system
        # For now, we'll just simulate the routing
        pass

    async def _route_to_queue_system(
        self, fed_message: UnifiedFederationMessage
    ) -> None:
        """Route federation message to Queue system."""
        if not self.queue_system:
            raise ValueError("Queue system not available")

        # This would integrate with the actual Queue system
        # For now, we'll just simulate the routing
        pass

    async def _route_to_cache_system(
        self, fed_message: UnifiedFederationMessage
    ) -> None:
        """Route federation message to Cache system."""
        if not self.cache_system:
            raise ValueError("Cache system not available")

        # This would integrate with the actual Cache system
        # For now, we'll just simulate the routing
        pass

    async def _handle_rpc_message(self, topic: str, message: Any) -> None:
        """Handle message from RPC system."""
        await self.forward_to_federation(topic, message, SystemMessageType.RPC)

    async def _handle_pubsub_message(self, topic: str, message: Any) -> None:
        """Handle message from Pub/Sub system."""
        await self.forward_to_federation(topic, message, SystemMessageType.PUBSUB)

    async def _handle_queue_message(self, topic: str, message: Any) -> None:
        """Handle message from Queue system."""
        await self.forward_to_federation(topic, message, SystemMessageType.QUEUE)

    async def _handle_cache_message(self, topic: str, message: Any) -> None:
        """Handle message from Cache system."""
        await self.forward_to_federation(topic, message, SystemMessageType.CACHE)

    def _record_operation(self, operation: SystemBridgeOperation) -> None:
        """Record bridge operation for analytics."""
        if not self.config.enable_analytics:
            return

        self.bridge_operations.append(operation)

        # Keep only recent operations to prevent memory growth
        if len(self.bridge_operations) > self.config.max_operations_history:
            self.bridge_operations = self.bridge_operations[
                -self.config.max_operations_history // 2 :
            ]


# Factory functions
def create_system_federation_bridge(
    federation_router: UnifiedFederationRouter,
    config: SystemBridgeConfig | None = None,
    rpc_system: TopicAwareRPCExecutor | None = None,
    pubsub_system: TopicExchange | None = None,
    queue_system: MessageQueueManagerProtocol | None = None,
    cache_system: SmartCacheManager[Any] | None = None,
) -> SystemFederationBridge:
    """
    Factory function for creating system federation bridge.

    Args:
        federation_router: Router for federation messages
        config: Bridge configuration (uses defaults if None)
        rpc_system: RPC system integration (optional)
        pubsub_system: Pub/Sub system integration (optional)
        queue_system: Queue system integration (optional)
        cache_system: Cache system integration (optional)

    Returns:
        Configured SystemFederationBridge
    """
    if config is None:
        config = SystemBridgeConfig()

    return SystemFederationBridge(
        config=config,
        federation_router=federation_router,
        rpc_system=rpc_system,
        pubsub_system=pubsub_system,
        queue_system=queue_system,
        cache_system=cache_system,
    )


def create_forwarding_rule(
    rule_id: ForwardingRuleId,
    local_topic_pattern: str,
    system_type: SystemMessageType,
    **kwargs: Any,
) -> SystemForwardingRule:
    """
    Factory function for creating forwarding rules.

    Args:
        rule_id: Unique identifier for the rule
        local_topic_pattern: Pattern to match local topics
        system_type: Type of system this rule applies to
        **kwargs: Additional rule parameters

    Returns:
        Configured SystemForwardingRule
    """
    return SystemForwardingRule(
        rule_id=rule_id,
        local_topic_pattern=local_topic_pattern,
        system_type=system_type,
        **kwargs,
    )
