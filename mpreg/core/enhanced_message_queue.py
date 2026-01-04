"""
Enhanced Message Queue Manager with Topic-Pattern Routing.

This module extends the existing MessageQueueManager with comprehensive topic-pattern
routing capabilities, integrating seamlessly with the TopicQueueRouter for advanced
message routing scenarios. It maintains full backward compatibility while adding
sophisticated topic-based routing features.

Key Enhancements:
- Topic-pattern message routing using AMQP-style wildcards
- Consumer group management with load balancing
- Federation-aware queue routing across clusters
- ULID-based message tracking for end-to-end observability
- Performance optimization with caching and statistics
- Comprehensive delivery guarantee preservation

Design Principles:
- Extends existing MessageQueueManager via composition
- Maintains backward compatibility with all existing APIs
- Integrates with unified monitoring and federation systems
- Follows MPREG dataclass and semantic type patterns
- Self-managing interfaces with automatic resource cleanup
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import asdict, dataclass
from typing import Any

import ulid

from mpreg.core.message_queue import (
    DeliveryGuarantee,
    DeliveryResult,
    QueuedMessage,
)
from mpreg.core.message_queue_manager import (
    MessageQueueManager,
    QueueManagerConfiguration,
)
from mpreg.core.topic_queue_routing import (
    ConsumerGroupId,
    RoutingStrategy,
    TopicQueueMessage,
    TopicQueueRouter,
    TopicQueueRoutingConfig,
    TopicQueueRoutingStats,
    TopicRoutedQueue,
    TopicRoutingMetadata,
)
from mpreg.datastructures.type_aliases import (
    CorrelationId,
    MessagePayload,
    PatternString,
    QueueName,
    RequestId,
    TopicName,
)

# Type aliases for enhanced queue operations
type TopicQueueSubscriptionId = str
type MessageProcessingResult = str


@dataclass(frozen=True, slots=True)
class TopicQueueSubscription:
    """Subscription to messages matching a topic pattern."""

    subscription_id: TopicQueueSubscriptionId
    topic_pattern: PatternString
    queue_name: QueueName
    consumer_group: ConsumerGroupId | None
    delivery_guarantee: DeliveryGuarantee
    created_at: float
    last_activity: float
    message_count: int = 0
    error_count: int = 0


@dataclass(frozen=True, slots=True)
class TopicQueueSendResult:
    """Result from topic-pattern message sending operations."""

    success: bool
    topic: TopicName
    matched_queues: list[QueueName]
    message_ids: list[str]
    routing_latency_ms: float
    tracking_id: str
    send_results: list[DeliveryResult]
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class EnhancedQueueStats:
    """Enhanced statistics combining queue and topic routing metrics."""

    # Basic queue statistics
    total_messages_sent: int
    total_messages_received: int
    active_queues: int
    active_subscriptions: int

    # Topic routing statistics
    topic_routing_stats: TopicQueueRoutingStats

    # Performance metrics
    average_send_latency_ms: float
    average_routing_latency_ms: float
    messages_per_second: float

    # Subscription analytics
    active_topic_subscriptions: int
    consumer_groups: int
    topic_patterns_registered: int

    # Error tracking
    routing_errors: int
    send_errors: int
    subscription_errors: int


class TopicEnhancedMessageQueueManager:
    """
    Enhanced Message Queue Manager with comprehensive topic-pattern routing.

    Extends the existing MessageQueueManager with sophisticated topic routing
    capabilities while maintaining full backward compatibility. Provides AMQP-style
    topic pattern matching, consumer group management, federation support, and
    comprehensive monitoring integration.

    Features:
    - Topic-pattern message routing with * and # wildcards
    - Consumer group load balancing and sticky routing
    - Federation-aware cross-cluster queue routing
    - ULID-based message tracking for observability
    - Performance optimization with intelligent caching
    - Comprehensive statistics and monitoring integration
    """

    def __init__(
        self,
        config: QueueManagerConfiguration,
        topic_routing_config: TopicQueueRoutingConfig | None = None,
    ):
        # Core message queue functionality
        self.base_manager = MessageQueueManager(config)
        self.config = config

        # Topic routing functionality
        self.topic_routing_config = topic_routing_config or TopicQueueRoutingConfig()
        self.topic_router = TopicQueueRouter(
            self.topic_routing_config, self.base_manager
        )

        # Subscription management
        self.topic_subscriptions: dict[
            TopicQueueSubscriptionId, TopicQueueSubscription
        ] = {}
        self.pattern_to_subscriptions: dict[
            PatternString, set[TopicQueueSubscriptionId]
        ] = {}

        # Performance tracking
        self.send_count = 0
        self.routing_latency_total = 0.0
        self.send_latency_total = 0.0
        self.error_counts = {
            "routing_errors": 0,
            "send_errors": 0,
            "subscription_errors": 0,
        }

        # Background tasks
        self._background_tasks: set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()

    def _normalize_queue_pattern(self, pattern: str) -> str:
        """Normalize queue pattern for topic matching."""
        return pattern

    # Enhanced topic-based sending methods

    async def send_via_topic_pattern(
        self,
        topic: TopicName,
        payload: MessagePayload,
        delivery_guarantee: DeliveryGuarantee,
        routing_strategy: RoutingStrategy | None = None,
        correlation_id: CorrelationId | None = None,
        request_id: RequestId | None = None,
    ) -> TopicQueueSendResult:
        """
        Send message using topic pattern routing.

        Routes the message to all queues that match the topic pattern using
        sophisticated AMQP-style pattern matching. Provides comprehensive
        routing analytics and performance tracking.

        Args:
            topic: Topic pattern for routing (e.g., "order.*.created")
            payload: Message payload to send
            delivery_guarantee: Delivery guarantee for message processing
            routing_strategy: Strategy for selecting among multiple matching queues
            correlation_id: Optional correlation ID for request tracking
            request_id: Optional request ID for operation tracking

        Returns:
            TopicQueueSendResult with routing details and send results
        """
        start_time = time.time()
        tracking_id = f"tqsend-{str(ulid.new())}"

        try:
            # Route message to matching queues
            topic_queue_message = await self.topic_router.send_via_topic(
                topic=topic,
                message=payload,
                delivery_guarantee=delivery_guarantee,
                routing_strategy=routing_strategy,
            )

            # Extract routing results
            matched_queues = topic_queue_message.routed_queues
            routing_latency_ms = topic_queue_message.routing_metadata.routing_latency_ms

            # Send to all matched queues using base manager
            send_results = []
            message_ids = []

            for queue_name in matched_queues:
                try:
                    result = await self.base_manager.send_message(
                        queue_name=queue_name,
                        topic=topic,
                        payload=payload,
                        delivery_guarantee=delivery_guarantee,
                    )
                    send_results.append(result)
                    # DeliveryResult always has message_id property
                    message_ids.append(str(result.message_id))

                except Exception as e:
                    self.error_counts["send_errors"] += 1
                    # Continue with other queues even if one fails
                    continue

            # Calculate total latency
            total_latency_ms = (time.time() - start_time) * 1000.0

            # Update performance statistics
            self.send_count += 1
            self.routing_latency_total += routing_latency_ms
            self.send_latency_total += total_latency_ms

            success = len(send_results) > 0

            return TopicQueueSendResult(
                success=success,
                topic=topic,
                matched_queues=matched_queues,
                message_ids=message_ids,
                routing_latency_ms=routing_latency_ms,
                tracking_id=tracking_id,
                send_results=send_results,
                error_message=None
                if success
                else f"No successful sends to {len(matched_queues)} queues",
            )

        except Exception as e:
            self.error_counts["routing_errors"] += 1
            return TopicQueueSendResult(
                success=False,
                topic=topic,
                matched_queues=[],
                message_ids=[],
                routing_latency_ms=(time.time() - start_time) * 1000.0,
                tracking_id=tracking_id,
                send_results=[],
                error_message=str(e),
            )

    async def subscribe_to_topic_pattern(
        self,
        pattern: PatternString,
        consumer_id: str,
        queue_name: QueueName | None = None,
        consumer_group: ConsumerGroupId | None = None,
        delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
    ) -> TopicQueueSubscription:
        """
        Subscribe to messages matching a topic pattern.

        Creates a subscription that will receive messages matching the specified
        topic pattern. Supports consumer groups for load balancing and provides
        comprehensive subscription lifecycle management.

        Args:
            pattern: Topic pattern with AMQP-style wildcards
            consumer_id: Unique identifier for the consumer
            queue_name: Optional specific queue name (if None, uses pattern-based naming)
            consumer_group: Optional consumer group for load balancing
            delivery_guarantee: Delivery guarantee for subscription messages

        Returns:
            TopicQueueSubscription with subscription details
        """
        try:
            subscription_id = f"tqsub-{str(ulid.new())}"
            actual_queue_name = (
                queue_name
                or f"topic-queue-{pattern.replace('*', 'star').replace('#', 'hash')}"
            )

            if actual_queue_name not in self.base_manager.queues:
                await self.base_manager.create_queue(actual_queue_name)

            # Register the queue pattern with the topic router
            topic_routed_queue = TopicRoutedQueue(
                queue_name=actual_queue_name,
                topic_patterns=[pattern],
                routing_priority=1,
                delivery_guarantee=delivery_guarantee,
                consumer_groups={consumer_group} if consumer_group else set(),
            )

            await self.topic_router.register_queue_pattern(topic_routed_queue)

            # Create subscription record
            subscription = TopicQueueSubscription(
                subscription_id=subscription_id,
                topic_pattern=pattern,
                queue_name=actual_queue_name,
                consumer_group=consumer_group,
                delivery_guarantee=delivery_guarantee,
                created_at=time.time(),
                last_activity=time.time(),
                message_count=0,
                error_count=0,
            )

            # Store subscription
            self.topic_subscriptions[subscription_id] = subscription
            if pattern not in self.pattern_to_subscriptions:
                self.pattern_to_subscriptions[pattern] = set()
            self.pattern_to_subscriptions[pattern].add(subscription_id)

            return subscription

        except Exception as e:
            self.error_counts["subscription_errors"] += 1
            raise ValueError(f"Failed to create topic pattern subscription: {e}")

    async def unsubscribe_from_topic_pattern(
        self, subscription_id: TopicQueueSubscriptionId
    ) -> bool:
        """
        Unsubscribe from a topic pattern subscription.

        Args:
            subscription_id: ID of the subscription to remove

        Returns:
            True if subscription was found and removed, False otherwise
        """
        if subscription_id not in self.topic_subscriptions:
            return False

        subscription = self.topic_subscriptions[subscription_id]

        # Unregister from topic router
        await self.topic_router.unregister_queue_pattern(subscription.queue_name)

        # Remove from tracking
        del self.topic_subscriptions[subscription_id]
        if subscription.topic_pattern in self.pattern_to_subscriptions:
            self.pattern_to_subscriptions[subscription.topic_pattern].discard(
                subscription_id
            )
            if not self.pattern_to_subscriptions[subscription.topic_pattern]:
                del self.pattern_to_subscriptions[subscription.topic_pattern]

        return True

    # Consumer group management

    async def create_consumer_group(
        self,
        group_id: ConsumerGroupId,
        topic_patterns: list[PatternString],
        delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
    ) -> list[TopicQueueSubscription]:
        """
        Create a consumer group with multiple topic pattern subscriptions.

        Args:
            group_id: Unique identifier for the consumer group
            topic_patterns: List of topic patterns to subscribe to
            delivery_guarantee: Delivery guarantee for group messages

        Returns:
            List of subscriptions created for the consumer group
        """
        subscriptions = []

        for pattern in topic_patterns:
            try:
                subscription = await self.subscribe_to_topic_pattern(
                    pattern=pattern,
                    consumer_id=f"group-{group_id}",
                    consumer_group=group_id,
                    delivery_guarantee=delivery_guarantee,
                )
                subscriptions.append(subscription)
            except Exception as e:
                # Continue with other patterns even if one fails
                continue

        return subscriptions

    async def get_consumer_group_subscriptions(
        self, group_id: ConsumerGroupId
    ) -> list[TopicQueueSubscription]:
        """Get all subscriptions for a specific consumer group."""
        return [
            subscription
            for subscription in self.topic_subscriptions.values()
            if subscription.consumer_group == group_id
        ]

    # Message consumption with topic awareness

    async def consume_topic_messages(
        self,
        subscription_id: TopicQueueSubscriptionId,
        max_messages: int = 10,
        timeout_seconds: float = 30.0,
    ) -> AsyncIterator[TopicQueueMessage]:
        """
        Consume messages from a topic pattern subscription.

        Args:
            subscription_id: ID of the subscription to consume from
            max_messages: Maximum number of messages to consume
            timeout_seconds: Timeout for message consumption

        Yields:
            TopicQueueMessage instances with routing metadata
        """
        if subscription_id not in self.topic_subscriptions:
            raise ValueError(f"Subscription not found: {subscription_id}")

        subscription = self.topic_subscriptions[subscription_id]

        # Consume from the underlying queue using subscription-based approach
        try:
            delivery_queue: asyncio.Queue[QueuedMessage] = asyncio.Queue()

            def _on_message(message: QueuedMessage) -> None:
                delivery_queue.put_nowait(message)

            subscription_id_local = self.base_manager.subscribe_to_queue(
                queue_name=subscription.queue_name,
                subscriber_id=f"topic-consumer-{subscription_id}",
                topic_pattern=self._normalize_queue_pattern(subscription.topic_pattern),
                callback=_on_message,
                auto_acknowledge=True,
            )
            if subscription_id_local is None:
                raise RuntimeError("Failed to subscribe to queue for topic consumption")

            try:
                received = 0
                while received < max_messages:
                    message = await asyncio.wait_for(
                        delivery_queue.get(), timeout=timeout_seconds
                    )
                    received += 1
                    routing_metadata = TopicRoutingMetadata(
                        routing_id=f"route-{str(ulid.new())}",
                        matched_patterns=[subscription.topic_pattern],
                        selected_queues=[subscription.queue_name],
                        routing_strategy=RoutingStrategy.FANOUT_ALL,
                        routing_latency_ms=0.0,
                        pattern_match_count=1,
                        timestamp=time.time(),
                    )
                    yield TopicQueueMessage(
                        topic=message.topic,
                        original_queue=subscription.queue_name,
                        routed_queues=[subscription.queue_name],
                        routing_metadata=routing_metadata,
                        message=message,
                    )
            except TimeoutError:
                return
            finally:
                self.base_manager.unsubscribe_from_queue(
                    subscription.queue_name, subscription_id_local
                )

        except Exception as e:
            self.error_counts["subscription_errors"] += 1
            raise

    # Statistics and monitoring

    async def get_enhanced_statistics(self) -> EnhancedQueueStats:
        """
        Get comprehensive statistics combining queue and topic routing metrics.

        Returns:
            EnhancedQueueStats with complete performance analytics
        """
        # Get base queue statistics
        base_stats = self.base_manager.get_global_statistics()

        # Get topic routing statistics
        topic_stats = await self.topic_router.get_routing_statistics()

        # Calculate performance metrics
        avg_routing_latency = (
            self.routing_latency_total / self.send_count if self.send_count > 0 else 0.0
        )
        avg_send_latency = (
            self.send_latency_total / self.send_count if self.send_count > 0 else 0.0
        )

        # Calculate messages per second (simplified)
        messages_per_second = float(
            self.send_count
        )  # Would be time-based in real implementation

        return EnhancedQueueStats(
            total_messages_sent=self.send_count,
            total_messages_received=base_stats.total_messages_received,
            active_queues=base_stats.total_queues,
            active_subscriptions=len(self.topic_subscriptions),
            topic_routing_stats=topic_stats,
            average_send_latency_ms=avg_send_latency,
            average_routing_latency_ms=avg_routing_latency,
            messages_per_second=messages_per_second,
            active_topic_subscriptions=len(self.topic_subscriptions),
            consumer_groups=len(
                set(
                    sub.consumer_group
                    for sub in self.topic_subscriptions.values()
                    if sub.consumer_group
                )
            ),
            topic_patterns_registered=len(self.pattern_to_subscriptions),
            routing_errors=self.error_counts["routing_errors"],
            send_errors=self.error_counts["send_errors"],
            subscription_errors=self.error_counts["subscription_errors"],
        )

    # Backward compatibility methods - delegate to base manager

    async def send_message(
        self,
        queue_name: QueueName,
        topic: TopicName,
        payload: MessagePayload,
        delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
    ) -> DeliveryResult:
        """Send message to specific queue (backward compatibility)."""
        return await self.base_manager.send_message(
            queue_name, topic, payload, delivery_guarantee
        )

    async def receive_message(
        self, queue_name: QueueName, timeout_seconds: float = 30.0
    ) -> QueuedMessage | None:
        """Receive message from specific queue (backward compatibility)."""
        delivery_queue: asyncio.Queue[QueuedMessage] = asyncio.Queue()

        def _on_message(message: QueuedMessage) -> None:
            delivery_queue.put_nowait(message)

        subscription_id = self.base_manager.subscribe_to_queue(
            queue_name=queue_name,
            subscriber_id=f"queue-receiver-{str(ulid.new())}",
            topic_pattern="#",
            callback=_on_message,
            auto_acknowledge=True,
        )
        if subscription_id is None:
            return None

        try:
            return await asyncio.wait_for(delivery_queue.get(), timeout=timeout_seconds)
        except TimeoutError:
            return None
        finally:
            self.base_manager.unsubscribe_from_queue(queue_name, subscription_id)

    async def consume_messages(
        self,
        queue_name: QueueName,
        max_messages: int = 10,
        timeout_seconds: float = 30.0,
    ) -> AsyncIterator[QueuedMessage]:
        """Consume messages from specific queue (backward compatibility)."""
        delivery_queue: asyncio.Queue[QueuedMessage] = asyncio.Queue()

        def _on_message(message: QueuedMessage) -> None:
            delivery_queue.put_nowait(message)

        subscription_id = self.base_manager.subscribe_to_queue(
            queue_name=queue_name,
            subscriber_id=f"queue-consumer-{str(ulid.new())}",
            topic_pattern="#",
            callback=_on_message,
            auto_acknowledge=True,
        )
        if subscription_id is None:
            return

        try:
            received = 0
            while received < max_messages:
                try:
                    message = await asyncio.wait_for(
                        delivery_queue.get(), timeout=timeout_seconds
                    )
                except TimeoutError:
                    return
                received += 1
                yield message
        finally:
            self.base_manager.unsubscribe_from_queue(queue_name, subscription_id)

    async def acknowledge_message(self, queue_name: QueueName, message_id: str) -> bool:
        """Acknowledge message processing (backward compatibility)."""
        return await self.base_manager.acknowledge_message(
            queue_name, message_id, "system"
        )

    async def get_queue_statistics(self, queue_name: QueueName) -> dict[str, Any]:
        """Get statistics for specific queue (backward compatibility)."""
        stats = self.base_manager.get_queue_statistics(queue_name)
        return asdict(stats) if stats else {}

    async def get_statistics(self) -> dict[str, Any]:
        """Get basic statistics (backward compatibility)."""
        stats = self.base_manager.get_global_statistics()
        return asdict(stats)

    # Lifecycle management

    async def start(self) -> None:
        """Start the enhanced message queue manager."""
        # MessageQueueManager doesn't have start method - it's ready on construction
        pass
        # Start any background tasks for topic routing optimization

    async def stop(self) -> None:
        """Stop the enhanced message queue manager."""
        self._shutdown_event.set()

        # Cancel background tasks
        for task in self._background_tasks:
            if not task.done():
                task.cancel()

        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        await self.base_manager.shutdown()

    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()


# Factory functions for common configurations


def create_topic_enhanced_queue_manager(
    base_config: QueueManagerConfiguration | None = None,
    topic_routing_config: TopicQueueRoutingConfig | None = None,
    enable_high_performance: bool = False,
) -> TopicEnhancedMessageQueueManager:
    """Create a topic-enhanced message queue manager with standard configuration."""

    if base_config is None:
        base_config = QueueManagerConfiguration()

    if topic_routing_config is None:
        if enable_high_performance:
            topic_routing_config = TopicQueueRoutingConfig(
                max_queue_fanout=100,
                routing_cache_ttl_ms=60000.0,
                default_routing_strategy=RoutingStrategy.LOAD_BALANCED,
                enable_load_balancing=True,
            )
        else:
            topic_routing_config = TopicQueueRoutingConfig()

    return TopicEnhancedMessageQueueManager(base_config, topic_routing_config)


def create_simple_topic_queue_manager() -> TopicEnhancedMessageQueueManager:
    """Create a simple topic-enhanced queue manager for basic use cases."""
    return TopicEnhancedMessageQueueManager(
        QueueManagerConfiguration(),
        TopicQueueRoutingConfig(enable_pattern_routing=True),
    )
