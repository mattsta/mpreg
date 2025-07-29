"""
SQS-like Message Queuing System for MPREG.

This module provides a comprehensive message queuing system with various delivery
guarantees, similar to AWS SQS but integrated with MPREG's distributed architecture.

Delivery Guarantees Supported:
- FIRE_AND_FORGET: Send once, no tracking
- AT_LEAST_ONCE: Deliver with acknowledgment and retry
- BROADCAST: Deliver to all subscribers
- QUORUM: Require N acknowledgments before considering delivered

All data structures use proper dataclasses following MPREG's clean design principles.
"""

import asyncio
import time
import uuid
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger
from sortedcontainers import SortedSet  # type: ignore

from ..datastructures import MessageId
from .task_manager import ManagedObject

# Type aliases for semantic clarity
type TopicPattern = str
type SubscriberId = str
type MessageFingerprint = str
type Timestamp = float
type SubscriptionId = str
type MessageIdStr = str


class DeliveryGuarantee(Enum):
    """Message delivery guarantee types."""

    FIRE_AND_FORGET = "fire_and_forget"  # Send once, no tracking
    AT_LEAST_ONCE = "at_least_once"  # Retry until acknowledged
    BROADCAST = "broadcast"  # Deliver to all subscribers
    QUORUM = "quorum"  # Require N acknowledgments


class QueueMessageStatus(Enum):
    """Message delivery status for queue operations."""

    PENDING = "pending"  # Waiting to be delivered
    IN_FLIGHT = "in_flight"  # Delivered, awaiting acknowledgment
    ACKNOWLEDGED = "acknowledged"  # Successfully acknowledged
    FAILED = "failed"  # Failed delivery (max retries exceeded)
    EXPIRED = "expired"  # Expired before delivery


class QueueType(Enum):
    """Queue implementation types."""

    FIFO = "fifo"  # First-In-First-Out
    PRIORITY = "priority"  # Priority-based ordering
    DELAY = "delay"  # Delayed delivery


# MessageId imported from centralized datastructures


@dataclass(slots=True)
class QueuedMessage:
    """A message in the queue system with full metadata."""

    id: MessageId  # Uses centralized MessageId
    topic: str
    payload: Any
    delivery_guarantee: DeliveryGuarantee
    priority: int = 0
    delay_seconds: float = 0.0
    visibility_timeout_seconds: float = 30.0
    max_retries: int = 3
    acknowledgment_timeout_seconds: float = 300.0  # 5 minutes default
    required_acknowledgments: int = 1  # For quorum delivery
    created_at: Timestamp = field(default_factory=time.time)
    headers: dict[str, str] = field(default_factory=dict)
    _delivery_attempt: int = 1  # Track delivery attempts
    _fingerprint: MessageFingerprint = ""  # For deduplication

    def is_ready_for_delivery(self) -> bool:
        """Check if message is ready for delivery (delay has passed)."""
        return (time.time() - self.created_at) >= self.delay_seconds

    def is_expired(self, ttl_seconds: float | None = None) -> bool:
        """Check if message has expired."""
        if ttl_seconds is None:
            return False
        return (time.time() - self.created_at) > ttl_seconds


@dataclass(slots=True)
class InFlightMessage:
    """A message that has been delivered and is awaiting acknowledgment."""

    message: QueuedMessage
    delivery_attempt: int
    delivered_at: Timestamp
    delivered_to: set[SubscriberId] = field(default_factory=set)
    acknowledged_by: set[SubscriberId] = field(default_factory=set)
    status: QueueMessageStatus = QueueMessageStatus.IN_FLIGHT

    def is_acknowledgment_expired(self) -> bool:
        """Check if acknowledgment timeout has been exceeded."""
        return (
            time.time() - self.delivered_at
        ) > self.message.acknowledgment_timeout_seconds

    def is_fully_acknowledged(self) -> bool:
        """Check if message has received all required acknowledgments."""
        if self.message.delivery_guarantee == DeliveryGuarantee.QUORUM:
            return len(self.acknowledged_by) >= self.message.required_acknowledgments
        elif self.message.delivery_guarantee == DeliveryGuarantee.BROADCAST:
            return len(self.acknowledged_by) >= len(self.delivered_to)
        else:
            return len(self.acknowledged_by) > 0


@dataclass(frozen=True, slots=True)
class QueueSubscription:
    """Subscription to a message queue."""

    subscriber_id: SubscriberId
    topic_pattern: TopicPattern
    callback: Callable[[QueuedMessage], None] | None = None
    auto_acknowledge: bool = True
    subscription_id: SubscriptionId = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: Timestamp = field(default_factory=time.time)
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class QueueConfiguration:
    """Configuration for a message queue."""

    name: str
    queue_type: QueueType = QueueType.FIFO
    max_size: int = 10000
    default_visibility_timeout_seconds: float = 30.0
    default_acknowledgment_timeout_seconds: float = 300.0
    message_ttl_seconds: float | None = None
    enable_dead_letter_queue: bool = True
    dead_letter_max_receives: int = 3
    enable_deduplication: bool = False
    deduplication_window_seconds: float = 300.0
    max_retries: int = 3


@dataclass(slots=True)
class QueueStatistics:
    """Statistics for queue operations."""

    messages_sent: int = 0
    messages_received: int = 0
    messages_acknowledged: int = 0
    messages_failed: int = 0
    messages_expired: int = 0
    messages_requeued: int = 0
    current_queue_size: int = 0
    current_in_flight_count: int = 0
    average_processing_time_seconds: float = 0.0
    last_reset_time: float = field(default_factory=time.time)

    def success_rate(self) -> float:
        """Calculate message processing success rate."""
        total = self.messages_acknowledged + self.messages_failed
        return self.messages_acknowledged / total if total > 0 else 0.0


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    """Result of a message delivery operation."""

    success: bool
    message_id: MessageId
    delivered_to: set[SubscriberId] = field(default_factory=set)
    failed_deliveries: set[SubscriberId] = field(default_factory=set)
    error_message: str | None = None
    delivery_timestamp: Timestamp = field(default_factory=time.time)


class MessageQueue(ManagedObject):
    """
    SQS-like message queue with configurable delivery guarantees.

    Supports various delivery patterns:
    - Fire and forget: Send once, no tracking
    - At-least-once: Retry until acknowledged with timeout
    - Broadcast: Deliver to all subscribers
    - Quorum: Require N acknowledgments before considering delivered
    """

    def __init__(self, config: QueueConfiguration) -> None:
        super().__init__(name=f"MessageQueue-{config.name}")
        self.config = config

        # Message storage
        self.pending_messages: deque[QueuedMessage] = deque()
        self.in_flight_messages: dict[MessageIdStr, InFlightMessage] = {}
        self.dead_letter_queue: deque[QueuedMessage] = deque()

        # Subscription management
        self.subscriptions: dict[SubscriptionId, QueueSubscription] = {}
        # Track subscribers by topic pattern for efficient lookup
        self.topic_subscribers: defaultdict[TopicPattern, set[SubscriberId]] = (
            defaultdict(set)
        )

        # Message fingerprints with timestamps for sliding window deduplication
        # SortedSet of (timestamp, fingerprint) tuples for O(log n) range operations
        self.fingerprint_history: SortedSet[tuple[Timestamp, MessageFingerprint]] = (
            SortedSet()
        )

        # Statistics
        self.statistics = QueueStatistics()

        # Start background workers
        self._start_workers()

    def _start_workers(self) -> None:
        """Start background worker tasks."""
        try:
            self.create_task(self._delivery_worker(), name="delivery_worker")
            self.create_task(self._timeout_worker(), name="timeout_worker")
            self.create_task(self._cleanup_worker(), name="cleanup_worker")
            logger.info(
                f"Started {len(self._task_manager)} workers for queue {self.config.name}"
            )
        except RuntimeError:
            logger.warning("No event loop running, skipping background workers")

    async def send_message(
        self,
        topic: str,
        payload: Any,
        delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
        **options: Any,
    ) -> DeliveryResult:
        """
        Send a message to the queue.

        Args:
            topic: Target topic for the message
            payload: Message payload (any serializable data)
            delivery_guarantee: How to handle delivery
            **options: Additional message options (priority, delay, etc.)
        """
        try:
            # Create message
            message_id = MessageId(source_node=self.config.name)
            message = QueuedMessage(
                id=message_id,
                topic=topic,
                payload=payload,
                delivery_guarantee=delivery_guarantee,
                priority=options.get("priority", 0),
                delay_seconds=options.get("delay_seconds", 0.0),
                visibility_timeout_seconds=options.get(
                    "visibility_timeout_seconds",
                    self.config.default_visibility_timeout_seconds,
                ),
                max_retries=options.get("max_retries", self.config.max_retries),
                acknowledgment_timeout_seconds=options.get(
                    "acknowledgment_timeout_seconds",
                    self.config.default_acknowledgment_timeout_seconds,
                ),
                required_acknowledgments=options.get("required_acknowledgments", 1),
                headers=options.get("headers", {}),
            )

            # Check for deduplication
            if self.config.enable_deduplication:
                message._fingerprint = self._create_message_fingerprint(message)
                current_time = time.time()
                cutoff_time = current_time - self.config.deduplication_window_seconds

                # Remove old fingerprints outside the window using efficient range operation
                # Find all entries with timestamp < cutoff_time and remove them
                old_entries = self.fingerprint_history.irange(maximum=(cutoff_time, ""))
                for entry in list(
                    old_entries
                ):  # Convert to list to avoid modification during iteration
                    self.fingerprint_history.remove(entry)

                # Check for duplicates within the window using efficient search
                # Look for any entry with our fingerprint in the valid time range
                for timestamp, fingerprint in self.fingerprint_history.irange(
                    minimum=(cutoff_time, "")
                ):
                    if fingerprint == message._fingerprint:
                        logger.debug(f"Duplicate message detected: {message_id}")
                        return DeliveryResult(
                            success=False,
                            message_id=message_id,
                            error_message="Duplicate message within deduplication window",
                        )

                # Add new fingerprint with current timestamp
                self.fingerprint_history.add((current_time, message._fingerprint))

            # Check queue capacity
            if len(self.pending_messages) >= self.config.max_size:
                return DeliveryResult(
                    success=False,
                    message_id=message_id,
                    error_message="Queue at maximum capacity",
                )

            # Handle fire-and-forget immediately
            if delivery_guarantee == DeliveryGuarantee.FIRE_AND_FORGET:
                return await self._deliver_fire_and_forget(message)

            # Add to pending queue
            if self.config.queue_type == QueueType.PRIORITY:
                # Insert based on priority (higher priority first)
                inserted = False
                for i, pending_msg in enumerate(self.pending_messages):
                    if message.priority > pending_msg.priority:
                        self.pending_messages.insert(i, message)
                        inserted = True
                        break
                if not inserted:
                    self.pending_messages.append(message)
            else:
                # FIFO or DELAY queue
                self.pending_messages.append(message)

            self.statistics.messages_sent += 1
            self.statistics.current_queue_size = len(self.pending_messages)

            logger.debug(f"Queued message {message_id} for topic {topic}")

            return DeliveryResult(success=True, message_id=message_id)

        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return DeliveryResult(
                success=False, message_id=MessageId(), error_message=str(e)
            )

    def subscribe(
        self,
        subscriber_id: str,
        topic_pattern: str,
        callback: Callable[[QueuedMessage], None] | None = None,
        auto_acknowledge: bool = True,
        **metadata: str,
    ) -> str:
        """
        Subscribe to messages matching a topic pattern.

        Returns the subscription ID for later unsubscription.
        """
        subscription = QueueSubscription(
            subscriber_id=subscriber_id,
            topic_pattern=topic_pattern,
            callback=callback,
            auto_acknowledge=auto_acknowledge,
            metadata=metadata,
        )

        self.subscriptions[subscription.subscription_id] = subscription
        self.topic_subscribers[topic_pattern].add(subscriber_id)

        logger.info(
            f"Added subscription {subscription.subscription_id} for {subscriber_id} to {topic_pattern}"
        )

        return subscription.subscription_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Remove a subscription."""
        if subscription_id not in self.subscriptions:
            return False

        subscription = self.subscriptions.pop(subscription_id)
        self.topic_subscribers[subscription.topic_pattern].discard(
            subscription.subscriber_id
        )

        # Clean up empty topic entries
        if not self.topic_subscribers[subscription.topic_pattern]:
            del self.topic_subscribers[subscription.topic_pattern]

        logger.info(f"Removed subscription {subscription_id}")
        return True

    async def acknowledge_message(self, message_id: str, subscriber_id: str) -> bool:
        """Acknowledge receipt of a message."""
        if message_id not in self.in_flight_messages:
            logger.warning(f"Cannot acknowledge unknown message: {message_id}")
            return False

        in_flight = self.in_flight_messages[message_id]
        in_flight.acknowledged_by.add(subscriber_id)

        logger.debug(f"Message {message_id} acknowledged by {subscriber_id}")

        # Check if fully acknowledged
        if in_flight.is_fully_acknowledged():
            self._complete_message(message_id, success=True)
            return True

        return True

    def get_statistics(self) -> QueueStatistics:
        """Get current queue statistics."""
        self.statistics.current_queue_size = len(self.pending_messages)
        self.statistics.current_in_flight_count = len(self.in_flight_messages)
        return self.statistics

    async def _deliver_fire_and_forget(self, message: QueuedMessage) -> DeliveryResult:
        """Deliver a fire-and-forget message immediately."""
        try:
            subscribers = self._find_subscribers(message.topic)
            delivered_to = set()
            failed_deliveries = set()

            # For fire-and-forget, success even if no subscribers
            if not subscribers:
                logger.debug(
                    f"Fire-and-forget message {message.id} sent with no subscribers"
                )
                return DeliveryResult(
                    success=True,  # Fire-and-forget always succeeds
                    message_id=message.id,
                    delivered_to=set(),
                    failed_deliveries=set(),
                )

            for subscriber_id in subscribers:
                subscription = self._get_subscription_for_subscriber(
                    subscriber_id, message.topic
                )
                if subscription and subscription.callback:
                    try:
                        subscription.callback(message)
                        delivered_to.add(subscriber_id)
                    except Exception as e:
                        logger.error(
                            f"Fire-and-forget delivery failed to {subscriber_id}: {e}"
                        )
                        failed_deliveries.add(subscriber_id)

            self.statistics.messages_received += len(delivered_to)
            if delivered_to:
                self.statistics.messages_acknowledged += 1

            return DeliveryResult(
                success=True,  # Fire-and-forget always succeeds
                message_id=message.id,
                delivered_to=delivered_to,
                failed_deliveries=failed_deliveries,
            )

        except Exception as e:
            logger.error(f"Fire-and-forget delivery failed: {e}")
            return DeliveryResult(
                success=False, message_id=message.id, error_message=str(e)
            )

    async def _delivery_worker(self) -> None:
        """Background worker to process pending messages."""
        try:
            while True:
                try:
                    await asyncio.sleep(0.1)  # Process every 100ms

                    if not self.pending_messages:
                        continue

                    # Get next message ready for delivery
                    message = None
                    for i, pending_msg in enumerate(self.pending_messages):
                        if pending_msg.is_ready_for_delivery():
                            message = (
                                self.pending_messages.popleft()
                                if i == 0
                                else self.pending_messages[i]
                            )
                            if i > 0:
                                del self.pending_messages[i]
                            break

                    if not message:
                        continue

                    # Deliver the message
                    await self._deliver_message(message)

                except Exception as e:
                    logger.error(f"Delivery worker error: {e}")
                    await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            logger.info("Delivery worker cancelled")
        except Exception as e:
            logger.error(f"Delivery worker fatal error: {e}")
        finally:
            logger.info("Delivery worker stopped")

    async def _timeout_worker(self) -> None:
        """Background worker to handle message timeouts and retries."""
        try:
            while True:
                try:
                    await asyncio.sleep(0.1)  # Check every 100ms for responsiveness

                    expired_messages = []
                    current_time = time.time()

                    for msg_id, in_flight in self.in_flight_messages.items():
                        if in_flight.is_acknowledgment_expired():
                            expired_messages.append(msg_id)

                    for msg_id in expired_messages:
                        await self._handle_timeout(msg_id)

                except Exception as e:
                    logger.error(f"Timeout worker error: {e}")
                    await asyncio.sleep(5.0)

        except asyncio.CancelledError:
            logger.info("Timeout worker cancelled")
        except Exception as e:
            logger.error(f"Timeout worker fatal error: {e}")
        finally:
            logger.info("Timeout worker stopped")

    async def _cleanup_worker(self) -> None:
        """Background worker for cleanup and maintenance."""
        try:
            while True:
                try:
                    await asyncio.sleep(60.0)  # Cleanup every minute

                    # Clean up old deduplication fingerprints using efficient range operation
                    if self.config.enable_deduplication:
                        current_time = time.time()
                        cutoff_time = (
                            current_time - self.config.deduplication_window_seconds
                        )

                        # Remove all entries older than cutoff using O(log n) range operation
                        old_entries = self.fingerprint_history.irange(
                            maximum=(cutoff_time, "")
                        )
                        for entry in list(old_entries):
                            self.fingerprint_history.remove(entry)

                    # Clean up expired messages
                    if self.config.message_ttl_seconds:
                        expired_pending = []
                        for i, message in enumerate(self.pending_messages):
                            if message.is_expired(self.config.message_ttl_seconds):
                                expired_pending.append(i)

                        # Remove expired messages (in reverse order to maintain indices)
                        for i in reversed(expired_pending):
                            expired_msg = self.pending_messages[i]
                            del self.pending_messages[i]
                            self.statistics.messages_expired += 1
                            logger.debug(f"Expired pending message {expired_msg.id}")

                    logger.debug(f"Queue cleanup completed for {self.config.name}")

                except Exception as e:
                    logger.error(f"Cleanup worker error: {e}")
                    await asyncio.sleep(60.0)

        except asyncio.CancelledError:
            logger.info("Cleanup worker cancelled")
        except Exception as e:
            logger.error(f"Cleanup worker fatal error: {e}")
        finally:
            logger.info("Cleanup worker stopped")

    async def _deliver_message(self, message: QueuedMessage) -> None:
        """Deliver a message based on its delivery guarantee."""
        try:
            subscribers = self._find_subscribers(message.topic)

            if not subscribers:
                logger.warning(f"No subscribers found for topic {message.topic}")
                self._move_to_dead_letter_queue(message, "No subscribers")
                return

            # Create in-flight tracking
            in_flight = InFlightMessage(
                message=message,
                delivery_attempt=message._delivery_attempt,
                delivered_at=time.time(),
                delivered_to=set(),
                acknowledged_by=set(),
            )

            # Deliver to subscribers
            delivered_count = 0
            for subscriber_id in subscribers:
                subscription = self._get_subscription_for_subscriber(
                    subscriber_id, message.topic
                )
                if subscription:
                    try:
                        if subscription.callback:
                            subscription.callback(message)

                        in_flight.delivered_to.add(subscriber_id)
                        delivered_count += 1

                        # Auto-acknowledge if configured
                        if subscription.auto_acknowledge:
                            in_flight.acknowledged_by.add(subscriber_id)

                    except Exception as e:
                        logger.error(f"Delivery failed to {subscriber_id}: {e}")

            if delivered_count == 0:
                self._move_to_dead_letter_queue(message, "All deliveries failed")
                return

            # Handle immediate completion for auto-acknowledged messages
            if in_flight.is_fully_acknowledged():
                self._complete_message(str(message.id), success=True)
            else:
                # Track for acknowledgment
                self.in_flight_messages[str(message.id)] = in_flight

            self.statistics.messages_received += delivered_count
            logger.debug(
                f"Delivered message {message.id} to {delivered_count} subscribers"
            )

        except Exception as e:
            logger.error(f"Message delivery failed: {e}")
            self._move_to_dead_letter_queue(message, str(e))

    async def _handle_timeout(self, message_id: str) -> None:
        """Handle acknowledgment timeout for an in-flight message."""
        if message_id not in self.in_flight_messages:
            return

        in_flight = self.in_flight_messages[message_id]
        message = in_flight.message

        # Check if we should retry
        if in_flight.delivery_attempt <= message.max_retries:
            # Retry delivery
            in_flight.delivery_attempt += 1
            message._delivery_attempt = in_flight.delivery_attempt

            logger.info(
                f"Retrying message {message_id} (attempt {in_flight.delivery_attempt})"
            )

            # Re-queue for delivery
            self.pending_messages.appendleft(message)
            del self.in_flight_messages[message_id]
            self.statistics.messages_requeued += 1

        else:
            # Max retries exceeded
            logger.warning(f"Message {message_id} exceeded max retries, moving to DLQ")
            self._move_to_dead_letter_queue(message, "Max retries exceeded")
            self._complete_message(message_id, success=False)

    def _complete_message(self, message_id: str, success: bool) -> None:
        """Complete processing of a message."""
        if message_id in self.in_flight_messages:
            del self.in_flight_messages[message_id]

        if success:
            self.statistics.messages_acknowledged += 1
        else:
            self.statistics.messages_failed += 1

    def _move_to_dead_letter_queue(self, message: QueuedMessage, reason: str) -> None:
        """Move a message to the dead letter queue."""
        if self.config.enable_dead_letter_queue:
            self.dead_letter_queue.append(message)
            logger.warning(f"Moved message {message.id} to DLQ: {reason}")

    def _find_subscribers(self, topic: str) -> set[str]:
        """Find all subscribers for a given topic."""
        subscribers = set()

        for pattern, pattern_subscribers in self.topic_subscribers.items():
            if self._topic_matches_pattern(topic, pattern):
                subscribers.update(pattern_subscribers)

        return subscribers

    def _get_subscription_for_subscriber(
        self, subscriber_id: str, topic: str
    ) -> QueueSubscription | None:
        """Get the subscription for a subscriber that matches the topic."""
        for subscription in self.subscriptions.values():
            if (
                subscription.subscriber_id == subscriber_id
                and self._topic_matches_pattern(topic, subscription.topic_pattern)
            ):
                return subscription
        return None

    def _topic_matches_pattern(self, topic: str, pattern: str) -> bool:
        """Check if a topic matches a subscription pattern."""
        # Enhanced wildcard matching with support for patterns like "*.alert.*"
        if pattern == "*" or pattern == topic:
            return True

        # Convert pattern to regex-like matching
        import fnmatch

        return fnmatch.fnmatch(topic, pattern)

    def _create_message_fingerprint(self, message: QueuedMessage) -> str:
        """Create a fingerprint for deduplication."""
        import hashlib

        content = f"{message.topic}:{message.payload}:{message.headers}"
        return hashlib.sha256(content.encode()).hexdigest()

    async def shutdown(self) -> None:
        """Async shutdown for proper task cleanup."""
        logger.info(f"Shutting down MessageQueue {self.config.name}...")

        # Shutdown task manager (cancels all tasks)
        await super().shutdown()

        # Clear all data structures
        self.pending_messages.clear()
        self.in_flight_messages.clear()
        self.dead_letter_queue.clear()
        self.subscriptions.clear()
        self.topic_subscribers.clear()
        self.fingerprint_history.clear()

        logger.info(f"MessageQueue {self.config.name} shutdown complete")

    def shutdown_sync(self) -> None:
        """Synchronous shutdown for non-async contexts."""
        import asyncio

        try:
            # Try to run async shutdown if event loop exists
            loop = asyncio.get_running_loop()
            # Create a task to run async shutdown
            task = loop.create_task(self.shutdown())
        except RuntimeError:
            # No event loop running, just clear resources
            self.pending_messages.clear()
            self.in_flight_messages.clear()
            self.dead_letter_queue.clear()
            self.subscriptions.clear()
            self.topic_subscribers.clear()
            self.fingerprint_history.clear()
            logger.info(f"MessageQueue {self.config.name} shutdown (sync)")
