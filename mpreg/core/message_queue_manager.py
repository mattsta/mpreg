"""
Message Queue Manager for MPREG's SQS-like System.

This module provides a centralized manager for multiple message queues
and integrates with MPREG's existing topic exchange system.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from .message_queue import (
    DeliveryGuarantee,
    DeliveryResult,
    MessageQueue,
    QueueConfiguration,
    QueueStatistics,
    QueueType,
)
from .persistence.registry import PersistenceRegistry
from .task_manager import ManagedObject
from .topic_exchange import TopicExchange

queue_mgr_log = logger

# Type aliases for semantic clarity
type QueueName = str
type TopicPattern = str
type SubscriberId = str


@dataclass(frozen=True, slots=True)
class TopicRouting:
    """Topic to queue routing configuration."""

    topic_pattern: TopicPattern
    queue_name: QueueName
    delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE
    created_at: float = field(default_factory=time.time)


@dataclass(slots=True)
class QueueManagerStatistics:
    """Statistics for the entire queue manager."""

    total_queues: int = 0
    total_messages_sent: int = 0
    total_messages_received: int = 0
    total_messages_acknowledged: int = 0
    total_messages_failed: int = 0
    active_subscriptions: int = 0

    def overall_success_rate(self) -> float:
        """Calculate overall message processing success rate."""
        total = self.total_messages_acknowledged + self.total_messages_failed
        return self.total_messages_acknowledged / total if total > 0 else 0.0


@dataclass(frozen=True, slots=True)
class QueueManagerConfiguration:
    """Configuration for the message queue manager."""

    default_queue_type: QueueType = QueueType.FIFO
    default_max_queue_size: int = 10000
    default_visibility_timeout_seconds: float = 30.0
    default_acknowledgment_timeout_seconds: float = 300.0
    enable_auto_queue_creation: bool = True
    max_queues: int = 1000
    integration_topic_prefix: str = "mpreg.queue"
    enable_topic_exchange_integration: bool = True


class MessageQueueManager(ManagedObject):
    """
    Centralized manager for SQS-like message queues with MPREG integration.

    Features:
    - Multiple named queues with different configurations
    - Integration with MPREG's topic exchange system
    - Automatic queue creation and management
    - Centralized statistics and monitoring
    - Support for all delivery guarantees
    """

    def __init__(
        self,
        config: QueueManagerConfiguration,
        topic_exchange: TopicExchange | None = None,
        *,
        persistence_registry: PersistenceRegistry | None = None,
    ) -> None:
        super().__init__(name="MessageQueueManager")
        self.config = config
        self.topic_exchange = topic_exchange
        self.persistence_registry = persistence_registry

        # Queue management
        self.queues: dict[QueueName, MessageQueue] = {}
        self.queue_configs: dict[QueueName, QueueConfiguration] = {}

        # Global statistics
        self.statistics = QueueManagerStatistics()

        # Topic exchange integration using proper dataclasses
        self.topic_routings: list[TopicRouting] = []

        if topic_exchange and config.enable_topic_exchange_integration:
            self._setup_topic_exchange_integration()

        queue_mgr_log.info("Message Queue Manager initialized")

    async def create_queue(
        self, name: str, config: QueueConfiguration | None = None
    ) -> bool:
        """Create a new message queue."""
        if name in self.queues:
            queue_mgr_log.warning(f"Queue {name} already exists")
            return False

        if len(self.queues) >= self.config.max_queues:
            queue_mgr_log.error(
                f"Maximum queue limit reached: {self.config.max_queues}"
            )
            return False

        # Use provided config or create default
        if config is None:
            config = QueueConfiguration(
                name=name,
                queue_type=self.config.default_queue_type,
                max_size=self.config.default_max_queue_size,
                default_visibility_timeout_seconds=self.config.default_visibility_timeout_seconds,
                default_acknowledgment_timeout_seconds=self.config.default_acknowledgment_timeout_seconds,
            )

        try:
            if self.persistence_registry is not None:
                await self.persistence_registry.open()
            queue_store = (
                self.persistence_registry.queue_store(name)
                if self.persistence_registry is not None
                else None
            )
            queue = MessageQueue(
                config,
                queue_store=queue_store,
                autostart=queue_store is None,
            )
            if queue_store is not None:
                await queue.restore_from_store()
                await queue_store.save_config(queue.config)
                queue.start_workers()
            self.queues[name] = queue
            self.queue_configs[name] = queue.config
            self.statistics.total_queues += 1

            queue_mgr_log.info(f"Created queue: {name}")
            return True

        except Exception as e:
            queue_mgr_log.error(f"Failed to create queue {name}: {e}")
            return False

    async def restore_persisted_queues(self) -> None:
        """Restore queues persisted on disk."""
        if self.persistence_registry is None:
            return
        await self.persistence_registry.open()
        queue_names = await self.persistence_registry.list_queue_names()
        for queue_name in queue_names:
            if queue_name in self.queues:
                continue
            await self.create_queue(queue_name)

    async def delete_queue(self, name: str) -> bool:
        """Delete a message queue."""
        if name not in self.queues:
            queue_mgr_log.warning(f"Queue {name} does not exist")
            return False

        try:
            queue = self.queues[name]
            await queue.shutdown()

            del self.queues[name]
            del self.queue_configs[name]
            self.statistics.total_queues -= 1

            # Clean up topic routings
            self.topic_routings = [
                routing for routing in self.topic_routings if routing.queue_name != name
            ]

            queue_mgr_log.info(f"Deleted queue: {name}")
            return True

        except Exception as e:
            queue_mgr_log.error(f"Failed to delete queue {name}: {e}")
            return False

    async def send_message(
        self,
        queue_name: str,
        topic: str,
        payload: Any,
        delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
        **options: Any,
    ) -> DeliveryResult:
        """Send a message to a specific queue."""
        # Auto-create queue if enabled
        if queue_name not in self.queues and self.config.enable_auto_queue_creation:
            await self.create_queue(queue_name)

        if queue_name not in self.queues:
            return DeliveryResult(
                success=False,
                message_id=None,  # type: ignore
                error_message=f"Queue {queue_name} does not exist",
            )

        try:
            queue = self.queues[queue_name]
            result = await queue.send_message(
                topic, payload, delivery_guarantee, **options
            )

            # Update global statistics
            if result.success:
                self.statistics.total_messages_sent += 1
            else:
                self.statistics.total_messages_failed += 1

            return result

        except Exception as e:
            queue_mgr_log.error(f"Failed to send message to queue {queue_name}: {e}")
            return DeliveryResult(
                success=False,
                message_id=None,  # type: ignore
                error_message=str(e),
            )

    def subscribe_to_queue(
        self,
        queue_name: str,
        subscriber_id: str,
        topic_pattern: str,
        callback: Any = None,
        auto_acknowledge: bool = True,
        **metadata: str,
    ) -> str | None:
        """Subscribe to messages from a specific queue."""
        if queue_name not in self.queues:
            queue_mgr_log.error(f"Cannot subscribe to non-existent queue: {queue_name}")
            return None

        try:
            queue = self.queues[queue_name]
            subscription_id = queue.subscribe(
                subscriber_id=subscriber_id,
                topic_pattern=topic_pattern,
                callback=callback,
                auto_acknowledge=auto_acknowledge,
                **metadata,
            )

            self.statistics.active_subscriptions += 1
            queue_mgr_log.info(f"Subscribed {subscriber_id} to queue {queue_name}")

            return subscription_id

        except Exception as e:
            queue_mgr_log.error(f"Failed to subscribe to queue {queue_name}: {e}")
            return None

    def unsubscribe_from_queue(self, queue_name: str, subscription_id: str) -> bool:
        """Remove a subscription from a specific queue."""
        if queue_name not in self.queues:
            return False

        try:
            queue = self.queues[queue_name]
            if queue.unsubscribe(subscription_id):
                self.statistics.active_subscriptions -= 1
                return True
            return False

        except Exception as e:
            queue_mgr_log.error(f"Failed to unsubscribe from queue {queue_name}: {e}")
            return False

    async def acknowledge_message(
        self, queue_name: str, message_id: str, subscriber_id: str
    ) -> bool:
        """Acknowledge a message in a specific queue."""
        if queue_name not in self.queues:
            return False

        try:
            queue = self.queues[queue_name]
            return await queue.acknowledge_message(message_id, subscriber_id)

        except Exception as e:
            queue_mgr_log.error(
                f"Failed to acknowledge message in queue {queue_name}: {e}"
            )
            return False

    def get_queue_statistics(self, queue_name: str) -> QueueStatistics | None:
        """Get statistics for a specific queue."""
        if queue_name not in self.queues:
            return None

        return self.queues[queue_name].get_statistics()

    def get_global_statistics(self) -> QueueManagerStatistics:
        """Get global statistics across all queues."""
        # Update statistics from all queues
        total_sent = 0
        total_received = 0
        total_acknowledged = 0
        total_failed = 0

        for queue in self.queues.values():
            stats = queue.get_statistics()
            total_sent += stats.messages_sent
            total_received += stats.messages_received
            total_acknowledged += stats.messages_acknowledged
            total_failed += stats.messages_failed

        # Use max to preserve any manually tracked statistics from the manager
        self.statistics.total_messages_sent = max(
            self.statistics.total_messages_sent, total_sent
        )
        self.statistics.total_messages_received = max(
            self.statistics.total_messages_received, total_received
        )
        self.statistics.total_messages_acknowledged = max(
            self.statistics.total_messages_acknowledged, total_acknowledged
        )
        self.statistics.total_messages_failed = max(
            self.statistics.total_messages_failed, total_failed
        )
        self.statistics.total_queues = len(self.queues)

        return self.statistics

    def list_queues(self) -> list[str]:
        """Get a list of all queue names."""
        return list(self.queues.keys())

    def get_queue_info(self, queue_name: str) -> dict[str, Any] | None:
        """Get detailed information about a specific queue."""
        if queue_name not in self.queues:
            return None

        queue = self.queues[queue_name]
        config = self.queue_configs[queue_name]
        stats = queue.get_statistics()

        return {
            "name": queue_name,
            "config": {
                "queue_type": config.queue_type.value,
                "max_size": config.max_size,
                "default_visibility_timeout_seconds": config.default_visibility_timeout_seconds,
                "default_acknowledgment_timeout_seconds": config.default_acknowledgment_timeout_seconds,
                "enable_dead_letter_queue": config.enable_dead_letter_queue,
                "enable_deduplication": config.enable_deduplication,
            },
            "statistics": {
                "messages_sent": stats.messages_sent,
                "messages_received": stats.messages_received,
                "messages_acknowledged": stats.messages_acknowledged,
                "messages_failed": stats.messages_failed,
                "current_queue_size": stats.current_queue_size,
                "current_in_flight_count": stats.current_in_flight_count,
                "success_rate": stats.success_rate(),
            },
            "subscriptions": len(queue.subscriptions),
        }

    # Topic Exchange Integration

    def _setup_topic_exchange_integration(self) -> None:
        """Set up integration with MPREG's topic exchange system."""
        if not self.topic_exchange:
            return

        # Subscribe to queue management topics
        management_topic = f"{self.config.integration_topic_prefix}.management.*"
        # Note: In a real implementation, we'd set up proper topic exchange subscriptions
        queue_mgr_log.info(
            f"Set up topic exchange integration with prefix: {self.config.integration_topic_prefix}"
        )

    async def publish_to_topic_exchange(
        self,
        topic: str,
        payload: Any,
        delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.FIRE_AND_FORGET,
    ) -> bool:
        """Publish a message to MPREG's topic exchange system."""
        if not self.topic_exchange:
            queue_mgr_log.warning("Topic exchange not available")
            return False

        try:
            from .model import PubSubMessage

            # Convert to MPREG's PubSubMessage format
            message = PubSubMessage(
                topic=topic,
                payload=payload,
                timestamp=time.time(),
                message_id=str(uuid.uuid4()),
                publisher="message-queue-manager",
            )

            # Publish via topic exchange
            self.topic_exchange.publish_message(message)

            queue_mgr_log.debug(f"Published message to topic exchange: {topic}")
            return True

        except Exception as e:
            queue_mgr_log.error(f"Failed to publish to topic exchange: {e}")
            return False

    async def route_topic_to_queue(
        self,
        topic_pattern: str,
        queue_name: str,
        delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
    ) -> bool:
        """Route messages from topic exchange to a specific queue."""
        try:
            # Auto-create queue if needed
            if queue_name not in self.queues and self.config.enable_auto_queue_creation:
                await self.create_queue(queue_name)

            if queue_name not in self.queues:
                queue_mgr_log.error(f"Cannot route to non-existent queue: {queue_name}")
                return False

            # Set up routing with proper dataclass
            routing = TopicRouting(
                topic_pattern=topic_pattern,
                queue_name=queue_name,
                delivery_guarantee=delivery_guarantee,
            )
            self.topic_routings.append(routing)

            queue_mgr_log.info(
                f"Set up routing from topic {topic_pattern} to queue {queue_name}"
            )
            return True

        except Exception as e:
            queue_mgr_log.error(f"Failed to set up topic routing: {e}")
            return False

    async def shutdown(self) -> None:
        """Shutdown all queues and cleanup resources."""
        queue_mgr_log.info("Shutting down Message Queue Manager...")

        # Shutdown all queues
        shutdown_tasks = []
        for queue_name, queue in self.queues.items():
            shutdown_tasks.append(queue.shutdown())

        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)

        # Clear all data structures
        self.queues.clear()
        self.queue_configs.clear()
        self.topic_routings.clear()

        # Shutdown task manager
        await super().shutdown()

        queue_mgr_log.info("Message Queue Manager shutdown complete")


# Factory functions for common configurations


def create_standard_queue_manager(
    topic_exchange: TopicExchange | None = None,
    *,
    persistence_registry: PersistenceRegistry | None = None,
) -> MessageQueueManager:
    """Create a standard message queue manager."""
    config = QueueManagerConfiguration()
    return MessageQueueManager(
        config, topic_exchange, persistence_registry=persistence_registry
    )


def create_high_throughput_queue_manager(
    topic_exchange: TopicExchange | None = None,
    *,
    persistence_registry: PersistenceRegistry | None = None,
) -> MessageQueueManager:
    """Create a high-throughput optimized queue manager."""
    config = QueueManagerConfiguration(
        default_max_queue_size=50000,
        default_visibility_timeout_seconds=10.0,
        default_acknowledgment_timeout_seconds=60.0,
        max_queues=5000,
    )
    return MessageQueueManager(
        config, topic_exchange, persistence_registry=persistence_registry
    )


def create_reliable_queue_manager(
    topic_exchange: TopicExchange | None = None,
    *,
    persistence_registry: PersistenceRegistry | None = None,
) -> MessageQueueManager:
    """Create a reliability-focused queue manager."""
    config = QueueManagerConfiguration(
        default_queue_type=QueueType.FIFO,
        default_visibility_timeout_seconds=60.0,
        default_acknowledgment_timeout_seconds=600.0,  # 10 minutes
        enable_auto_queue_creation=False,  # Explicit queue creation
    )
    return MessageQueueManager(
        config, topic_exchange, persistence_registry=persistence_registry
    )
