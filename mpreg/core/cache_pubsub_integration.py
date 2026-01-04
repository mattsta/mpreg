"""
Cache-PubSub Integration for MPREG.

This module provides seamless integration between the advanced cache operations
and the topic pub/sub fabric routing system, enabling:

- Automatic pub/sub notifications on cache operations
- Cache invalidation via pub/sub messages
- Global cache consistency using pub/sub fabric
- Real-time cache events for monitoring and alerting
- Distributed cache coordination
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from .advanced_cache_ops import (
    AdvancedCacheOperations,
    AtomicOperationRequest,
    AtomicOperationResult,
    DataStructureOperation,
    DataStructureResult,
    NamespaceOperation,
    NamespaceResult,
)
from .global_cache import CacheOptions, GlobalCacheKey, GlobalCacheManager
from .model import PubSubMessage, PubSubSubscription, TopicPattern
from .task_manager import ManagedObject
from .topic_exchange import TopicExchange


class CacheEventType(Enum):
    """Types of cache events that can trigger pub/sub notifications."""

    CACHE_HIT = "cache_hit"
    CACHE_MISS = "cache_miss"
    CACHE_PUT = "cache_put"
    CACHE_DELETE = "cache_delete"
    CACHE_INVALIDATE = "cache_invalidate"
    ATOMIC_OPERATION = "atomic_operation"
    DATA_STRUCTURE_OPERATION = "data_structure_operation"
    NAMESPACE_OPERATION = "namespace_operation"
    CACHE_EVICTION = "cache_eviction"
    CACHE_PROMOTION = "cache_promotion"
    CONSTRAINT_VIOLATION = "constraint_violation"


@dataclass(slots=True)
class CachePubSubIntegrationStats:
    """Statistics for cache-pub/sub integration."""

    notifications_sent: int = 0
    notifications_failed: int = 0
    events_processed: int = 0
    cache_operations_triggered: int = 0


@dataclass(frozen=True, slots=True)
class CacheNotificationConfig:
    """Configuration for cache-triggered pub/sub notifications."""

    notify_on_change: bool = False
    notification_topic: str = ""
    notification_payload: dict[str, Any] = field(default_factory=dict)
    notification_condition: dict[str, Any] | None = None
    event_types: list[CacheEventType] = field(default_factory=list)
    include_cache_metadata: bool = True
    include_performance_metrics: bool = False
    async_notification: bool = True
    max_notification_delay_ms: int = 100


@dataclass(frozen=True, slots=True)
class CacheEvent:
    """Cache event data for pub/sub notifications."""

    event_type: CacheEventType
    cache_key: GlobalCacheKey
    old_value: Any = None
    new_value: Any = None
    cache_level: str | None = None
    operation_metadata: dict[str, Any] = field(default_factory=dict)
    performance_metrics: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    cluster_id: str = ""


class CachePubSubIntegration(ManagedObject):
    """
    Integration layer between cache operations and pub/sub fabric.

    Provides bidirectional integration:
    1. Cache operations → Pub/Sub notifications
    2. Pub/Sub messages → Cache operations (invalidation, coordination)
    """

    def __init__(
        self,
        cache_manager: GlobalCacheManager,
        advanced_cache_ops: AdvancedCacheOperations,
        topic_exchange: TopicExchange,
        cluster_id: str = "default",
    ):
        super().__init__(name=f"CachePubSubIntegration-{cluster_id}")
        self.cache_manager = cache_manager
        self.advanced_cache_ops = advanced_cache_ops
        self.topic_exchange = topic_exchange
        self.cluster_id = cluster_id

        # Notification configuration per namespace
        self.notification_configs: dict[str, CacheNotificationConfig] = {}

        # Event listeners
        self.event_listeners: dict[
            CacheEventType, list[Callable[[CacheEvent], None]]
        ] = {event_type: [] for event_type in CacheEventType}

        # Async notification queue
        self.notification_queue: asyncio.Queue[CacheEvent] = asyncio.Queue()

        # Statistics
        self.stats = CachePubSubIntegrationStats()

        # Start background notification processor using task manager
        self._start_notification_processor()

        # Subscribe to cache coordination topics
        self._setup_cache_coordination_subscriptions()

    def configure_notifications(
        self, namespace: str, config: CacheNotificationConfig
    ) -> None:
        """Configure pub/sub notifications for a cache namespace."""
        self.notification_configs[namespace] = config
        logger.info(f"Configured cache notifications for namespace: {namespace}")

    def add_event_listener(
        self, event_type: CacheEventType, listener: Callable[[CacheEvent], None]
    ) -> None:
        """Add a listener for cache events."""
        self.event_listeners[event_type].append(listener)

    async def notify_cache_event(
        self,
        event: CacheEvent,
        config: CacheNotificationConfig | None = None,
        from_processor: bool = False,
    ) -> None:
        """Send a cache event notification via pub/sub."""
        try:
            if config is None:
                config = self.notification_configs.get(event.cache_key.namespace)
                if config is None:
                    return

            # Check if event type should trigger notification
            if config.event_types and event.event_type not in config.event_types:
                return

            # Check notification condition if specified
            if config.notification_condition and not self._evaluate_condition(
                event, config.notification_condition
            ):
                return

            # Prepare notification payload
            payload = {
                "event_type": event.event_type.value,
                "cache_key": {
                    "namespace": event.cache_key.namespace,
                    "identifier": event.cache_key.identifier,
                    "version": event.cache_key.version,
                    "tags": list(event.cache_key.tags),
                },
                "timestamp": event.timestamp,
                "event_id": event.event_id,
                "cluster_id": event.cluster_id,
                **config.notification_payload,
            }

            # Include cache metadata if requested
            if config.include_cache_metadata:
                payload.update(
                    {
                        "old_value": event.old_value,
                        "new_value": event.new_value,
                        "cache_level": event.cache_level,
                        "operation_metadata": event.operation_metadata,
                    }
                )

            # Include performance metrics if requested
            if config.include_performance_metrics:
                payload["performance_metrics"] = event.performance_metrics

            # Create pub/sub message
            message = PubSubMessage(
                topic=config.notification_topic,
                payload=payload,
                timestamp=time.time(),
                message_id=f"cache-event-{event.event_id}",
                publisher=f"cache-system-{self.cluster_id}",
                headers={"event_type": event.event_type.value},
            )

            # Send notification
            if config.async_notification and not from_processor:
                # Only queue if not already being processed from the queue
                await self.notification_queue.put(event)
            else:
                # Send directly (either sync notification or from processor)
                await self._send_notification(message)

            self.stats.notifications_sent += 1

        except Exception as e:
            logger.error(f"Failed to send cache event notification: {e}")
            self.stats.notifications_failed += 1

    def _evaluate_condition(self, event: CacheEvent, condition: dict[str, Any]) -> bool:
        """Evaluate whether an event meets notification conditions."""
        try:
            # Support simple field path conditions
            for field_path, constraint in condition.items():
                value = self._get_field_value(event, field_path)

                if isinstance(constraint, dict):
                    # MongoDB-style operators
                    for op, op_value in constraint.items():
                        if op == "$lt":
                            if not (value < op_value):
                                return False
                        elif op == "$gt":
                            if not (value > op_value):
                                return False
                        elif op == "$eq":
                            if value != op_value:
                                return False
                        elif op == "$ne":
                            if value == op_value:
                                return False
                        elif op == "$in":
                            # Check if value is in op_value, or if value is a list, if any item is in op_value
                            if isinstance(value, list | tuple | set):
                                if not any(item in op_value for item in value):
                                    return False
                            elif value not in op_value:
                                return False
                        elif op == "$nin":
                            # Check if value is not in op_value, or if value is a list, if no item is in op_value
                            if isinstance(value, list | tuple | set):
                                if any(item in op_value for item in value):
                                    return False
                            elif value in op_value:
                                return False
                        else:
                            # Unknown operator - log warning and return False to be safe
                            logger.warning(
                                f"Unknown condition operator: '{op}' (repr: {repr(op)})"
                            )
                            return False
                else:
                    # Direct equality check
                    if value != constraint:
                        return False

            return True

        except Exception as e:
            logger.warning(f"Failed to evaluate notification condition: {e}")
            return False

    def _get_field_value(self, event: CacheEvent, field_path: str) -> Any:
        """Extract field value from event using dot notation for known CacheEvent fields."""
        # Handle known field paths for CacheEvent dataclass
        if field_path == "event_type":
            return event.event_type
        elif field_path == "cache_key":
            return event.cache_key
        elif field_path == "cache_key.namespace":
            return event.cache_key.namespace
        elif field_path == "cache_key.identifier":
            return event.cache_key.identifier
        elif field_path == "old_value":
            return event.old_value
        elif field_path == "new_value":
            return event.new_value
        elif field_path == "cache_level":
            return event.cache_level
        elif field_path == "operation_metadata":
            return event.operation_metadata
        elif field_path.startswith("operation_metadata."):
            # Navigate through nested operation_metadata dictionary
            keys = field_path[len("operation_metadata.") :].split(".")
            current = event.operation_metadata
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None
            return current
        elif field_path == "timestamp":
            return event.timestamp
        else:
            # Unknown field path
            return None

    async def _send_notification(self, message: PubSubMessage) -> None:
        """Send a pub/sub notification message."""
        try:
            self.topic_exchange.publish_message(message)
        except Exception as e:
            logger.error(f"Failed to send pub/sub notification: {e}")
            raise

    def _start_notification_processor(self) -> None:
        """Start background task to process notification queue using task manager."""
        try:
            self.create_task(
                self._notification_processor(), name="notification_processor"
            )
        except RuntimeError:
            # No event loop running, skip background task
            logger.debug("No event loop running, skipping notification processor")

    async def _notification_processor(self) -> None:
        """Background task to process notification queue."""
        try:
            while True:
                try:
                    # Get event from queue with timeout
                    event = await asyncio.wait_for(
                        self.notification_queue.get(), timeout=1.0
                    )

                    # Get notification config
                    config = self.notification_configs.get(event.cache_key.namespace)
                    if config:
                        await self.notify_cache_event(
                            event, config, from_processor=True
                        )

                    self.stats.events_processed += 1

                except TimeoutError:
                    # Normal timeout, continue processing
                    continue
                except Exception as e:
                    logger.error(f"Error in notification processor: {e}")
                    # If event loop is gone, break the loop
                    if "no running event loop" in str(e):
                        break

        except asyncio.CancelledError:
            logger.debug("Cache notification processor cancelled")
        except Exception as e:
            logger.error(f"Notification processor error: {e}")
        finally:
            logger.debug("Cache notification processor stopped")

    def _setup_cache_coordination_subscriptions(self) -> None:
        """Set up subscriptions for cache coordination messages."""
        try:
            # Subscribe to cache invalidation messages
            invalidation_subscription = PubSubSubscription(
                subscription_id=f"cache-invalidation-{self.cluster_id}",
                patterns=(
                    TopicPattern(pattern="cache.invalidation.#", exact_match=False),
                ),
                subscriber=f"cache-coordinator-{self.cluster_id}",
                created_at=time.time(),
            )

            # Subscribe to cache coordination messages
            coordination_subscription = PubSubSubscription(
                subscription_id=f"cache-coordination-{self.cluster_id}",
                patterns=(
                    TopicPattern(pattern="cache.coordination.#", exact_match=False),
                ),
                subscriber=f"cache-coordinator-{self.cluster_id}",
                created_at=time.time(),
            )

            # Note: These would be processed in the main message handler
            # For now, we set up the framework

        except Exception as e:
            logger.error(f"Failed to setup cache coordination subscriptions: {e}")

    async def handle_cache_coordination_message(self, message: PubSubMessage) -> None:
        """Handle incoming cache coordination messages."""
        try:
            topic_parts = message.topic.split(".")

            if len(topic_parts) >= 3 and topic_parts[1] == "invalidation":
                await self._handle_invalidation_message(message)
            elif len(topic_parts) >= 3 and topic_parts[1] == "coordination":
                await self._handle_coordination_message(message)

            self.stats.cache_operations_triggered += 1

        except Exception as e:
            logger.error(f"Failed to handle cache coordination message: {e}")

    async def _handle_invalidation_message(self, message: PubSubMessage) -> None:
        """Handle cache invalidation messages."""
        try:
            payload = message.payload

            if "cache_key" in payload:
                # Invalidate specific key
                key_data = payload["cache_key"]
                cache_key = GlobalCacheKey(
                    namespace=key_data["namespace"],
                    identifier=key_data["identifier"],
                    version=key_data.get("version", "v1.0.0"),
                    tags=frozenset(key_data.get("tags", [])),
                )

                await self.cache_manager.delete(cache_key)
                logger.info(f"Invalidated cache key via pub/sub: {cache_key}")

            elif "pattern" in payload:
                # Invalidate by pattern
                pattern = payload["pattern"]
                result = await self.cache_manager.invalidate(pattern)
                logger.info(f"Invalidated cache pattern via pub/sub: {pattern}")

        except Exception as e:
            logger.error(f"Failed to handle invalidation message: {e}")

    async def _handle_coordination_message(self, message: PubSubMessage) -> None:
        """Handle cache coordination messages."""
        try:
            payload = message.payload
            coordination_type = payload.get("type", "")

            if coordination_type == "cache_warming":
                await self._handle_cache_warming(payload)
            elif coordination_type == "cache_migration":
                await self._handle_cache_migration(payload)
            elif coordination_type == "consistency_check":
                await self._handle_consistency_check(payload)

        except Exception as e:
            logger.error(f"Failed to handle coordination message: {e}")

    async def _handle_cache_warming(self, payload: dict[str, Any]) -> None:
        """Handle cache warming coordination."""
        # Implementation for distributed cache warming
        logger.info("Cache warming coordination message received")

    async def _handle_cache_migration(self, payload: dict[str, Any]) -> None:
        """Handle cache migration coordination."""
        # Implementation for cache migration between clusters
        logger.info("Cache migration coordination message received")

    async def _handle_consistency_check(self, payload: dict[str, Any]) -> None:
        """Handle cache consistency check coordination."""
        # Implementation for distributed consistency checks
        logger.info("Cache consistency check coordination message received")

    async def broadcast_cache_invalidation(
        self, cache_key: GlobalCacheKey | None = None, pattern: str | None = None
    ) -> None:
        """Broadcast cache invalidation to all clusters via pub/sub."""
        try:
            payload = {
                "timestamp": time.time(),
                "source_cluster": self.cluster_id,
                "invalidation_id": str(uuid.uuid4()),
            }

            if cache_key:
                payload["cache_key"] = {
                    "namespace": cache_key.namespace,
                    "identifier": cache_key.identifier,
                    "version": cache_key.version,
                    "tags": list(cache_key.tags),
                }
                topic = f"cache.invalidation.key.{cache_key.namespace}"
            elif pattern:
                payload["pattern"] = pattern
                topic = f"cache.invalidation.pattern.{pattern.replace('*', 'wildcard').replace('#', 'multi')}"
            else:
                raise ValueError("Either cache_key or pattern must be provided")

            message = PubSubMessage(
                topic=topic,
                payload=payload,
                timestamp=time.time(),
                message_id=f"invalidation-{payload['invalidation_id']}",
                publisher=f"cache-system-{self.cluster_id}",
                headers={"message_type": "cache_invalidation"},
            )

            await self._send_notification(message)
            self.stats.notifications_sent += 1
            logger.info(f"Broadcasted cache invalidation: {topic}")

        except Exception as e:
            logger.error(f"Failed to broadcast cache invalidation: {e}")
            raise

    def get_statistics(self) -> dict[str, Any]:
        """Get cache-pub/sub integration statistics."""
        return {
            "notifications_sent": self.stats.notifications_sent,
            "notifications_failed": self.stats.notifications_failed,
            "events_processed": self.stats.events_processed,
            "cache_operations_triggered": self.stats.cache_operations_triggered,
            "notification_configs": len(self.notification_configs),
            "event_listeners": {
                event_type.value: len(listeners)
                for event_type, listeners in self.event_listeners.items()
            },
            "notification_queue_size": self.notification_queue.qsize(),
        }

    async def shutdown(self) -> None:
        """Gracefully shutdown the integration."""
        try:
            # Shutdown using task manager for proper cleanup
            await super().shutdown()
            logger.info("Cache-PubSub integration shut down successfully")

        except Exception as e:
            logger.error(f"Error during cache-pub/sub integration shutdown: {e}")


class EnhancedAdvancedCacheOperations(AdvancedCacheOperations):
    """
    Enhanced cache operations with integrated pub/sub notifications.

    Extends AdvancedCacheOperations to automatically trigger pub/sub events
    for cache operations when configured.
    """

    def __init__(
        self,
        cache_manager: GlobalCacheManager,
        pubsub_integration: CachePubSubIntegration | None = None,
    ):
        super().__init__(cache_manager)
        self.pubsub_integration = pubsub_integration

    async def atomic_operation(
        self, request: AtomicOperationRequest, options: CacheOptions | None = None
    ) -> AtomicOperationResult:
        """Execute atomic operation with pub/sub notification."""
        # Execute the operation
        result = await super().atomic_operation(request, options)

        # Send notification if configured
        if self.pubsub_integration:
            event = CacheEvent(
                event_type=CacheEventType.ATOMIC_OPERATION,
                cache_key=request.key,
                old_value=result.old_value,
                new_value=result.new_value,
                operation_metadata={
                    "operation": request.operation.value,
                    "success": result.success,
                    "constraint_violations": result.constraint_violations,
                },
                cluster_id=self.pubsub_integration.cluster_id,
            )

            await self.pubsub_integration.notify_cache_event(event)

        return result

    async def data_structure_operation(
        self, operation: DataStructureOperation, options: CacheOptions | None = None
    ) -> DataStructureResult:
        """Execute data structure operation with pub/sub notification."""
        # Execute the operation
        result = await super().data_structure_operation(operation, options)

        # Send notification if configured
        if self.pubsub_integration:
            event = CacheEvent(
                event_type=CacheEventType.DATA_STRUCTURE_OPERATION,
                cache_key=operation.key,
                new_value=result.value,
                operation_metadata={
                    "structure_type": operation.structure_type.value,
                    "operation": operation.operation,
                    "success": result.success,
                    "size": result.size,
                },
                cluster_id=self.pubsub_integration.cluster_id,
            )

            await self.pubsub_integration.notify_cache_event(event)

        return result

    async def namespace_operation(
        self, operation: NamespaceOperation, options: CacheOptions | None = None
    ) -> NamespaceResult:
        """Execute namespace operation with pub/sub notification."""
        # Execute the operation
        result = await super().namespace_operation(operation, options)

        # Send notification if configured
        if self.pubsub_integration:
            # Create a dummy cache key for namespace operations
            namespace_key = GlobalCacheKey(
                namespace=operation.namespace,
                identifier="namespace_operation",
                version="v1.0.0",
            )

            event = CacheEvent(
                event_type=CacheEventType.NAMESPACE_OPERATION,
                cache_key=namespace_key,
                operation_metadata={
                    "operation": operation.operation,
                    "pattern": operation.pattern,
                    "success": result.success,
                    "count": result.count,
                    "cleared_count": result.cleared_count,
                },
                cluster_id=self.pubsub_integration.cluster_id,
            )

            await self.pubsub_integration.notify_cache_event(event)

        return result
