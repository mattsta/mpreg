"""
Client API extensions for MPREG pub/sub operations.

This module extends the existing MPREGClientAPI with topic-based
publish/subscribe capabilities.
"""

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import ulid
from loguru import logger

from ..core.model import (
    PubSubAck,
    PubSubMessage,
    PubSubNotification,
    PubSubPublish,
    PubSubSubscribe,
    PubSubSubscription,
    PubSubUnsubscribe,
    TopicPattern,
)
from ..core.statistics import (
    ClientStatistics,
    MessageHeaders,
    PublishResponse,
    SubscriptionInfo,
    TopicMetrics,
)
from .client_api import MPREGClientAPI


@dataclass(slots=True)
class SubscriptionCallback:
    """Callback function for topic subscriptions."""

    callback: Callable[[PubSubMessage], None]
    patterns: list[str]
    subscription_id: str
    created_at: float


@dataclass(slots=True)
class MPREGPubSubClient:
    """
    Extended client API for MPREG pub/sub operations.

    Provides high-level API for publishing messages to topics and
    subscribing to topic patterns with callbacks.
    """

    base_client: MPREGClientAPI
    subscriptions: dict[str, SubscriptionCallback] = field(default_factory=dict)
    notification_handlers: dict[str, asyncio.Task] = field(default_factory=dict)
    _client_id: str = field(default_factory=lambda: f"pubsub_client_{str(ulid.new())}")
    _running: bool = False
    _notification_queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    _notification_task: asyncio.Task | None = None

    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()

    async def start(self):
        """Start the pub/sub client."""
        self._running = True
        self._notification_task = asyncio.create_task(self._notification_handler())

    async def stop(self):
        """Stop the pub/sub client and cleanup subscriptions."""
        self._running = False

        # Unsubscribe from all topics
        for subscription_id in list(self.subscriptions.keys()):
            try:
                await self.unsubscribe(subscription_id)
            except Exception as e:
                logger.warning(f"Error unsubscribing from {subscription_id}: {e}")

        # Cancel notification handler
        if self._notification_task and not self._notification_task.done():
            self._notification_task.cancel()
            try:
                await self._notification_task
            except asyncio.CancelledError:
                pass

    async def publish(
        self, topic: str, payload: Any, headers: MessageHeaders | None = None
    ) -> bool:
        """
        Publish a message to a topic.

        Args:
            topic: The topic to publish to
            payload: The message payload
            headers: Optional message headers

        Returns:
            True if published successfully, False otherwise
        """
        message = PubSubMessage(
            topic=topic,
            payload=payload,
            timestamp=time.time(),
            message_id=str(ulid.new()),
            publisher=self._client_id,
            headers=headers.to_dict() if headers else {},
        )

        publish_req = PubSubPublish(message=message, u=str(ulid.new()))

        try:
            # Send publish request through the base client
            response = await self.base_client._client.send_raw_message(
                publish_req.model_dump()
            )

            # Check if it's an acknowledgment
            if response.get("role") == "pubsub-ack":
                ack = PubSubAck.model_validate(response)
                return ack.success

            return False
        except Exception as e:
            logger.error(f"Error publishing to topic {topic}: {e}")
            return False

    async def publish_with_reply(
        self,
        topic: str,
        payload: Any,
        headers: MessageHeaders | None = None,
        timeout: float = 30.0,
    ) -> PublishResponse:
        """
        Publish a message to a topic and wait for a reply.

        Args:
            topic: The topic to publish to
            payload: The message payload
            headers: Optional message headers (must include reply_to for replies)
            timeout: Timeout in seconds to wait for reply

        Returns:
            PublishResponse containing success status and optional reply
        """
        # Generate a reply topic if not specified in headers
        reply_topic = f"reply.{str(ulid.new())}"

        # Set up headers with reply_to
        if headers is None:
            headers = MessageHeaders(reply_to=reply_topic)
        else:
            # Create new headers with reply_to set
            headers = MessageHeaders(
                content_type=headers.content_type,
                correlation_id=headers.correlation_id,
                reply_to=reply_topic,
                priority=headers.priority,
                ttl_seconds=headers.ttl_seconds,
                custom_headers=headers.custom_headers,
            )

        # Set up a temporary subscription for the reply
        reply_received = asyncio.Event()
        reply_message = None
        reply_error = None

        def reply_callback(message: PubSubMessage):
            nonlocal reply_message
            reply_message = message
            reply_received.set()

        try:
            # Subscribe to reply topic
            reply_subscription = await self.subscribe(
                patterns=[reply_topic],
                callback=reply_callback,
                get_backlog=False,
            )

            # Publish the original message
            publish_success = await self.publish(topic, payload, headers)

            if not publish_success:
                return PublishResponse(
                    success=False,
                    message_id="",
                    error="Failed to publish message",
                )

            # Wait for reply
            try:
                await asyncio.wait_for(reply_received.wait(), timeout=timeout)

                if reply_message:
                    return PublishResponse(
                        success=True,
                        message_id=reply_message.message_id,
                        reply_payload=reply_message.payload,
                        reply_headers=MessageHeaders(
                            content_type=reply_message.headers.get("content_type"),
                            correlation_id=reply_message.headers.get("correlation_id"),
                            reply_to=reply_message.headers.get("reply_to"),
                            priority=reply_message.headers.get("priority"),
                            ttl_seconds=reply_message.headers.get("ttl_seconds"),
                        ),
                    )
                else:
                    return PublishResponse(
                        success=False,
                        message_id="",
                        error="No reply received",
                    )

            except TimeoutError:
                return PublishResponse(
                    success=False,
                    message_id="",
                    error=f"Reply timeout after {timeout} seconds",
                )

        finally:
            # Clean up reply subscription
            try:
                await self.unsubscribe(reply_subscription)
            except Exception as e:
                logger.warning(f"Error cleaning up reply subscription: {e}")

    async def subscribe(
        self,
        patterns: list[str],
        callback: Callable[[PubSubMessage], None],
        get_backlog: bool = True,
        backlog_seconds: int = 300,
    ) -> str:
        """
        Subscribe to topic patterns with a callback.

        Args:
            patterns: List of topic patterns to subscribe to
            callback: Function to call when messages are received
            get_backlog: Whether to receive recent message backlog
            backlog_seconds: How many seconds of backlog to receive

        Returns:
            Subscription ID for managing the subscription
        """
        subscription_id = str(ulid.new())

        # Create topic patterns
        topic_patterns = [
            TopicPattern(
                pattern=pattern, exact_match="*" not in pattern and "#" not in pattern
            )
            for pattern in patterns
        ]

        # Create subscription
        subscription = PubSubSubscription(
            subscription_id=subscription_id,
            patterns=tuple(topic_patterns),
            subscriber=self._client_id,
            created_at=time.time(),
            get_backlog=get_backlog,
            backlog_seconds=backlog_seconds,
        )

        # Send subscription request
        subscribe_req = PubSubSubscribe(subscription=subscription, u=str(ulid.new()))

        try:
            response = await self.base_client._client.send_raw_message(
                subscribe_req.model_dump()
            )

            # Check if it's an acknowledgment
            if response.get("role") == "pubsub-ack":
                ack = PubSubAck.model_validate(response)
                if ack.success:
                    # Store subscription callback
                    self.subscriptions[subscription_id] = SubscriptionCallback(
                        callback=callback,
                        patterns=patterns,
                        subscription_id=subscription_id,
                        created_at=time.time(),
                    )
                    return subscription_id
                else:
                    raise Exception(f"Subscription failed: {ack.error}")

            raise Exception("Invalid response to subscription request")

        except Exception as e:
            logger.error(f"Error subscribing to patterns {patterns}: {e}")
            raise

    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from a topic subscription.

        Args:
            subscription_id: The subscription ID to cancel

        Returns:
            True if unsubscribed successfully, False otherwise
        """
        if subscription_id not in self.subscriptions:
            return False

        unsubscribe_req = PubSubUnsubscribe(
            subscription_id=subscription_id, u=str(ulid.new())
        )

        try:
            response = await self.base_client._client.send_raw_message(
                unsubscribe_req.model_dump()
            )

            # Check if it's an acknowledgment
            if response.get("role") == "pubsub-ack":
                ack = PubSubAck.model_validate(response)
                if ack.success:
                    # Remove subscription callback
                    del self.subscriptions[subscription_id]
                    return True
                else:
                    logger.error(f"Unsubscribe failed: {ack.error}")
                    return False

            return False

        except Exception as e:
            logger.error(f"Error unsubscribing from {subscription_id}: {e}")
            return False

    async def _notification_handler(self):
        """Handle incoming pub/sub notifications."""
        # Get the notification queue from the base client
        notification_queue = self.base_client._client.get_notification_queue()

        while self._running:
            try:
                # Wait indefinitely for notifications - no timeout needed
                notification = await notification_queue.get()

                # Process the notification
                self._handle_notification(notification)

                # Mark the queue task as done
                notification_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in notification handler: {e}")
                await asyncio.sleep(0.1)  # Brief pause before retrying

    def _handle_notification(self, notification: PubSubNotification):
        """Handle a received notification."""
        if notification.subscription_id in self.subscriptions:
            callback_info = self.subscriptions[notification.subscription_id]
            try:
                callback_info.callback(notification.message)
            except Exception as e:
                logger.error(f"Error in subscription callback: {e}")

    def list_subscriptions(self) -> list[SubscriptionInfo]:
        """List all active subscriptions."""
        return [
            SubscriptionInfo(
                subscription_id=sub_id,
                patterns=callback.patterns,
                created_at=callback.created_at,
                subscriber=self._client_id,
                backlog_enabled=False,  # Default value, could be enhanced later
            )
            for sub_id, callback in self.subscriptions.items()
        ]

    def get_stats(self) -> ClientStatistics:
        """Get pub/sub client statistics."""
        return ClientStatistics(
            client_id=self._client_id,
            active_subscriptions=len(self.subscriptions),
            running=self._running,
            oldest_subscription=min(
                (cb.created_at for cb in self.subscriptions.values()), default=None
            ),
        )


@dataclass(slots=True)
class MPREGPubSubExtendedClient(MPREGClientAPI):
    """
    Extended MPREG client with built-in pub/sub capabilities.

    Combines the existing RPC functionality with pub/sub operations
    in a single client interface.
    """

    # Fields assigned in __post_init__
    pubsub: MPREGPubSubClient = field(init=False)

    def __post_init__(self) -> None:
        """Initialize the pub/sub client after parent initialization."""
        # Initialize the parent manually since MPREGClientAPI doesn't have __post_init__
        from .client import Client

        self._client = Client(url=self.url, full_log=self.full_log)
        self._connected = False
        self.pubsub = MPREGPubSubClient(base_client=self)

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        await self.pubsub.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.pubsub.stop()
        await self.disconnect()

    # Convenience methods for pub/sub operations
    async def publish(
        self, topic: str, payload: Any, headers: MessageHeaders | None = None
    ) -> bool:
        """Publish a message to a topic."""
        return await self.pubsub.publish(topic, payload, headers)

    async def subscribe(
        self,
        patterns: list[str],
        callback: Callable[[PubSubMessage], None],
        get_backlog: bool = True,
        backlog_seconds: int = 300,
    ) -> str:
        """Subscribe to topic patterns with a callback."""
        return await self.pubsub.subscribe(
            patterns, callback, get_backlog, backlog_seconds
        )

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from a topic subscription."""
        return await self.pubsub.unsubscribe(subscription_id)

    async def publish_with_reply(
        self,
        topic: str,
        payload: Any,
        headers: MessageHeaders | None = None,
        timeout: float = 30.0,
    ) -> PublishResponse:
        """Publish a message to a topic and wait for a reply."""
        return await self.pubsub.publish_with_reply(topic, payload, headers, timeout)


# Utility functions for common pub/sub patterns
async def create_topic_logger(
    client: MPREGPubSubExtendedClient,
    log_pattern: str = "logs.#",
    log_level: str = "INFO",
) -> str:
    """Create a subscription that logs all messages matching a pattern."""

    def log_callback(message: PubSubMessage):
        logger.info(
            f"[TOPIC:{message.topic}] {message.payload} (from {message.publisher})",
        )

    return await client.subscribe([log_pattern], log_callback)


async def create_topic_metrics_collector(
    client: MPREGPubSubExtendedClient, metrics_pattern: str = "metrics.#"
) -> TopicMetrics:
    """Create a subscription that collects metrics from topic messages."""

    subscription_id = str(ulid.new())  # Generate subscription ID first

    metrics = TopicMetrics(
        subscription_id=subscription_id,
        message_count=0,
        topics_seen=set(),
        publishers_seen=set(),
        last_message_time=None,
    )

    def metrics_callback(message: PubSubMessage):
        metrics.message_count += 1
        if metrics.topics_seen:
            metrics.topics_seen.add(message.topic)
        if metrics.publishers_seen:
            metrics.publishers_seen.add(message.publisher)
        metrics.last_message_time = message.timestamp

    # Subscribe and update the subscription_id in metrics
    actual_subscription_id = await client.subscribe([metrics_pattern], metrics_callback)
    metrics.subscription_id = actual_subscription_id

    return metrics


async def create_topic_forwarder(
    source_client: MPREGPubSubExtendedClient,
    target_client: MPREGPubSubExtendedClient,
    source_pattern: str,
    target_topic_prefix: str = "forwarded",
) -> str:
    """Create a subscription that forwards messages from one client to another."""

    def forward_callback(message: PubSubMessage):
        # Forward the message to the target client with a prefix
        target_topic = f"{target_topic_prefix}.{message.topic}"

        # Convert message headers dict to MessageHeaders
        headers = (
            MessageHeaders(
                content_type=message.headers.get("content_type"),
                correlation_id=message.headers.get("correlation_id"),
                reply_to=message.headers.get("reply_to"),
                priority=message.headers.get("priority"),
                ttl_seconds=message.headers.get("ttl_seconds"),
                custom_headers={
                    k: v
                    for k, v in message.headers.items()
                    if k
                    not in {
                        "content_type",
                        "correlation_id",
                        "reply_to",
                        "priority",
                        "ttl_seconds",
                    }
                },
            )
            if message.headers
            else None
        )

        asyncio.create_task(
            target_client.publish(target_topic, message.payload, headers)
        )

    return await source_client.subscribe([source_pattern], forward_callback)


# Example usage patterns
async def example_basic_pubsub():
    """Example of basic pub/sub operations."""
    async with MPREGPubSubExtendedClient("ws://localhost:9001") as client:
        # Subscribe to user events
        def user_callback(message: PubSubMessage):
            print(f"User event: {message.topic} -> {message.payload}")

        subscription_id = await client.subscribe(
            patterns=["user.*.login", "user.*.logout"], callback=user_callback
        )

        # Publish some messages
        await client.publish(
            "user.123.login", {"username": "alice", "timestamp": time.time()}
        )
        await client.publish(
            "user.456.logout", {"username": "bob", "timestamp": time.time()}
        )

        # Wait for messages to be processed
        await asyncio.sleep(1)

        # Unsubscribe
        await client.unsubscribe(subscription_id)


async def example_advanced_topic_routing():
    """Example of advanced topic routing patterns."""
    async with MPREGPubSubExtendedClient("ws://localhost:9001") as client:
        # Create multiple specialized subscriptions
        error_sub = await client.subscribe(
            patterns=["*.error", "system.*.critical"],
            callback=lambda msg: print(f"🚨 ERROR: {msg.topic} - {msg.payload}"),
        )

        metrics_sub = await client.subscribe(
            patterns=["metrics.#"],
            callback=lambda msg: print(f"📊 METRIC: {msg.topic} - {msg.payload}"),
        )

        user_activity_sub = await client.subscribe(
            patterns=["user.*.activity.*"],
            callback=lambda msg: print(f"👤 USER: {msg.topic} - {msg.payload}"),
        )

        # Publish various types of messages
        await client.publish("system.auth.critical", {"error": "Auth system down"})
        await client.publish("metrics.cpu.usage", {"cpu_percent": 85.3})
        await client.publish("user.789.activity.page_view", {"page": "/dashboard"})

        await asyncio.sleep(2)

        # Cleanup
        await client.unsubscribe(error_sub)
        await client.unsubscribe(metrics_sub)
        await client.unsubscribe(user_activity_sub)
