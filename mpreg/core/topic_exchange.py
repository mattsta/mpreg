"""
High-performance AMQP-style topic exchange system for MPREG.

This module implements a trie-based topic matching engine that supports:
- Hierarchical topic patterns (e.g., user.123.login)
- Wildcard matching (* for single level, # for multi-level)
- Million+ topic scale performance
- Message backlog with time-windowed storage
- Integration with MPREG's gossip protocol
"""

from __future__ import annotations

import heapq
import re
import time
from collections import defaultdict, deque
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from threading import RLock

# Import for type annotations
from typing import Any

from mpreg.core.statistics import (
    BacklogStatistics,
    TopicExchangeComprehensiveStats,
)
from mpreg.datastructures.trie import TopicTrie

from .model import (
    PubSubMessage,
    PubSubNotification,
    PubSubSubscription,
    TopicAdvertisement,
)


@dataclass(slots=True, frozen=True)
class StoredMessage:
    """A message stored in the backlog."""

    message: PubSubMessage
    stored_at: float
    size_bytes: int


@dataclass(slots=True)
class MessageBacklog:
    """
    Time-windowed message storage for topic backlogs.

    Efficiently stores recent messages with automatic cleanup.
    """

    max_age_seconds: int = 3600
    max_messages_per_topic: int = 1000
    backlogs: dict[str, deque[Any]] = field(default_factory=dict)
    cleanup_heap: list[tuple[float, str, str]] = field(default_factory=list)
    total_messages: int = 0
    total_size_bytes: int = 0
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """Initialize backlogs with correct maxlen after dataclass creation."""
        self.backlogs = defaultdict(lambda: deque(maxlen=self.max_messages_per_topic))

    def add_message(self, message: PubSubMessage) -> None:
        """Add a message to the backlog."""
        with self._lock:
            stored_msg = StoredMessage(
                message=message,
                stored_at=time.time(),
                size_bytes=len(str(message.payload)),  # Rough size estimate
            )

            # Add to topic backlog
            topic_backlog = self.backlogs[message.topic]
            topic_backlog.append(stored_msg)

            # Add to cleanup heap (using message timestamp for proper expiration)
            heapq.heappush(
                self.cleanup_heap,
                (message.timestamp, message.topic, message.message_id),
            )

            self.total_messages += 1
            self.total_size_bytes += stored_msg.size_bytes

            # Periodic cleanup
            if self.total_messages % 10 == 0:  # More frequent cleanup for testing
                self._cleanup_expired()

    def get_backlog(
        self, topic_pattern: str, max_age_seconds: int
    ) -> list[PubSubMessage]:
        """Get recent messages matching a topic pattern."""
        with self._lock:
            cutoff_time = time.time() - max_age_seconds
            messages = []

            # Simple approach: check all topics (could be optimized with pattern matching)
            for topic, backlog in self.backlogs.items():
                if self._matches_pattern(topic, topic_pattern):
                    for stored_msg in backlog:
                        # Filter by message timestamp, not stored_at time
                        if stored_msg.message.timestamp >= cutoff_time:
                            messages.append(stored_msg.message)

            # Sort by timestamp
            messages.sort(key=lambda m: m.timestamp)
            return messages

    def _matches_pattern(self, topic: str, pattern: str) -> bool:
        """Simple pattern matching for backlog retrieval."""
        if pattern == "#":
            return True

        # Convert AMQP pattern to regex
        regex_pattern = pattern.replace(".", r"\.")
        regex_pattern = regex_pattern.replace("*", r"[^.]+")
        regex_pattern = regex_pattern.replace("#", r".*")
        regex_pattern = f"^{regex_pattern}$"

        return re.match(regex_pattern, topic) is not None

    def _cleanup_expired(self) -> None:
        """Remove expired messages from backlogs."""
        cutoff_time = time.time() - self.max_age_seconds

        # Efficiently remove expired messages using the heap
        while self.cleanup_heap and self.cleanup_heap[0][0] < cutoff_time:
            _, topic, message_id = heapq.heappop(self.cleanup_heap)

            # Remove from topic backlog if it exists
            if topic in self.backlogs:
                backlog = self.backlogs[topic]
                # Remove expired messages from the front (deque is ordered by time)
                while backlog and backlog[0].message.timestamp < cutoff_time:
                    removed = backlog.popleft()
                    self.total_messages -= 1
                    self.total_size_bytes -= removed.size_bytes

                # Clean up empty backlogs
                if not backlog:
                    del self.backlogs[topic]

    def get_stats(self) -> BacklogStatistics:
        """Get backlog statistics."""
        return BacklogStatistics(
            total_messages=self.total_messages,
            total_size_bytes=self.total_size_bytes,
            total_size_mb=self.total_size_bytes / (1024 * 1024),
            active_topics=len(self.backlogs),
            cleanup_queue_size=len(self.cleanup_heap),
        )


@dataclass(slots=True)
class TopicExchange:
    """
    Main pub/sub exchange engine for MPREG.

    Handles high-performance topic routing, subscription management,
    and message backlog with gossip protocol integration.
    """

    server_url: str
    cluster_id: str
    trie: TopicTrie = field(default_factory=TopicTrie)
    backlog: MessageBacklog = field(default_factory=MessageBacklog)
    subscriptions: dict[str, PubSubSubscription] = field(default_factory=dict)
    client_subscriptions: dict[str, set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    messages_published: int = 0
    messages_delivered: int = 0
    active_subscribers: int = 0
    topic_advertisements: dict[str, TopicAdvertisement] = field(default_factory=dict)
    remote_topic_servers: dict[str, set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    internal_callbacks: dict[
        str, Callable[[PubSubNotification], Coroutine[Any, Any, None] | None]
    ] = field(default_factory=dict)
    _backlog_disabled: set[str] = field(default_factory=set)
    _backlog_disabled_prefixes: set[str] = field(default_factory=set)
    _lock: RLock = field(default_factory=RLock)

    def add_subscription(self, subscription: PubSubSubscription) -> None:
        """Add a new subscription."""
        with self._lock:
            self.subscriptions[subscription.subscription_id] = subscription
            self.client_subscriptions[subscription.subscriber].add(
                subscription.subscription_id
            )

            # Add patterns to trie
            for pattern in subscription.patterns:
                self.trie.add_pattern(pattern.pattern, subscription.subscription_id)

            # Send backlog if requested
            if subscription.get_backlog:
                self._send_backlog(subscription)

            self.active_subscribers = len(self.client_subscriptions)

    def add_internal_subscription(
        self,
        subscription: PubSubSubscription,
        callback: Callable[[PubSubNotification], Coroutine[Any, Any, None] | None],
    ) -> None:
        """Add an internal subscription with an in-process callback."""
        self.add_subscription(subscription)
        with self._lock:
            self.internal_callbacks[subscription.subscription_id] = callback

    def remove_subscription(self, subscription_id: str) -> bool:
        """Remove a subscription."""
        with self._lock:
            if subscription_id not in self.subscriptions:
                return False

            subscription = self.subscriptions[subscription_id]

            # Remove from trie
            for pattern in subscription.patterns:
                self.trie.remove_pattern(pattern.pattern, subscription_id)

            # Remove from tracking
            del self.subscriptions[subscription_id]
            self.client_subscriptions[subscription.subscriber].discard(subscription_id)
            self.internal_callbacks.pop(subscription_id, None)

            # Clean up empty client entries
            if not self.client_subscriptions[subscription.subscriber]:
                del self.client_subscriptions[subscription.subscriber]

            self.active_subscribers = len(self.client_subscriptions)
            return True

    def publish_message(self, message: PubSubMessage) -> list[PubSubNotification]:
        """Publish a message and return notifications for subscribers."""
        internal_deliveries: list[
            tuple[
                Callable[[PubSubNotification], Coroutine[Any, Any, None] | None],
                PubSubNotification,
            ]
        ] = []
        with self._lock:
            # Store in backlog unless disabled for this topic
            if message.topic not in self._backlog_disabled and not any(
                message.topic.startswith(prefix)
                for prefix in self._backlog_disabled_prefixes
            ):
                self.backlog.add_message(message)

            # Find matching subscriptions
            matching_subscriptions = self.trie.match_topic(message.topic)

            # Create notifications
            notifications = []
            for subscription_id in matching_subscriptions:
                if subscription_id in self.subscriptions:
                    notification = PubSubNotification(
                        message=message,
                        subscription_id=subscription_id,
                        u=f"notification_{message.message_id}_{subscription_id}",
                    )
                    notifications.append(notification)
                    callback = self.internal_callbacks.get(subscription_id)
                    if callback is not None:
                        internal_deliveries.append((callback, notification))

            self.messages_published += 1
            self.messages_delivered += len(notifications)

        if internal_deliveries:
            import asyncio

            for callback, notification in internal_deliveries:
                result = callback(notification)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)

        return notifications

    def set_backlog_enabled(self, topic: str, *, enabled: bool) -> None:
        """Enable or disable backlog storage for a specific topic."""
        with self._lock:
            if enabled:
                self._backlog_disabled.discard(topic)
            else:
                self._backlog_disabled.add(topic)

    def set_backlog_prefix_enabled(self, prefix: str, *, enabled: bool) -> None:
        """Enable or disable backlog storage for topics with the prefix."""
        with self._lock:
            if enabled:
                self._backlog_disabled_prefixes.discard(prefix)
            else:
                self._backlog_disabled_prefixes.add(prefix)

    def _send_backlog(
        self, subscription: PubSubSubscription
    ) -> list[PubSubNotification]:
        """Send backlog messages to a new subscriber."""
        notifications = []

        for pattern in subscription.patterns:
            backlog_messages = self.backlog.get_backlog(
                pattern.pattern, subscription.backlog_seconds
            )

            for message in backlog_messages:
                notification = PubSubNotification(
                    message=message,
                    subscription_id=subscription.subscription_id,
                    u=f"backlog_{message.message_id}_{subscription.subscription_id}",
                )
                notifications.append(notification)

        return notifications

    def get_topic_advertisement(self) -> TopicAdvertisement:
        """Get advertisement for gossip protocol."""
        with self._lock:
            # Collect all active topic patterns
            active_patterns = set()
            for subscription in self.subscriptions.values():
                for pattern in subscription.patterns:
                    active_patterns.add(pattern.pattern)

            return TopicAdvertisement(
                server_url=self.server_url,
                topics=tuple(active_patterns),
                subscriber_count=len(self.subscriptions),
                last_activity=time.time(),
            )

    def update_remote_topics(self, advertisements: list[TopicAdvertisement]) -> None:
        """Update knowledge of remote topic servers."""
        with self._lock:
            # Clear old data
            self.remote_topic_servers.clear()

            # Add new advertisements
            for ad in advertisements:
                for topic_pattern in ad.topics:
                    self.remote_topic_servers[topic_pattern].add(ad.server_url)

    def get_stats(self) -> TopicExchangeComprehensiveStats:
        """Get comprehensive statistics."""
        trie_stats = self.trie.get_statistics()
        backlog_stats = self.backlog.get_stats()

        return TopicExchangeComprehensiveStats(
            server_url=self.server_url,
            cluster_id=self.cluster_id,
            active_subscriptions=len(self.subscriptions),
            active_subscribers=self.active_subscribers,
            messages_published=self.messages_published,
            messages_delivered=self.messages_delivered,
            delivery_ratio=self.messages_delivered / max(self.messages_published, 1),
            trie_stats=trie_stats,
            backlog_stats=backlog_stats,
            remote_servers=len(set().union(*self.remote_topic_servers.values()))
            if self.remote_topic_servers
            else 0,
        )


# Performance testing utilities
@dataclass(slots=True)
class TopicMatchingBenchmark:
    """Benchmark utilities for topic matching performance."""

    @staticmethod
    def generate_test_topics(count: int) -> list[str]:
        """Generate realistic test topics."""
        prefixes = ["user", "order", "payment", "inventory", "analytics", "system"]
        middle_parts = ["create", "update", "delete", "view", "process", "validate"]
        suffixes = ["success", "failed", "pending", "completed", "cancelled"]

        topics = []
        for i in range(count):
            prefix = prefixes[i % len(prefixes)]
            middle = middle_parts[i % len(middle_parts)]
            suffix = suffixes[i % len(suffixes)]
            user_id = 1000 + (i % 9000)  # Realistic user IDs

            topic = f"{prefix}.{user_id}.{middle}.{suffix}"
            topics.append(topic)

        return topics

    @staticmethod
    def generate_test_patterns(count: int) -> list[str]:
        """Generate realistic subscription patterns."""
        patterns = [
            "user.*.login.*",
            "order.#",
            "payment.*.success",
            "inventory.*.update.*",
            "analytics.#",
            "system.*.error",
            "user.*.profile.update",
            "order.*.cancelled",
            "payment.*.failed",
            "inventory.*.low_stock",
        ]

        # Generate more patterns by varying the base ones
        extended_patterns = []
        for i in range(count):
            base_pattern = patterns[i % len(patterns)]
            extended_patterns.append(base_pattern)

        return extended_patterns

    @staticmethod
    async def benchmark_matching_performance(
        topic_count: int = 100000,
        pattern_count: int = 1000,
        match_iterations: int = 10000,
    ) -> dict[str, Any]:
        """Benchmark topic matching performance."""

        # Setup
        trie = TopicTrie()
        topics = TopicMatchingBenchmark.generate_test_topics(topic_count)
        patterns = TopicMatchingBenchmark.generate_test_patterns(pattern_count)

        # Add patterns to trie
        pattern_start = time.time()
        for i, pattern in enumerate(patterns):
            trie.add_pattern(pattern, f"sub_{i}")
        pattern_time = time.time() - pattern_start

        # Benchmark matching
        match_start = time.time()
        total_matches = 0

        for i in range(match_iterations):
            topic = topics[i % len(topics)]
            matches = trie.match_topic(topic)
            total_matches += len(matches)

        match_time = time.time() - match_start

        # Get stats
        stats = trie.get_statistics()

        return {
            "setup": {
                "topics_generated": topic_count,
                "patterns_added": pattern_count,
                "pattern_setup_time": pattern_time,
                "patterns_per_second": pattern_count / pattern_time,
            },
            "matching": {
                "match_iterations": match_iterations,
                "match_time": match_time,
                "matches_per_second": match_iterations / match_time,
                "average_matches_per_topic": total_matches / match_iterations,
                "total_matches_found": total_matches,
            },
            "trie_stats": stats,
            "memory_efficiency": {
                "topics_per_node": topic_count / stats.total_nodes,
                "cache_efficiency": stats.cache_hit_ratio,
            },
        }
