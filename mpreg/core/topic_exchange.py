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

from mpreg.core.native_codec import estimate_size_bytes
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

def estimate_payload_size_bytes(payload: Any) -> int:
    """Bounded payload size for backlog stats (never nested str/repr)."""
    return estimate_size_bytes(payload)

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
    _message_ids_in_backlog: set[str] = field(default_factory=set)
    _lock: RLock = field(default_factory=RLock)
    _adds_since_cleanup: int = 0

    def __post_init__(self) -> None:
        """Initialize backlogs with correct maxlen after dataclass creation."""
        self.backlogs = defaultdict(lambda: deque(maxlen=self.max_messages_per_topic))

    def add_message(self, message: PubSubMessage) -> None:
        """Add a message to the backlog."""
        with self._lock:
            stored_msg = StoredMessage(
                message=message,
                stored_at=time.time(),
                size_bytes=estimate_payload_size_bytes(message.payload),
            )

            # Add to topic backlog. When maxlen drops an older entry, account for it.
            topic_backlog = self.backlogs[message.topic]
            dropped: StoredMessage | None = None
            if (
                topic_backlog.maxlen is not None
                and len(topic_backlog) >= topic_backlog.maxlen
            ):
                dropped = topic_backlog[0]
            topic_backlog.append(stored_msg)
            if dropped is not None:
                self.total_messages = max(0, self.total_messages - 1)
                self.total_size_bytes = max(
                    0, self.total_size_bytes - dropped.size_bytes
                )
                self._message_ids_in_backlog.discard(dropped.message.message_id)

            # Add to cleanup heap (using message timestamp for proper expiration)
            heapq.heappush(
                self.cleanup_heap,
                (message.timestamp, message.topic, message.message_id),
            )
            self._message_ids_in_backlog.add(message.message_id)

            self.total_messages += 1
            self.total_size_bytes += stored_msg.size_bytes

            # Periodic cleanup (age-based + stale heap entries from maxlen drops)
            self._adds_since_cleanup += 1
            if self._adds_since_cleanup >= 32:
                self._adds_since_cleanup = 0
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
        """Remove expired messages from backlogs and prune stale heap entries."""
        cutoff_time = time.time() - self.max_age_seconds

        # Efficiently remove expired messages using the heap. Heap entries may
        # outlive their messages when deque maxlen drops them; discard those.
        while self.cleanup_heap and self.cleanup_heap[0][0] < cutoff_time:
            _, topic, message_id = heapq.heappop(self.cleanup_heap)

            if message_id not in self._message_ids_in_backlog:
                continue

            # Remove from topic backlog if it exists
            if topic in self.backlogs:
                backlog = self.backlogs[topic]
                # Remove expired messages from the front (deque is ordered by time)
                while backlog and backlog[0].message.timestamp < cutoff_time:
                    removed = backlog.popleft()
                    self.total_messages = max(0, self.total_messages - 1)
                    self.total_size_bytes = max(
                        0, self.total_size_bytes - removed.size_bytes
                    )
                    self._message_ids_in_backlog.discard(removed.message.message_id)

                # Clean up empty backlogs
                if not backlog:
                    del self.backlogs[topic]

        # Cap heap growth from maxlen-evicted entries that are still "fresh".
        if len(self.cleanup_heap) > max(self.total_messages * 4, 1024):
            self.cleanup_heap = [
                entry
                for entry in self.cleanup_heap
                if entry[2] in self._message_ids_in_backlog
            ]
            heapq.heapify(self.cleanup_heap)

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
    # Optional namespace/tenant gate (set by server when discovery_policy_enabled).
    namespace_policy: Any | None = field(default=None, repr=False)

    def attach_namespace_policy(self, engine: Any | None) -> None:
        """Bind namespace/tenant data-plane gate for publish/subscribe."""
        self.namespace_policy = engine

    def _topic_namespace(self, topic_or_pattern: str) -> str:
        prefix = topic_or_pattern
        if "*" in prefix:
            prefix = prefix.split("*", 1)[0]
        if "#" in prefix:
            prefix = prefix.split("#", 1)[0]
        return prefix.rstrip(".")

    def _data_plane_allowed(
        self, namespace: str, *, write: bool
    ) -> tuple[bool, str]:
        engine = self.namespace_policy
        if engine is None or not getattr(engine, "enabled", False):
            return True, "policy_disabled"
        from mpreg.core.namespace_policy import (
            get_actor_cluster_id,
            get_actor_tenant_id,
        )

        decision = engine.allows_data_access(
            namespace,
            actor_cluster=get_actor_cluster_id() or self.cluster_id,
            actor_tenant_id=get_actor_tenant_id(),
            write=write,
        )
        return decision.allowed, decision.reason

    def add_subscription(self, subscription: PubSubSubscription) -> bool:
        """Add a new subscription. Returns False when namespace policy denies."""
        for pattern in subscription.patterns:
            ns = self._topic_namespace(pattern.pattern)
            if not ns:
                continue
            allowed, reason = self._data_plane_allowed(ns, write=False)
            if not allowed:
                return False

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
        return True

    def add_internal_subscription(
        self,
        subscription: PubSubSubscription,
        callback: Callable[[PubSubNotification], Coroutine[Any, Any, None] | None],
    ) -> bool:
        """Add an internal subscription with an in-process callback."""
        if not self.add_subscription(subscription):
            return False
        with self._lock:
            self.internal_callbacks[subscription.subscription_id] = callback
        return True

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

    def has_matching_subscribers(self, topic: str) -> bool:
        """True if any live subscription would receive ``topic``.

        Used to skip expensive payload materialization for control-plane
        topics (discovery deltas) when nothing is watching.
        """
        with self._lock:
            for subscription_id in self.trie.match_topic(topic):
                if subscription_id in self.subscriptions:
                    return True
            return False

    def publish_message(self, message: PubSubMessage) -> list[PubSubNotification]:
        """Publish a message and return notifications for subscribers.

        When a namespace policy is bound and denies the topic namespace, returns
        an empty list without fan-out (caller should treat as denied).
        """
        ns = self._topic_namespace(message.topic)
        if ns:
            allowed, _reason = self._data_plane_allowed(ns, write=True)
            if not allowed:
                return []

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
