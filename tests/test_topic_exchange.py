"""
Comprehensive test suite for MPREG topic exchange system.

Tests all aspects of the AMQP-style pub/sub implementation including:
- High-performance trie-based topic matching
- Message backlog with time-windowed storage
- Topic subscription management
- Gossip protocol integration
- Client API functionality
"""

import time

import pytest

from mpreg.core.model import (
    PubSubMessage,
    PubSubSubscription,
    TopicAdvertisement,
    TopicPattern,
)
from mpreg.core.topic_exchange import (
    MessageBacklog,
    TopicExchange,
    TopicMatchingBenchmark,
    TopicTrie,
)


class TestTopicTrie:
    """Test the high-performance trie-based topic matching engine."""

    def test_exact_match_patterns(self):
        """Test exact topic matching without wildcards."""
        trie = TopicTrie()

        # Add exact match patterns
        trie.add_pattern("user.login", "sub1")
        trie.add_pattern("user.logout", "sub2")
        trie.add_pattern("system.start", "sub3")

        # Test exact matches
        assert trie.match_topic("user.login") == ["sub1"]
        assert trie.match_topic("user.logout") == ["sub2"]
        assert trie.match_topic("system.start") == ["sub3"]

        # Test non-matches
        assert trie.match_topic("user.unknown") == []
        assert trie.match_topic("system.unknown") == []

    def test_single_wildcard_patterns(self):
        """Test single-level wildcard (*) matching."""
        trie = TopicTrie()

        # Add single wildcard patterns
        trie.add_pattern("user.*.login", "sub1")
        trie.add_pattern("*.error", "sub2")
        trie.add_pattern("system.*.status", "sub3")

        # Test single wildcard matches
        assert trie.match_topic("user.123.login") == ["sub1"]
        assert trie.match_topic("user.456.login") == ["sub1"]
        assert trie.match_topic("auth.error") == ["sub2"]
        assert trie.match_topic("payment.error") == ["sub2"]
        assert trie.match_topic("system.web.status") == ["sub3"]

        # Test non-matches (too many/few segments)
        assert trie.match_topic("user.123.profile.login") == []
        assert trie.match_topic("user.login") == []
        assert trie.match_topic("error") == []

    def test_multi_wildcard_patterns(self):
        """Test multi-level wildcard (#) matching."""
        trie = TopicTrie()

        # Add multi wildcard patterns
        trie.add_pattern("user.#", "sub1")
        trie.add_pattern("system.logs.#", "sub2")
        trie.add_pattern("metrics.#", "sub3")

        # Test multi wildcard matches
        assert trie.match_topic("user.123") == ["sub1"]
        assert trie.match_topic("user.123.login") == ["sub1"]
        assert trie.match_topic("user.123.profile.update") == ["sub1"]
        assert trie.match_topic("system.logs.error") == ["sub2"]
        assert trie.match_topic("system.logs.debug.verbose") == ["sub2"]
        assert trie.match_topic("metrics.cpu") == ["sub3"]
        assert trie.match_topic("metrics.memory.usage.high") == ["sub3"]

        # Test non-matches
        assert trie.match_topic("admin.123") == []
        assert trie.match_topic("system.config") == []

    def test_mixed_wildcard_patterns(self):
        """Test patterns with both single and multi wildcards."""
        trie = TopicTrie()

        # Add mixed patterns
        trie.add_pattern("user.*.activity.#", "sub1")
        trie.add_pattern("*.logs.#", "sub2")

        # Test mixed matches
        assert trie.match_topic("user.123.activity.page_view") == ["sub1"]
        assert trie.match_topic("user.456.activity.login.success") == ["sub1"]
        assert trie.match_topic("system.logs.error") == ["sub2"]
        assert trie.match_topic("auth.logs.debug.trace") == ["sub2"]

        # Test non-matches
        assert trie.match_topic("user.activity.page_view") == []
        assert trie.match_topic("user.123.profile.update") == []

    def test_multiple_subscriptions_same_pattern(self):
        """Test multiple subscriptions to the same pattern."""
        trie = TopicTrie()

        # Add multiple subscriptions to same pattern
        trie.add_pattern("user.*.login", "sub1")
        trie.add_pattern("user.*.login", "sub2")
        trie.add_pattern("user.*.login", "sub3")

        # Should return all subscriptions
        matches = trie.match_topic("user.123.login")
        assert len(matches) == 3
        assert set(matches) == {"sub1", "sub2", "sub3"}

    def test_overlapping_patterns(self):
        """Test overlapping patterns that should all match."""
        trie = TopicTrie()

        # Add overlapping patterns
        trie.add_pattern("user.#", "sub1")  # Matches everything under user
        trie.add_pattern("user.*.login", "sub2")  # Matches specific login events
        trie.add_pattern("user.123.#", "sub3")  # Matches everything for user 123

        # Test overlapping matches
        matches = trie.match_topic("user.123.login")
        assert len(matches) == 3
        assert set(matches) == {"sub1", "sub2", "sub3"}

        matches = trie.match_topic("user.456.login")
        assert len(matches) == 2
        assert set(matches) == {"sub1", "sub2"}

    def test_pattern_removal(self):
        """Test removing patterns from the trie."""
        trie = TopicTrie()

        # Add patterns
        trie.add_pattern("user.*.login", "sub1")
        trie.add_pattern("user.*.login", "sub2")
        trie.add_pattern("user.*.logout", "sub3")

        # Verify initial state
        assert set(trie.match_topic("user.123.login")) == {"sub1", "sub2"}
        assert trie.match_topic("user.123.logout") == ["sub3"]

        # Remove one subscription
        trie.remove_pattern("user.*.login", "sub1")
        assert trie.match_topic("user.123.login") == ["sub2"]
        assert trie.match_topic("user.123.logout") == ["sub3"]

        # Remove another subscription
        trie.remove_pattern("user.*.login", "sub2")
        assert trie.match_topic("user.123.login") == []
        assert trie.match_topic("user.123.logout") == ["sub3"]

    def test_performance_stats(self):
        """Test trie performance statistics."""
        trie = TopicTrie()

        # Add some patterns
        trie.add_pattern("user.*.login", "sub1")
        trie.add_pattern("system.#", "sub2")

        # Perform some matches
        trie.match_topic("user.123.login")
        trie.match_topic("user.456.login")
        trie.match_topic("system.start")

        # Check stats
        stats = trie.get_stats()
        assert stats.cache_hits >= 0
        assert stats.cache_misses >= 0
        assert stats.total_nodes > 0

    def test_cache_behavior(self):
        """Test pattern matching cache behavior."""
        trie = TopicTrie()
        trie.add_pattern("user.*.login", "sub1")

        # First match should be cache miss
        matches1 = trie.match_topic("user.123.login")
        initial_misses = trie.cache_misses

        # Second match should be cache hit
        matches2 = trie.match_topic("user.123.login")

        assert matches1 == matches2
        assert trie.cache_hits > 0
        assert trie.cache_misses == initial_misses


class TestMessageBacklog:
    """Test the time-windowed message backlog system."""

    def test_message_storage(self):
        """Test basic message storage and retrieval."""
        backlog = MessageBacklog(max_age_seconds=3600, max_messages_per_topic=100)

        # Create test messages
        msg1 = PubSubMessage(
            topic="user.123.login",
            payload={"username": "alice"},
            timestamp=time.time(),
            message_id="msg1",
            publisher="test_client",
        )

        msg2 = PubSubMessage(
            topic="user.456.login",
            payload={"username": "bob"},
            timestamp=time.time(),
            message_id="msg2",
            publisher="test_client",
        )

        # Store messages
        backlog.add_message(msg1)
        backlog.add_message(msg2)

        # Retrieve messages
        messages = backlog.get_backlog("user.*.login", 3600)
        assert len(messages) == 2
        assert messages[0].message_id in ["msg1", "msg2"]
        assert messages[1].message_id in ["msg1", "msg2"]

    def test_time_windowed_retrieval(self):
        """Test time-windowed message retrieval."""
        backlog = MessageBacklog(max_age_seconds=10, max_messages_per_topic=100)

        # Create messages with different timestamps
        current_time = time.time()

        old_msg = PubSubMessage(
            topic="test.topic",
            payload={"data": "old"},
            timestamp=current_time - 20,  # 20 seconds ago
            message_id="old_msg",
            publisher="test_client",
        )

        recent_msg = PubSubMessage(
            topic="test.topic",
            payload={"data": "recent"},
            timestamp=current_time - 5,  # 5 seconds ago
            message_id="recent_msg",
            publisher="test_client",
        )

        # Store both messages
        backlog.add_message(old_msg)
        backlog.add_message(recent_msg)

        # Retrieve only recent messages (within 10 seconds)
        messages = backlog.get_backlog("test.topic", 10)
        assert len(messages) == 1
        assert messages[0].message_id == "recent_msg"

    def test_pattern_matching_in_backlog(self):
        """Test pattern matching when retrieving backlog."""
        backlog = MessageBacklog()

        # Create messages for different topics
        topics = ["user.123.login", "user.456.login", "system.start", "user.789.logout"]
        for i, topic in enumerate(topics):
            msg = PubSubMessage(
                topic=topic,
                payload={"data": f"message_{i}"},
                timestamp=time.time(),
                message_id=f"msg_{i}",
                publisher="test_client",
            )
            backlog.add_message(msg)

        # Test different pattern matches
        login_messages = backlog.get_backlog("user.*.login", 3600)
        assert len(login_messages) == 2

        user_messages = backlog.get_backlog("user.#", 3600)
        assert len(user_messages) == 3

        all_messages = backlog.get_backlog("#", 3600)
        assert len(all_messages) == 4

    def test_backlog_stats(self):
        """Test backlog statistics tracking."""
        backlog = MessageBacklog()

        # Add some messages
        for i in range(10):
            msg = PubSubMessage(
                topic=f"test.topic.{i}",
                payload={"data": f"message_{i}"},
                timestamp=time.time(),
                message_id=f"msg_{i}",
                publisher="test_client",
            )
            backlog.add_message(msg)

        # Check stats
        stats = backlog.get_stats()
        assert stats.total_messages == 10
        assert stats.total_size_bytes > 0
        assert stats.active_topics == 10

    def test_automatic_cleanup(self):
        """Test automatic cleanup of expired messages."""
        backlog = MessageBacklog(max_age_seconds=1, max_messages_per_topic=100)

        # Add a message
        msg = PubSubMessage(
            topic="test.topic",
            payload={"data": "test"},
            timestamp=time.time() - 2,  # 2 seconds ago
            message_id="expired_msg",
            publisher="test_client",
        )
        backlog.add_message(msg)

        # Trigger cleanup by adding more messages
        for i in range(10):
            new_msg = PubSubMessage(
                topic=f"test.topic.{i}",
                payload={"data": f"new_{i}"},
                timestamp=time.time(),
                message_id=f"new_msg_{i}",
                publisher="test_client",
            )
            backlog.add_message(new_msg)

        # Check that expired message is cleaned up
        messages = backlog.get_backlog("test.topic", 5)
        message_ids = [msg.message_id for msg in messages]
        assert "expired_msg" not in message_ids


class TestTopicExchange:
    """Test the main topic exchange engine."""

    def test_subscription_management(self):
        """Test adding and removing subscriptions."""
        exchange = TopicExchange("ws://localhost:9001", "test_cluster")

        # Create a subscription
        subscription = PubSubSubscription(
            subscription_id="sub1",
            patterns=(
                TopicPattern(pattern="user.*.login", exact_match=False),
                TopicPattern(pattern="user.*.logout", exact_match=False),
            ),
            subscriber="test_client",
            created_at=time.time(),
            get_backlog=False,
        )

        # Add subscription
        exchange.add_subscription(subscription)
        assert "sub1" in exchange.subscriptions
        assert len(exchange.subscriptions) == 1

        # Remove subscription
        assert exchange.remove_subscription("sub1") is True
        assert "sub1" not in exchange.subscriptions
        assert len(exchange.subscriptions) == 0

        # Try to remove non-existent subscription
        assert exchange.remove_subscription("nonexistent") is False

    def test_message_publishing(self):
        """Test message publishing and notification generation."""
        exchange = TopicExchange("ws://localhost:9001", "test_cluster")

        # Add a subscription
        subscription = PubSubSubscription(
            subscription_id="sub1",
            patterns=(TopicPattern(pattern="user.*.login", exact_match=False),),
            subscriber="test_client",
            created_at=time.time(),
            get_backlog=False,
        )
        exchange.add_subscription(subscription)

        # Publish a matching message
        message = PubSubMessage(
            topic="user.123.login",
            payload={"username": "alice"},
            timestamp=time.time(),
            message_id="msg1",
            publisher="test_publisher",
        )

        notifications = exchange.publish_message(message)
        assert len(notifications) == 1
        assert notifications[0].subscription_id == "sub1"
        assert notifications[0].message.message_id == "msg1"

        # Publish a non-matching message
        non_matching_message = PubSubMessage(
            topic="system.start",
            payload={"status": "started"},
            timestamp=time.time(),
            message_id="msg2",
            publisher="test_publisher",
        )

        notifications = exchange.publish_message(non_matching_message)
        assert len(notifications) == 0

    def test_multiple_subscriptions_same_topic(self):
        """Test multiple subscriptions matching the same topic."""
        exchange = TopicExchange("ws://localhost:9001", "test_cluster")

        # Add multiple subscriptions
        for i in range(3):
            subscription = PubSubSubscription(
                subscription_id=f"sub{i}",
                patterns=(TopicPattern(pattern="user.*.login", exact_match=False),),
                subscriber=f"client{i}",
                created_at=time.time(),
                get_backlog=False,
            )
            exchange.add_subscription(subscription)

        # Publish a message
        message = PubSubMessage(
            topic="user.123.login",
            payload={"username": "alice"},
            timestamp=time.time(),
            message_id="msg1",
            publisher="test_publisher",
        )

        notifications = exchange.publish_message(message)
        assert len(notifications) == 3

        subscription_ids = [n.subscription_id for n in notifications]
        assert set(subscription_ids) == {"sub0", "sub1", "sub2"}

    def test_topic_advertisement(self):
        """Test topic advertisement generation for gossip protocol."""
        exchange = TopicExchange("ws://localhost:9001", "test_cluster")

        # Add subscriptions
        patterns = ["user.*.login", "system.#", "metrics.cpu.*"]
        for i, pattern in enumerate(patterns):
            subscription = PubSubSubscription(
                subscription_id=f"sub{i}",
                patterns=(TopicPattern(pattern=pattern, exact_match=False),),
                subscriber=f"client{i}",
                created_at=time.time(),
                get_backlog=False,
            )
            exchange.add_subscription(subscription)

        # Get advertisement
        ad = exchange.get_topic_advertisement()
        assert ad.server_url == "ws://localhost:9001"
        assert len(ad.topics) == 3
        assert set(ad.topics) == set(patterns)
        assert ad.subscriber_count == 3

    def test_remote_topic_updates(self):
        """Test updating knowledge of remote topic servers."""
        exchange = TopicExchange("ws://localhost:9001", "test_cluster")

        # Create remote advertisements
        advertisements = [
            TopicAdvertisement(
                server_url="ws://remote1:9001",
                topics=("user.*.login", "system.#"),
                subscriber_count=5,
                last_activity=time.time(),
            ),
            TopicAdvertisement(
                server_url="ws://remote2:9001",
                topics=("metrics.#", "logs.error"),
                subscriber_count=3,
                last_activity=time.time(),
            ),
        ]

        # Update remote topics
        exchange.update_remote_topics(advertisements)

        # Check that remote servers are tracked
        assert len(exchange.remote_topic_servers) == 4  # 4 unique patterns
        assert "ws://remote1:9001" in exchange.remote_topic_servers["user.*.login"]
        assert "ws://remote2:9001" in exchange.remote_topic_servers["metrics.#"]

    def test_exchange_statistics(self):
        """Test comprehensive exchange statistics."""
        exchange = TopicExchange("ws://localhost:9001", "test_cluster")

        # Add subscription and publish messages
        subscription = PubSubSubscription(
            subscription_id="sub1",
            patterns=(TopicPattern(pattern="test.#", exact_match=False),),
            subscriber="test_client",
            created_at=time.time(),
            get_backlog=False,
        )
        exchange.add_subscription(subscription)

        # Publish messages
        for i in range(5):
            message = PubSubMessage(
                topic=f"test.topic.{i}",
                payload={"data": f"message_{i}"},
                timestamp=time.time(),
                message_id=f"msg_{i}",
                publisher="test_publisher",
            )
            exchange.publish_message(message)

        # Get stats
        stats = exchange.get_stats()
        assert stats.active_subscriptions == 1
        assert stats.active_subscribers == 1
        assert stats.messages_published == 5
        assert stats.messages_delivered == 5
        assert stats.delivery_ratio == 1.0
        assert stats.trie_stats is not None
        assert stats.backlog_stats is not None


class TestTopicMatchingBenchmark:
    """Test topic matching performance benchmarks."""

    def test_topic_generation(self):
        """Test realistic topic generation."""
        topics = TopicMatchingBenchmark.generate_test_topics(100)
        assert len(topics) == 100

        # Check topic structure
        for topic in topics:
            parts = topic.split(".")
            assert len(parts) == 4
            assert parts[0] in [
                "user",
                "order",
                "payment",
                "inventory",
                "analytics",
                "system",
            ]
            assert parts[1].isdigit()
            assert parts[2] in [
                "create",
                "update",
                "delete",
                "view",
                "process",
                "validate",
            ]
            assert parts[3] in [
                "success",
                "failed",
                "pending",
                "completed",
                "cancelled",
            ]

    def test_pattern_generation(self):
        """Test realistic pattern generation."""
        patterns = TopicMatchingBenchmark.generate_test_patterns(20)
        assert len(patterns) == 20

        # Check that patterns contain wildcards
        wildcard_patterns = [p for p in patterns if "*" in p or "#" in p]
        assert len(wildcard_patterns) > 0

    @pytest.mark.asyncio
    async def test_performance_benchmark(self):
        """Test the performance benchmarking functionality."""
        # Run a small benchmark
        results = await TopicMatchingBenchmark.benchmark_matching_performance(
            topic_count=1000, pattern_count=50, match_iterations=1000
        )

        # Check benchmark results structure
        assert "setup" in results
        assert "matching" in results
        assert "trie_stats" in results
        assert "memory_efficiency" in results

        # Check that performance metrics are reasonable
        assert results["setup"]["topics_generated"] == 1000
        assert results["setup"]["patterns_added"] == 50
        assert results["matching"]["match_iterations"] == 1000
        assert results["matching"]["matches_per_second"] > 0
        assert results["trie_stats"].total_nodes > 0


class TestIntegrationScenarios:
    """Integration tests for realistic pub/sub scenarios."""

    def test_ecommerce_event_routing(self):
        """Test e-commerce event routing scenario."""
        exchange = TopicExchange("ws://localhost:9001", "ecommerce_cluster")

        # Set up subscriptions for different services
        subscriptions = [
            # Analytics service
            PubSubSubscription(
                subscription_id="analytics_sub",
                patterns=(TopicPattern(pattern="order.#", exact_match=False),),
                subscriber="analytics_service",
                created_at=time.time(),
                get_backlog=False,
            ),
            # Inventory service
            PubSubSubscription(
                subscription_id="inventory_sub",
                patterns=(
                    TopicPattern(pattern="order.*.created", exact_match=False),
                    TopicPattern(pattern="order.*.cancelled", exact_match=False),
                ),
                subscriber="inventory_service",
                created_at=time.time(),
                get_backlog=False,
            ),
            # Email service
            PubSubSubscription(
                subscription_id="email_sub",
                patterns=(
                    TopicPattern(pattern="order.*.completed", exact_match=False),
                    TopicPattern(pattern="user.*.registered", exact_match=False),
                ),
                subscriber="email_service",
                created_at=time.time(),
                get_backlog=False,
            ),
        ]

        # Add all subscriptions
        for sub in subscriptions:
            exchange.add_subscription(sub)

        # Simulate e-commerce events
        events = [
            (
                "order.123.created",
                {"order_id": "123", "user_id": "456", "amount": 99.99},
            ),
            ("order.123.completed", {"order_id": "123", "status": "shipped"}),
            ("order.456.cancelled", {"order_id": "456", "reason": "out_of_stock"}),
            ("user.789.registered", {"user_id": "789", "email": "user@example.com"}),
            ("payment.123.processed", {"payment_id": "pay_123", "amount": 99.99}),
        ]

        # Track notifications by service
        service_notifications: dict[str, int] = {
            "analytics_service": 0,
            "inventory_service": 0,
            "email_service": 0,
        }

        # Publish events and count notifications
        for topic, payload in events:
            message = PubSubMessage(
                topic=topic,
                payload=payload,
                timestamp=time.time(),
                message_id=f"msg_{topic}",
                publisher="ecommerce_app",
            )

            notifications = exchange.publish_message(message)
            for notification in notifications:
                subscriber = exchange.subscriptions[
                    notification.subscription_id
                ].subscriber
                service_notifications[subscriber] += 1

        # Verify correct routing
        assert service_notifications["analytics_service"] == 3  # All order events
        assert service_notifications["inventory_service"] == 2  # Created and cancelled
        assert service_notifications["email_service"] == 2  # Completed and registered

    def test_iot_sensor_data_routing(self):
        """Test IoT sensor data routing scenario."""
        exchange = TopicExchange("ws://localhost:9001", "iot_cluster")

        # Set up subscriptions for different monitoring services
        subscriptions = [
            # Temperature monitoring
            PubSubSubscription(
                subscription_id="temp_monitor",
                patterns=(
                    TopicPattern(pattern="sensor.*.temperature", exact_match=False),
                ),
                subscriber="temperature_service",
                created_at=time.time(),
                get_backlog=False,
            ),
            # Critical alerts
            PubSubSubscription(
                subscription_id="alert_monitor",
                patterns=(
                    TopicPattern(pattern="sensor.*.critical", exact_match=False),
                ),
                subscriber="alert_service",
                created_at=time.time(),
                get_backlog=False,
            ),
            # All sensor data for analytics
            PubSubSubscription(
                subscription_id="analytics_monitor",
                patterns=(TopicPattern(pattern="sensor.#", exact_match=False),),
                subscriber="analytics_service",
                created_at=time.time(),
                get_backlog=False,
            ),
            # Specific room monitoring
            PubSubSubscription(
                subscription_id="room_a_monitor",
                patterns=(TopicPattern(pattern="sensor.room_a.#", exact_match=False),),
                subscriber="room_a_service",
                created_at=time.time(),
                get_backlog=False,
            ),
        ]

        # Add all subscriptions
        for sub in subscriptions:
            exchange.add_subscription(sub)

        # Simulate IoT sensor events
        sensor_events = [
            ("sensor.room_a.temperature", {"value": 22.5, "unit": "celsius"}),
            ("sensor.room_b.temperature", {"value": 24.1, "unit": "celsius"}),
            ("sensor.room_a.humidity", {"value": 65, "unit": "percent"}),
            ("sensor.room_a.critical", {"type": "fire_alarm", "severity": "high"}),
            ("sensor.room_c.motion", {"detected": True, "timestamp": time.time()}),
        ]

        # Track notifications by service
        service_notifications: dict[str, int] = {}

        # Publish sensor events
        for topic, payload in sensor_events:
            message = PubSubMessage(
                topic=topic,
                payload=payload,
                timestamp=time.time(),
                message_id=f"msg_{topic}",
                publisher="iot_gateway",
            )

            notifications = exchange.publish_message(message)
            for notification in notifications:
                subscriber = exchange.subscriptions[
                    notification.subscription_id
                ].subscriber
                service_notifications[subscriber] = (
                    service_notifications.get(subscriber, 0) + 1
                )

        # Verify correct routing
        assert (
            service_notifications["temperature_service"] == 2
        )  # 2 temperature readings
        assert service_notifications["alert_service"] == 1  # 1 critical alert
        assert service_notifications["analytics_service"] == 5  # All sensor data
        assert service_notifications["room_a_service"] == 3  # 3 room_a events

    def test_high_frequency_trading_scenario(self):
        """Test high-frequency trading event routing."""
        exchange = TopicExchange("ws://localhost:9001", "trading_cluster")

        # Set up trading subscriptions
        subscriptions = [
            # Bitcoin price alerts
            PubSubSubscription(
                subscription_id="btc_alerts",
                patterns=(
                    TopicPattern(pattern="market.crypto.btc.price", exact_match=True),
                ),
                subscriber="btc_trader",
                created_at=time.time(),
                get_backlog=False,
            ),
            # All crypto market data
            PubSubSubscription(
                subscription_id="crypto_analytics",
                patterns=(TopicPattern(pattern="market.crypto.#", exact_match=False),),
                subscriber="crypto_analytics",
                created_at=time.time(),
                get_backlog=False,
            ),
            # Risk management for all markets
            PubSubSubscription(
                subscription_id="risk_mgmt",
                patterns=(TopicPattern(pattern="market.#", exact_match=False),),
                subscriber="risk_management",
                created_at=time.time(),
                get_backlog=False,
            ),
            # Specific stock alerts
            PubSubSubscription(
                subscription_id="aapl_alerts",
                patterns=(
                    TopicPattern(pattern="market.stocks.aapl.#", exact_match=False),
                ),
                subscriber="aapl_trader",
                created_at=time.time(),
                get_backlog=False,
            ),
        ]

        # Add all subscriptions
        for sub in subscriptions:
            exchange.add_subscription(sub)

        # Simulate high-frequency market events
        market_events = [
            ("market.crypto.btc.price", {"price": 45000.50, "volume": 1.2}),
            ("market.crypto.eth.price", {"price": 3200.75, "volume": 5.8}),
            ("market.stocks.aapl.price", {"price": 150.25, "volume": 1000}),
            ("market.stocks.aapl.volume", {"volume": 2500, "avg_price": 150.30}),
            ("market.forex.usd_eur.rate", {"rate": 0.85, "trend": "up"}),
            ("market.crypto.btc.volume", {"volume": 15.5, "avg_price": 45001.20}),
        ]

        # Track notifications by service
        service_notifications: dict[str, int] = {}

        # Publish market events
        for topic, payload in market_events:
            message = PubSubMessage(
                topic=topic,
                payload=payload,
                timestamp=time.time(),
                message_id=f"msg_{topic}",
                publisher="market_data_feed",
            )

            notifications = exchange.publish_message(message)
            for notification in notifications:
                subscriber = exchange.subscriptions[
                    notification.subscription_id
                ].subscriber
                service_notifications[subscriber] = (
                    service_notifications.get(subscriber, 0) + 1
                )

        # Verify correct routing
        assert service_notifications["btc_trader"] == 1  # Only BTC price
        assert service_notifications["crypto_analytics"] == 3  # All crypto events
        assert service_notifications["risk_management"] == 6  # All market events
        assert service_notifications["aapl_trader"] == 2  # AAPL price and volume
