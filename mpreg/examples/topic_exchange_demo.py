#!/usr/bin/env python3
"""
MPREG Topic Exchange Demo

This script demonstrates the new AMQP-style topic exchange system for MPREG,
showing how to use hierarchical topic patterns, wildcard matching, and
message backlogs in a distributed environment.

Usage:
    poetry run python mpreg/examples/topic_exchange_demo.py
"""

import asyncio
import sys
import time
from dataclasses import dataclass, field
from typing import Any

# Add the parent directory to Python path for imports
sys.path.insert(0, ".")

from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.core.topic_exchange import TopicExchange


@dataclass(slots=True)
class TopicExchangeDemo:
    """Comprehensive demo of the MPREG topic exchange system."""

    exchange: TopicExchange = field(
        default_factory=lambda: TopicExchange("ws://localhost:9001", "demo_cluster")
    )
    message_count: int = 0

    async def run_demo(self):
        """Run the complete topic exchange demonstration."""
        print("🚀 MPREG Topic Exchange System Demo")
        print("=" * 50)

        # Demo 1: Basic topic matching
        await self.demo_basic_topic_matching()

        # Demo 2: Wildcard patterns
        await self.demo_wildcard_patterns()

        # Demo 3: Real-world e-commerce scenario
        await self.demo_ecommerce_scenario()

        # Demo 4: IoT sensor data routing
        await self.demo_iot_sensor_routing()

        # Demo 5: Message backlog functionality
        await self.demo_message_backlog()

        # Demo 6: Performance characteristics
        await self.demo_performance_characteristics()

        print("\n🎉 Demo completed successfully!")
        print("The MPREG topic exchange system provides:")
        print("  ✅ High-performance pattern matching with trie-based routing")
        print("  ✅ AMQP-style wildcards (* and #) for flexible subscriptions")
        print("  ✅ Message backlog for new subscribers")
        print("  ✅ Cluster-wide gossip integration for distributed routing")
        print("  ✅ Sub-millisecond latency for topic matching")

    async def demo_basic_topic_matching(self):
        """Demonstrate basic topic matching capabilities."""
        print("\n📊 Demo 1: Basic Topic Matching")
        print("-" * 30)

        # Create subscriptions
        subscriptions = [
            ("user_events", ["user.login", "user.logout"]),
            ("system_events", ["system.start", "system.stop"]),
            ("order_events", ["order.created", "order.completed"]),
        ]

        for sub_id, patterns in subscriptions:
            subscription = PubSubSubscription(
                subscription_id=sub_id,
                patterns=tuple(
                    TopicPattern(pattern=p, exact_match=True) for p in patterns
                ),
                subscriber=f"service_{sub_id}",
                created_at=time.time(),
                get_backlog=False,
            )
            self.exchange.add_subscription(subscription)
            print(f"  📝 Created subscription '{sub_id}' for patterns: {patterns}")

        # Publish messages
        test_messages: list[tuple[str, dict[str, Any]]] = [
            ("user.login", {"username": "alice", "ip": "192.168.1.100"}),
            ("user.logout", {"username": "alice", "duration": 3600}),
            ("system.start", {"service": "web-server", "port": 8080}),
            ("order.created", {"order_id": "12345", "amount": 99.99}),
            ("unknown.event", {"data": "should not match any subscription"}),
        ]

        print("\n  📨 Publishing messages:")
        for topic, payload in test_messages:
            message = self.create_message(topic, payload)
            notifications = self.exchange.publish_message(message)

            print(f"    Topic: {topic}")
            print(f"    Subscribers notified: {len(notifications)}")
            for notification in notifications:
                sub_id = notification.subscription_id
                print(f"      → {sub_id}")

        # Show statistics
        stats = self.exchange.get_stats()
        print(
            f"\n  📊 Statistics: {stats.messages_published} published, {stats.messages_delivered} delivered"
        )

    async def demo_wildcard_patterns(self):
        """Demonstrate wildcard pattern matching."""
        print("\n🎯 Demo 2: Wildcard Pattern Matching")
        print("-" * 35)

        # Clear previous subscriptions
        for sub_id in list(self.exchange.subscriptions.keys()):
            self.exchange.remove_subscription(sub_id)

        # Create wildcard subscriptions
        wildcard_subscriptions = [
            ("single_wildcard", ["user.*.login", "order.*.created"]),
            ("multi_wildcard", ["system.#", "metrics.#"]),
            ("mixed_wildcards", ["user.*.activity.#", "order.*.items.#"]),
        ]

        for sub_id, patterns in wildcard_subscriptions:
            subscription = PubSubSubscription(
                subscription_id=sub_id,
                patterns=tuple(
                    TopicPattern(pattern=p, exact_match=False) for p in patterns
                ),
                subscriber=f"service_{sub_id}",
                created_at=time.time(),
                get_backlog=False,
            )
            self.exchange.add_subscription(subscription)
            print(f"  📝 Created wildcard subscription '{sub_id}' for: {patterns}")

        # Test wildcard matching
        wildcard_test_messages: list[tuple[str, dict[str, Any]]] = [
            ("user.123.login", {"username": "alice"}),
            ("user.456.login", {"username": "bob"}),
            ("order.789.created", {"amount": 149.99}),
            ("system.web.started", {"port": 8080}),
            ("system.database.connected", {"host": "db.example.com"}),
            ("metrics.cpu.usage", {"value": 85.3}),
            ("metrics.memory.available", {"value": "2.1GB"}),
            ("user.123.activity.page_view", {"page": "/dashboard"}),
            ("user.456.activity.button_click", {"button": "checkout"}),
            ("order.789.items.added", {"item": "laptop", "quantity": 1}),
        ]

        print("\n  🎯 Testing wildcard matching:")
        for topic, payload in wildcard_test_messages:
            message = self.create_message(topic, payload)
            notifications = self.exchange.publish_message(message)

            print(f"    Topic: {topic}")
            print(f"    Matched subscriptions: {len(notifications)}")
            for notification in notifications:
                sub_id = notification.subscription_id
                print(f"      → {sub_id}")

        # Show trie performance
        trie_stats = self.exchange.get_stats().trie_stats
        print(
            f"\n  ⚡ Trie performance: {trie_stats.cache_hit_ratio:.1%} cache hit ratio"
        )

    async def demo_ecommerce_scenario(self):
        """Demonstrate a realistic e-commerce event routing scenario."""
        print("\n🛒 Demo 3: E-commerce Event Routing")
        print("-" * 35)

        # Clear previous subscriptions
        for sub_id in list(self.exchange.subscriptions.keys()):
            self.exchange.remove_subscription(sub_id)

        # Create service subscriptions
        ecommerce_services = [
            ("analytics_service", ["order.#", "user.#", "product.#"]),
            (
                "inventory_service",
                ["order.*.created", "order.*.cancelled", "product.*.stock_change"],
            ),
            (
                "email_service",
                ["user.*.registered", "order.*.completed", "order.*.shipped"],
            ),
            ("fraud_detection", ["payment.#", "user.*.login_failed"]),
            (
                "recommendation_engine",
                ["user.*.viewed", "user.*.purchased", "product.*.viewed"],
            ),
        ]

        for service_id, patterns in ecommerce_services:
            subscription = PubSubSubscription(
                subscription_id=service_id,
                patterns=tuple(
                    TopicPattern(pattern=p, exact_match=False) for p in patterns
                ),
                subscriber=service_id,
                created_at=time.time(),
                get_backlog=False,
            )
            self.exchange.add_subscription(subscription)
            print(f"  🏪 {service_id}: {patterns}")

        # Simulate e-commerce events
        ecommerce_events: list[tuple[str, dict[str, Any]]] = [
            (
                "user.alice.registered",
                {"email": "alice@example.com", "signup_method": "google"},
            ),
            (
                "user.alice.viewed",
                {"product_id": "laptop-123", "category": "electronics"},
            ),
            ("product.laptop-123.viewed", {"user_id": "alice", "view_duration": 45}),
            (
                "order.ord-789.created",
                {"user_id": "alice", "items": ["laptop-123"], "total": 1299.99},
            ),
            (
                "payment.pay-456.processing",
                {"order_id": "ord-789", "method": "credit_card"},
            ),
            ("payment.pay-456.completed", {"order_id": "ord-789", "amount": 1299.99}),
            ("inventory.laptop-123.stock_change", {"old_stock": 15, "new_stock": 14}),
            (
                "order.ord-789.completed",
                {"order_id": "ord-789", "status": "processing"},
            ),
            (
                "order.ord-789.shipped",
                {"order_id": "ord-789", "tracking": "1Z999AA1234567890"},
            ),
        ]

        print("\n  📦 E-commerce event flow:")
        service_activity: dict[str, int] = {}

        for topic, payload in ecommerce_events:
            message = self.create_message(topic, payload)
            notifications = self.exchange.publish_message(message)

            print(f"    📨 {topic}")
            for notification in notifications:
                service = notification.subscription_id
                service_activity[service] = service_activity.get(service, 0) + 1
                print(f"      → {service}")

        print("\n  📊 Service activity summary:")
        for service, count in sorted(service_activity.items()):
            print(f"    {service}: {count} events processed")

    async def demo_iot_sensor_routing(self):
        """Demonstrate IoT sensor data routing."""
        print("\n🌡️  Demo 4: IoT Sensor Data Routing")
        print("-" * 35)

        # Clear previous subscriptions
        for sub_id in list(self.exchange.subscriptions.keys()):
            self.exchange.remove_subscription(sub_id)

        # Create IoT monitoring subscriptions
        iot_services = [
            ("temperature_monitor", ["sensor.*.temperature", "hvac.*.temperature_set"]),
            ("security_system", ["sensor.*.motion", "sensor.*.door_open", "alarm.#"]),
            (
                "energy_management",
                ["sensor.*.power_usage", "device.*.power_on", "device.*.power_off"],
            ),
            (
                "predictive_maintenance",
                ["sensor.*.vibration", "sensor.*.pressure", "device.*.error"],
            ),
            ("data_analytics", ["sensor.#"]),  # Collect all sensor data
            ("building_automation", ["hvac.#", "lighting.#", "security.#"]),
        ]

        for service_id, patterns in iot_services:
            subscription = PubSubSubscription(
                subscription_id=service_id,
                patterns=tuple(
                    TopicPattern(pattern=p, exact_match=False) for p in patterns
                ),
                subscriber=service_id,
                created_at=time.time(),
                get_backlog=False,
            )
            self.exchange.add_subscription(subscription)
            print(f"  🏭 {service_id}: monitoring {len(patterns)} pattern(s)")

        # Simulate IoT sensor events
        iot_events: list[tuple[str, dict[str, Any]]] = [
            (
                "sensor.room_a.temperature",
                {"value": 22.5, "unit": "celsius", "device_id": "temp_001"},
            ),
            (
                "sensor.room_b.temperature",
                {"value": 24.1, "unit": "celsius", "device_id": "temp_002"},
            ),
            (
                "sensor.lobby.motion",
                {"detected": True, "confidence": 0.95, "device_id": "motion_001"},
            ),
            (
                "sensor.entrance.door_open",
                {"opened": True, "duration": 5, "device_id": "door_001"},
            ),
            (
                "sensor.server_room.power_usage",
                {"watts": 1250, "device_id": "power_001"},
            ),
            ("device.hvac_unit_1.power_on", {"mode": "cooling", "target_temp": 21}),
            ("hvac.zone_1.temperature_set", {"target": 21, "current": 22.5}),
            (
                "sensor.pump_a.vibration",
                {"frequency": 60, "amplitude": 0.02, "device_id": "vib_001"},
            ),
            ("alarm.fire.triggered", {"location": "kitchen", "severity": "high"}),
            ("lighting.room_a.brightness_set", {"level": 75, "auto_adjust": True}),
        ]

        print("\n  📡 IoT sensor event processing:")
        service_notifications: dict[str, int] = {}

        for topic, payload in iot_events:
            message = self.create_message(topic, payload)
            notifications = self.exchange.publish_message(message)

            print(f"    📊 {topic}: {len(notifications)} services notified")
            for notification in notifications:
                service = notification.subscription_id
                service_notifications[service] = (
                    service_notifications.get(service, 0) + 1
                )

        print("\n  🎯 Service notification summary:")
        for service, count in sorted(service_notifications.items()):
            print(f"    {service}: {count} notifications")

    async def demo_message_backlog(self):
        """Demonstrate message backlog functionality."""
        print("\n📚 Demo 5: Message Backlog")
        print("-" * 25)

        # Clear previous subscriptions
        for sub_id in list(self.exchange.subscriptions.keys()):
            self.exchange.remove_subscription(sub_id)

        # Publish some messages before creating subscriptions
        historical_messages = [
            ("metrics.cpu.usage", {"value": 45.2, "timestamp": time.time() - 300}),
            ("metrics.memory.usage", {"value": 67.8, "timestamp": time.time() - 240}),
            ("metrics.disk.usage", {"value": 89.1, "timestamp": time.time() - 180}),
            (
                "metrics.network.throughput",
                {"value": 125.5, "timestamp": time.time() - 120},
            ),
            ("metrics.cpu.temperature", {"value": 65.4, "timestamp": time.time() - 60}),
        ]

        print("  📝 Publishing historical messages...")
        for topic, payload in historical_messages:
            message = self.create_message(topic, payload)
            self.exchange.publish_message(message)
            print(f"    📊 {topic}")

        # Create subscription that requests backlog
        backlog_subscription = PubSubSubscription(
            subscription_id="metrics_analyzer",
            patterns=(TopicPattern(pattern="metrics.#", exact_match=False),),
            subscriber="metrics_service",
            created_at=time.time(),
            get_backlog=True,
            backlog_seconds=600,  # 10 minutes
        )

        print("\n  🔄 Creating subscription with backlog request...")
        self.exchange.add_subscription(backlog_subscription)

        # Simulate backlog delivery (in real implementation, this would be sent to the client)
        backlog_messages = self.exchange.backlog.get_backlog("metrics.#", 600)
        print(f"    📚 Backlog contains {len(backlog_messages)} messages")

        for message in backlog_messages:
            age = time.time() - message.timestamp
            print(f"      📊 {message.topic} (age: {age:.0f}s)")

        # Show backlog statistics
        backlog_stats = self.exchange.backlog.get_stats()
        print("\n  📊 Backlog statistics:")
        print(f"    Total messages: {backlog_stats.total_messages}")
        print(f"    Total size: {backlog_stats.total_size_mb:.2f} MB")
        print(f"    Active topics: {backlog_stats.active_topics}")

    async def demo_performance_characteristics(self):
        """Demonstrate performance characteristics."""
        print("\n⚡ Demo 6: Performance Characteristics")
        print("-" * 40)

        # Clear previous subscriptions
        for sub_id in list(self.exchange.subscriptions.keys()):
            self.exchange.remove_subscription(sub_id)

        # Create many subscriptions for performance testing
        print("  📝 Creating 100 subscriptions with various patterns...")

        pattern_templates = [
            "user.*.login",
            "user.*.logout",
            "order.*.created",
            "order.*.completed",
            "payment.*.processed",
            "inventory.*.updated",
            "system.*.started",
            "metrics.cpu.#",
            "metrics.memory.#",
            "metrics.disk.#",
            "logs.#",
        ]

        for i in range(100):
            pattern = pattern_templates[i % len(pattern_templates)]
            subscription = PubSubSubscription(
                subscription_id=f"perf_sub_{i}",
                patterns=(TopicPattern(pattern=pattern, exact_match=False),),
                subscriber=f"perf_client_{i}",
                created_at=time.time(),
                get_backlog=False,
            )
            self.exchange.add_subscription(subscription)

        # Generate test topics
        test_topics = [
            "user.123.login",
            "user.456.logout",
            "order.789.created",
            "order.101.completed",
            "payment.202.processed",
            "inventory.303.updated",
            "system.web.started",
            "metrics.cpu.usage",
            "metrics.memory.available",
            "metrics.disk.free",
            "logs.error.critical",
        ]

        # Performance test: publish many messages
        print("  🚀 Publishing 1000 messages for performance testing...")

        start_time = time.time()
        total_notifications = 0

        for i in range(1000):
            topic = test_topics[i % len(test_topics)]
            message = self.create_message(
                topic, {"iteration": i, "timestamp": time.time()}
            )
            notifications = self.exchange.publish_message(message)
            total_notifications += len(notifications)

        end_time = time.time()
        duration = end_time - start_time

        print("  📊 Performance results:")
        print("    Messages published: 1000")
        print(f"    Total notifications: {total_notifications}")
        print(f"    Duration: {duration:.3f} seconds")
        print(f"    Messages/second: {1000 / duration:.0f}")
        print(f"    Notifications/second: {total_notifications / duration:.0f}")
        print(
            f"    Average fan-out: {total_notifications / 1000:.1f} notifications per message"
        )

        # Show trie statistics
        trie_stats = self.exchange.get_stats().trie_stats
        print("  🧠 Trie performance:")
        print(f"    Cache hit ratio: {trie_stats.cache_hit_ratio:.1%}")
        print(f"    Total nodes: {trie_stats.total_nodes}")
        print(f"    Cached patterns: {trie_stats.cached_patterns}")

        # Show overall exchange statistics
        exchange_stats = self.exchange.get_stats()
        print("  🎯 Exchange statistics:")
        print(f"    Active subscriptions: {exchange_stats.active_subscriptions}")
        print(f"    Messages published: {exchange_stats.messages_published}")
        print(f"    Messages delivered: {exchange_stats.messages_delivered}")
        print(f"    Delivery ratio: {exchange_stats.delivery_ratio:.2f}")

    def create_message(self, topic: str, payload: dict[str, Any]) -> PubSubMessage:
        """Create a test message."""
        self.message_count += 1
        return PubSubMessage(
            topic=topic,
            payload=payload,
            timestamp=time.time(),
            message_id=f"msg_{self.message_count}",
            publisher="demo_client",
        )


def main():
    """Run the topic exchange demo."""
    demo = TopicExchangeDemo()
    asyncio.run(demo.run_demo())


if __name__ == "__main__":
    main()
