#!/usr/bin/env python3
"""
MPREG Federated Pub/Sub Demo

This script demonstrates the federated topic exchange system with:
- Multiple clusters in different regions
- Cross-cluster message routing
- Bloom filter-based subscription aggregation
- Planet-scale federation without N² gossip overhead

Usage:
    poetry run python mpreg/examples/federation_demo.py
"""

import asyncio
import sys
import time
from dataclasses import dataclass, field

# Add the parent directory to Python path for imports
sys.path.insert(0, ".")

from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.federation import ClusterIdentity
from mpreg.federation.federated_topic_exchange import (
    FederatedTopicExchange,
    connect_clusters,
    create_federated_cluster,
)


@dataclass(slots=True)
class FederationDemo:
    """Comprehensive demo of the MPREG federated pub/sub system."""

    clusters: dict[str, FederatedTopicExchange] = field(default_factory=dict)
    message_count: int = 0

    async def run_demo(self):
        """Run the complete federation demonstration."""
        print("🌍 MPREG Federated Pub/Sub System Demo")
        print("=" * 60)
        print("🚀 Demonstrating planet-scale federation without N² gossip overhead")
        print()

        # Demo 1: Create federated clusters
        await self.demo_create_federated_clusters()

        # Demo 2: Connect clusters via bridges
        await self.demo_connect_clusters()

        # Demo 3: Cross-region subscription routing
        await self.demo_cross_region_subscriptions()

        # Demo 4: Global message broadcasting
        await self.demo_global_message_broadcasting()

        # Demo 5: Bloom filter efficiency
        await self.demo_bloom_filter_efficiency()

        # Demo 6: Federation health monitoring
        await self.demo_federation_health()

        # Demo 7: Cluster failure handling
        await self.demo_cluster_failure_handling()

        print("\n🎉 Federation demo completed successfully!")
        print("The MPREG federated pub/sub system provides:")
        print("  ✅ Planet-scale federation with minimal overhead")
        print("  ✅ Smart bloom filter-based subscription routing")
        print("  ✅ Automatic cluster discovery and synchronization")
        print("  ✅ Graceful handling of cluster failures")
        print("  ✅ Local performance with global reach")

    async def demo_create_federated_clusters(self):
        """Demonstrate creating federated clusters in different regions."""
        print("🏗️  Demo 1: Creating Federated Clusters")
        print("-" * 45)

        # Define cluster configurations
        cluster_configs = [
            {
                "cluster_id": "us-west-1",
                "cluster_name": "US West Coast",
                "region": "us-west",
                "server_url": "ws://us-west-1.mpreg.example.com:9001",
                "bridge_url": "ws://bridge-us-west-1.mpreg.example.com:9002",
            },
            {
                "cluster_id": "eu-central-1",
                "cluster_name": "EU Central",
                "region": "eu-central",
                "server_url": "ws://eu-central-1.mpreg.example.com:9001",
                "bridge_url": "ws://bridge-eu-central-1.mpreg.example.com:9002",
            },
            {
                "cluster_id": "ap-southeast-1",
                "cluster_name": "Asia Pacific",
                "region": "ap-southeast",
                "server_url": "ws://ap-southeast-1.mpreg.example.com:9001",
                "bridge_url": "ws://bridge-ap-southeast-1.mpreg.example.com:9002",
            },
        ]

        print("  🌎 Creating clusters in different regions...")

        for config in cluster_configs:
            cluster = await create_federated_cluster(
                server_url=config["server_url"],
                cluster_id=config["cluster_id"],
                cluster_name=config["cluster_name"],
                region=config["region"],
                bridge_url=config["bridge_url"],
                public_key_hash=f"key_hash_{config['cluster_id']}",
            )

            self.clusters[config["cluster_id"]] = cluster

            print(f"    ✅ {config['cluster_name']} ({config['cluster_id']})")
            print(f"       Region: {config['region']}")
            print(f"       Bridge: {config['bridge_url']}")

        # Show federation status
        print(f"\n  📊 Created {len(self.clusters)} federated clusters")
        for cluster_id, cluster in self.clusters.items():
            stats = cluster.get_stats()
            print(
                f"    {cluster_id}: Federation {'enabled' if stats.federation.enabled else 'disabled'}"
            )

    async def demo_connect_clusters(self):
        """Demonstrate connecting clusters via bridge connections."""
        print("\n🔗 Demo 2: Connecting Clusters via Bridges")
        print("-" * 45)

        cluster_ids = list(self.clusters.keys())

        print("  🌐 Establishing federation connections...")

        # Connect clusters in a hub-and-spoke pattern (more efficient than full mesh)
        hub_cluster_id = cluster_ids[0]  # us-west-1 as hub
        hub_cluster = self.clusters[hub_cluster_id]

        print(f"    🎯 Using {hub_cluster_id} as federation hub")

        # Connect all other clusters to the hub
        for i, cluster_id in enumerate(cluster_ids[1:], 1):
            spoke_cluster = self.clusters[cluster_id]

            print(f"    🔗 Connecting {hub_cluster_id} ↔ {cluster_id}...")

            success_a, success_b = await connect_clusters(hub_cluster, spoke_cluster)

            if success_a and success_b:
                print("      ✅ Bidirectional connection established")
            else:
                print(
                    f"      ⚠️  Partial connection: hub→spoke={success_a}, spoke→hub={success_b}"
                )

        # Wait for initial synchronization
        print("  ⏳ Waiting for initial synchronization...")
        await asyncio.sleep(2.0)

        # Show federation topology
        print("\n  🗺️  Federation Topology:")
        for cluster_id, cluster in self.clusters.items():
            federation_stats = cluster.get_stats().federation
            federated_clusters = getattr(federation_stats, "connected_clusters", 0)
            print(
                f"    {cluster_id}: Connected to {federated_clusters} remote clusters"
            )

    async def demo_cross_region_subscriptions(self):
        """Demonstrate cross-region subscription setup and routing."""
        print("\n📡 Demo 3: Cross-Region Subscription Routing")
        print("-" * 50)

        print("  📝 Setting up region-specific subscriptions...")

        # US West: E-commerce platform
        us_west = self.clusters["us-west-1"]
        us_subscriptions = [
            (
                "us_ecommerce",
                ["order.us.*.created", "payment.us.*.completed", "inventory.us.#"],
            ),
            ("us_analytics", ["user.us.*.activity", "order.us.#", "marketing.us.#"]),
            ("us_notifications", ["alert.us.#", "system.us.*.error"]),
        ]

        for sub_id, patterns in us_subscriptions:
            subscription = PubSubSubscription(
                subscription_id=f"{sub_id}_us_west",
                patterns=tuple(
                    TopicPattern(pattern=p, exact_match=False) for p in patterns
                ),
                subscriber=f"service_{sub_id}",
                created_at=time.time(),
                get_backlog=False,
            )
            us_west.add_subscription(subscription)
            print(f"    🇺🇸 US West: {sub_id} → {patterns}")

        # EU Central: Financial services
        eu_central = self.clusters["eu-central-1"]
        eu_subscriptions = [
            (
                "eu_banking",
                ["transaction.eu.*.processed", "compliance.eu.#", "audit.eu.#"],
            ),
            ("eu_trading", ["market.eu.*.price_update", "trading.eu.*.executed"]),
            ("eu_risk", ["risk.eu.#", "alert.eu.*.high_priority"]),
        ]

        for sub_id, patterns in eu_subscriptions:
            subscription = PubSubSubscription(
                subscription_id=f"{sub_id}_eu_central",
                patterns=tuple(
                    TopicPattern(pattern=p, exact_match=False) for p in patterns
                ),
                subscriber=f"service_{sub_id}",
                created_at=time.time(),
                get_backlog=False,
            )
            eu_central.add_subscription(subscription)
            print(f"    🇪🇺 EU Central: {sub_id} → {patterns}")

        # Asia Pacific: IoT and gaming
        ap_southeast = self.clusters["ap-southeast-1"]
        ap_subscriptions = [
            ("ap_iot", ["sensor.ap.*.telemetry", "device.ap.#", "iot.ap.#"]),
            ("ap_gaming", ["game.ap.*.player_action", "match.ap.*.completed"]),
            ("ap_mobile", ["mobile.ap.*.user_engagement", "app.ap.*.crash_report"]),
        ]

        for sub_id, patterns in ap_subscriptions:
            subscription = PubSubSubscription(
                subscription_id=f"{sub_id}_ap_southeast",
                patterns=tuple(
                    TopicPattern(pattern=p, exact_match=False) for p in patterns
                ),
                subscriber=f"service_{sub_id}",
                created_at=time.time(),
                get_backlog=False,
            )
            ap_southeast.add_subscription(subscription)
            print(f"    🌏 Asia Pacific: {sub_id} → {patterns}")

        # Global subscriptions (interested in all regions)
        global_subscriptions = [
            ("global_security", ["security.#", "alert.*.critical", "breach.#"]),
            ("global_analytics", ["user.*.registration", "revenue.#", "kpi.#"]),
        ]

        print("\n  🌍 Setting up global subscriptions...")
        for cluster_id, cluster in self.clusters.items():
            for sub_id, patterns in global_subscriptions:
                subscription = PubSubSubscription(
                    subscription_id=f"{sub_id}_{cluster_id}",
                    patterns=tuple(
                        TopicPattern(pattern=p, exact_match=False) for p in patterns
                    ),
                    subscriber=f"global_service_{sub_id}",
                    created_at=time.time(),
                    get_backlog=False,
                )
                cluster.add_subscription(subscription)

        for sub_id, patterns in global_subscriptions:
            print(f"    🌍 Global: {sub_id} → {patterns} (all clusters)")

        # Wait for subscription propagation
        print("  ⏳ Waiting for subscription propagation...")
        await asyncio.sleep(3.0)

        # Show subscription summary
        print("\n  📊 Subscription Summary:")
        total_local_subs = 0
        total_remote_interests = 0

        for cluster_id, cluster in self.clusters.items():
            stats = cluster.get_stats()
            local_subs = stats.active_subscriptions
            remote_subs = getattr(stats.federation, "total_remote_subscriptions", 0)

            total_local_subs += local_subs
            total_remote_interests += remote_subs

            print(
                f"    {cluster_id}: {local_subs} local, {remote_subs} remote interests"
            )

        print(
            f"  📈 Total: {total_local_subs} local subscriptions, {total_remote_interests} remote interests"
        )

    async def demo_global_message_broadcasting(self):
        """Demonstrate global message broadcasting across federated clusters."""
        print("\n📢 Demo 4: Global Message Broadcasting")
        print("-" * 45)

        print("  🚀 Publishing messages from different regions...")

        # Messages that should stay local (region-specific)
        local_messages = [
            (
                "us-west-1",
                "order.us.12345.created",
                {"order_id": "12345", "amount": 299.99, "region": "us"},
            ),
            (
                "eu-central-1",
                "transaction.eu.67890.processed",
                {"transaction_id": "67890", "amount": 150.00, "currency": "EUR"},
            ),
            (
                "ap-southeast-1",
                "sensor.ap.device_001.telemetry",
                {"device_id": "device_001", "temperature": 28.5, "humidity": 65},
            ),
        ]

        print("  📍 Local messages (should not cross regions):")
        for cluster_id, topic, payload in local_messages:
            cluster = self.clusters[cluster_id]
            message = self.create_message(topic, payload)

            notifications = cluster.publish_message(message)
            local_notifications = [
                n
                for n in notifications
                if not n.subscription_id.startswith("federated_")
            ]
            federated_notifications = [
                n for n in notifications if n.subscription_id.startswith("federated_")
            ]

            print(f"    📨 {cluster_id}: {topic}")
            print(f"       Local: {len(local_notifications)} notifications")
            print(f"       Federated: {len(federated_notifications)} notifications")

        # Messages that should go global (security, alerts, etc.)
        global_messages = [
            (
                "us-west-1",
                "security.breach.detected",
                {
                    "severity": "critical",
                    "source": "us-west",
                    "affected_systems": ["payment", "user_data"],
                },
            ),
            (
                "eu-central-1",
                "alert.gdpr.violation",
                {
                    "severity": "high",
                    "region": "eu",
                    "compliance_issue": "data_retention",
                },
            ),
            (
                "ap-southeast-1",
                "kpi.daily.revenue",
                {"date": "2024-01-15", "region": "ap", "revenue": 125000.50},
            ),
        ]

        print("\n  🌍 Global messages (should reach all regions):")
        for cluster_tuple in global_messages:
            cluster_id, topic, payload = cluster_tuple  # type: ignore
            cluster = self.clusters[cluster_id]
            message = self.create_message(topic, payload)

            notifications = cluster.publish_message(message)
            local_notifications = [
                n
                for n in notifications
                if not n.subscription_id.startswith("federated_")
            ]
            federated_notifications = [
                n for n in notifications if n.subscription_id.startswith("federated_")
            ]

            print(f"    📨 {cluster_id}: {topic}")
            print(f"       Local: {len(local_notifications)} notifications")
            print(f"       Federated: {len(federated_notifications)} notifications")

            if federated_notifications:
                print(
                    f"       🌐 Routed to: {', '.join(set(n.subscription_id.split('_')[1] for n in federated_notifications))}"
                )

        # Show federation message statistics
        print("\n  📊 Federation Statistics:")
        for cluster_id, cluster in self.clusters.items():
            federation_stats = cluster.get_stats().federation
            sent = federation_stats.federated_messages_sent
            received = federation_stats.federated_messages_received

            print(f"    {cluster_id}: {sent} sent, {received} received")

    async def demo_bloom_filter_efficiency(self):
        """Demonstrate bloom filter efficiency for subscription aggregation."""
        print("\n🌸 Demo 5: Bloom Filter Efficiency")
        print("-" * 40)

        print("  📏 Analyzing bloom filter memory efficiency...")

        total_bloom_memory = 0
        total_patterns = 0

        for cluster_id, cluster in self.clusters.items():
            stats = cluster.get_stats()
            federation_stats = stats.federation

            if federation_stats.enabled:
                bloom_memory = federation_stats.bloom_filter_memory_bytes
                local_patterns = federation_stats.local_patterns
                fp_rate = federation_stats.bloom_filter_fp_rate

                total_bloom_memory += bloom_memory
                total_patterns += local_patterns

                print(f"    {cluster_id}:")
                print(f"      🔍 Patterns: {local_patterns}")
                print(
                    f"      💾 Bloom filter: {bloom_memory:,} bytes ({bloom_memory / 1024:.1f} KB)"
                )
                print(f"      🎯 False positive rate: {fp_rate:.3%}")

        # Calculate efficiency metrics
        avg_bytes_per_pattern = total_bloom_memory / max(1, total_patterns)

        print("\n  📊 Overall Efficiency:")
        print(f"    Total patterns: {total_patterns}")
        print(
            f"    Total bloom memory: {total_bloom_memory:,} bytes ({total_bloom_memory / 1024:.1f} KB)"
        )
        print(f"    Bytes per pattern: {avg_bytes_per_pattern:.1f}")
        print(
            f"    Memory efficiency: {'Excellent' if avg_bytes_per_pattern < 100 else 'Good' if avg_bytes_per_pattern < 500 else 'Poor'}"
        )

        # Compare with naive approach
        naive_memory = total_patterns * 50  # Assume 50 bytes per pattern string
        compression_ratio = naive_memory / max(1, total_bloom_memory)

        print("\n  🗜️  Compression Analysis:")
        print(
            f"    Naive storage: {naive_memory:,} bytes ({naive_memory / 1024:.1f} KB)"
        )
        print(
            f"    Bloom filter: {total_bloom_memory:,} bytes ({total_bloom_memory / 1024:.1f} KB)"
        )
        print(f"    Compression ratio: {compression_ratio:.1f}x smaller")
        print(
            f"    Space savings: {((naive_memory - total_bloom_memory) / naive_memory * 100):.1f}%"
        )

    async def demo_federation_health(self):
        """Demonstrate federation health monitoring."""
        print("\n🏥 Demo 6: Federation Health Monitoring")
        print("-" * 45)

        print("  💊 Checking federation health status...")

        for cluster_id, cluster in self.clusters.items():
            health = cluster.get_federation_health()

            print(f"\n    🏥 {cluster_id} Health Report:")
            print(
                f"      Overall: {health.overall_health} ({health.health_percentage:.1f}%)"
            )
            print(
                f"      Clusters: {health.healthy_clusters}/{health.total_clusters} healthy"
            )

            if health.connectivity_issues:
                print("      Connectivity issues:")
                for issue in health.connectivity_issues:
                    print(f"        ❌ {issue}")

            errors = len(health.connectivity_issues)
            if errors > 0:
                print(f"      ⚠️  Errors: {errors}")

        # Simulate some federation activity
        print("\n  🔄 Simulating federation activity...")

        # Trigger manual sync
        sync_results = []
        for cluster_id, cluster in self.clusters.items():
            if cluster.federation_enabled:
                success = await cluster.sync_federation()
                sync_results.append((cluster_id, success))
                print(
                    f"    📡 {cluster_id}: Sync {'successful' if success else 'failed'}"
                )

        successful_syncs = sum(1 for _, success in sync_results if success)
        print(f"    📊 Sync results: {successful_syncs}/{len(sync_results)} successful")

    async def demo_cluster_failure_handling(self):
        """Demonstrate graceful handling of cluster failures."""
        print("\n💥 Demo 7: Cluster Failure Handling")
        print("-" * 40)

        print("  🎭 Simulating cluster failure scenarios...")

        # Simulate failure of EU cluster
        eu_cluster = self.clusters["eu-central-1"]

        print("  ⚠️  Simulating failure of eu-central-1...")

        # Disable federation to simulate failure
        await eu_cluster.disable_federation()

        print("    📡 EU cluster federation disabled")

        # Wait for other clusters to detect the failure
        await asyncio.sleep(2.0)

        # Check how other clusters handle the failure
        print("  🔍 Checking impact on other clusters...")

        for cluster_id, cluster in self.clusters.items():
            if cluster_id != "eu-central-1":
                health = cluster.get_federation_health()

                print(f"    {cluster_id}:")
                print(f"      Health: {health.overall_health}")
                print(
                    f"      Healthy clusters: {health.healthy_clusters}/{health.total_clusters}"
                )

        # Test message delivery during failure
        print("\n  📨 Testing message delivery during failure...")

        us_cluster = self.clusters["us-west-1"]
        test_message = self.create_message(
            "security.cluster.failure",
            {"failed_cluster": "eu-central-1", "timestamp": time.time()},
        )

        notifications = us_cluster.publish_message(test_message)
        print(
            f"    📊 Global security message: {len(notifications)} notifications delivered"
        )
        print("    ✅ System continues operating despite cluster failure")

        # Simulate recovery
        print("\n  🔄 Simulating cluster recovery...")

        # Re-enable federation for EU cluster
        cluster_identity = ClusterIdentity(
            cluster_id="eu-central-1",
            cluster_name="EU Central",
            region="eu-central",
            bridge_url="ws://bridge-eu-central-1.mpreg.example.com:9002",
            public_key_hash="key_hash_eu-central-1",
            created_at=time.time(),
        )

        await eu_cluster.enable_federation(cluster_identity)

        # Reconnect to other clusters
        us_cluster = self.clusters["us-west-1"]
        success_a, success_b = await connect_clusters(us_cluster, eu_cluster)

        print(
            f"    🔗 Reconnection: {'successful' if success_a and success_b else 'partial'}"
        )

        # Wait for recovery
        await asyncio.sleep(2.0)

        # Check final health
        print("  🏥 Post-recovery health check...")
        for cluster_id, cluster in self.clusters.items():
            health = cluster.get_federation_health()
            print(
                f"    {cluster_id}: {health.overall_health} "
                f"({health.healthy_clusters}/{health.total_clusters})"
            )

    def create_message(self, topic: str, payload: dict) -> PubSubMessage:
        """Create a test message."""
        self.message_count += 1
        return PubSubMessage(
            topic=topic,
            payload=payload,
            timestamp=time.time(),
            message_id=f"fed_msg_{self.message_count}",
            publisher="federation_demo",
        )


def main():
    """Run the federation demo."""
    demo = FederationDemo()
    asyncio.run(demo.run_demo())


if __name__ == "__main__":
    main()
