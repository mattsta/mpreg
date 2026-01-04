#!/usr/bin/env python3
"""
Simple test to debug federation message forwarding directly.
"""

import asyncio
import logging
import time

from mpreg.fabric.federation_bridge import GraphAwareFederationBridge

from mpreg.core.config import MPREGSettings
from mpreg.core.model import PubSubMessage
from mpreg.fabric.federation_optimized import ClusterIdentity
from mpreg.server import MPREGServer

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("mpreg.fabric").setLevel(logging.DEBUG)


async def test_simple_forward():
    """Test simple federation message forwarding."""
    print("🧪 Testing simple federation message forwarding...")

    # Create simple servers
    server1_settings = MPREGSettings(
        host="127.0.0.1", port=16000, name="Server1", cluster_id="test-1"
    )
    server2_settings = MPREGSettings(
        host="127.0.0.1", port=16001, name="Server2", cluster_id="test-2"
    )

    server1 = MPREGServer(settings=server1_settings)
    server2 = MPREGServer(settings=server2_settings)

    # Start servers
    task1 = asyncio.create_task(server1.server())
    task2 = asyncio.create_task(server2.server())
    await asyncio.sleep(1.0)

    try:
        # Create minimal federation setup
        cluster1_identity = ClusterIdentity(
            cluster_id="test-1",
            cluster_name="Test1",
            region="test",
            bridge_url="ws://127.0.0.1:16000",
            public_key_hash="hash1",
            created_at=time.time(),
            geographic_coordinates=(0.0, 0.0),
            network_tier=1,
            max_bandwidth_mbps=100,
            preference_weight=1.0,
        )
        cluster2_identity = ClusterIdentity(
            cluster_id="test-2",
            cluster_name="Test2",
            region="test",
            bridge_url="ws://127.0.0.1:16001",
            public_key_hash="hash2",
            created_at=time.time(),
            geographic_coordinates=(1.0, 1.0),
            network_tier=1,
            max_bandwidth_mbps=100,
            preference_weight=1.0,
        )

        # Use the server's internal topic exchange instead of creating new ones
        topic_exchange1 = server1.topic_exchange
        topic_exchange2 = server2.topic_exchange

        bridge1 = GraphAwareFederationBridge(
            local_cluster=topic_exchange1,
            cluster_identity=cluster1_identity,
            enable_graph_routing=False,
            enable_monitoring=False,
        )
        bridge2 = GraphAwareFederationBridge(
            local_cluster=topic_exchange2,
            cluster_identity=cluster2_identity,
            enable_graph_routing=False,
            enable_monitoring=False,
        )

        topic_exchange1.set_federation_bridge(bridge1)
        topic_exchange2.set_federation_bridge(bridge2)

        await bridge1.start()
        await bridge2.start()

        await bridge1.add_cluster(cluster2_identity)
        await bridge2.add_cluster(cluster1_identity)

        await asyncio.sleep(0.5)

        print("📊 Initial bridge stats:")
        print(f"  Bridge1: {dict(bridge1.message_stats)}")
        print(f"  Bridge2: {dict(bridge2.message_stats)}")

        # Create and send a test federation message
        test_message = PubSubMessage(
            topic="mpreg.fabric.test.simple",
            payload={"test": "federation forwarding"},
            timestamp=time.time(),
            message_id="test-forward-001",
            publisher="test-publisher",
            headers={},
        )

        print("📤 Publishing federation message to topic exchange 1...")
        notifications = topic_exchange1.publish_message(test_message)
        print(f"  Notifications: {len(notifications) if notifications else 0}")

        # Wait for processing
        print("⏰ Waiting for federation processing...")
        for i in range(3):
            await asyncio.sleep(1.0)
            print(f"  Second {i + 1}/3:")
            print(
                f"    Bridge1 outbound queue: {bridge1._outbound_message_queue.qsize()}"
            )
            print(
                f"    Bridge2 inbound queue: {bridge2._inbound_federation_queue.qsize()}"
            )
            print(
                f"    Bridge1 messages_forwarded: {bridge1.message_stats.get('messages_forwarded', 0)}"
            )
            print(
                f"    Bridge2 messages_received: {bridge2.message_stats.get('messages_received', 0)}"
            )
            print(
                f"    Topic exchange 2 messages_published: {topic_exchange2.messages_published}"
            )

        # Final stats
        print("📊 Final bridge stats:")
        print(f"  Bridge1: {dict(bridge1.message_stats)}")
        print(f"  Bridge2: {dict(bridge2.message_stats)}")

        # Check cluster health
        print("🩺 Cluster health:")
        for cluster_id, connection in bridge1.remote_clusters.items():
            print(
                f"  Bridge1 -> {cluster_id}: {connection.is_healthy()} (status: {connection.status})"
            )
        for cluster_id, connection in bridge2.remote_clusters.items():
            print(
                f"  Bridge2 -> {cluster_id}: {connection.is_healthy()} (status: {connection.status})"
            )

        # Manual forwarding test
        print("🧪 Testing manual message forwarding...")
        try:
            await bridge1._forward_message_to_all_clusters(test_message)
            print("  ✅ Manual forwarding completed")
        except Exception as e:
            print(f"  ❌ Manual forwarding failed: {e}")
            import traceback

            traceback.print_exc()

        await asyncio.sleep(1.0)
        print("📊 Post-manual bridge stats:")
        print(f"  Bridge1: {dict(bridge1.message_stats)}")
        print(f"  Bridge2: {dict(bridge2.message_stats)}")

    finally:
        # Cleanup
        try:
            await bridge1.stop()
            await bridge2.stop()
        except (AttributeError, asyncio.CancelledError, ConnectionError):
            pass  # Bridge cleanup errors during teardown are expected


if __name__ == "__main__":
    asyncio.run(test_simple_forward())
