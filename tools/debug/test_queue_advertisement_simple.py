#!/usr/bin/env python3
"""
Simple test to debug queue advertisement and discovery flow.
"""

import asyncio
import logging
import time

from mpreg.core.config import MPREGSettings
from mpreg.core.federated_message_queue import FederatedMessageQueueManager
from mpreg.core.message_queue import QueueConfiguration, QueueType
from mpreg.core.message_queue_manager import QueueManagerConfiguration
from mpreg.federation.federation_bridge import GraphAwareFederationBridge
from mpreg.federation.federation_optimized import ClusterIdentity
from mpreg.server import MPREGServer

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("mpreg.federation").setLevel(logging.DEBUG)
logging.getLogger("mpreg.core.federated_message_queue").setLevel(logging.DEBUG)


async def test_queue_advertisement_simple():
    """Test simple queue advertisement and discovery."""
    print("🧪 Testing queue advertisement and discovery flow...")

    # Create servers
    server1_settings = MPREGSettings(
        host="127.0.0.1", port=17000, name="Server1", cluster_id="test-1"
    )
    server2_settings = MPREGSettings(
        host="127.0.0.1", port=17001, name="Server2", cluster_id="test-2"
    )

    server1 = MPREGServer(settings=server1_settings)
    server2 = MPREGServer(settings=server2_settings)

    # Start servers
    task1 = asyncio.create_task(server1.server())
    task2 = asyncio.create_task(server2.server())
    await asyncio.sleep(1.0)

    try:
        # Create cluster identities
        cluster1_identity = ClusterIdentity(
            cluster_id="test-1",
            cluster_name="Test1",
            region="test",
            bridge_url="ws://127.0.0.1:17000",
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
            bridge_url="ws://127.0.0.1:17001",
            public_key_hash="hash2",
            created_at=time.time(),
            geographic_coordinates=(1.0, 1.0),
            network_tier=1,
            max_bandwidth_mbps=100,
            preference_weight=1.0,
        )

        # Use server's internal topic exchanges
        topic_exchange1 = server1.topic_exchange
        topic_exchange2 = server2.topic_exchange

        # Create federation bridges
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

        # Connect bridges to topic exchanges
        topic_exchange1.set_federation_bridge(bridge1)
        topic_exchange2.set_federation_bridge(bridge2)

        await bridge1.start()
        await bridge2.start()

        await bridge1.add_cluster(cluster2_identity)
        await bridge2.add_cluster(cluster1_identity)

        # Create federated queue managers
        config = QueueManagerConfiguration(
            max_queues=100, default_max_queue_size=1000, enable_auto_queue_creation=True
        )
        manager1 = FederatedMessageQueueManager(
            config=config, federation_bridge=bridge1, local_cluster_id="test-1"
        )
        manager2 = FederatedMessageQueueManager(
            config=config, federation_bridge=bridge2, local_cluster_id="test-2"
        )

        # Register managers with bridges
        bridge1.register_federated_queue_manager(manager1)
        bridge2.register_federated_queue_manager(manager2)

        await asyncio.sleep(0.5)

        print("📋 Creating federated queue with advertisement...")

        # Create a queue on manager1 and advertise globally
        queue_config = QueueConfiguration(
            name="test-advert-queue",
            queue_type=QueueType.FIFO,
            max_size=1000,
            enable_deduplication=True,
        )

        result = await manager1.create_federated_queue(
            "test-advert-queue", queue_config, advertise_globally=True
        )

        print(f"✅ Queue creation result: {result}")

        # Wait for federation propagation
        print("⏰ Waiting for federation advertisement...")
        for i in range(5):
            await asyncio.sleep(1.0)
            print(f"  Second {i + 1}/5:")
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

        print("🔍 Attempting queue discovery...")
        discovered_queues = await manager2.discover_federated_queues("test-advert-*")
        print(f"Discovered queues: {discovered_queues}")

        # Check federation statistics
        stats1 = manager1.get_federation_statistics()
        stats2 = manager2.get_federation_statistics()
        print(f"Manager1 federation stats: {stats1}")
        print(f"Manager2 federation stats: {stats2}")

        # Final status
        if "test-1" in discovered_queues:
            print("🎉 SUCCESS: Queue advertisement and discovery working!")
        else:
            print("❌ FAILURE: Queue advertisement/discovery broken")

    finally:
        # Cleanup
        try:
            await manager1.shutdown()
            await manager2.shutdown()
            await bridge1.stop()
            await bridge2.stop()
        except (AttributeError, asyncio.CancelledError, ConnectionError):
            pass  # Bridge cleanup errors during teardown are expected


if __name__ == "__main__":
    asyncio.run(test_queue_advertisement_simple())
