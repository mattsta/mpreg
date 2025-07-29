#!/usr/bin/env python3
"""
Debug script to trace federation queue advertisement flow end-to-end.
"""

import asyncio
import logging
import time
from dataclasses import dataclass

from mpreg.core.config import MPREGSettings
from mpreg.core.federated_message_queue import FederatedMessageQueueManager
from mpreg.core.message_queue import QueueConfiguration, QueueType
from mpreg.core.message_queue_manager import QueueManagerConfiguration
from mpreg.federation.federation_bridge import GraphAwareFederationBridge
from mpreg.federation.federation_optimized import ClusterIdentity
from mpreg.server import MPREGServer

# Configure logging to see debug messages
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s:%(funcName)s:%(lineno)d - %(message)s",
)

# Enable debug logging for federation components
logging.getLogger("mpreg.federation").setLevel(logging.DEBUG)
logging.getLogger("mpreg.core.federated_message_queue").setLevel(logging.DEBUG)
logging.getLogger("mpreg.core.topic_exchange").setLevel(logging.DEBUG)


@dataclass
class DebugTestSetup:
    server1: MPREGServer
    server2: MPREGServer
    manager1: FederatedMessageQueueManager
    manager2: FederatedMessageQueueManager
    bridge1: GraphAwareFederationBridge
    bridge2: GraphAwareFederationBridge


async def setup_federation_debug() -> DebugTestSetup:
    """Set up two federated clusters for debugging."""
    print("🔧 Setting up federation debug environment...")

    # Use fixed ports for debugging
    port1, port2 = 15000, 15001

    # Create servers
    server1_settings = MPREGSettings(
        host="127.0.0.1",
        port=port1,
        name="Debug Server 1",
        cluster_id="debug-cluster-1",
        resources={"debug-resource-1"},
    )
    server2_settings = MPREGSettings(
        host="127.0.0.1",
        port=port2,
        name="Debug Server 2",
        cluster_id="debug-cluster-2",
        resources={"debug-resource-2"},
    )

    server1 = MPREGServer(settings=server1_settings)
    server2 = MPREGServer(settings=server2_settings)

    # Start servers
    print(f"🚀 Starting servers on ports {port1} and {port2}...")
    task1 = asyncio.create_task(server1.server())
    task2 = asyncio.create_task(server2.server())
    await asyncio.sleep(1.0)  # Wait for servers to start

    # Create cluster identities
    cluster1_identity = ClusterIdentity(
        cluster_id="debug-cluster-1",
        cluster_name="Debug Cluster 1",
        region="debug-east",
        bridge_url=f"ws://127.0.0.1:{port1}",
        public_key_hash="debug_hash_1",
        created_at=time.time(),
        geographic_coordinates=(40.0, -74.0),
        network_tier=1,
        max_bandwidth_mbps=1000,
        preference_weight=1.0,
    )
    cluster2_identity = ClusterIdentity(
        cluster_id="debug-cluster-2",
        cluster_name="Debug Cluster 2",
        region="debug-west",
        bridge_url=f"ws://127.0.0.1:{port2}",
        public_key_hash="debug_hash_2",
        created_at=time.time(),
        geographic_coordinates=(37.0, -122.0),
        network_tier=1,
        max_bandwidth_mbps=1000,
        preference_weight=1.0,
    )

    # Use the server's internal topic exchanges instead of creating new ones
    topic_exchange1 = server1.topic_exchange
    topic_exchange2 = server2.topic_exchange

    # Create federation bridges
    bridge1 = GraphAwareFederationBridge(
        local_cluster=topic_exchange1,
        cluster_identity=cluster1_identity,
        enable_graph_routing=True,
        enable_monitoring=True,
    )
    bridge2 = GraphAwareFederationBridge(
        local_cluster=topic_exchange2,
        cluster_identity=cluster2_identity,
        enable_graph_routing=True,
        enable_monitoring=True,
    )

    # Connect bridges to topic exchanges
    topic_exchange1.set_federation_bridge(bridge1)
    topic_exchange2.set_federation_bridge(bridge2)

    print("🔗 Starting federation bridges...")
    await bridge1.start()
    await bridge2.start()

    # Connect bridges to each other
    print("🤝 Connecting clusters...")
    await bridge1.add_cluster(cluster2_identity)
    await bridge2.add_cluster(cluster1_identity)

    # Create federated queue managers
    config = QueueManagerConfiguration(
        max_queues=100, default_max_queue_size=1000, enable_auto_queue_creation=True
    )
    manager1 = FederatedMessageQueueManager(
        config=config, federation_bridge=bridge1, local_cluster_id="debug-cluster-1"
    )
    manager2 = FederatedMessageQueueManager(
        config=config, federation_bridge=bridge2, local_cluster_id="debug-cluster-2"
    )

    # Register managers with bridges
    bridge1.register_federated_queue_manager(manager1)
    bridge2.register_federated_queue_manager(manager2)

    print("✅ Federation setup complete!")
    await asyncio.sleep(1.0)  # Allow setup to stabilize

    return DebugTestSetup(server1, server2, manager1, manager2, bridge1, bridge2)


async def debug_federation_flow():
    """Debug the complete federation flow step by step."""
    setup = await setup_federation_debug()

    try:
        print("\n" + "=" * 80)
        print("🧪 Federation Queue Advertisement Flow Debug")
        print("=" * 80)

        # Step 1: Create federated queue with advertisement
        print("\n📋 Step 1: Creating federated queue on cluster-1...")
        queue_config = QueueConfiguration(
            name="debug-global-queue",
            queue_type=QueueType.FIFO,
            max_size=1000,
            enable_deduplication=True,
        )

        print("📨 Calling create_federated_queue with advertise_globally=True...")
        result = await setup.manager1.create_federated_queue(
            "debug-global-queue", queue_config, advertise_globally=True
        )
        print(f"✅ Queue creation result: {result}")

        # Step 2: Check federation bridge statistics
        print("\n📊 Step 2: Checking federation bridge statistics...")
        bridge1_stats = setup.bridge1.message_stats
        bridge2_stats = setup.bridge2.message_stats
        print(f"Bridge1 stats: {dict(bridge1_stats)}")
        print(f"Bridge2 stats: {dict(bridge2_stats)}")

        # Step 3: Wait and monitor message flow
        print("\n⏰ Step 3: Waiting for federation propagation...")
        for i in range(5):
            await asyncio.sleep(1.0)
            print(f"   Second {i + 1}/5:")
            print(
                f"     Bridge1 outbound queue: {setup.bridge1._outbound_message_queue.qsize()}"
            )
            print(
                f"     Bridge2 inbound queue: {setup.bridge2._inbound_federation_queue.qsize()}"
            )
            print(
                f"     Bridge1 messages_forwarded: {bridge1_stats.get('messages_forwarded', 0)}"
            )
            print(
                f"     Bridge2 messages_received: {bridge2_stats.get('messages_received', 0)}"
            )

        # Step 4: Try discovery
        print("\n🔍 Step 4: Attempting queue discovery...")
        discovered_queues = await setup.manager2.discover_federated_queues(
            "debug-global-*"
        )
        print(f"Discovered queues: {discovered_queues}")

        # Step 5: Check manager statistics
        print("\n📈 Step 5: Checking manager federation statistics...")
        manager1_stats = setup.manager1.get_federation_statistics()
        manager2_stats = setup.manager2.get_federation_statistics()
        print(f"Manager1 federation stats: {manager1_stats}")
        print(f"Manager2 federation stats: {manager2_stats}")

        # Step 6: Manual advertisement test
        print("\n🧪 Step 6: Manual advertisement test...")
        print("Manually triggering advertisement...")
        await setup.manager1._advertise_queue("debug-global-queue", queue_config)
        await asyncio.sleep(2.0)

        discovered_queues_manual = await setup.manager2.discover_federated_queues(
            "debug-global-*"
        )
        print(f"Post-manual discovered queues: {discovered_queues_manual}")

        # Final results
        print("\n" + "=" * 80)
        if "debug-cluster-1" in discovered_queues_manual:
            print("🎉 SUCCESS: Federation flow is working!")
        else:
            print("❌ FAILURE: Federation flow is broken")
            print("Expected to find 'debug-cluster-1' in discovered queues")
            print(f"Actual discovered queues: {discovered_queues_manual}")
        print("=" * 80)

    except Exception as e:
        print(f"💥 Error during debug: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Cleanup
        print("\n🧹 Cleaning up...")
        try:
            await setup.manager1.shutdown()
            await setup.manager2.shutdown()
            await setup.bridge1.stop()
            await setup.bridge2.stop()
        except Exception as e:
            print(f"Cleanup error: {e}")


if __name__ == "__main__":
    asyncio.run(debug_federation_flow())
