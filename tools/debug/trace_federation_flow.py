#!/usr/bin/env python3
"""
Comprehensive trace of the federation flow to understand where it breaks.
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

# Configure comprehensive logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s",
)
logging.getLogger("mpreg.federation").setLevel(logging.DEBUG)
logging.getLogger("mpreg.core.federated_message_queue").setLevel(logging.DEBUG)
logging.getLogger("mpreg.core.topic_exchange").setLevel(logging.DEBUG)


async def trace_complete_federation_flow():
    """Trace the complete federation flow step by step."""
    print("=" * 80)
    print("🔍 TRACING COMPLETE FEDERATION FLOW")
    print("=" * 80)

    # Create servers
    server1_settings = MPREGSettings(
        host="127.0.0.1", port=18000, name="TraceServer1", cluster_id="trace-1"
    )
    server2_settings = MPREGSettings(
        host="127.0.0.1", port=18001, name="TraceServer2", cluster_id="trace-2"
    )

    server1 = MPREGServer(settings=server1_settings)
    server2 = MPREGServer(settings=server2_settings)

    # Start servers
    print("🚀 Starting servers...")
    task1 = asyncio.create_task(server1.server())
    task2 = asyncio.create_task(server2.server())
    await asyncio.sleep(1.0)

    try:
        # Create cluster identities
        cluster1_identity = ClusterIdentity(
            cluster_id="trace-1",
            cluster_name="Trace1",
            region="test",
            bridge_url="ws://127.0.0.1:18000",
            public_key_hash="hash1",
            created_at=time.time(),
            geographic_coordinates=(0.0, 0.0),
            network_tier=1,
            max_bandwidth_mbps=100,
            preference_weight=1.0,
        )
        cluster2_identity = ClusterIdentity(
            cluster_id="trace-2",
            cluster_name="Trace2",
            region="test",
            bridge_url="ws://127.0.0.1:18001",
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

        print("🔗 Creating federation bridges...")
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

        print("▶️  Starting federation bridges...")
        await bridge1.start()
        await bridge2.start()

        print("🤝 Connecting clusters...")
        await bridge1.add_cluster(cluster2_identity)
        await bridge2.add_cluster(cluster1_identity)

        # Create federated queue managers
        config = QueueManagerConfiguration(
            max_queues=100, default_max_queue_size=1000, enable_auto_queue_creation=True
        )
        manager1 = FederatedMessageQueueManager(
            config=config, federation_bridge=bridge1, local_cluster_id="trace-1"
        )
        manager2 = FederatedMessageQueueManager(
            config=config, federation_bridge=bridge2, local_cluster_id="trace-2"
        )

        # Register managers with bridges
        bridge1.register_federated_queue_manager(manager1)
        bridge2.register_federated_queue_manager(manager2)

        await asyncio.sleep(0.5)

        print("\n" + "=" * 50)
        print("📋 STEP 1: Creating federated queue")
        print("=" * 50)

        queue_config = QueueConfiguration(
            name="trace-queue",
            queue_type=QueueType.FIFO,
            max_size=1000,
            enable_deduplication=True,
        )

        # Monitor federation bridge stats before
        print("🔍 Before queue creation:")
        print(
            f"  Bridge1 outbound queue size: {bridge1._outbound_message_queue.qsize()}"
        )
        print(
            f"  Bridge1 messages forwarded: {bridge1.message_stats.get('messages_forwarded', 0)}"
        )
        print(
            f"  Bridge2 inbound queue size: {bridge2._inbound_federation_queue.qsize()}"
        )
        print(
            f"  Bridge2 messages received: {bridge2.message_stats.get('messages_received', 0)}"
        )

        result = await manager1.create_federated_queue(
            "trace-queue", queue_config, advertise_globally=True
        )

        print(f"✅ Queue creation result: {result}")

        # Monitor immediately after
        print("🔍 Immediately after queue creation:")
        print(
            f"  Bridge1 outbound queue size: {bridge1._outbound_message_queue.qsize()}"
        )
        print(
            f"  Bridge1 messages forwarded: {bridge1.message_stats.get('messages_forwarded', 0)}"
        )
        print(
            f"  Bridge2 inbound queue size: {bridge2._inbound_federation_queue.qsize()}"
        )
        print(
            f"  Bridge2 messages received: {bridge2.message_stats.get('messages_received', 0)}"
        )

        print("\n" + "=" * 50)
        print("⏰ STEP 2: Waiting for message forwarding")
        print("=" * 50)

        for i in range(10):
            await asyncio.sleep(0.5)
            print(f"  Second {i + 1}/10:")
            print(
                f"    Bridge1 outbound queue: {bridge1._outbound_message_queue.qsize()}"
            )
            print(
                f"    Bridge1 messages_forwarded: {bridge1.message_stats.get('messages_forwarded', 0)}"
            )
            print(
                f"    Bridge2 inbound queue: {bridge2._inbound_federation_queue.qsize()}"
            )
            print(
                f"    Bridge2 messages_received: {bridge2.message_stats.get('messages_received', 0)}"
            )

            # Check cluster health
            cluster2_conn = bridge1.remote_clusters.get("trace-2")
            if cluster2_conn:
                print(
                    f"    Cluster2 health: {cluster2_conn.is_healthy()} (status: {cluster2_conn.status})"
                )

        print("\n" + "=" * 50)
        print("🔍 STEP 3: Checking queue discovery")
        print("=" * 50)

        discovered_queues = await manager2.discover_federated_queues("trace-*")
        print(f"Discovered queues: {discovered_queues}")

        # Final statistics
        print("\n" + "=" * 50)
        print("📊 FINAL STATISTICS")
        print("=" * 50)
        stats1 = manager1.get_federation_statistics()
        stats2 = manager2.get_federation_statistics()
        print(f"Manager1 stats: {stats1}")
        print(f"Manager2 stats: {stats2}")
        print(f"Bridge1 stats: {dict(bridge1.message_stats)}")
        print(f"Bridge2 stats: {dict(bridge2.message_stats)}")

        # Check remote queues
        print(f"Manager2 remote queues: {dict(manager2.remote_queues)}")

        # Success assessment
        if "trace-1" in discovered_queues:
            print("🎉 SUCCESS: Federation flow working!")
        else:
            print("❌ FAILURE: Federation flow broken")
            print("🔧 Diagnostics:")
            if bridge1.message_stats.get("messages_forwarded", 0) == 0:
                print("  - No messages were forwarded from bridge1")
            if bridge2.message_stats.get("messages_received", 0) == 0:
                print("  - No messages were received by bridge2")
            print(
                f"  - Bridge1 cluster connections: {list(bridge1.remote_clusters.keys())}"
            )
            print(
                f"  - Bridge2 cluster connections: {list(bridge2.remote_clusters.keys())}"
            )

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
    asyncio.run(trace_complete_federation_flow())
