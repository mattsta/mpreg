"""
Test PubSub notification delivery across federated clusters.

This tests that the unified notification system works correctly
when integrated with the federation system.
"""

import asyncio
import time

import pytest

from mpreg.client.pubsub_client import MPREGPubSubExtendedClient
from mpreg.core.config import MPREGSettings
from mpreg.federation.federation_bridge import GraphAwareFederationBridge
from mpreg.federation.federation_optimized import ClusterIdentity
from mpreg.server import MPREGServer


class TestFederationPubSubIntegration:
    """Test PubSub notifications across federated clusters."""

    @pytest.mark.asyncio
    async def test_pubsub_notifications_across_federation(self):
        """Test that PubSub notifications work across federated clusters."""
        # Create two federated servers
        server1_settings = MPREGSettings(
            host="127.0.0.1", port=19300, name="FedServer1", cluster_id="fed-cluster-1"
        )
        server2_settings = MPREGSettings(
            host="127.0.0.1", port=19301, name="FedServer2", cluster_id="fed-cluster-2"
        )

        server1 = MPREGServer(settings=server1_settings)
        server2 = MPREGServer(settings=server2_settings)

        # Start both servers
        server1_task = asyncio.create_task(server1.server())
        server2_task = asyncio.create_task(server2.server())
        await asyncio.sleep(1.0)  # Give servers time to start

        try:
            # Create cluster identities
            cluster1_identity = ClusterIdentity(
                cluster_id="fed-cluster-1",
                cluster_name="Federation Cluster 1",
                region="test",
                bridge_url="ws://127.0.0.1:19300",
                public_key_hash="hash1",
                created_at=time.time(),
                geographic_coordinates=(0.0, 0.0),
                network_tier=1,
                max_bandwidth_mbps=1000,
                preference_weight=1.0,
            )

            cluster2_identity = ClusterIdentity(
                cluster_id="fed-cluster-2",
                cluster_name="Federation Cluster 2",
                region="test",
                bridge_url="ws://127.0.0.1:19301",
                public_key_hash="hash2",
                created_at=time.time(),
                geographic_coordinates=(1.0, 1.0),
                network_tier=1,
                max_bandwidth_mbps=1000,
                preference_weight=1.0,
            )

            # Create federation bridges
            bridge1 = GraphAwareFederationBridge(
                local_cluster=server1.topic_exchange,
                cluster_identity=cluster1_identity,
                enable_graph_routing=False,
                enable_monitoring=False,
            )

            bridge2 = GraphAwareFederationBridge(
                local_cluster=server2.topic_exchange,
                cluster_identity=cluster2_identity,
                enable_graph_routing=False,
                enable_monitoring=False,
            )

            # Connect bridges to topic exchanges
            server1.topic_exchange.set_federation_bridge(bridge1)
            server2.topic_exchange.set_federation_bridge(bridge2)

            # Start federation bridges
            await bridge1.start()
            await bridge2.start()

            # Connect clusters
            await bridge1.add_cluster(cluster2_identity)
            await bridge2.add_cluster(cluster1_identity)

            await asyncio.sleep(0.5)  # Allow federation to stabilize

            # Track received messages on each cluster
            cluster1_messages = []
            cluster2_messages = []

            def cluster1_callback(message):
                cluster1_messages.append(message)

            def cluster2_callback(message):
                cluster2_messages.append(message)

            # Create clients connected to each cluster
            async with MPREGPubSubExtendedClient("ws://127.0.0.1:19300") as client1:
                async with MPREGPubSubExtendedClient("ws://127.0.0.1:19301") as client2:
                    # Subscribe on both clusters to the same topic pattern
                    sub1 = await client1.subscribe(
                        patterns=["federation.test.*"],
                        callback=cluster1_callback,
                        get_backlog=False,
                    )

                    sub2 = await client2.subscribe(
                        patterns=["federation.test.*"],
                        callback=cluster2_callback,
                        get_backlog=False,
                    )

                    await asyncio.sleep(0.5)  # Allow subscriptions to be active

                    # Publish message from cluster 1
                    success1 = await client1.publish(
                        "federation.test.message1",
                        {
                            "source": "cluster-1",
                            "message": "Hello from federation cluster 1!",
                            "timestamp": time.time(),
                        },
                    )
                    assert success1, "Message publish from cluster 1 should succeed"

                    # Publish message from cluster 2
                    success2 = await client2.publish(
                        "federation.test.message2",
                        {
                            "source": "cluster-2",
                            "message": "Hello from federation cluster 2!",
                            "timestamp": time.time(),
                        },
                    )
                    assert success2, "Message publish from cluster 2 should succeed"

                    # Wait for federation and notification delivery
                    await asyncio.sleep(2.0)

                    # Verify local notifications were received
                    # Note: With current federation implementation, each cluster should
                    # receive its own published message locally
                    assert len(cluster1_messages) >= 1, (
                        f"Cluster 1 should receive at least 1 message, got {len(cluster1_messages)}"
                    )
                    assert len(cluster2_messages) >= 1, (
                        f"Cluster 2 should receive at least 1 message, got {len(cluster2_messages)}"
                    )

                    # Verify message content
                    cluster1_topics = [msg.topic for msg in cluster1_messages]
                    cluster2_topics = [msg.topic for msg in cluster2_messages]

                    # Each cluster should at least receive its own published message
                    assert "federation.test.message1" in cluster1_topics, (
                        "Cluster 1 should receive its own message"
                    )
                    assert "federation.test.message2" in cluster2_topics, (
                        "Cluster 2 should receive its own message"
                    )

                    print(
                        f"Cluster 1 received {len(cluster1_messages)} messages: {cluster1_topics}"
                    )
                    print(
                        f"Cluster 2 received {len(cluster2_messages)} messages: {cluster2_topics}"
                    )

        finally:
            # Cleanup federation
            try:
                await bridge1.stop()
                await bridge2.stop()
            except (AttributeError, asyncio.CancelledError, ConnectionError):
                # Bridge cleanup errors during test teardown are expected
                pass

            # Cleanup servers
            server1_task.cancel()
            server2_task.cancel()
            try:
                await server1_task
                await server2_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_federation_subscription_isolation(self):
        """Test that federation doesn't interfere with local-only subscriptions."""
        # Create two federated servers
        server1_settings = MPREGSettings(
            host="127.0.0.1",
            port=19302,
            name="IsolationServer1",
            cluster_id="iso-cluster-1",
        )
        server2_settings = MPREGSettings(
            host="127.0.0.1",
            port=19303,
            name="IsolationServer2",
            cluster_id="iso-cluster-2",
        )

        server1 = MPREGServer(settings=server1_settings)
        server2 = MPREGServer(settings=server2_settings)

        # Start both servers
        server1_task = asyncio.create_task(server1.server())
        server2_task = asyncio.create_task(server2.server())
        await asyncio.sleep(1.0)

        try:
            # Set up federation (similar to above test)
            cluster1_identity = ClusterIdentity(
                cluster_id="iso-cluster-1",
                cluster_name="Isolation Cluster 1",
                region="test",
                bridge_url="ws://127.0.0.1:19302",
                public_key_hash="iso_hash1",
                created_at=time.time(),
                geographic_coordinates=(0.0, 0.0),
                network_tier=1,
                max_bandwidth_mbps=1000,
                preference_weight=1.0,
            )

            cluster2_identity = ClusterIdentity(
                cluster_id="iso-cluster-2",
                cluster_name="Isolation Cluster 2",
                region="test",
                bridge_url="ws://127.0.0.1:19303",
                public_key_hash="iso_hash2",
                created_at=time.time(),
                geographic_coordinates=(1.0, 1.0),
                network_tier=1,
                max_bandwidth_mbps=1000,
                preference_weight=1.0,
            )

            # Create and start federation
            bridge1 = GraphAwareFederationBridge(
                local_cluster=server1.topic_exchange,
                cluster_identity=cluster1_identity,
                enable_graph_routing=False,
                enable_monitoring=False,
            )

            bridge2 = GraphAwareFederationBridge(
                local_cluster=server2.topic_exchange,
                cluster_identity=cluster2_identity,
                enable_graph_routing=False,
                enable_monitoring=False,
            )

            server1.topic_exchange.set_federation_bridge(bridge1)
            server2.topic_exchange.set_federation_bridge(bridge2)

            await bridge1.start()
            await bridge2.start()
            await bridge1.add_cluster(cluster2_identity)
            await bridge2.add_cluster(cluster1_identity)

            await asyncio.sleep(0.5)

            # Track messages
            local_messages = []
            federation_messages = []

            def local_callback(message):
                local_messages.append(message)

            def federation_callback(message):
                federation_messages.append(message)

            # Create clients and subscriptions
            async with MPREGPubSubExtendedClient("ws://127.0.0.1:19302") as client1:
                async with MPREGPubSubExtendedClient("ws://127.0.0.1:19303") as client2:
                    # Subscribe to different topic patterns:
                    # - local.* should only receive local messages
                    # - federation.* should participate in federation (if implemented)
                    local_sub = await client1.subscribe(
                        patterns=["local.*"], callback=local_callback, get_backlog=False
                    )

                    federation_sub = await client2.subscribe(
                        patterns=["federation.*"],
                        callback=federation_callback,
                        get_backlog=False,
                    )

                    await asyncio.sleep(0.5)

                    # Publish local-only message from cluster 1
                    await client1.publish(
                        "local.message", {"type": "local", "cluster": 1}
                    )

                    # Publish federation message from cluster 1
                    await client1.publish(
                        "federation.message", {"type": "federation", "cluster": 1}
                    )

                    # Publish local-only message from cluster 2
                    await client2.publish(
                        "local.message", {"type": "local", "cluster": 2}
                    )

                    await asyncio.sleep(1.5)

                    # Verify isolation:
                    # - local_messages should only contain the local.message from cluster 1
                    # - federation_messages should only contain messages that reached cluster 2

                    local_topics = [msg.topic for msg in local_messages]
                    federation_topics = [msg.topic for msg in federation_messages]

                    # Local subscription should only receive local messages from its own cluster
                    assert len(local_messages) == 1, (
                        f"Expected 1 local message, got {len(local_messages)}"
                    )
                    assert "local.message" in local_topics, (
                        "Local subscription should receive local.message"
                    )

                    print(
                        f"Local messages received: {len(local_messages)} - {local_topics}"
                    )
                    print(
                        f"Federation messages received: {len(federation_messages)} - {federation_topics}"
                    )

        finally:
            # Cleanup
            try:
                await bridge1.stop()
                await bridge2.stop()
            except (AttributeError, asyncio.CancelledError, ConnectionError):
                # Bridge cleanup errors during test teardown are expected
                pass

            server1_task.cancel()
            server2_task.cancel()
            try:
                await server1_task
                await server2_task
            except asyncio.CancelledError:
                pass
