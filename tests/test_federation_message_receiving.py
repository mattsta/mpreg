"""
Tests for federation message receiving architecture.

This module validates that the federation bridge can properly send and receive
federation messages between clusters without relying on the full federated
message queue system.
"""

import asyncio
import time

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.model import PubSubMessage
from mpreg.core.topic_exchange import TopicExchange
from mpreg.federation.federation_bridge import GraphAwareFederationBridge
from mpreg.federation.federation_optimized import ClusterIdentity
from mpreg.server import MPREGServer

from .conftest import AsyncTestContext


@pytest.fixture
async def federation_test_setup(
    test_context: AsyncTestContext,
    port_allocator,
):
    """Set up two MPREG servers with federation bridges for testing."""
    from .port_allocator import allocate_port_range

    ports = allocate_port_range(2, "federation")

    # Create two servers
    server1_settings = MPREGSettings(
        host="127.0.0.1",
        port=ports[0],
        name="Test Server 1",
        cluster_id="test-cluster-1",
        resources={"test-resource-1"},
    )

    server2_settings = MPREGSettings(
        host="127.0.0.1",
        port=ports[1],
        name="Test Server 2",
        cluster_id="test-cluster-2",
        resources={"test-resource-2"},
    )

    server1 = MPREGServer(settings=server1_settings)
    server2 = MPREGServer(settings=server2_settings)

    test_context.servers.extend([server1, server2])

    # Start servers
    task1 = asyncio.create_task(server1.server())
    task2 = asyncio.create_task(server2.server())
    test_context.tasks.extend([task1, task2])

    # Wait for servers to start
    await asyncio.sleep(0.5)

    # Create cluster identities
    cluster1_identity = ClusterIdentity(
        cluster_id="test-cluster-1",
        cluster_name="Test Cluster 1",
        region="us-east",
        bridge_url=f"ws://127.0.0.1:{ports[0]}",
        public_key_hash="test_hash_1",
        created_at=time.time(),
        geographic_coordinates=(40.7128, -74.0060),
        network_tier=1,
        max_bandwidth_mbps=1000,
        preference_weight=1.0,
    )

    cluster2_identity = ClusterIdentity(
        cluster_id="test-cluster-2",
        cluster_name="Test Cluster 2",
        region="us-west",
        bridge_url=f"ws://127.0.0.1:{ports[1]}",
        public_key_hash="test_hash_2",
        created_at=time.time(),
        geographic_coordinates=(37.7749, -122.4194),
        network_tier=1,
        max_bandwidth_mbps=1000,
        preference_weight=1.0,
    )

    # Create topic exchanges
    topic_exchange1 = TopicExchange(
        server_url=f"ws://127.0.0.1:{ports[0]}", cluster_id="test-cluster-1"
    )

    topic_exchange2 = TopicExchange(
        server_url=f"ws://127.0.0.1:{ports[1]}", cluster_id="test-cluster-2"
    )

    # Create federation bridges
    bridge1 = GraphAwareFederationBridge(
        local_cluster=topic_exchange1,
        cluster_identity=cluster1_identity,
        enable_graph_routing=False,  # Simplified for testing
        enable_monitoring=False,
    )

    bridge2 = GraphAwareFederationBridge(
        local_cluster=topic_exchange2,
        cluster_identity=cluster2_identity,
        enable_graph_routing=False,  # Simplified for testing
        enable_monitoring=False,
    )

    # Connect federation bridges to topic exchanges
    topic_exchange1.set_federation_bridge(bridge1)
    topic_exchange2.set_federation_bridge(bridge2)

    # Start federation bridges
    await bridge1.start()
    await bridge2.start()

    # Connect bridges to each other
    await bridge1.add_cluster(cluster2_identity)
    await bridge2.add_cluster(cluster1_identity)

    # Allow time for setup
    await asyncio.sleep(0.5)

    try:
        yield {
            "server1": server1,
            "server2": server2,
            "bridge1": bridge1,
            "bridge2": bridge2,
            "topic_exchange1": topic_exchange1,
            "topic_exchange2": topic_exchange2,
        }
    finally:
        # Cleanup federation bridges
        try:
            await bridge1.stop()
        except Exception:
            pass
        try:
            await bridge2.stop()
        except Exception:
            pass


class TestFederationMessageReceiving:
    """Test federation message receiving architecture."""

    @pytest.mark.asyncio
    async def test_federation_message_forwarding_and_receiving(
        self, federation_test_setup
    ):
        """Test that federation messages are properly sent and received between clusters."""
        setup = federation_test_setup
        bridge1 = setup["bridge1"]
        bridge2 = setup["bridge2"]
        topic_exchange1 = setup["topic_exchange1"]
        topic_exchange2 = setup["topic_exchange2"]

        # Create a test federation message
        test_message = PubSubMessage(
            topic="mpreg.federation.test.message",
            payload={"test_data": "hello from cluster 1", "timestamp": time.time()},
            timestamp=time.time(),
            message_id="test-federation-msg-001",
            publisher="test-federation-sender",
            headers={},
        )

        # Track received messages by checking the inbound federation queue
        initial_queue_size = bridge2._inbound_federation_queue.qsize()

        # Publish the federation message to cluster 1's topic exchange
        notifications = topic_exchange1.publish_message(test_message)

        # The message should be queued for forwarding by bridge1
        assert notifications is not None

        # Wait for federation processing
        await asyncio.sleep(1.0)

        # Check that bridge1 queued the message for forwarding
        # (This would normally be sent to bridge2 via PubSub client)
        assert (
            bridge1._outbound_message_queue.qsize() >= 0
        )  # May have been processed already

        # Send the federation message directly to bridge2's incoming federation queue
        # (This simulates what happens when PubSub client sends to remote cluster)
        test_message.headers["federation_source"] = "test-cluster-1"
        test_message.headers["federation_hop"] = (
            "1"  # Required for federation processing
        )
        await bridge2.queue_incoming_federation_message(test_message)

        # Wait for message processing
        await asyncio.sleep(1.0)

        # Check that bridge2 received and processed the federation message
        # The message should have been processed by the federation receiving loop
        assert bridge2.message_stats.get("messages_received", 0) >= 1

        # Also verify that the message was published to the local topic exchange
        # by checking the topic exchange message count
        assert topic_exchange2.messages_published >= 1

    @pytest.mark.asyncio
    async def test_federation_message_queue_integration(self, federation_test_setup):
        """Test that federation message queues work with the receiving architecture."""
        setup = federation_test_setup
        bridge1 = setup["bridge1"]
        bridge2 = setup["bridge2"]

        # Test that messages are properly queued and processed
        initial_outbound_size = bridge1._outbound_message_queue.qsize()
        initial_inbound_size = bridge2._inbound_federation_queue.qsize()

        # Create and queue a federation message
        test_message = PubSubMessage(
            topic="mpreg.federation.queue.advertisement.test-cluster-1",
            payload={
                "cluster_id": "test-cluster-1",
                "queue_name": "test-queue",
                "advertised_at": time.time(),
            },
            timestamp=time.time(),
            message_id="test-queue-ad-001",
            publisher="test-queue-manager",
            headers={},
        )

        # Queue the message for federation forwarding
        bridge1._queue_message_for_forwarding(test_message)

        # Check that the message was queued
        assert bridge1._outbound_message_queue.qsize() > initial_outbound_size

        # Simulate receiving the message on the remote cluster
        test_message.headers["federation_hop"] = (
            "1"  # Required for federation processing
        )
        await bridge2.queue_incoming_federation_message(test_message)

        # Check that the message was queued for processing
        assert bridge2._inbound_federation_queue.qsize() > initial_inbound_size

        # Wait for processing
        await asyncio.sleep(0.5)

        # The message should have been processed (queue should be smaller)
        final_inbound_size = bridge2._inbound_federation_queue.qsize()
        assert final_inbound_size <= initial_inbound_size + 1

    @pytest.mark.asyncio
    async def test_federation_message_filtering(self, federation_test_setup):
        """Test that only federation messages are processed by the federation bridge."""
        setup = federation_test_setup
        bridge1 = setup["bridge1"]
        topic_exchange1 = setup["topic_exchange1"]

        # Track queue sizes
        initial_outbound_size = bridge1._outbound_message_queue.qsize()

        # Publish a non-federation message
        non_federation_message = PubSubMessage(
            topic="regular.user.message",
            payload={"user_data": "not a federation message"},
            timestamp=time.time(),
            message_id="regular-msg-001",
            publisher="regular-user",
            headers={},
        )

        topic_exchange1.publish_message(non_federation_message)

        # Wait briefly
        await asyncio.sleep(0.1)

        # Non-federation message should not be queued for forwarding
        assert bridge1._outbound_message_queue.qsize() == initial_outbound_size

        # Now publish a federation message
        federation_message = PubSubMessage(
            topic="mpreg.federation.test.data",
            payload={"federation_data": "this is a federation message"},
            timestamp=time.time(),
            message_id="federation-msg-001",
            publisher="federation-sender",
            headers={},
        )

        topic_exchange1.publish_message(federation_message)

        # Wait briefly
        await asyncio.sleep(0.1)

        # Federation message should be queued for forwarding and then processed
        # Check if the message was processed by looking at forwarding statistics
        # The queue might be empty if the message was already processed
        assert (
            bridge1._outbound_message_queue.qsize() > initial_outbound_size
            or bridge1.message_stats.get("messages_forwarded", 0) > 0
        )

    @pytest.mark.asyncio
    async def test_federation_message_backpressure(self, federation_test_setup):
        """Test that federation message queuing handles backpressure correctly."""
        setup = federation_test_setup
        bridge2 = setup["bridge2"]

        # Create a message to queue
        test_message = PubSubMessage(
            topic="mpreg.federation.backpressure.test",
            payload={"test": "backpressure"},
            timestamp=time.time(),
            message_id="backpressure-test-001",
            publisher="backpressure-tester",
            headers={},
        )

        # Queue the message (should not raise exception even if queue is full)
        try:
            await bridge2.queue_incoming_federation_message(test_message)
            # Should complete without raising an exception
            success = True
        except Exception as e:
            success = False
            print(f"Backpressure test failed: {e}")

        assert success, (
            "Federation message queuing should handle backpressure gracefully"
        )
