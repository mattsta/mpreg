"""
Simple live tests for federated message queue system.

This module provides basic live tests that validate the federated message queue system
works correctly with actual running MPREG servers, starting with single-server tests
and building up to simple cross-cluster scenarios.
"""

import asyncio
import time

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.federated_message_queue import (
    FederatedMessageQueueManager,
)
from mpreg.core.message_queue import DeliveryGuarantee, QueueConfiguration, QueueType
from mpreg.core.message_queue_manager import QueueManagerConfiguration
from mpreg.core.topic_exchange import TopicExchange
from mpreg.federation.federation_bridge import GraphAwareFederationBridge
from mpreg.federation.federation_optimized import ClusterIdentity
from mpreg.server import MPREGServer

from .conftest import AsyncTestContext


@pytest.fixture
async def simple_federated_setup(
    test_context: AsyncTestContext,
    server_port: int,
):
    """Creates a simple single-server federated queue setup for testing."""
    # Create server
    settings = MPREGSettings(
        host="127.0.0.1",
        port=server_port,
        name="Simple Fed Server",
        cluster_id="simple-fed-cluster",
        resources={"queue-processor", "federation-hub"},
        peers=None,
        connect=None,
        advertised_urls=None,
        gossip_interval=1.0,
    )

    server = MPREGServer(settings=settings)
    test_context.servers.append(server)

    # Start server
    server_task = asyncio.create_task(server.server())
    test_context.tasks.append(server_task)
    await asyncio.sleep(0.3)  # Give server time to start

    # Create topic exchange
    topic_exchange = TopicExchange(
        server_url=f"ws://127.0.0.1:{server_port}", cluster_id="simple-fed-cluster"
    )

    # Create cluster identity
    cluster_identity = ClusterIdentity(
        cluster_id="simple-fed-cluster",
        cluster_name="Simple Fed Server",
        region="local",
        bridge_url=f"ws://127.0.0.1:{server_port}",
        public_key_hash="test_hash",
        created_at=time.time(),
        geographic_coordinates=(0.0, 0.0),
        network_tier=1,
        max_bandwidth_mbps=1000,
        preference_weight=1.0,
    )

    # Create federation bridge
    federation_bridge = GraphAwareFederationBridge(
        local_cluster=topic_exchange,
        cluster_identity=cluster_identity,
        enable_graph_routing=True,
        enable_monitoring=True,
    )

    # Create federated queue manager
    config = QueueManagerConfiguration(
        max_queues=100,
        default_max_queue_size=1000,
        enable_auto_queue_creation=True,
    )

    manager = FederatedMessageQueueManager(
        config=config,
        federation_bridge=federation_bridge,
        local_cluster_id="simple-fed-cluster",
    )

    # Allow federation workers to start
    await asyncio.sleep(0.5)

    try:
        yield server, manager
    finally:
        # Proper cleanup of async objects
        try:
            await manager.shutdown()
        except Exception as e:
            print(f"Warning: Failed to shutdown manager: {e}")

        try:
            await federation_bridge.stop()
        except Exception as e:
            print(f"Warning: Failed to stop federation bridge: {e}")

        # Topic exchange cleanup would go here if it had a shutdown method
        # Currently TopicExchange doesn't implement shutdown/stop/close
        pass


class TestSimpleLiveFederatedMessageQueues:
    """Test federated message queues with live servers - basic functionality."""

    @pytest.mark.asyncio
    async def test_create_federated_queue_locally(
        self, simple_federated_setup: tuple[MPREGServer, FederatedMessageQueueManager]
    ):
        """Test creating a federated queue on a single server."""
        server, manager = simple_federated_setup

        # Create a federated queue
        queue_config = QueueConfiguration(
            name="simple-test-queue",
            queue_type=QueueType.FIFO,
            max_size=1000,
            enable_deduplication=True,
        )

        result = await manager.create_federated_queue(
            "simple-test-queue", queue_config, advertise_globally=True
        )

        assert result is True

        # Verify queue exists in local manager
        local_queues = manager.local_queue_manager.list_queues()
        assert "simple-test-queue" in local_queues

        # Verify queue advertisement was created
        assert len(manager.queue_advertisements) >= 1

        # Check that we can discover our own queue
        discovered = await manager.discover_federated_queues("simple-test-*")
        assert "simple-fed-cluster" in discovered
        assert len(discovered["simple-fed-cluster"]) >= 1

        found_queue = next(
            (
                q
                for q in discovered["simple-fed-cluster"]
                if q.queue_name == "simple-test-queue"
            ),
            None,
        )
        assert found_queue is not None
        assert found_queue.cluster_id == "simple-fed-cluster"

    @pytest.mark.asyncio
    async def test_send_message_locally(
        self, simple_federated_setup: tuple[MPREGServer, FederatedMessageQueueManager]
    ):
        """Test sending messages within the same cluster via federated interface."""
        server, manager = simple_federated_setup

        # Create queue
        await manager.create_federated_queue(
            "local-message-queue",
            QueueConfiguration(
                name="local-message-queue", queue_type=QueueType.PRIORITY, max_size=1000
            ),
            advertise_globally=True,
        )

        # Send message globally (should route locally)
        result = await manager.send_message_globally(
            queue_name="local-message-queue",
            topic="test.local.message",
            payload={"message": "Hello local queue!", "timestamp": time.time()},
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
        )

        assert result.success
        assert result.message_id is not None

        # Check federation statistics
        stats = manager.get_federation_statistics()
        assert stats.total_federated_messages >= 0  # May be 0 if routed locally

    @pytest.mark.asyncio
    async def test_global_subscription(
        self, simple_federated_setup: tuple[MPREGServer, FederatedMessageQueueManager]
    ):
        """Test global subscription within a single cluster."""
        server, manager = simple_federated_setup

        # Create queue
        await manager.create_federated_queue(
            "subscription-test-queue",
            QueueConfiguration(
                name="subscription-test-queue", queue_type=QueueType.FIFO, max_size=500
            ),
            advertise_globally=True,
        )

        # Create global subscription
        subscription_id = await manager.subscribe_globally(
            subscriber_id="test-subscriber",
            queue_pattern="subscription-test-*",
            topic_pattern="test.*",
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
        )

        assert subscription_id is not None
        assert subscription_id in manager.federated_subscriptions

        # Check subscription details
        subscription = manager.federated_subscriptions[subscription_id]
        assert subscription.subscriber_id == "test-subscriber"
        assert subscription.queue_pattern == "subscription-test-*"
        assert subscription.topic_pattern == "test.*"
        assert subscription.local_cluster_id == "simple-fed-cluster"

    @pytest.mark.asyncio
    async def test_federation_statistics(
        self, simple_federated_setup: tuple[MPREGServer, FederatedMessageQueueManager]
    ):
        """Test federation statistics collection."""
        server, manager = simple_federated_setup

        # Get initial statistics
        stats = manager.get_federation_statistics()

        # Should have basic structure
        assert hasattr(stats, "total_federated_messages")
        assert hasattr(stats, "successful_cross_cluster_deliveries")
        assert hasattr(stats, "failed_federation_attempts")
        assert hasattr(stats, "active_federated_subscriptions")
        assert hasattr(stats, "known_remote_queues")

        # Should start with reasonable values
        assert stats.total_federated_messages >= 0
        assert stats.successful_cross_cluster_deliveries >= 0
        assert stats.failed_federation_attempts >= 0
        assert stats.federation_success_rate() >= 0.0
        assert stats.federation_success_rate() <= 1.0

    @pytest.mark.asyncio
    async def test_queue_patterns_and_discovery(
        self, simple_federated_setup: tuple[MPREGServer, FederatedMessageQueueManager]
    ):
        """Test queue pattern matching and discovery functionality."""
        server, manager = simple_federated_setup

        # Create multiple queues with different patterns
        queue_configs = [
            ("user-queue-auth", QueueType.PRIORITY),
            ("user-queue-billing", QueueType.FIFO),
            ("system-queue-logs", QueueType.PRIORITY),
            ("system-queue-metrics", QueueType.FIFO),
        ]

        for queue_name, queue_type in queue_configs:
            await manager.create_federated_queue(
                queue_name,
                QueueConfiguration(
                    name=queue_name, queue_type=queue_type, max_size=1000
                ),
                advertise_globally=True,
            )

        # Test pattern-based discovery
        user_queues = await manager.discover_federated_queues("user-queue-*")
        system_queues = await manager.discover_federated_queues("system-queue-*")
        all_queues = await manager.discover_federated_queues("*")

        # Verify pattern matching works
        assert "simple-fed-cluster" in user_queues
        assert len(user_queues["simple-fed-cluster"]) == 2

        assert "simple-fed-cluster" in system_queues
        assert len(system_queues["simple-fed-cluster"]) == 2

        assert "simple-fed-cluster" in all_queues
        assert len(all_queues["simple-fed-cluster"]) >= 4

        # Verify specific queues are found
        user_queue_names = {q.queue_name for q in user_queues["simple-fed-cluster"]}
        assert "user-queue-auth" in user_queue_names
        assert "user-queue-billing" in user_queue_names

        system_queue_names = {q.queue_name for q in system_queues["simple-fed-cluster"]}
        assert "system-queue-logs" in system_queue_names
        assert "system-queue-metrics" in system_queue_names


# Simple integration test that validates the complete pipeline
@pytest.mark.asyncio
async def test_simple_end_to_end_federated_workflow(
    simple_federated_setup: tuple[MPREGServer, FederatedMessageQueueManager],
):
    """Complete end-to-end test of federated message queue workflow within single server."""
    server, manager = simple_federated_setup

    # Phase 1: Set up queues
    workflow_queues = [
        ("workflow-input", QueueType.FIFO),
        ("workflow-processing", QueueType.PRIORITY),
        ("workflow-output", QueueType.FIFO),
    ]

    for queue_name, queue_type in workflow_queues:
        result = await manager.create_federated_queue(
            queue_name,
            QueueConfiguration(
                name=queue_name,
                queue_type=queue_type,
                max_size=1000,
                enable_deduplication=True,
            ),
            advertise_globally=True,
        )
        assert result is True

    # Phase 2: Set up subscriptions
    subscriptions = []
    for queue_name, _ in workflow_queues:
        subscription_id = await manager.subscribe_globally(
            subscriber_id=f"processor-{queue_name}",
            queue_pattern=queue_name,
            topic_pattern="workflow.*",
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
        )
        subscriptions.append(subscription_id)
        assert subscription_id is not None

    # Phase 3: Execute workflow steps
    workflow_data = {
        "workflow_id": "simple-test-workflow-001",
        "user_id": "user-12345",
        "operation": "data_processing",
        "step": 1,
    }

    # Step 1: Submit to input queue
    input_result = await manager.send_message_globally(
        queue_name="workflow-input",
        topic="workflow.start",
        payload=workflow_data,
        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
    )
    assert input_result.success

    # Step 2: Process and forward
    processing_data = {**workflow_data, "step": 2, "status": "processing"}
    processing_result = await manager.send_message_globally(
        queue_name="workflow-processing",
        topic="workflow.process",
        payload=processing_data,
        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
    )
    assert processing_result.success

    # Step 3: Complete workflow
    output_data = {
        **workflow_data,
        "step": 3,
        "status": "completed",
        "result": "success",
    }
    output_result = await manager.send_message_globally(
        queue_name="workflow-output",
        topic="workflow.complete",
        payload=output_data,
        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
    )
    assert output_result.success

    # Phase 4: Verify results
    stats = manager.get_federation_statistics()
    assert stats.active_federated_subscriptions == 3

    # Verify all queues are discoverable
    discovered = await manager.discover_federated_queues("workflow-*")
    assert "simple-fed-cluster" in discovered
    assert len(discovered["simple-fed-cluster"]) >= 3

    workflow_queue_names = {q.queue_name for q in discovered["simple-fed-cluster"]}
    assert "workflow-input" in workflow_queue_names
    assert "workflow-processing" in workflow_queue_names
    assert "workflow-output" in workflow_queue_names

    print("✅ Simple end-to-end workflow completed successfully:")
    print(f"   Subscriptions: {len(subscriptions)}")
    print(f"   Queues discovered: {len(discovered['simple-fed-cluster'])}")
    print(
        f"   Federation statistics: {stats.active_federated_subscriptions} active subscriptions"
    )
