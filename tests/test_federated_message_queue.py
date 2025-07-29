"""
Comprehensive test suite for federated message queue operations.

Tests all aspects of the federated message queue system including:
- Cross-cluster queue discovery and subscription
- Federation-aware delivery guarantees
- Multi-hop acknowledgment routing
- Gossip protocol integration for queue advertisements
- Byzantine fault tolerance in consensus operations
- Circuit breaker protection for federation failures
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mpreg.core.federated_delivery_guarantees import (
    ClusterWeight,
    FederatedDeliveryCoordinator,
    GlobalConsensusRound,
)
from mpreg.core.federated_message_queue import (
    FederatedMessageQueueManager,
    FederatedSubscription,
    QueueAdvertisement,
)
from mpreg.core.federated_queue_gossip import (
    FederatedQueueGossipProtocol,
    QueueAdvertisementGossipPayload,
    QueueDiscoveryRequestPayload,
)
from mpreg.core.message_queue import (
    DeliveryGuarantee,
    MessageIdStr,
    QueueConfiguration,
    QueuedMessage,
)
from mpreg.core.message_queue_manager import QueueManagerConfiguration


@pytest.fixture
def mock_federation_bridge():
    """Create a mock federation bridge for testing."""
    bridge = MagicMock()
    bridge.local_cluster = MagicMock()
    bridge.local_cluster.publish_message = MagicMock()
    bridge.local_cluster.publish_message_async = AsyncMock()
    # Legacy support for old interface
    bridge.topic_exchange = bridge.local_cluster
    return bridge


@pytest.fixture
def cluster_config():
    """Create test cluster configuration."""
    return QueueManagerConfiguration(
        default_max_queue_size=1000, enable_auto_queue_creation=True, max_queues=100
    )


@pytest.fixture
async def federated_queue_manager(mock_federation_bridge, cluster_config):
    """Create a federated queue manager for testing."""
    manager = FederatedMessageQueueManager(
        config=cluster_config,
        federation_bridge=mock_federation_bridge,
        local_cluster_id="test-cluster-1",
    )
    yield manager
    await manager.shutdown()


@pytest.fixture
async def delivery_coordinator(mock_federation_bridge):
    """Create a delivery coordinator for testing."""
    coordinator = FederatedDeliveryCoordinator(
        cluster_id="test-cluster-1",
        federation_bridge=mock_federation_bridge,
        federated_queue_manager=MagicMock(),
    )
    yield coordinator
    await coordinator.shutdown()


@pytest.fixture
async def gossip_protocol(mock_federation_bridge):
    """Create a gossip protocol for testing."""
    base_gossip = MagicMock()
    base_gossip.gossip_message = AsyncMock()

    protocol = FederatedQueueGossipProtocol(
        cluster_id="test-cluster-1",
        base_gossip_protocol=base_gossip,
        federated_queue_manager=MagicMock(),
    )
    yield protocol
    await protocol.shutdown()


class TestFederatedMessageQueueManager:
    """Test federated message queue manager functionality."""

    async def test_create_federated_queue(self, federated_queue_manager):
        """Test creating and advertising a federated queue."""
        # Create a federated queue
        success = await federated_queue_manager.create_federated_queue(
            "test-queue", QueueConfiguration(name="test-queue"), advertise_globally=True
        )

        assert success
        assert "test-queue" in federated_queue_manager.local_queue_manager.list_queues()

        # Check that advertisement was created
        assert len(federated_queue_manager.queue_advertisements) > 0

    async def test_global_subscription(self, federated_queue_manager):
        """Test global subscription across federation."""
        # Create a test queue first
        await federated_queue_manager.create_federated_queue("global-test-queue")

        # Create global subscription
        subscription_id = await federated_queue_manager.subscribe_globally(
            subscriber_id="test-subscriber",
            queue_pattern="global-*",
            topic_pattern="test.*",
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
        )

        assert subscription_id is not None
        assert subscription_id in federated_queue_manager.federated_subscriptions

        # Check subscription details
        subscription = federated_queue_manager.federated_subscriptions[subscription_id]
        assert subscription.subscriber_id == "test-subscriber"
        assert subscription.queue_pattern == "global-*"
        assert subscription.topic_pattern == "test.*"

    async def test_discover_federated_queues(self, federated_queue_manager):
        """Test federated queue discovery."""
        # Create local queues
        await federated_queue_manager.create_federated_queue("local-queue-1")
        await federated_queue_manager.create_federated_queue("local-queue-2")

        # Add some mock remote queues
        remote_ad = QueueAdvertisement(
            cluster_id="remote-cluster",
            queue_name="remote-queue-1",
            supported_guarantees=[DeliveryGuarantee.AT_LEAST_ONCE],
            queue_health="healthy",
        )
        federated_queue_manager.remote_queues["remote-cluster"]["remote-queue-1"] = (
            remote_ad
        )

        # Discover all queues
        discovered = await federated_queue_manager.discover_federated_queues()

        # Should find local queues
        assert "test-cluster-1" in discovered
        local_queues = {ad.queue_name for ad in discovered["test-cluster-1"]}
        assert "local-queue-1" in local_queues
        assert "local-queue-2" in local_queues

        # Should find remote queues
        assert "remote-cluster" in discovered
        remote_queues = {ad.queue_name for ad in discovered["remote-cluster"]}
        assert "remote-queue-1" in remote_queues

    async def test_send_message_globally_local(self, federated_queue_manager):
        """Test sending message to local queue via global interface."""
        # Create local queue
        await federated_queue_manager.create_federated_queue("local-target")

        # Send message globally (should route to local)
        result = await federated_queue_manager.send_message_globally(
            queue_name="local-target",
            topic="test.message",
            payload={"test": "data"},
            delivery_guarantee=DeliveryGuarantee.FIRE_AND_FORGET,
        )

        assert result.success
        assert result.message_id is not None

    async def test_send_message_globally_remote(self, federated_queue_manager):
        """Test sending message to remote queue via federation."""
        # Add mock remote queue
        remote_ad = QueueAdvertisement(
            cluster_id="remote-cluster",
            queue_name="remote-target",
            supported_guarantees=[DeliveryGuarantee.AT_LEAST_ONCE],
            queue_health="healthy",
        )
        federated_queue_manager.remote_queues["remote-cluster"]["remote-target"] = (
            remote_ad
        )

        # Send message globally (should route via federation)
        result = await federated_queue_manager.send_message_globally(
            queue_name="remote-target",
            topic="test.message",
            payload={"test": "remote_data"},
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            target_cluster="remote-cluster",
        )

        assert result.success
        assert result.message_id is not None

        # Verify federation message was sent
        federated_queue_manager.federation_bridge.local_cluster.publish_message.assert_called()

    async def test_acknowledge_federated_message(self, federated_queue_manager):
        """Test acknowledging federated messages."""
        # Create mock federated in-flight message
        message = QueuedMessage(
            id=MagicMock(),
            topic="test.topic",
            payload={"test": "data"},
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
        )

        ack_token = "test-ack-token"
        federated_queue_manager.federated_in_flight[ack_token] = MagicMock()
        federated_queue_manager.federated_in_flight[ack_token].message = message

        # Acknowledge the message
        success = await federated_queue_manager.acknowledge_federated_message(
            ack_token=ack_token, subscriber_id="test-subscriber", success=True
        )

        assert success
        assert (
            federated_queue_manager.federation_stats.cross_cluster_acknowledgments == 1
        )

    async def test_federation_statistics(self, federated_queue_manager):
        """Test federation statistics tracking."""
        stats = federated_queue_manager.get_federation_statistics()

        assert stats.total_federated_messages >= 0
        assert stats.successful_cross_cluster_deliveries >= 0
        assert stats.failed_federation_attempts >= 0
        assert stats.federation_success_rate() >= 0.0
        assert stats.federation_success_rate() <= 1.0


class TestFederatedDeliveryCoordinator:
    """Test federated delivery coordinator functionality."""

    async def test_global_quorum_consensus(self, delivery_coordinator):
        """Test global quorum consensus delivery."""
        # Create test message
        message = QueuedMessage(
            id=MagicMock(),
            topic="consensus.test",
            payload={"consensus": "data"},
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
        )

        target_clusters = {"cluster-1", "cluster-2", "cluster-3"}

        # Mock successful consensus
        with patch.object(delivery_coordinator, "_wait_for_consensus") as mock_wait:
            # Create a mock result object
            mock_result = type(
                "MockResult", (), {"success": True, "message_id": message.id}
            )()
            mock_wait.return_value = mock_result

            result = await delivery_coordinator.deliver_with_global_quorum(
                message=message,
                target_clusters=target_clusters,
                required_weight_threshold=0.67,
            )

        assert result.success
        assert delivery_coordinator.delivery_stats.global_consensus_rounds == 1
        assert delivery_coordinator.delivery_stats.successful_global_consensus == 1

    async def test_consensus_vote_handling(self, delivery_coordinator):
        """Test handling consensus votes from clusters."""
        # Create consensus round
        consensus_round = GlobalConsensusRound(
            consensus_id="test-consensus",
            target_clusters={"cluster-1", "cluster-2"},
            cluster_weights={
                "cluster-1": ClusterWeight(cluster_id="cluster-1", weight=1.0),
                "cluster-2": ClusterWeight(cluster_id="cluster-2", weight=1.0),
            },
        )
        delivery_coordinator.active_consensus_rounds["test-consensus"] = consensus_round

        # Handle votes
        await delivery_coordinator.handle_consensus_vote(
            vote_payload="test-consensus",
            voting_cluster="cluster-1",
            vote=True,
            vector_clock={"cluster-1": 1},
        )

        await delivery_coordinator.handle_consensus_vote(
            vote_payload="test-consensus",
            voting_cluster="cluster-2",
            vote=True,
            vector_clock={"cluster-2": 1},
        )

        # Check votes were recorded
        assert consensus_round.votes_received["cluster-1"] is True
        assert consensus_round.votes_received["cluster-2"] is True
        assert consensus_round.has_quorum()

    async def test_byzantine_fault_detection(self, delivery_coordinator):
        """Test Byzantine fault detection in consensus."""
        # Create consensus round with conflicting responses
        consensus_round = GlobalConsensusRound(
            consensus_id="byzantine-test",
            cluster_weights={
                "honest-cluster": ClusterWeight(
                    cluster_id="honest-cluster", weight=1.0
                ),
                "byzantine-cluster": ClusterWeight(
                    cluster_id="byzantine-cluster", weight=1.0
                ),
            },
        )

        # Add conflicting responses from Byzantine cluster
        consensus_round.conflicting_responses["byzantine-cluster"] = [
            {"response": "A"},
            {"response": "B"},  # Conflicting response
        ]

        # Detect Byzantine faults
        byzantine_clusters = consensus_round.detect_byzantine_faults()

        assert "byzantine-cluster" in byzantine_clusters
        assert "honest-cluster" not in byzantine_clusters

    async def test_causal_consistency_delivery(self, delivery_coordinator):
        """Test causal consistency delivery."""
        message = QueuedMessage(
            id=MagicMock(),
            topic="causal.test",
            payload={"causal": "data"},
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
        )

        vector_clock = {"cluster-1": 1, "cluster-2": 2}
        causal_dependencies: set[MessageIdStr] = set()
        target_clusters = {"cluster-1", "cluster-2"}

        # Mock causal delivery check
        with patch.object(
            delivery_coordinator, "_can_deliver_causally"
        ) as mock_can_deliver:
            mock_can_deliver.return_value = True

            with patch.object(
                delivery_coordinator, "_deliver_to_clusters"
            ) as mock_deliver:
                # Create a mock result object
                mock_result = type(
                    "MockResult",
                    (),
                    {
                        "success": True,
                        "message_id": message.id,
                        "delivered_to": target_clusters,
                    },
                )()
                mock_deliver.return_value = mock_result

                result = await delivery_coordinator.deliver_with_causal_consistency(
                    message=message,
                    vector_clock=vector_clock,
                    causal_dependencies=causal_dependencies,
                    target_clusters=target_clusters,
                )

        assert result.success

    async def test_federated_broadcast_delivery(self, delivery_coordinator):
        """Test federated broadcast delivery."""
        message = QueuedMessage(
            id=MagicMock(),
            topic="broadcast.test",
            payload={"broadcast": "data"},
            delivery_guarantee=DeliveryGuarantee.BROADCAST,
        )

        target_clusters = {"cluster-1", "cluster-2", "cluster-3"}

        # Mock successful delivery to all clusters
        with patch.object(
            delivery_coordinator, "_deliver_to_single_cluster"
        ) as mock_deliver:
            mock_deliver.return_value = True

            result = await delivery_coordinator.deliver_with_federated_broadcast(
                message=message,
                target_clusters=target_clusters,
                require_all_clusters=True,
            )

        assert result.success
        assert len(result.delivered_to) == 3
        assert delivery_coordinator.delivery_stats.federated_broadcast_deliveries == 1


class TestFederatedQueueGossipProtocol:
    """Test federated queue gossip protocol functionality."""

    async def test_gossip_queue_advertisement(self, gossip_protocol):
        """Test gossiping queue advertisements."""
        advertisement = QueueAdvertisement(
            cluster_id="test-cluster-1",
            queue_name="gossip-test-queue",
            supported_guarantees=[DeliveryGuarantee.AT_LEAST_ONCE],
            queue_health="healthy",
        )

        await gossip_protocol.gossip_queue_advertisement(advertisement, "advertise")

        # Check advertisement was stored
        assert len(gossip_protocol.advertised_queues) == 1
        assert gossip_protocol.gossip_stats.advertisements_sent == 1

        # Check vector clock was incremented
        assert gossip_protocol.queue_vector_clock["test-cluster-1"] == 1

    async def test_handle_queue_advertisement(self, gossip_protocol):
        """Test handling incoming queue advertisements."""
        advertisement = QueueAdvertisement(
            cluster_id="remote-cluster",
            queue_name="remote-queue",
            supported_guarantees=[DeliveryGuarantee.BROADCAST],
            queue_health="healthy",
        )

        payload = QueueAdvertisementGossipPayload(
            cluster_id="remote-cluster",
            vector_clock={"remote-cluster": 5},
            advertisement=advertisement,
            operation="advertise",
        )

        # Ensure queue_manager mock has required attributes
        gossip_protocol.queue_manager.remote_queues = {}
        gossip_protocol.queue_manager.queue_advertisements = {}

        await gossip_protocol._handle_queue_advertisement(
            payload.to_dict(), "remote-cluster"
        )

        # Check vector clock was updated
        assert gossip_protocol.queue_vector_clock["remote-cluster"] == 5
        assert gossip_protocol.gossip_stats.advertisements_received == 1

    async def test_queue_discovery_request(self, gossip_protocol):
        """Test queue discovery request handling."""
        # Add mock queue advertisements to manager
        gossip_protocol.queue_manager.queue_advertisements = {
            "ad-1": QueueAdvertisement(
                cluster_id="test-cluster-1",
                queue_name="discoverable-queue",
                supported_guarantees=[DeliveryGuarantee.AT_LEAST_ONCE],
                queue_health="healthy",
            )
        }

        request_payload = QueueDiscoveryRequestPayload(
            cluster_id="requesting-cluster",
            requesting_cluster="requesting-cluster",
            queue_pattern="discoverable-*",
            discovery_id="test-discovery",
        )

        await gossip_protocol._handle_discovery_request(
            request_payload.to_dict(), "requesting-cluster"
        )

        # Check that response was sent (via gossip)
        assert gossip_protocol.gossip_stats.discovery_responses_sent == 1

    async def test_gossip_statistics(self, gossip_protocol):
        """Test gossip statistics tracking."""
        stats = gossip_protocol.get_gossip_statistics()

        assert stats.advertisements_sent >= 0
        assert stats.advertisements_received >= 0
        assert stats.discovery_requests_sent >= 0
        assert stats.discovery_responses_sent >= 0
        assert stats.active_queue_advertisements >= 0


class TestDataStructures:
    """Test federated queue data structures."""

    def test_queue_advertisement_expiration(self):
        """Test queue advertisement expiration logic."""
        # Create advertisement that should be expired
        old_advertisement = QueueAdvertisement(
            cluster_id="test-cluster",
            queue_name="old-queue",
            advertised_at=time.time() - 400,  # 400 seconds ago
            ttl_seconds=300.0,  # 5 minute TTL
        )

        assert old_advertisement.is_expired()

        # Create fresh advertisement
        fresh_advertisement = QueueAdvertisement(
            cluster_id="test-cluster", queue_name="fresh-queue", ttl_seconds=300.0
        )

        assert not fresh_advertisement.is_expired()

    def test_federated_subscription_structure(self):
        """Test federated subscription data structure."""
        subscription = FederatedSubscription(
            subscriber_id="test-subscriber",
            local_cluster_id="local-cluster",
            queue_pattern="test-*",
            topic_pattern="events.*",
            target_clusters={"cluster-1", "cluster-2"},
            delivery_guarantee=DeliveryGuarantee.QUORUM,
        )

        assert subscription.subscriber_id == "test-subscriber"
        assert subscription.local_cluster_id == "local-cluster"
        assert subscription.queue_pattern == "test-*"
        assert subscription.topic_pattern == "events.*"
        assert len(subscription.target_clusters) == 2
        assert subscription.delivery_guarantee == DeliveryGuarantee.QUORUM

    def test_cluster_weight_calculations(self):
        """Test cluster weight calculations for consensus."""
        # Trusted cluster with good reliability
        trusted_weight = ClusterWeight(
            cluster_id="trusted-cluster",
            weight=2.0,
            reliability_score=0.95,
            is_trusted=True,
        )

        assert trusted_weight.effective_weight() == 1.9  # 2.0 * 0.95

        # Untrusted cluster
        untrusted_weight = ClusterWeight(
            cluster_id="untrusted-cluster",
            weight=2.0,
            reliability_score=0.95,
            is_trusted=False,
        )

        assert untrusted_weight.effective_weight() == 0.0

    def test_global_consensus_round_quorum(self):
        """Test global consensus round quorum calculations."""
        consensus_round = GlobalConsensusRound(
            cluster_weights={
                "cluster-1": ClusterWeight(cluster_id="cluster-1", weight=1.0),
                "cluster-2": ClusterWeight(cluster_id="cluster-2", weight=1.0),
                "cluster-3": ClusterWeight(cluster_id="cluster-3", weight=1.0),
            },
            required_weight_threshold=0.67,  # Need 67% of weight
        )

        # No votes yet
        assert not consensus_round.has_quorum()

        # One vote (33% weight)
        consensus_round.votes_received["cluster-1"] = True
        assert not consensus_round.has_quorum()

        # Two votes (67% weight)
        consensus_round.votes_received["cluster-2"] = True
        assert consensus_round.has_quorum()


class TestIntegration:
    """Integration tests for federated queue system."""

    async def test_end_to_end_federated_delivery(self):
        """Test complete end-to-end federated message delivery."""
        # Create mock federation bridge
        mock_bridge = MagicMock()
        mock_bridge.topic_exchange = MagicMock()
        mock_bridge.topic_exchange.publish_message_async = AsyncMock()

        # Create federated queue manager
        config = QueueManagerConfiguration()
        manager = FederatedMessageQueueManager(
            config=config,
            federation_bridge=mock_bridge,
            local_cluster_id="integration-test-cluster",
        )

        try:
            # Create queue and subscription
            await manager.create_federated_queue("integration-queue")

            subscription_id = await manager.subscribe_globally(
                subscriber_id="integration-subscriber",
                queue_pattern="integration-*",
                topic_pattern="integration.*",
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            )

            # Send message
            result = await manager.send_message_globally(
                queue_name="integration-queue",
                topic="integration.test",
                payload={"integration": "test_data"},
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            )

            # Verify results
            assert result.success
            assert subscription_id in manager.federated_subscriptions
            assert "integration-queue" in manager.local_queue_manager.list_queues()

        finally:
            await manager.shutdown()

    async def test_federation_failure_handling(self):
        """Test handling of federation failures and circuit breakers."""
        # Create mock federation bridge that fails
        mock_bridge = MagicMock()
        mock_bridge.local_cluster = MagicMock()
        mock_bridge.local_cluster.publish_message_async = AsyncMock(
            side_effect=Exception("Federation failure")
        )
        mock_bridge.local_cluster.publish_message = MagicMock(
            side_effect=Exception("Federation failure")
        )

        config = QueueManagerConfiguration()
        manager = FederatedMessageQueueManager(
            config=config,
            federation_bridge=mock_bridge,
            local_cluster_id="failure-test-cluster",
        )

        try:
            # Add mock remote queue
            remote_ad = QueueAdvertisement(
                cluster_id="failing-cluster",
                queue_name="remote-queue",
                supported_guarantees=[DeliveryGuarantee.AT_LEAST_ONCE],
                queue_health="healthy",
            )
            manager.remote_queues["failing-cluster"]["remote-queue"] = remote_ad

            # Attempt to send message to failing cluster
            result = await manager.send_message_globally(
                queue_name="remote-queue",
                topic="failure.test",
                payload={"test": "failure"},
                target_cluster="failing-cluster",
            )

            # Should handle failure gracefully
            assert not result.success
            assert "Federation failure" in (result.error_message or "")
            assert manager.federation_stats.failed_federation_attempts > 0

        finally:
            await manager.shutdown()


# Run tests with proper async support
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
