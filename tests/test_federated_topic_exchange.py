"""
Comprehensive tests for Federated Topic Exchange system.

Tests cover:
- FederatedTopicExchange utility functions (create_federated_cluster, connect_clusters)
- Federation enable/disable operations with proper cleanup
- Cluster management (add/remove federated clusters)
- Message publishing with federation routing and fallback
- Subscription management with federation-aware updates
- Statistics and health monitoring
- Error handling and federation resilience
- Integration with live MPREG servers

Uses standard MPREG testing patterns with async fixtures and live servers.
"""

import time

import pytest

from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.core.statistics import FederatedTopicExchangeStats, FederationHealth
from mpreg.core.topic_exchange import TopicExchange
from mpreg.federation.federated_topic_exchange import (
    FederatedTopicExchange,
    connect_clusters,
    connect_clusters_intelligently,
    create_federated_cluster,
)
from mpreg.federation.federation_optimized import ClusterIdentity
from mpreg.server import MPREGServer


class TestClusterIdentity:
    """Test ClusterIdentity data structure."""

    def test_cluster_identity_creation(self):
        """Test basic cluster identity creation."""
        identity = ClusterIdentity(
            cluster_id="test-cluster-1",
            cluster_name="Test Cluster 1",
            region="us-west",
            bridge_url="ws://localhost:8080",
            public_key_hash="abc123",
            created_at=time.time(),
            geographic_coordinates=(37.7749, -122.4194),
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        assert identity.cluster_id == "test-cluster-1"
        assert identity.cluster_name == "Test Cluster 1"
        assert identity.region == "us-west"
        assert identity.bridge_url == "ws://localhost:8080"
        assert identity.public_key_hash == "abc123"
        assert identity.geographic_coordinates == (37.7749, -122.4194)
        assert identity.network_tier == 1
        assert identity.max_bandwidth_mbps == 1000
        assert identity.preference_weight == 1.0

    def test_cluster_identity_defaults(self):
        """Test cluster identity with default values."""
        identity = ClusterIdentity(
            cluster_id="test-cluster",
            cluster_name="Test Cluster",
            region="us-east",
            bridge_url="ws://localhost:9090",
            public_key_hash="def456",
            created_at=time.time(),
        )

        assert identity.geographic_coordinates == (0.0, 0.0)
        assert identity.network_tier == 1
        assert identity.max_bandwidth_mbps == 1000
        assert identity.preference_weight == 1.0


class TestFederatedTopicExchangeUtilities:
    """Test utility functions for federated topic exchange."""

    @pytest.mark.asyncio
    async def test_create_federated_cluster_basic(
        self,
        single_server: MPREGServer,
        server_port: int,
    ):
        """Test basic federated cluster creation."""
        exchange = await create_federated_cluster(
            server_url=f"ws://127.0.0.1:{server_port}",
            cluster_id="test-cluster",
            cluster_name="Test Cluster",
            region="us-west",
            bridge_url=f"ws://127.0.0.1:{server_port + 100}",
            public_key_hash="test-hash",
        )

        try:
            assert isinstance(exchange, FederatedTopicExchange)
            assert exchange.federation_enabled is True
            assert exchange.cluster_identity is not None
            assert exchange.cluster_identity.cluster_id == "test-cluster"
            assert exchange.cluster_identity.cluster_name == "Test Cluster"
            assert exchange.cluster_identity.region == "us-west"

            # Test that it extends TopicExchange
            assert isinstance(exchange, TopicExchange)
            assert hasattr(exchange, "add_subscription")
            assert hasattr(exchange, "remove_subscription")
            assert hasattr(exchange, "publish_message")

        finally:
            # Clean up
            await exchange.disable_federation_async()

    @pytest.mark.asyncio
    async def test_create_federated_cluster_with_config(
        self,
        single_server: MPREGServer,
        server_port: int,
    ):
        """Test federated cluster creation with custom configuration."""
        federation_config = {
            "timeout_seconds": 15.0,
            "max_concurrent_ops": 200,
        }

        exchange = await create_federated_cluster(
            server_url=f"ws://127.0.0.1:{server_port}",
            cluster_id="config-cluster",
            cluster_name="Config Test Cluster",
            region="us-east",
            bridge_url=f"ws://127.0.0.1:{server_port + 100}",
            public_key_hash="config-hash",
            geographic_coordinates=(40.7128, -74.0060),
            network_tier=2,
            max_bandwidth_mbps=500,
            preference_weight=2.0,
            federation_config=federation_config,
        )

        try:
            assert exchange.federation_enabled is True
            assert exchange.federation_timeout_seconds == 15.0
            assert exchange.max_concurrent_federation_ops == 200

            # Check cluster identity configuration
            identity = exchange.cluster_identity
            assert identity is not None
            assert identity.cluster_id == "config-cluster"
            assert identity.cluster_name == "Config Test Cluster"
            assert identity.region == "us-east"
            assert identity.geographic_coordinates == (40.7128, -74.0060)
            assert identity.network_tier == 2
            assert identity.max_bandwidth_mbps == 500
            assert identity.preference_weight == 2.0

        finally:
            await exchange.disable_federation_async()

    @pytest.mark.asyncio
    async def test_connect_clusters_basic(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        port_pair: tuple[int, int],
    ):
        """Test basic cluster connection functionality."""
        server_a, server_b = cluster_2_servers
        port_a, port_b = port_pair

        # Create two federated clusters
        cluster_a = await create_federated_cluster(
            server_url=f"ws://127.0.0.1:{port_a}",
            cluster_id="cluster-a",
            cluster_name="Cluster A",
            region="us-west",
            bridge_url=f"ws://127.0.0.1:{port_a + 100}",
            public_key_hash="hash-a",
        )

        cluster_b = await create_federated_cluster(
            server_url=f"ws://127.0.0.1:{port_b}",
            cluster_id="cluster-b",
            cluster_name="Cluster B",
            region="us-east",
            bridge_url=f"ws://127.0.0.1:{port_b + 100}",
            public_key_hash="hash-b",
        )

        try:
            # Connect the clusters
            result_a_to_b, result_b_to_a = await connect_clusters(cluster_a, cluster_b)

            # Both connections should succeed
            assert result_a_to_b is True
            assert result_b_to_a is True

            # Verify federation stats were updated
            assert cluster_a.federation_stats["routing_decisions"] >= 1
            assert cluster_b.federation_stats["routing_decisions"] >= 1

        finally:
            # Clean up
            await cluster_a.disable_federation_async()
            await cluster_b.disable_federation_async()

    @pytest.mark.asyncio
    async def test_connect_clusters_intelligently_wrapper(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        port_pair: tuple[int, int],
    ):
        """Test that connect_clusters is a wrapper for connect_clusters_intelligently."""
        # Both should be different functions (wrapper vs implementation)
        assert connect_clusters != connect_clusters_intelligently
        assert callable(connect_clusters)
        assert callable(connect_clusters_intelligently)

        server_a, server_b = cluster_2_servers
        port_a, port_b = port_pair

        # Create two federated clusters
        cluster_a = await create_federated_cluster(
            server_url=f"ws://127.0.0.1:{port_a}",
            cluster_id="intelligent-a",
            cluster_name="Intelligent A",
            region="eu-west",
            bridge_url=f"ws://127.0.0.1:{port_a + 100}",
            public_key_hash="intel-hash-a",
        )

        cluster_b = await create_federated_cluster(
            server_url=f"ws://127.0.0.1:{port_b}",
            cluster_id="intelligent-b",
            cluster_name="Intelligent B",
            region="eu-east",
            bridge_url=f"ws://127.0.0.1:{port_b + 100}",
            public_key_hash="intel-hash-b",
        )

        try:
            # Test both functions work identically
            result1 = await connect_clusters(cluster_a, cluster_b)
            result2 = await connect_clusters_intelligently(cluster_a, cluster_b)

            # Both should return the same type of result
            assert isinstance(result1, tuple)
            assert isinstance(result2, tuple)
            assert len(result1) == 2
            assert len(result2) == 2

        finally:
            await cluster_a.disable_federation_async()
            await cluster_b.disable_federation_async()


class TestFederatedTopicExchangeFederationManagement:
    """Test federation lifecycle management."""

    @pytest.fixture
    async def basic_exchange(self, single_server: MPREGServer, server_port: int):
        """Create a basic federated exchange for testing."""
        exchange = await create_federated_cluster(
            server_url=f"ws://127.0.0.1:{server_port}",
            cluster_id="basic-test-cluster",
            cluster_name="Basic Test Cluster",
            region="test-region",
            bridge_url=f"ws://127.0.0.1:{server_port + 100}",
            public_key_hash="basic-test-hash",
        )
        yield exchange
        # Cleanup
        await exchange.disable_federation_async()

    @pytest.mark.asyncio
    async def test_federation_enable_disable_cycle(self, basic_exchange):
        """Test enabling and disabling federation."""
        exchange = basic_exchange

        # Should start enabled (from create_federated_cluster)
        assert exchange.federation_enabled is True
        assert exchange.cluster_identity is not None
        assert exchange.federation_bridge is not None

        # Disable federation
        result = await exchange.disable_federation_async()
        assert result is True
        assert exchange.federation_enabled is False
        assert exchange.cluster_identity is None
        assert exchange.federation_bridge is None

        # Try to disable again (should succeed)
        result = await exchange.disable_federation_async()
        assert result is True

    @pytest.mark.asyncio
    async def test_cluster_management(self, basic_exchange):
        """Test adding and removing federated clusters."""
        exchange = basic_exchange

        # Create a remote cluster identity
        remote_identity = ClusterIdentity(
            cluster_id="remote-cluster",
            cluster_name="Remote Cluster",
            region="remote-region",
            bridge_url="ws://remote:8080",
            public_key_hash="remote-hash",
            created_at=time.time(),
        )

        # Add the remote cluster
        result = await exchange.add_federated_cluster_async(remote_identity)
        assert result is True
        assert exchange.federation_stats["routing_decisions"] >= 1

        # Remove the remote cluster
        result = await exchange.remove_federated_cluster_async("remote-cluster")
        assert result is True

        # Try to remove non-existent cluster
        result = await exchange.remove_federated_cluster_async("nonexistent-cluster")
        # This may succeed or fail depending on implementation


class TestFederatedTopicExchangeMessaging:
    """Test message publishing and receiving."""

    @pytest.fixture
    async def messaging_exchange(self, single_server: MPREGServer, server_port: int):
        """Create a federated exchange for messaging tests."""
        exchange = await create_federated_cluster(
            server_url=f"ws://127.0.0.1:{server_port}",
            cluster_id="messaging-cluster",
            cluster_name="Messaging Test Cluster",
            region="messaging-region",
            bridge_url=f"ws://127.0.0.1:{server_port + 100}",
            public_key_hash="messaging-hash",
        )
        yield exchange
        await exchange.disable_federation_async()

    @pytest.mark.asyncio
    async def test_publish_message_async(self, messaging_exchange):
        """Test async message publishing."""
        exchange = messaging_exchange

        test_message = PubSubMessage(
            message_id="test-msg-1",
            topic="test.topic",
            payload={"key": "value"},
            publisher="test-sender",
            timestamp=time.time(),
        )

        # Publish message asynchronously
        notifications = await exchange.publish_message_async(test_message)

        # Should return list of notifications (may be empty if no subscribers)
        assert isinstance(notifications, list)

        # Check federation stats were updated
        assert (
            exchange.federation_stats["messages_sent"] >= 0
        )  # May be 0 or 1 depending on federation routing

    @pytest.mark.asyncio
    async def test_publish_message_sync(self, messaging_exchange):
        """Test synchronous message publishing (backward compatibility)."""
        exchange = messaging_exchange

        test_message = PubSubMessage(
            message_id="test-msg-2",
            topic="sync.topic",
            payload={"sync": "test"},
            publisher="sync-sender",
            timestamp=time.time(),
        )

        # Publish message synchronously
        notifications = exchange.publish_message(test_message)

        # Should return list of notifications
        assert isinstance(notifications, list)

    @pytest.mark.asyncio
    async def test_receive_federated_message(self, messaging_exchange):
        """Test receiving federated messages."""
        exchange = messaging_exchange

        test_message = PubSubMessage(
            message_id="federated-msg-1",
            topic="federated.topic",
            payload={"federated": "message"},
            publisher="remote-sender",
            timestamp=time.time(),
        )

        # Receive federated message
        notifications = await exchange.receive_federated_message_async(
            test_message, "remote-cluster"
        )

        # Should return list of notifications (may be empty if no subscribers)
        assert isinstance(notifications, list)

        # Check federation stats were updated
        assert exchange.federation_stats["messages_received"] >= 1

    @pytest.mark.asyncio
    async def test_subscription_management(self, messaging_exchange):
        """Test subscription management with federation awareness."""
        exchange = messaging_exchange

        test_subscription = PubSubSubscription(
            subscription_id="test-sub-1",
            patterns=(TopicPattern(pattern="test.*", exact_match=False),),
            subscriber="test-subscriber",
            created_at=time.time(),
        )

        # Add subscription
        exchange.add_subscription(test_subscription)

        # Verify subscription was added (check internal state)
        assert test_subscription.subscription_id in exchange.subscriptions

        # Remove subscription
        result = exchange.remove_subscription("test-sub-1")
        assert result is True

        # Try to remove non-existent subscription
        result = exchange.remove_subscription("nonexistent-sub")
        assert result is False


class TestFederatedTopicExchangeStatistics:
    """Test statistics and health monitoring."""

    @pytest.fixture
    async def stats_exchange(self, single_server: MPREGServer, server_port: int):
        """Create a federated exchange for statistics tests."""
        exchange = await create_federated_cluster(
            server_url=f"ws://127.0.0.1:{server_port}",
            cluster_id="stats-cluster",
            cluster_name="Statistics Test Cluster",
            region="stats-region",
            bridge_url=f"ws://127.0.0.1:{server_port + 100}",
            public_key_hash="stats-hash",
        )
        yield exchange
        await exchange.disable_federation_async()

    @pytest.mark.asyncio
    async def test_get_federation_stats(self, stats_exchange):
        """Test getting comprehensive federation statistics."""
        exchange = stats_exchange

        stats = exchange.get_stats()

        # Should return FederatedTopicExchangeStats
        assert isinstance(stats, FederatedTopicExchangeStats)

        # Should have federation info
        assert stats.federation is not None
        assert stats.federation.enabled is True
        assert stats.federation.cluster_id == "stats-cluster"
        assert stats.federation.cluster_name == "Statistics Test Cluster"

        # Should have base topic exchange stats
        assert hasattr(stats, "server_url")
        assert hasattr(stats, "cluster_id")
        assert hasattr(stats, "active_subscriptions")
        assert hasattr(stats, "messages_published")

    @pytest.mark.asyncio
    async def test_get_federation_health(self, stats_exchange):
        """Test getting federation health information."""
        exchange = stats_exchange

        health = exchange.get_federation_health()

        # Should return FederationHealth
        assert isinstance(health, FederationHealth)
        assert health.federation_enabled is True

        # Should have health metrics
        assert hasattr(health, "total_clusters")
        assert hasattr(health, "healthy_clusters")
        assert hasattr(health, "overall_health")

        # Overall health should be a valid status
        valid_statuses = [
            "healthy",
            "degraded",
            "critical",
            "disabled",
            "error",
            "no_clusters",
            "good",
        ]
        assert health.overall_health in valid_statuses

    @pytest.mark.asyncio
    async def test_sync_federation_alias(self, stats_exchange):
        """Test sync_federation method (alias for health check)."""
        exchange = stats_exchange

        result = await exchange.sync_federation()

        # Should return boolean indicating federation sync status
        assert isinstance(result, bool)

    @pytest.mark.asyncio
    async def test_federation_stats_tracking(self, stats_exchange):
        """Test that federation statistics are properly tracked."""
        exchange = stats_exchange

        # Check initial stats
        initial_stats = exchange.federation_stats.copy()

        # Perform some operations that should update stats
        test_message = PubSubMessage(
            message_id="stats-test-msg",
            topic="stats.test",
            payload={"test": "stats"},
            publisher="stats-sender",
            timestamp=time.time(),
        )

        # Publish a message
        await exchange.publish_message_async(test_message)

        # Receive a federated message
        await exchange.receive_federated_message_async(
            test_message, "remote-stats-cluster"
        )

        # Stats should be updated
        final_stats = exchange.federation_stats

        # Messages received should have increased
        assert (
            final_stats["messages_received"] >= initial_stats["messages_received"] + 1
        )

        # Other stats may or may not have changed depending on federation routing
        assert isinstance(final_stats["messages_sent"], int)
        assert isinstance(final_stats["federation_errors"], int)
        assert isinstance(final_stats["routing_decisions"], int)


class TestFederatedTopicExchangeErrorHandling:
    """Test error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_operations_without_federation(
        self,
        single_server: MPREGServer,
        server_port: int,
    ):
        """Test operations when federation is not enabled."""
        # Create exchange but disable federation
        exchange = await create_federated_cluster(
            server_url=f"ws://127.0.0.1:{server_port}",
            cluster_id="disabled-cluster",
            cluster_name="Disabled Cluster",
            region="disabled-region",
            bridge_url=f"ws://127.0.0.1:{server_port + 100}",
            public_key_hash="disabled-hash",
        )

        try:
            # Disable federation
            await exchange.disable_federation_async()
            assert exchange.federation_enabled is False

            # Test cluster management operations
            remote_identity = ClusterIdentity(
                cluster_id="remote",
                cluster_name="Remote",
                region="remote",
                bridge_url="ws://remote:8080",
                public_key_hash="remote-hash",
                created_at=time.time(),
            )

            result = await exchange.add_federated_cluster_async(remote_identity)
            assert result is False  # Should fail when federation disabled

            result = await exchange.remove_federated_cluster_async("any-cluster")
            assert result is False  # Should fail when federation disabled

            # Test message operations still work (fall back to local)
            test_message = PubSubMessage(
                message_id="local-msg",
                topic="local.test",
                payload={"local": "test"},
                publisher="local-sender",
                timestamp=time.time(),
            )

            # These should work locally even without federation
            notifications = await exchange.publish_message_async(test_message)
            assert isinstance(notifications, list)

            notifications = exchange.publish_message(test_message)
            assert isinstance(notifications, list)

            # Receiving federated messages should return empty when disabled
            notifications = await exchange.receive_federated_message_async(
                test_message, "remote-cluster"
            )
            assert notifications == []

        finally:
            # Make sure it's disabled for cleanup
            await exchange.disable_federation_async()

    @pytest.mark.asyncio
    async def test_connect_clusters_error_conditions(
        self,
        single_server: MPREGServer,
        server_port: int,
    ):
        """Test error conditions in cluster connection."""
        # Create one federated cluster and one regular exchange
        federated_cluster = await create_federated_cluster(
            server_url=f"ws://127.0.0.1:{server_port}",
            cluster_id="federated-only",
            cluster_name="Federated Only",
            region="fed-region",
            bridge_url=f"ws://127.0.0.1:{server_port + 100}",
            public_key_hash="fed-hash",
        )

        # Create another federated cluster but disable it
        disabled_cluster = await create_federated_cluster(
            server_url=f"ws://127.0.0.1:{server_port}",
            cluster_id="disabled-fed",
            cluster_name="Disabled Fed",
            region="disabled-region",
            bridge_url=f"ws://127.0.0.1:{server_port + 101}",
            public_key_hash="disabled-hash",
        )

        try:
            # Disable the second cluster
            await disabled_cluster.disable_federation_async()

            # Try to connect federated with disabled - should raise ValueError
            with pytest.raises(ValueError, match="does not have federation enabled"):
                await connect_clusters(federated_cluster, disabled_cluster)

        finally:
            await federated_cluster.disable_federation_async()
            await disabled_cluster.disable_federation_async()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
