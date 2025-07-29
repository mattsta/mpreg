"""
Comprehensive tests for cache federation bridge.

This test suite validates the cache federation bridge functionality including:
- Cross-cluster cache invalidation and replication
- Topic-pattern based message routing
- Vector clock coordination and consistency
- Error handling and timeout management
- Performance monitoring and statistics
"""

import asyncio
import time
from unittest.mock import MagicMock

import pytest
from hypothesis import given, settings

from mpreg.core.topic_exchange import TopicExchange
from mpreg.datastructures.federated_cache_coherence import (
    CacheCoherenceMetadata,
    CacheInvalidationMessage,
    CacheOperationType,
    CacheReplicationMessage,
    FederatedCacheKey,
    FederatedCacheStatistics,
    ReplicationStrategy,
    federated_cache_key_strategy,
)
from mpreg.datastructures.vector_clock import VectorClock
from mpreg.federation.cache_federation_bridge import (
    CacheFederationBridge,
    CacheFederationConfiguration,
    PendingCacheOperation,
)


class TestCacheFederationConfiguration:
    """Test cache federation configuration validation."""

    def test_configuration_creation(self) -> None:
        """Test basic configuration creation."""
        config = CacheFederationConfiguration(cluster_id="test-cluster")

        assert config.cluster_id == "test-cluster"
        assert config.invalidation_timeout_seconds == 30.0
        assert config.replication_timeout_seconds == 60.0
        assert config.max_pending_operations == 1000
        assert config.enable_async_replication is True
        assert config.enable_sync_replication is True

    def test_configuration_validation(self) -> None:
        """Test configuration parameter validation."""
        # Empty cluster ID should raise error
        with pytest.raises(ValueError, match="Cluster ID cannot be empty"):
            CacheFederationConfiguration(cluster_id="")

        # Invalid timeout values should raise errors
        with pytest.raises(ValueError, match="Invalidation timeout must be positive"):
            CacheFederationConfiguration(
                cluster_id="test-cluster", invalidation_timeout_seconds=-1.0
            )

        with pytest.raises(ValueError, match="Replication timeout must be positive"):
            CacheFederationConfiguration(
                cluster_id="test-cluster", replication_timeout_seconds=0.0
            )

        with pytest.raises(ValueError, match="Max pending operations must be positive"):
            CacheFederationConfiguration(
                cluster_id="test-cluster", max_pending_operations=0
            )

    def test_configuration_custom_values(self) -> None:
        """Test configuration with custom values."""
        config = CacheFederationConfiguration(
            cluster_id="custom-cluster",
            invalidation_timeout_seconds=15.0,
            replication_timeout_seconds=45.0,
            max_pending_operations=500,
            enable_async_replication=False,
            default_replication_strategy=ReplicationStrategy.SYNC_REPLICATION,
            topic_prefix="custom.cache.fed",
        )

        assert config.cluster_id == "custom-cluster"
        assert config.invalidation_timeout_seconds == 15.0
        assert config.replication_timeout_seconds == 45.0
        assert config.max_pending_operations == 500
        assert config.enable_async_replication is False
        assert (
            config.default_replication_strategy == ReplicationStrategy.SYNC_REPLICATION
        )
        assert config.topic_prefix == "custom.cache.fed"


class TestPendingCacheOperation:
    """Test pending cache operation tracking."""

    def test_pending_operation_creation(self) -> None:
        """Test basic pending operation creation."""
        cache_key = FederatedCacheKey.create("test", "key", "cluster-01")
        vector_clock = VectorClock.empty().increment("cluster-01")

        operation = PendingCacheOperation(
            operation_id="test-op-123",
            operation_type=CacheOperationType.SINGLE_KEY_INVALIDATION,
            cache_key=cache_key,
            target_clusters=frozenset(["cluster-02", "cluster-03"]),
            created_at=time.time(),
            timeout_seconds=30.0,
            vector_clock=vector_clock,
        )

        assert operation.operation_id == "test-op-123"
        assert operation.operation_type == CacheOperationType.SINGLE_KEY_INVALIDATION
        assert operation.cache_key == cache_key
        assert operation.target_clusters == frozenset(["cluster-02", "cluster-03"])
        assert operation.priority == "1.0"  # Default priority

    def test_pending_operation_expiration(self) -> None:
        """Test pending operation expiration logic."""
        cache_key = FederatedCacheKey.create("test", "key", "cluster-01")
        vector_clock = VectorClock.empty().increment("cluster-01")

        # Create operation that's already expired
        operation = PendingCacheOperation(
            operation_id="expired-op",
            operation_type=CacheOperationType.ASYNC_REPLICATION,
            cache_key=cache_key,
            target_clusters=frozenset(["cluster-02"]),
            created_at=time.time() - 100.0,  # Created 100 seconds ago
            timeout_seconds=30.0,  # 30 second timeout
            vector_clock=vector_clock,
        )

        assert operation.is_expired()

        # Create operation that's not expired
        fresh_operation = PendingCacheOperation(
            operation_id="fresh-op",
            operation_type=CacheOperationType.SINGLE_KEY_INVALIDATION,
            cache_key=cache_key,
            target_clusters=frozenset(["cluster-02"]),
            created_at=time.time(),  # Just created
            timeout_seconds=30.0,
            vector_clock=vector_clock,
        )

        assert not fresh_operation.is_expired()

    def test_pending_operation_cluster_targeting(self) -> None:
        """Test cluster targeting logic for pending operations."""
        cache_key = FederatedCacheKey.create("test", "key", "cluster-01")
        vector_clock = VectorClock.empty().increment("cluster-01")

        # Operation targeting specific clusters
        targeted_operation = PendingCacheOperation(
            operation_id="targeted-op",
            operation_type=CacheOperationType.SINGLE_KEY_INVALIDATION,
            cache_key=cache_key,
            target_clusters=frozenset(["cluster-02", "cluster-03"]),
            created_at=time.time(),
            timeout_seconds=30.0,
            vector_clock=vector_clock,
        )

        assert targeted_operation.affects_cluster("cluster-02")
        assert targeted_operation.affects_cluster("cluster-03")
        assert not targeted_operation.affects_cluster("cluster-04")

        # Operation targeting all clusters (empty target set)
        broadcast_operation = PendingCacheOperation(
            operation_id="broadcast-op",
            operation_type=CacheOperationType.SINGLE_KEY_INVALIDATION,
            cache_key=cache_key,
            target_clusters=frozenset(),  # Empty = all clusters
            created_at=time.time(),
            timeout_seconds=30.0,
            vector_clock=vector_clock,
        )

        assert broadcast_operation.affects_cluster("any-cluster")
        assert broadcast_operation.affects_cluster("cluster-02")


class TestCacheFederationBridge:
    """Test cache federation bridge functionality."""

    def create_test_bridge(
        self, cluster_id: str = "test-cluster"
    ) -> CacheFederationBridge:
        """Create a test federation bridge with mock topic exchange."""
        config = CacheFederationConfiguration(cluster_id=cluster_id)

        # Create mock topic exchange
        topic_exchange = MagicMock(spec=TopicExchange)
        topic_exchange.publish_message.return_value = []

        bridge = CacheFederationBridge(config=config, topic_exchange=topic_exchange)
        # Disable background tasks for testing
        bridge._disable_background_tasks = True

        return bridge

    def test_bridge_initialization(self) -> None:
        """Test federation bridge initialization."""
        bridge = self.create_test_bridge("init-cluster")

        assert bridge.config.cluster_id == "init-cluster"
        assert bridge.local_cluster_id == "init-cluster"
        assert bridge.vector_clock.is_empty()
        assert len(bridge.pending_operations) == 0
        assert len(bridge.cluster_cache_statistics) == 0

    @pytest.mark.asyncio
    async def test_send_cache_invalidation(self) -> None:
        """Test sending cache invalidation message."""
        bridge = self.create_test_bridge("source-cluster")

        cache_key = FederatedCacheKey.create("users", "profile:123", "source-cluster")
        target_clusters = frozenset(["target-cluster-01", "target-cluster-02"])

        operation_id = await bridge.send_cache_invalidation(
            cache_key=cache_key,
            target_clusters=target_clusters,
            reason="user_profile_updated",
            priority="2.5",
        )

        # Verify operation was created and tracked
        assert operation_id in bridge.pending_operations

        pending_op = bridge.pending_operations[operation_id]
        assert pending_op.operation_type == CacheOperationType.SINGLE_KEY_INVALIDATION
        assert pending_op.cache_key == cache_key
        assert pending_op.target_clusters == target_clusters
        assert pending_op.priority == "2.5"

        # Verify vector clock was incremented
        assert not bridge.vector_clock.is_empty()
        assert bridge.vector_clock.get_timestamp("source-cluster") == 1

        # Verify statistics were updated
        assert bridge.statistics.invalidations_sent == 1
        assert bridge.statistics.pending_operations_count == 1

        # Verify topic exchange was called
        bridge.topic_exchange.publish_message.assert_called_once()  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_send_cache_replication_async(self) -> None:
        """Test sending async cache replication message."""
        bridge = self.create_test_bridge("source-cluster")

        cache_key = FederatedCacheKey.create("data", "item:456", "source-cluster")
        cache_value = {"data": "test_value", "version": 1}
        coherence_metadata = CacheCoherenceMetadata.create_initial("source-cluster")
        target_clusters = frozenset(["replica-cluster-01"])

        operation_id = await bridge.send_cache_replication(
            cache_key=cache_key,
            cache_value=cache_value,
            coherence_metadata=coherence_metadata,
            target_clusters=target_clusters,
            strategy=ReplicationStrategy.ASYNC_REPLICATION,
            priority="1.8",
        )

        # Verify operation was created and tracked
        assert operation_id in bridge.pending_operations

        pending_op = bridge.pending_operations[operation_id]
        assert pending_op.operation_type == CacheOperationType.ASYNC_REPLICATION
        assert pending_op.cache_key == cache_key
        assert pending_op.target_clusters == target_clusters
        assert pending_op.priority == "1.8"

        # Verify statistics were updated
        assert bridge.statistics.replications_sent == 1
        assert bridge.statistics.pending_operations_count == 1

        # Verify topic exchange was called
        bridge.topic_exchange.publish_message.assert_called_once()  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_send_cache_replication_sync(self) -> None:
        """Test sending sync cache replication message."""
        bridge = self.create_test_bridge("source-cluster")

        cache_key = FederatedCacheKey.create("cache", "sync:789", "source-cluster")
        cache_value = [1, 2, 3, 4, 5]
        coherence_metadata = CacheCoherenceMetadata.create_initial("source-cluster")
        target_clusters = frozenset(["sync-replica-01", "sync-replica-02"])

        operation_id = await bridge.send_cache_replication(
            cache_key=cache_key,
            cache_value=cache_value,
            coherence_metadata=coherence_metadata,
            target_clusters=target_clusters,
            strategy=ReplicationStrategy.SYNC_REPLICATION,
        )

        # Verify operation was created
        assert operation_id in bridge.pending_operations

        pending_op = bridge.pending_operations[operation_id]
        assert pending_op.operation_type == CacheOperationType.SYNC_REPLICATION

        # Verify statistics were updated
        assert bridge.statistics.replications_sent == 1

    @pytest.mark.asyncio
    async def test_send_cache_replication_validation(self) -> None:
        """Test cache replication validation."""
        bridge = self.create_test_bridge("source-cluster")

        cache_key = FederatedCacheKey.create("test", "key", "source-cluster")
        cache_value = "test_value"
        coherence_metadata = CacheCoherenceMetadata.create_initial("source-cluster")

        # Empty target clusters should raise error
        with pytest.raises(ValueError, match="Target clusters cannot be empty"):
            await bridge.send_cache_replication(
                cache_key=cache_key,
                cache_value=cache_value,
                coherence_metadata=coherence_metadata,
                target_clusters=frozenset(),  # Empty
            )

    @pytest.mark.asyncio
    async def test_handle_cache_invalidation_message(self) -> None:
        """Test handling incoming cache invalidation message."""
        bridge = self.create_test_bridge("target-cluster")

        # Create invalidation message targeting our cluster
        cache_key = FederatedCacheKey.create("users", "session:abc", "source-cluster")
        vector_clock = VectorClock.empty().increment("source-cluster")
        target_clusters = frozenset(["target-cluster"])

        invalidation_message = CacheInvalidationMessage.create_single_key_invalidation(
            cache_key=cache_key,
            source_cluster="source-cluster",
            target_clusters=target_clusters,
            vector_clock=vector_clock,
        )

        # Handle the message
        result = await bridge.handle_cache_invalidation_message(invalidation_message)

        assert result is True

        # Verify statistics were updated
        assert bridge.statistics.invalidations_received == 1
        assert bridge.statistics.invalidations_processed == 1

        # Verify vector clock was updated
        assert bridge.vector_clock.get_timestamp("source-cluster") == 1

        # Verify cluster statistics were updated
        source_stats = bridge.cluster_cache_statistics["source-cluster"]
        assert source_stats.invalidations_received == 1

    @pytest.mark.asyncio
    async def test_handle_cache_invalidation_not_targeted(self) -> None:
        """Test handling invalidation message not targeting our cluster."""
        bridge = self.create_test_bridge("other-cluster")

        # Create invalidation message targeting different cluster
        cache_key = FederatedCacheKey.create("test", "key", "source-cluster")
        vector_clock = VectorClock.empty().increment("source-cluster")
        target_clusters = frozenset(["different-cluster"])

        invalidation_message = CacheInvalidationMessage.create_single_key_invalidation(
            cache_key=cache_key,
            source_cluster="source-cluster",
            target_clusters=target_clusters,
            vector_clock=vector_clock,
        )

        # Handle the message
        result = await bridge.handle_cache_invalidation_message(invalidation_message)

        assert result is True

        # Verify no processing occurred
        assert bridge.statistics.invalidations_received == 0
        assert bridge.statistics.invalidations_processed == 0

    @pytest.mark.asyncio
    async def test_handle_cache_replication_message(self) -> None:
        """Test handling incoming cache replication message."""
        bridge = self.create_test_bridge("replica-cluster")

        # Create replication message targeting our cluster
        cache_key = FederatedCacheKey.create("data", "replicated:123", "source-cluster")
        cache_value = {"replicated": "data"}
        coherence_metadata = CacheCoherenceMetadata.create_initial("source-cluster")
        target_clusters = frozenset(["replica-cluster"])

        replication_message = CacheReplicationMessage.create_async_replication(
            cache_key=cache_key,
            cache_value=cache_value,
            coherence_metadata=coherence_metadata,
            source_cluster="source-cluster",
            target_clusters=target_clusters,
        )

        # Handle the message
        result = await bridge.handle_cache_replication_message(replication_message)

        assert result is True

        # Verify statistics were updated
        assert bridge.statistics.replications_received == 1
        assert bridge.statistics.replications_processed == 1

        # Verify vector clock was updated
        assert bridge.vector_clock.get_timestamp("source-cluster") == 1

        # Verify cluster statistics were updated
        source_stats = bridge.cluster_cache_statistics["source-cluster"]
        assert source_stats.replications_received == 1

    @pytest.mark.asyncio
    async def test_handle_cache_replication_not_targeted(self) -> None:
        """Test handling replication message not targeting our cluster."""
        bridge = self.create_test_bridge("other-cluster")

        # Create replication message targeting different cluster
        cache_key = FederatedCacheKey.create("test", "key", "source-cluster")
        cache_value = "test_value"
        coherence_metadata = CacheCoherenceMetadata.create_initial("source-cluster")
        target_clusters = frozenset(["different-cluster"])

        replication_message = CacheReplicationMessage.create_sync_replication(
            cache_key=cache_key,
            cache_value=cache_value,
            coherence_metadata=coherence_metadata,
            source_cluster="source-cluster",
            target_clusters=target_clusters,
        )

        # Handle the message
        result = await bridge.handle_cache_replication_message(replication_message)

        assert result is True

        # Verify no processing occurred
        assert bridge.statistics.replications_received == 0
        assert bridge.statistics.replications_processed == 0

    def test_complete_operation_success(self) -> None:
        """Test completing pending operation successfully."""
        bridge = self.create_test_bridge("test-cluster")

        # Add a pending operation
        cache_key = FederatedCacheKey.create("test", "key", "test-cluster")
        vector_clock = VectorClock.empty().increment("test-cluster")

        operation = PendingCacheOperation(
            operation_id="test-op-456",
            operation_type=CacheOperationType.SINGLE_KEY_INVALIDATION,
            cache_key=cache_key,
            target_clusters=frozenset(["target-cluster"]),
            created_at=time.time(),
            timeout_seconds=30.0,
            vector_clock=vector_clock,
        )

        bridge.pending_operations["test-op-456"] = operation

        # Complete the operation
        result = bridge.complete_operation("test-op-456", success=True)

        assert result is True
        assert "test-op-456" not in bridge.pending_operations
        assert bridge.statistics.pending_operations_count == 0
        assert bridge.statistics.failed_operations == 0

    def test_complete_operation_failure(self) -> None:
        """Test completing pending operation with failure."""
        bridge = self.create_test_bridge("test-cluster")

        # Add a pending operation
        cache_key = FederatedCacheKey.create("test", "key", "test-cluster")
        vector_clock = VectorClock.empty().increment("test-cluster")

        operation = PendingCacheOperation(
            operation_id="failed-op-789",
            operation_type=CacheOperationType.ASYNC_REPLICATION,
            cache_key=cache_key,
            target_clusters=frozenset(["target-cluster"]),
            created_at=time.time(),
            timeout_seconds=30.0,
            vector_clock=vector_clock,
        )

        bridge.pending_operations["failed-op-789"] = operation

        # Complete the operation with failure
        result = bridge.complete_operation("failed-op-789", success=False)

        assert result is True
        assert "failed-op-789" not in bridge.pending_operations
        assert bridge.statistics.pending_operations_count == 0
        assert bridge.statistics.failed_operations == 1

    def test_complete_operation_not_found(self) -> None:
        """Test completing non-existent operation."""
        bridge = self.create_test_bridge("test-cluster")

        result = bridge.complete_operation("non-existent-op", success=True)

        assert result is False
        assert bridge.statistics.failed_operations == 0

    def test_federation_statistics(self) -> None:
        """Test federation statistics collection."""
        bridge = self.create_test_bridge("test-cluster")

        # Add some pending operations
        cache_key = FederatedCacheKey.create("test", "key", "test-cluster")
        vector_clock = VectorClock.empty().increment("test-cluster")

        for i in range(3):
            operation = PendingCacheOperation(
                operation_id=f"op-{i}",
                operation_type=CacheOperationType.SINGLE_KEY_INVALIDATION,
                cache_key=cache_key,
                target_clusters=frozenset(["target-cluster"]),
                created_at=time.time() - i,  # Different creation times
                timeout_seconds=30.0,
                vector_clock=vector_clock,
            )
            bridge.pending_operations[f"op-{i}"] = operation

        # Update some statistics
        bridge.statistics.invalidations_sent = 5
        bridge.statistics.replications_received = 3
        bridge.statistics.failed_operations = 1

        stats = bridge.get_federation_statistics()

        assert stats.invalidations_sent == 5
        assert stats.replications_received == 3
        assert stats.failed_operations == 1
        assert stats.pending_operations_count == 3
        assert stats.average_operation_latency_ms > 0  # Should be calculated

    def test_cluster_statistics(self) -> None:
        """Test cluster-specific statistics."""
        bridge = self.create_test_bridge("test-cluster")

        # Update cluster statistics - create new instance since fields are read-only
        bridge.cluster_cache_statistics["remote-cluster"] = FederatedCacheStatistics(
            invalidations_received=10,
            replications_sent=5,
        )

        # Get statistics for specific cluster
        stats = bridge.get_cluster_statistics("remote-cluster")
        assert stats.invalidations_received == 10
        assert stats.replications_sent == 5

        # Get all cluster statistics
        all_stats = bridge.get_all_cluster_statistics()
        assert "remote-cluster" in all_stats
        assert all_stats["remote-cluster"].invalidations_received == 10

    @given(federated_cache_key_strategy())
    @settings(max_examples=50, deadline=2000)
    def test_bridge_operations_property_based(
        self, cache_key: FederatedCacheKey
    ) -> None:
        """Property-based testing for bridge operations."""
        bridge = self.create_test_bridge("property-test-cluster")

        # Test that cache key operations don't break the bridge
        target_clusters = frozenset(["target-1", "target-2"])

        # Create operations that should succeed
        async def test_operations():
            try:
                # Test invalidation
                inv_op_id = await bridge.send_cache_invalidation(
                    cache_key=cache_key, target_clusters=target_clusters
                )
                assert inv_op_id in bridge.pending_operations

                # Test replication with minimal data
                coherence_metadata = CacheCoherenceMetadata.create_initial(
                    bridge.local_cluster_id
                )
                repl_op_id = await bridge.send_cache_replication(
                    cache_key=cache_key,
                    cache_value="test_value",
                    coherence_metadata=coherence_metadata,
                    target_clusters=target_clusters,
                )
                assert repl_op_id in bridge.pending_operations

                # Verify statistics consistency
                stats = bridge.get_federation_statistics()
                assert stats.invalidations_sent >= 1
                assert stats.replications_sent >= 1
                assert stats.pending_operations_count >= 2

            except Exception as e:
                pytest.fail(f"Bridge operations failed with cache key {cache_key}: {e}")

        # Run the async test
        asyncio.run(test_operations())


class TestCacheFederationStatistics:
    """Test cache federation statistics calculations."""

    def test_statistics_success_rate_calculation(self) -> None:
        """Test success rate calculation."""
        bridge = self.create_test_bridge("stats-cluster")

        # Set up statistics
        bridge.statistics.invalidations_sent = 10
        bridge.statistics.replications_sent = 5
        bridge.statistics.timeout_operations = 2
        bridge.statistics.failed_operations = 1

        success_rate = bridge.statistics.success_rate()

        # Total operations = 15, failed = 3, success rate = 12/15 = 0.8
        assert success_rate == 0.8

    def test_statistics_processing_efficiency(self) -> None:
        """Test processing efficiency calculation."""
        bridge = self.create_test_bridge("efficiency-cluster")

        # Set up statistics
        bridge.statistics.invalidations_received = 8
        bridge.statistics.replications_received = 4
        bridge.statistics.invalidations_processed = 7
        bridge.statistics.replications_processed = 4

        efficiency = bridge.statistics.processing_efficiency()

        # Total received = 12, processed = 11, efficiency = 11/12 ≈ 0.917
        assert abs(efficiency - (11 / 12)) < 0.001

    def test_statistics_edge_cases(self) -> None:
        """Test statistics calculations with edge cases."""
        bridge = self.create_test_bridge("edge-case-cluster")

        # Empty statistics should handle division by zero
        assert bridge.statistics.success_rate() == 1.0
        assert bridge.statistics.processing_efficiency() == 1.0

    def create_test_bridge(self, cluster_id: str) -> CacheFederationBridge:
        """Helper to create test bridge."""
        config = CacheFederationConfiguration(cluster_id=cluster_id)
        topic_exchange = MagicMock(spec=TopicExchange)
        bridge = CacheFederationBridge(config=config, topic_exchange=topic_exchange)
        bridge._disable_background_tasks = True
        return bridge


# Integration test for complete workflow
class TestCacheFederationIntegration:
    """Integration tests for complete cache federation workflows."""

    @pytest.mark.asyncio
    async def test_complete_invalidation_workflow(self) -> None:
        """Test complete cache invalidation workflow across clusters."""
        # Create source and target bridges
        source_config = CacheFederationConfiguration(cluster_id="source-cluster")
        target_config = CacheFederationConfiguration(cluster_id="target-cluster")

        source_topic_exchange = MagicMock(spec=TopicExchange)
        target_topic_exchange = MagicMock(spec=TopicExchange)

        source_bridge = CacheFederationBridge(
            config=source_config, topic_exchange=source_topic_exchange
        )
        source_bridge._disable_background_tasks = True

        target_bridge = CacheFederationBridge(
            config=target_config, topic_exchange=target_topic_exchange
        )
        target_bridge._disable_background_tasks = True

        # Create cache key for invalidation
        cache_key = FederatedCacheKey.create(
            namespace="integration-test",
            key="workflow:123",
            cluster_id="source-cluster",
        )

        # Send invalidation from source
        operation_id = await source_bridge.send_cache_invalidation(
            cache_key=cache_key,
            target_clusters=frozenset(["target-cluster"]),
            reason="integration_test",
        )

        # Verify source bridge state
        assert operation_id in source_bridge.pending_operations
        assert source_bridge.statistics.invalidations_sent == 1

        # Simulate message delivery and processing at target
        # Get the published message
        source_topic_exchange.publish_message.assert_called_once()
        published_message = source_topic_exchange.publish_message.call_args[0][0]

        # Process message at target
        await target_bridge._process_federation_message(published_message)

        # Verify target bridge processed the invalidation
        assert target_bridge.statistics.invalidations_received == 1
        assert target_bridge.statistics.invalidations_processed == 1

        # Complete operation at source
        source_bridge.complete_operation(operation_id, success=True)

        # Verify operation cleanup
        assert operation_id not in source_bridge.pending_operations
        assert source_bridge.statistics.pending_operations_count == 0

        # Verify vector clocks were updated appropriately
        assert source_bridge.vector_clock.get_timestamp("source-cluster") >= 1
        assert target_bridge.vector_clock.get_timestamp("source-cluster") >= 1

    @pytest.mark.asyncio
    async def test_complete_replication_workflow(self) -> None:
        """Test complete cache replication workflow across clusters."""
        # Create source and replica bridges
        source_config = CacheFederationConfiguration(cluster_id="primary-cluster")
        replica_config = CacheFederationConfiguration(cluster_id="replica-cluster")

        source_topic_exchange = MagicMock(spec=TopicExchange)
        replica_topic_exchange = MagicMock(spec=TopicExchange)

        source_bridge = CacheFederationBridge(
            config=source_config, topic_exchange=source_topic_exchange
        )
        source_bridge._disable_background_tasks = True

        replica_bridge = CacheFederationBridge(
            config=replica_config, topic_exchange=replica_topic_exchange
        )
        replica_bridge._disable_background_tasks = True

        # Create cache data for replication
        cache_key = FederatedCacheKey.create(
            namespace="replication-test",
            key="data:456",
            cluster_id="primary-cluster",
            region="us-west-2",
        )
        cache_value = {
            "content": "replicated_data",
            "metadata": {"version": 2, "created_by": "integration_test"},
        }
        coherence_metadata = CacheCoherenceMetadata.create_initial("primary-cluster")

        # Send replication from source
        operation_id = await source_bridge.send_cache_replication(
            cache_key=cache_key,
            cache_value=cache_value,
            coherence_metadata=coherence_metadata,
            target_clusters=frozenset(["replica-cluster"]),
            strategy=ReplicationStrategy.ASYNC_REPLICATION,
        )

        # Verify source bridge state
        assert operation_id in source_bridge.pending_operations
        assert source_bridge.statistics.replications_sent == 1

        # Simulate message delivery and processing at replica
        source_topic_exchange.publish_message.assert_called_once()
        published_message = source_topic_exchange.publish_message.call_args[0][0]

        # Process message at replica
        await replica_bridge._process_federation_message(published_message)

        # Verify replica bridge processed the replication
        assert replica_bridge.statistics.replications_received == 1
        assert replica_bridge.statistics.replications_processed == 1

        # Complete operation at source
        source_bridge.complete_operation(operation_id, success=True)

        # Verify operation cleanup
        assert operation_id not in source_bridge.pending_operations
        assert source_bridge.statistics.pending_operations_count == 0

        # Verify cluster statistics were updated
        primary_stats = replica_bridge.cluster_cache_statistics["primary-cluster"]
        assert primary_stats.replications_received == 1
