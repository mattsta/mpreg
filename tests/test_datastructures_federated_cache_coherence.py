"""
Comprehensive tests for federated cache coherence datastructures.

This test suite validates the foundational datastructures for federated cache
coherence using property-based testing with Hypothesis to ensure correctness
across all possible input combinations.

Test Coverage:
- FederatedCacheKey creation and validation
- CacheCoherenceMetadata state transitions
- CacheInvalidationMessage lifecycle and routing
- CacheReplicationMessage creation and coordination
- FederatedCacheStatistics analytics and metrics
- Property-based testing for edge cases and invariants
"""

import time

import pytest
from hypothesis import given, settings

from mpreg.datastructures.federated_cache_coherence import (
    CacheCoherenceMetadata,
    CacheCoherenceState,
    CacheInvalidationMessage,
    CacheReplicationMessage,
    CoherenceProtocolType,
    FederatedCacheKey,
    FederatedCacheStatistics,
    InvalidationType,
    ReplicationStrategy,
    cache_coherence_metadata_strategy,
    cache_invalidation_message_strategy,
    federated_cache_key_strategy,
    federated_cache_statistics_strategy,
)
from mpreg.datastructures.vector_clock import VectorClock


class TestFederatedCacheKey:
    """Test FederatedCacheKey datastructure creation and validation."""

    def test_federated_cache_key_creation(self) -> None:
        """Test basic federated cache key creation."""
        key = FederatedCacheKey.create(
            namespace="test",
            key="user:123",
            cluster_id="cluster-01",
            region="us-east-1",
            partition_id="1",
        )

        assert key.base_key.namespace.name == "test"
        assert key.base_key.key == "user:123"
        assert key.cluster_id == "cluster-01"
        assert key.region == "us-east-1"
        assert key.partition_id == "1"

    def test_federated_cache_key_global_key(self) -> None:
        """Test global key generation for federated cache key."""
        key = FederatedCacheKey.create(
            namespace="users",
            key="profile:456",
            cluster_id="west-cluster",
            region="cache-region",
            partition_id="2",
        )

        global_key = key.global_key()
        assert global_key == "west-cluster:cache-region:2:users:profile:456"

    def test_federated_cache_key_local_key(self) -> None:
        """Test local key generation for federated cache key."""
        key = FederatedCacheKey.create(
            namespace="sessions",
            key="token:789",
            cluster_id="any-cluster",
        )

        local_key = key.local_key()
        assert local_key == "sessions:token:789"

    def test_federated_cache_key_with_cluster(self) -> None:
        """Test creating new key with different cluster ID."""
        original_key = FederatedCacheKey.create(
            namespace="data",
            key="item:100",
            cluster_id="cluster-a",
        )

        new_key = original_key.with_cluster("cluster-b")

        assert new_key.cluster_id == "cluster-b"
        assert new_key.base_key == original_key.base_key
        assert new_key.region == original_key.region
        assert new_key.partition_id == original_key.partition_id

    def test_federated_cache_key_with_region(self) -> None:
        """Test creating new key with different cache region."""
        original_key = FederatedCacheKey.create(
            namespace="metrics",
            key="counter:200",
            cluster_id="prod-cluster",
            region="prod-region",
        )

        new_key = original_key.with_region("staging-region")

        assert new_key.region == "staging-region"
        assert new_key.cluster_id == original_key.cluster_id
        assert new_key.base_key == original_key.base_key
        assert new_key.partition_id == original_key.partition_id

    def test_federated_cache_key_validation(self) -> None:
        """Test validation of federated cache key parameters."""
        # Empty cluster ID should raise error
        with pytest.raises(ValueError, match="Cluster ID cannot be empty"):
            FederatedCacheKey.create(
                namespace="test",
                key="key",
                cluster_id="",
            )

        # Empty region should raise error
        with pytest.raises(ValueError, match="Cache region cannot be empty"):
            FederatedCacheKey.create(
                namespace="test",
                key="key",
                cluster_id="cluster",
                region="",
            )

    @given(federated_cache_key_strategy())
    @settings(max_examples=100, deadline=2000)
    def test_federated_cache_key_properties(self, key: FederatedCacheKey) -> None:
        """Property-based testing for federated cache key invariants."""
        # Global key should contain all components
        global_key = key.global_key()
        assert key.cluster_id in global_key
        assert key.region in global_key
        assert key.partition_id in global_key
        assert key.base_key.namespace.name in global_key
        assert key.base_key.key in global_key

        # Local key should be deterministic
        local_key = key.local_key()
        expected_local_key = key.base_key.full_key()
        assert local_key == expected_local_key

        # Cluster and region modifications should preserve other fields
        new_cluster_key = key.with_cluster("new-cluster")
        assert new_cluster_key.cluster_id == "new-cluster"
        assert new_cluster_key.base_key == key.base_key
        assert new_cluster_key.region == key.region

        new_region_key = key.with_region("new-region")
        assert new_region_key.region == "new-region"
        assert new_region_key.cluster_id == key.cluster_id
        assert new_region_key.base_key == key.base_key


class TestCacheCoherenceMetadata:
    """Test CacheCoherenceMetadata state management and transitions."""

    def test_cache_coherence_metadata_creation(self) -> None:
        """Test basic cache coherence metadata creation."""
        metadata = CacheCoherenceMetadata.create_initial(
            owning_cluster="cluster-01",
            protocol=CoherenceProtocolType.MESI,
        )

        assert metadata.state == CacheCoherenceState.EXCLUSIVE
        assert metadata.version == 1
        assert metadata.owning_cluster == "cluster-01"
        assert metadata.coherence_protocol == CoherenceProtocolType.MESI
        assert len(metadata.sharing_clusters) == 0
        assert not metadata.invalidation_pending
        assert metadata.sync_token != ""

    def test_cache_coherence_metadata_state_transition(self) -> None:
        """Test coherence state transitions."""
        metadata = CacheCoherenceMetadata.create_initial("cluster-01")

        # Transition to modified state
        modified_metadata = metadata.with_state(CacheCoherenceState.MODIFIED)
        assert modified_metadata.state == CacheCoherenceState.MODIFIED
        assert modified_metadata.version == metadata.version + 1
        assert modified_metadata.last_modified_time > metadata.last_modified_time

    def test_cache_coherence_metadata_sharing_clusters(self) -> None:
        """Test sharing cluster management."""
        metadata = CacheCoherenceMetadata.create_initial("cluster-01")

        sharing_clusters = frozenset(["cluster-02", "cluster-03"])
        shared_metadata = metadata.with_sharing_clusters(sharing_clusters)

        assert shared_metadata.state == CacheCoherenceState.SHARED
        assert shared_metadata.sharing_clusters == sharing_clusters
        assert shared_metadata.version == metadata.version + 1

    def test_cache_coherence_metadata_state_checks(self) -> None:
        """Test coherence state checking methods."""
        # Test exclusive state
        exclusive_metadata = CacheCoherenceMetadata.create_initial("cluster-01")
        assert exclusive_metadata.is_exclusive()
        assert not exclusive_metadata.is_shared()
        assert exclusive_metadata.is_valid()
        assert exclusive_metadata.can_write()
        assert exclusive_metadata.can_read()

        # Test shared state
        sharing_clusters = frozenset(["cluster-02"])
        shared_metadata = exclusive_metadata.with_sharing_clusters(sharing_clusters)
        assert not shared_metadata.is_exclusive()
        assert shared_metadata.is_shared()
        assert shared_metadata.is_valid()
        assert not shared_metadata.can_write()
        assert shared_metadata.can_read()

        # Test invalid state
        invalid_metadata = shared_metadata.with_state(CacheCoherenceState.INVALID)
        assert not invalid_metadata.is_exclusive()
        assert not invalid_metadata.is_shared()
        assert not invalid_metadata.is_valid()
        assert not invalid_metadata.can_write()
        assert not invalid_metadata.can_read()

    def test_cache_coherence_metadata_validation(self) -> None:
        """Test validation of coherence metadata parameters."""
        # Invalid version should raise error
        with pytest.raises(ValueError, match="Version must be positive"):
            CacheCoherenceMetadata(
                state=CacheCoherenceState.EXCLUSIVE,
                version=0,
                last_modified_time=time.time(),
                last_access_time=time.time(),
                vector_clock=VectorClock.empty().increment("cluster"),
                owning_cluster="cluster",
            )

        # Empty owning cluster should raise error
        with pytest.raises(ValueError, match="Owning cluster cannot be empty"):
            CacheCoherenceMetadata(
                state=CacheCoherenceState.EXCLUSIVE,
                version=1,
                last_modified_time=time.time(),
                last_access_time=time.time(),
                vector_clock=VectorClock.empty().increment("cluster"),
                owning_cluster="",
            )

    @given(cache_coherence_metadata_strategy())
    @settings(max_examples=100, deadline=2000)
    def test_cache_coherence_metadata_properties(
        self, metadata: CacheCoherenceMetadata
    ) -> None:
        """Property-based testing for coherence metadata invariants."""
        # Version should always be positive
        assert metadata.version >= 1

        # Owning cluster should never be empty
        assert metadata.owning_cluster != ""

        # State transitions should increment version
        new_metadata = metadata.with_state(CacheCoherenceState.SHARED)
        assert new_metadata.version == metadata.version + 1

        # Adding sharing clusters should result in shared state (if clusters provided)
        sharing_clusters = frozenset(["test-cluster"])
        shared_metadata = metadata.with_sharing_clusters(sharing_clusters)
        if sharing_clusters:
            assert shared_metadata.state == CacheCoherenceState.SHARED
        else:
            assert shared_metadata.state == CacheCoherenceState.EXCLUSIVE

        # Can write should be consistent with exclusive states
        if metadata.is_exclusive():
            assert metadata.can_write()

        # Can read should be true for most states (except INVALID and PENDING)
        if metadata.state not in (
            CacheCoherenceState.INVALID,
            CacheCoherenceState.PENDING,
        ):
            assert metadata.can_read()


class TestCacheInvalidationMessage:
    """Test CacheInvalidationMessage creation and routing."""

    def test_cache_invalidation_message_single_key(self) -> None:
        """Test creation of single key invalidation message."""
        cache_key = FederatedCacheKey.create(
            namespace="users",
            key="profile:123",
            cluster_id="cluster-01",
        )

        target_clusters = frozenset(["cluster-02", "cluster-03"])
        vector_clock = VectorClock.empty().increment("cluster-01")

        message = CacheInvalidationMessage.create_single_key_invalidation(
            cache_key=cache_key,
            source_cluster="cluster-01",
            target_clusters=target_clusters,
            vector_clock=vector_clock,
        )

        assert message.cache_key == cache_key
        assert message.invalidation_type == InvalidationType.SINGLE_KEY
        assert message.source_cluster == "cluster-01"
        assert message.target_clusters == target_clusters
        assert message.priority == 2.0  # High priority for single key
        assert "cache.invalidation" in message.topic_pattern

    def test_cache_invalidation_message_pattern(self) -> None:
        """Test creation of pattern-based invalidation message."""
        pattern = "user.profile.*"
        region = "us-east-1"
        target_clusters = frozenset(["cluster-02"])
        vector_clock = VectorClock.empty().increment("cluster-01")

        message = CacheInvalidationMessage.create_pattern_invalidation(
            pattern=pattern,
            region=region,
            source_cluster="cluster-01",
            target_clusters=target_clusters,
            vector_clock=vector_clock,
        )

        assert message.invalidation_type == InvalidationType.PATTERN_BASED
        assert message.metadata.pattern == pattern
        assert message.cache_key.region == region
        assert message.priority == 1.5  # Medium-high priority for patterns
        assert "cache.invalidation" in message.topic_pattern

    def test_cache_invalidation_message_expiration(self) -> None:
        """Test invalidation message expiration logic."""
        cache_key = FederatedCacheKey.create("test", "key", "cluster-01")
        vector_clock = VectorClock.empty().increment("cluster-01")

        message = CacheInvalidationMessage(
            invalidation_id="test-id",
            cache_key=cache_key,
            invalidation_type=InvalidationType.SINGLE_KEY,
            source_cluster="cluster-01",
            target_clusters=frozenset(),
            topic_pattern="test.pattern",
            vector_clock=vector_clock,
            ttl_seconds=0.1,  # Very short TTL for testing
            created_at=time.time() - 1.0,  # Created 1 second ago
        )

        assert message.is_expired()

    def test_cache_invalidation_message_affects_cluster(self) -> None:
        """Test cluster targeting logic."""
        cache_key = FederatedCacheKey.create("test", "key", "cluster-01")
        vector_clock = VectorClock.empty().increment("cluster-01")

        # Specific target clusters
        target_clusters = frozenset(["cluster-02", "cluster-03"])
        message = CacheInvalidationMessage(
            invalidation_id="test-id",
            cache_key=cache_key,
            invalidation_type=InvalidationType.SINGLE_KEY,
            source_cluster="cluster-01",
            target_clusters=target_clusters,
            topic_pattern="test.pattern",
            vector_clock=vector_clock,
        )

        assert message.affects_cluster("cluster-02")
        assert message.affects_cluster("cluster-03")
        assert not message.affects_cluster("cluster-04")

        # Empty target clusters (broadcast to all)
        broadcast_message = CacheInvalidationMessage(
            invalidation_id="test-id",
            cache_key=cache_key,
            invalidation_type=InvalidationType.SINGLE_KEY,
            source_cluster="cluster-01",
            target_clusters=frozenset(),
            topic_pattern="test.pattern",
            vector_clock=vector_clock,
        )

        assert broadcast_message.affects_cluster("any-cluster")

    def test_cache_invalidation_message_validation(self) -> None:
        """Test validation of invalidation message parameters."""
        cache_key = FederatedCacheKey.create("test", "key", "cluster-01")
        vector_clock = VectorClock.empty().increment("cluster-01")

        # Empty invalidation ID should raise error
        with pytest.raises(ValueError, match="Invalidation ID cannot be empty"):
            CacheInvalidationMessage(
                invalidation_id="",
                cache_key=cache_key,
                invalidation_type=InvalidationType.SINGLE_KEY,
                source_cluster="cluster-01",
                target_clusters=frozenset(),
                topic_pattern="test.pattern",
                vector_clock=vector_clock,
            )

        # Empty source cluster should raise error
        with pytest.raises(ValueError, match="Source cluster cannot be empty"):
            CacheInvalidationMessage(
                invalidation_id="test-id",
                cache_key=cache_key,
                invalidation_type=InvalidationType.SINGLE_KEY,
                source_cluster="",
                target_clusters=frozenset(),
                topic_pattern="test.pattern",
                vector_clock=vector_clock,
            )

        # Invalid TTL should raise error
        with pytest.raises(ValueError, match="TTL must be positive"):
            CacheInvalidationMessage(
                invalidation_id="test-id",
                cache_key=cache_key,
                invalidation_type=InvalidationType.SINGLE_KEY,
                source_cluster="cluster-01",
                target_clusters=frozenset(),
                topic_pattern="test.pattern",
                vector_clock=vector_clock,
                ttl_seconds=-1.0,
            )

    @given(cache_invalidation_message_strategy())
    @settings(max_examples=50, deadline=3000)
    def test_cache_invalidation_message_properties(
        self, message: CacheInvalidationMessage
    ) -> None:
        """Property-based testing for invalidation message invariants."""
        # Invalidation ID should never be empty
        assert message.invalidation_id != ""

        # Source cluster should never be empty
        assert message.source_cluster != ""

        # TTL should always be positive
        assert message.ttl_seconds > 0

        # Topic pattern should be valid (allow any valid pattern from Hypothesis)
        assert isinstance(message.topic_pattern, str) and len(message.topic_pattern) > 0

        # Priority should be reasonable (0.1 to 3.0)
        assert 0.1 <= message.priority <= 3.0

        # Message should correctly identify affected clusters
        for cluster in message.target_clusters:
            assert message.affects_cluster(cluster)


class TestCacheReplicationMessage:
    """Test CacheReplicationMessage creation and coordination."""

    def test_cache_replication_message_async(self) -> None:
        """Test creation of async replication message."""
        cache_key = FederatedCacheKey.create("test", "key", "cluster-01")
        cache_value = {"data": "test_value"}
        coherence_metadata = CacheCoherenceMetadata.create_initial("cluster-01")
        target_clusters = frozenset(["cluster-02"])

        message = CacheReplicationMessage.create_async_replication(
            cache_key=cache_key,
            cache_value=cache_value,
            coherence_metadata=coherence_metadata,
            source_cluster="cluster-01",
            target_clusters=target_clusters,
        )

        assert message.cache_key == cache_key
        assert message.cache_value == cache_value
        assert message.coherence_metadata == coherence_metadata
        assert message.replication_strategy == ReplicationStrategy.ASYNC_REPLICATION
        assert message.consistency_level == "eventual"
        assert message.priority == 0.8  # Lower priority for async
        assert "async" in message.topic_pattern

    def test_cache_replication_message_sync(self) -> None:
        """Test creation of sync replication message."""
        cache_key = FederatedCacheKey.create("test", "key", "cluster-01")
        cache_value = [1, 2, 3]
        coherence_metadata = CacheCoherenceMetadata.create_initial("cluster-01")
        target_clusters = frozenset(["cluster-02", "cluster-03"])

        message = CacheReplicationMessage.create_sync_replication(
            cache_key=cache_key,
            cache_value=cache_value,
            coherence_metadata=coherence_metadata,
            source_cluster="cluster-01",
            target_clusters=target_clusters,
        )

        assert message.cache_key == cache_key
        assert message.cache_value == cache_value
        assert message.replication_strategy == ReplicationStrategy.SYNC_REPLICATION
        assert message.consistency_level == "strong"
        assert message.priority == 2.0  # High priority for sync
        assert "sync" in message.topic_pattern

    def test_cache_replication_message_validation(self) -> None:
        """Test validation of replication message parameters."""
        cache_key = FederatedCacheKey.create("test", "key", "cluster-01")
        coherence_metadata = CacheCoherenceMetadata.create_initial("cluster-01")

        # Empty replication ID should raise error
        with pytest.raises(ValueError, match="Replication ID cannot be empty"):
            CacheReplicationMessage(
                replication_id="",
                cache_key=cache_key,
                cache_value="test",
                coherence_metadata=coherence_metadata,
                replication_strategy=ReplicationStrategy.ASYNC_REPLICATION,
                source_cluster="cluster-01",
                target_clusters=frozenset(),
                topic_pattern="test.pattern",
            )

        # Empty source cluster should raise error
        with pytest.raises(ValueError, match="Source cluster cannot be empty"):
            CacheReplicationMessage(
                replication_id="test-id",
                cache_key=cache_key,
                cache_value="test",
                coherence_metadata=coherence_metadata,
                replication_strategy=ReplicationStrategy.ASYNC_REPLICATION,
                source_cluster="",
                target_clusters=frozenset(),
                topic_pattern="test.pattern",
            )


class TestFederatedCacheStatistics:
    """Test FederatedCacheStatistics analytics and metrics."""

    def test_federated_cache_statistics_creation(self) -> None:
        """Test basic federated cache statistics creation."""
        stats = FederatedCacheStatistics(
            local_hits=100,
            local_misses=20,
            federation_hits=50,
            federation_misses=10,
            invalidations_sent=5,
            invalidations_received=8,
            replications_sent=15,
            replications_received=12,
            coherence_violations=2,
            cross_cluster_requests=60,
        )

        assert stats.local_hits == 100
        assert stats.local_misses == 20
        assert stats.federation_hits == 50
        assert stats.federation_misses == 10

    def test_federated_cache_statistics_calculations(self) -> None:
        """Test federated cache statistics calculations."""
        stats = FederatedCacheStatistics(
            local_hits=80,
            local_misses=20,
            federation_hits=30,
            federation_misses=10,
            invalidations_sent=5,
            replications_sent=10,
            coherence_violations=1,
            cross_cluster_requests=40,
        )

        # Test total calculations
        assert stats.total_hits() == 110  # 80 + 30
        assert stats.total_misses() == 30  # 20 + 10
        assert stats.total_requests() == 140  # 110 + 30

        # Test hit rates
        assert stats.local_hit_rate() == 0.8  # 80 / (80 + 20)
        assert stats.federation_hit_rate() == 0.75  # 30 / (30 + 10)
        assert stats.overall_hit_rate() == 110 / 140

        # Test efficiency calculations
        coherence_efficiency = stats.coherence_efficiency()
        assert coherence_efficiency == 1.0 - (
            1 / 15
        )  # 1 - (1 violation / 15 total ops)

        federation_efficiency = stats.federation_efficiency()
        assert (
            federation_efficiency == 30 / 40
        )  # 30 fed hits / 40 cross-cluster requests

    def test_federated_cache_statistics_with_hits(self) -> None:
        """Test statistics update methods."""
        stats = FederatedCacheStatistics()

        # Test local hit addition
        local_hit_stats = stats.with_local_hit()
        assert local_hit_stats.local_hits == 1
        assert local_hit_stats.local_misses == 0
        assert local_hit_stats.federation_hits == 0

        # Test federation hit addition
        fed_hit_stats = stats.with_federation_hit()
        assert fed_hit_stats.federation_hits == 1
        assert fed_hit_stats.cross_cluster_requests == 1
        assert fed_hit_stats.local_hits == 0

    def test_federated_cache_statistics_edge_cases(self) -> None:
        """Test edge cases for statistics calculations."""
        # Empty statistics
        empty_stats = FederatedCacheStatistics()
        assert empty_stats.local_hit_rate() == 0.0
        assert empty_stats.federation_hit_rate() == 0.0
        assert empty_stats.overall_hit_rate() == 0.0
        assert empty_stats.coherence_efficiency() == 1.0
        assert empty_stats.federation_efficiency() == 0.0

        # Statistics with no coherence operations
        no_coherence_stats = FederatedCacheStatistics(
            local_hits=10,
            coherence_violations=5,
        )
        assert (
            no_coherence_stats.coherence_efficiency() == 1.0
        )  # No operations to violate

    def test_federated_cache_statistics_validation(self) -> None:
        """Test validation of statistics parameters."""
        # Negative values should raise errors
        with pytest.raises(ValueError, match="local_hits cannot be negative"):
            FederatedCacheStatistics(local_hits=-1)

        with pytest.raises(ValueError, match="federation_misses cannot be negative"):
            FederatedCacheStatistics(federation_misses=-1)

        with pytest.raises(ValueError, match="coherence_violations cannot be negative"):
            FederatedCacheStatistics(coherence_violations=-1)

    @given(federated_cache_statistics_strategy())
    @settings(max_examples=100, deadline=2000)
    def test_federated_cache_statistics_properties(
        self, stats: FederatedCacheStatistics
    ) -> None:
        """Property-based testing for statistics invariants."""
        # All counts should be non-negative
        assert stats.local_hits >= 0
        assert stats.local_misses >= 0
        assert stats.federation_hits >= 0
        assert stats.federation_misses >= 0
        assert stats.invalidations_sent >= 0
        assert stats.invalidations_received >= 0
        assert stats.replications_sent >= 0
        assert stats.replications_received >= 0
        assert stats.coherence_violations >= 0
        assert stats.cross_cluster_requests >= 0

        # Hit rates should be between 0 and 1
        assert 0.0 <= stats.local_hit_rate() <= 1.0
        assert 0.0 <= stats.federation_hit_rate() <= 1.0
        assert 0.0 <= stats.overall_hit_rate() <= 1.0

        # Efficiency should be between 0 and 1
        assert 0.0 <= stats.coherence_efficiency() <= 1.0
        assert 0.0 <= stats.federation_efficiency() <= 1.0

        # Total calculations should be consistent
        assert stats.total_hits() == stats.local_hits + stats.federation_hits
        assert stats.total_misses() == stats.local_misses + stats.federation_misses
        assert stats.total_requests() == stats.total_hits() + stats.total_misses()

        # Adding hits should increment correctly
        local_hit_stats = stats.with_local_hit()
        assert local_hit_stats.local_hits == stats.local_hits + 1
        assert local_hit_stats.total_hits() == stats.total_hits() + 1

        fed_hit_stats = stats.with_federation_hit()
        assert fed_hit_stats.federation_hits == stats.federation_hits + 1
        # Cross-cluster requests should be at least as large as federation hits for consistency
        assert fed_hit_stats.cross_cluster_requests >= fed_hit_stats.federation_hits


class TestHypothesisStrategies:
    """Test the Hypothesis strategies themselves for completeness."""

    @given(federated_cache_key_strategy())
    @settings(max_examples=50, deadline=2000)
    def test_federated_cache_key_strategy_generates_valid_keys(
        self, key: FederatedCacheKey
    ) -> None:
        """Test that the federated cache key strategy generates valid keys."""
        # Should be able to create global and local keys without errors
        global_key = key.global_key()
        local_key = key.local_key()

        assert isinstance(global_key, str)
        assert isinstance(local_key, str)
        assert len(global_key) > 0
        assert len(local_key) > 0

    @given(cache_coherence_metadata_strategy())
    @settings(max_examples=50, deadline=2000)
    def test_cache_coherence_metadata_strategy_generates_valid_metadata(
        self, metadata: CacheCoherenceMetadata
    ) -> None:
        """Test that the coherence metadata strategy generates valid metadata."""
        # All state check methods should work without errors
        assert isinstance(metadata.is_exclusive(), bool)
        assert isinstance(metadata.is_shared(), bool)
        assert isinstance(metadata.is_valid(), bool)
        assert isinstance(metadata.can_write(), bool)
        assert isinstance(metadata.can_read(), bool)

    @given(cache_invalidation_message_strategy())
    @settings(max_examples=30, deadline=3000)
    def test_cache_invalidation_message_strategy_generates_valid_messages(
        self, message: CacheInvalidationMessage
    ) -> None:
        """Test that the invalidation message strategy generates valid messages."""
        # Expiration check should work without errors
        assert isinstance(message.is_expired(), bool)

        # Cluster affect check should work
        assert isinstance(message.affects_cluster("test-cluster"), bool)

    @given(federated_cache_statistics_strategy())
    @settings(max_examples=50, deadline=2000)
    def test_federated_cache_statistics_strategy_generates_valid_stats(
        self, stats: FederatedCacheStatistics
    ) -> None:
        """Test that the statistics strategy generates valid statistics."""
        # All calculation methods should work without errors
        assert isinstance(stats.total_hits(), int)
        assert isinstance(stats.total_misses(), int)
        assert isinstance(stats.total_requests(), int)
        assert isinstance(stats.local_hit_rate(), float)
        assert isinstance(stats.federation_hit_rate(), float)
        assert isinstance(stats.overall_hit_rate(), float)
        assert isinstance(stats.coherence_efficiency(), float)
        assert isinstance(stats.federation_efficiency(), float)


# Integration tests for cross-datastructure interactions
class TestFederatedCacheCoherenceIntegration:
    """Test integration between different federated cache coherence datastructures."""

    def test_invalidation_message_with_federated_key(self) -> None:
        """Test invalidation message creation with properly structured federated key."""
        cache_key = FederatedCacheKey.create(
            namespace="user-sessions",
            key="session:abc123",
            cluster_id="prod-cluster-01",
            region="us-west-2",
        )

        vector_clock = VectorClock.empty().increment("prod-cluster-01")
        target_clusters = frozenset(["prod-cluster-02", "prod-cluster-03"])

        invalidation_msg = CacheInvalidationMessage.create_single_key_invalidation(
            cache_key=cache_key,
            source_cluster="prod-cluster-01",
            target_clusters=target_clusters,
            vector_clock=vector_clock,
        )

        # Verify the message properly incorporates the federated key
        assert invalidation_msg.cache_key == cache_key
        assert cache_key.region in invalidation_msg.topic_pattern
        assert cache_key.base_key.namespace.name in invalidation_msg.topic_pattern

        # Verify targeting logic
        assert invalidation_msg.affects_cluster("prod-cluster-02")
        assert invalidation_msg.affects_cluster("prod-cluster-03")
        assert not invalidation_msg.affects_cluster("prod-cluster-01")  # Source cluster

    def test_replication_message_with_coherence_metadata(self) -> None:
        """Test replication message with coherence metadata integration."""
        cache_key = FederatedCacheKey.create(
            namespace="distributed-cache",
            key="data:xyz789",
            cluster_id="cluster-alpha",
        )

        # Create coherence metadata with sharing clusters
        initial_metadata = CacheCoherenceMetadata.create_initial("cluster-alpha")
        sharing_clusters = frozenset(["cluster-beta", "cluster-gamma"])
        shared_metadata = initial_metadata.with_sharing_clusters(sharing_clusters)

        cache_value = {"content": "distributed_data", "version": 2}

        replication_msg = CacheReplicationMessage.create_sync_replication(
            cache_key=cache_key,
            cache_value=cache_value,
            coherence_metadata=shared_metadata,
            source_cluster="cluster-alpha",
            target_clusters=sharing_clusters,
        )

        # Verify proper integration
        assert replication_msg.coherence_metadata.state == CacheCoherenceState.SHARED
        assert replication_msg.coherence_metadata.sharing_clusters == sharing_clusters
        assert replication_msg.target_clusters == sharing_clusters
        assert replication_msg.consistency_level == "strong"  # Sync replication

    def test_statistics_tracking_full_workflow(self) -> None:
        """Test statistics tracking through a complete cache workflow."""
        stats = FederatedCacheStatistics()

        # Simulate local cache operations
        stats_after_local_hit = stats.with_local_hit()
        assert stats_after_local_hit.local_hits == 1
        assert stats_after_local_hit.local_hit_rate() == 1.0

        # Simulate federation operations
        stats_after_fed_hit = stats_after_local_hit.with_federation_hit()
        assert stats_after_fed_hit.federation_hits == 1
        assert stats_after_fed_hit.cross_cluster_requests == 1

        # Verify overall statistics are correct
        assert stats_after_fed_hit.total_hits() == 2
        assert stats_after_fed_hit.total_requests() == 2
        assert stats_after_fed_hit.overall_hit_rate() == 1.0
        assert stats_after_fed_hit.federation_efficiency() == 1.0
