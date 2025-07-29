"""
Tests for Location-Based Cache Consistency and Replication System.

Tests cover:
- Geographic cluster awareness and location-based routing
- Vector clock-based conflict resolution
- Cache entry replication with configurable strategies
- Location-aware cache pinning and migration
- Cross-cluster cache coordination and synchronization

Uses standard MPREG testing patterns with live servers where appropriate.
"""

import time
from collections.abc import Callable
from typing import Any

import pytest

from mpreg.core.advanced_cache_ops import AdvancedCacheOperations
from mpreg.core.cache_pubsub_integration import CachePubSubIntegration
from mpreg.core.global_cache import (
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.core.location_consistency import (
    ConflictResolution,
    ConsistencyLevel,
    LocationConsistencyConfig,
    LocationConsistencyManager,
    LocationInfo,
    ReplicatedCacheEntry,
    ReplicationStrategy,
)
from mpreg.core.topic_exchange import TopicExchange
from mpreg.datastructures.vector_clock import VectorClock
from mpreg.server import MPREGServer


class TestVectorClock:
    """Test vector clock implementation for causal consistency."""

    def test_vector_clock_creation(self):
        """Test basic vector clock creation and operations."""
        clock = VectorClock.empty()
        assert clock.to_dict() == {}

        # Test increment
        clock1 = clock.increment("cluster-1")
        assert clock1.to_dict() == {"cluster-1": 1}

        clock2 = clock1.increment("cluster-1")
        assert clock2.to_dict() == {"cluster-1": 2}

        clock3 = clock2.increment("cluster-2")
        assert clock3.to_dict() == {"cluster-1": 2, "cluster-2": 1}

    def test_vector_clock_merge(self):
        """Test merging vector clocks."""
        clock1 = VectorClock.from_dict({"cluster-1": 3, "cluster-2": 1})
        clock2 = VectorClock.from_dict({"cluster-1": 2, "cluster-2": 4, "cluster-3": 1})

        merged = clock1.update(clock2)
        expected = {"cluster-1": 3, "cluster-2": 4, "cluster-3": 1}
        assert merged.to_dict() == expected

    def test_vector_clock_happens_before(self):
        """Test causal ordering with happens-before relation."""
        clock1 = VectorClock.from_dict({"cluster-1": 1, "cluster-2": 2})
        clock2 = VectorClock.from_dict({"cluster-1": 2, "cluster-2": 3})
        clock3 = VectorClock.from_dict({"cluster-1": 1, "cluster-2": 3})

        # clock1 happens before clock2
        assert clock1.happens_before(clock2)
        assert not clock2.happens_before(clock1)

        # clock1 happens before clock3 (1,2) < (1,3)
        assert clock1.happens_before(clock3)
        assert not clock3.happens_before(clock1)

        # Test concurrent clocks
        clock4 = VectorClock.from_dict({"cluster-1": 2, "cluster-2": 1})
        assert clock1.concurrent_with(clock4)
        assert not clock1.happens_before(clock4)
        assert not clock4.happens_before(clock1)

    def test_vector_clock_concurrent(self):
        """Test concurrent vector clocks."""
        clock1 = VectorClock.from_dict({"cluster-1": 2, "cluster-2": 1})
        clock2 = VectorClock.from_dict({"cluster-1": 1, "cluster-2": 2})

        assert clock1.concurrent_with(clock2)
        assert clock2.concurrent_with(clock1)


class TestLocationInfo:
    """Test location information for geographic clusters."""

    def test_location_info_creation(self):
        """Test basic location info creation."""
        location = LocationInfo(
            cluster_id="us-west-1",
            region="us-west",
            zone="us-west-1a",
            latitude=37.7749,
            longitude=-122.4194,
            data_center="sf-dc-1",
            provider="aws",
            network_tier="premium",
        )

        assert location.cluster_id == "us-west-1"
        assert location.region == "us-west"
        assert location.zone == "us-west-1a"
        assert location.latitude == 37.7749
        assert location.longitude == -122.4194
        assert location.data_center == "sf-dc-1"
        assert location.provider == "aws"
        assert location.network_tier == "premium"


class TestReplicatedCacheEntry:
    """Test replicated cache entry structure."""

    def test_replicated_entry_creation(self):
        """Test creation of replicated cache entries."""
        cache_key = GlobalCacheKey(namespace="test", identifier="key1")
        vector_clock = VectorClock.from_dict({"cluster-1": 1})

        entry = ReplicatedCacheEntry(
            key=cache_key,
            value="test_value",
            version=1,
            vector_clock=vector_clock,
            origin_cluster="cluster-1",
            consistency_level=ConsistencyLevel.STRONG,
            replication_strategy=ReplicationStrategy.LOCATION_BASED,
        )

        assert entry.key == cache_key
        assert entry.value == "test_value"
        assert entry.version == 1
        assert entry.vector_clock == vector_clock
        assert entry.origin_cluster == "cluster-1"
        assert entry.consistency_level == ConsistencyLevel.STRONG
        assert entry.replication_strategy == ReplicationStrategy.LOCATION_BASED
        assert entry.created_at <= time.time()


class TestLocationConsistencyConfig:
    """Test location consistency configuration."""

    def test_default_config(self):
        """Test default configuration values."""
        config = LocationConsistencyConfig()

        assert config.default_consistency_level == ConsistencyLevel.EVENTUAL
        assert config.default_replication_strategy == ReplicationStrategy.LAZY
        assert config.conflict_resolution == ConflictResolution.VECTOR_CLOCK
        assert config.max_replicas_per_entry == 3
        assert config.min_replicas_per_entry == 1
        assert config.replication_factor == 0.5
        assert config.prefer_local_reads is True
        assert config.enable_location_pinning is True

    def test_custom_config(self):
        """Test custom configuration."""
        config = LocationConsistencyConfig(
            default_consistency_level=ConsistencyLevel.STRONG,
            default_replication_strategy=ReplicationStrategy.EAGER,
            conflict_resolution=ConflictResolution.LAST_WRITE_WINS,
            max_replicas_per_entry=5,
            replication_factor=0.8,
            prefer_local_reads=False,
        )

        assert config.default_consistency_level == ConsistencyLevel.STRONG
        assert config.default_replication_strategy == ReplicationStrategy.EAGER
        assert config.conflict_resolution == ConflictResolution.LAST_WRITE_WINS
        assert config.max_replicas_per_entry == 5
        assert config.replication_factor == 0.8
        assert config.prefer_local_reads is False


class TestLocationConsistencyManagerBasic:
    """Test basic location consistency manager functionality."""

    @pytest.fixture
    def mock_components(self):
        """Create mock components for testing."""
        from unittest.mock import Mock

        cache_manager = Mock(spec=GlobalCacheManager)
        advanced_ops = Mock(spec=AdvancedCacheOperations)
        pubsub_integration = Mock(spec=CachePubSubIntegration)

        return cache_manager, advanced_ops, pubsub_integration

    @pytest.fixture
    def location_info(self):
        """Create test location info."""
        return LocationInfo(
            cluster_id="test-cluster-1",
            region="us-west",
            zone="us-west-1a",
            latitude=37.7749,
            longitude=-122.4194,
        )

    @pytest.fixture
    def consistency_manager(self, mock_components, location_info):
        """Create location consistency manager with mocked components."""
        cache_manager, advanced_ops, pubsub_integration = mock_components

        # Mock the background task startup to avoid event loop issues
        from unittest.mock import patch

        with (
            patch.object(LocationConsistencyManager, "_start_background_tasks"),
            patch.object(
                LocationConsistencyManager, "_setup_replication_subscriptions"
            ),
        ):
            return LocationConsistencyManager(
                cache_manager=cache_manager,
                advanced_cache_ops=advanced_ops,
                pubsub_integration=pubsub_integration,
                location_info=location_info,
            )

    def test_manager_initialization(self, consistency_manager, location_info):
        """Test basic manager initialization."""
        assert consistency_manager.location_info == location_info
        assert (
            consistency_manager.location_info.cluster_id
            in consistency_manager.cluster_locations
        )
        assert consistency_manager.vector_clock.to_dict() == {
            location_info.cluster_id: 0
        }
        assert len(consistency_manager.replicated_entries) == 0
        assert consistency_manager.stats["replications_sent"] == 0

    def test_register_cluster_location(self, consistency_manager):
        """Test registering new cluster locations."""
        new_location = LocationInfo(
            cluster_id="test-cluster-2",
            region="us-east",
            zone="us-east-1a",
            latitude=40.7128,
            longitude=-74.0060,
        )

        consistency_manager.register_cluster_location(new_location)
        assert new_location.cluster_id in consistency_manager.cluster_locations
        assert (
            consistency_manager.cluster_locations[new_location.cluster_id]
            == new_location
        )

    def test_register_conflict_resolver(self, consistency_manager):
        """Test registering custom conflict resolvers."""

        def custom_resolver(entry1, entry2):
            return entry1  # Always prefer first entry

        consistency_manager.register_conflict_resolver(
            "test_namespace", custom_resolver
        )
        assert "test_namespace" in consistency_manager.conflict_resolvers
        assert (
            consistency_manager.conflict_resolvers["test_namespace"] == custom_resolver
        )

    def test_select_replication_targets(self, consistency_manager):
        """Test selection of replication targets."""
        # Register some additional clusters
        locations = [
            LocationInfo("cluster-2", "us-west", "us-west-1b", 37.7749, -122.4194),
            LocationInfo("cluster-3", "us-east", "us-east-1a", 40.7128, -74.0060),
            LocationInfo("cluster-4", "eu-west", "eu-west-1a", 51.5074, -0.1278),
        ]

        for loc in locations:
            consistency_manager.register_cluster_location(loc)

        cache_key = GlobalCacheKey(namespace="test", identifier="key1")
        targets = consistency_manager._select_replication_targets(cache_key)

        # Should select some targets but not include the local cluster
        assert isinstance(targets, frozenset)
        assert consistency_manager.location_info.cluster_id not in targets
        assert len(targets) <= consistency_manager.config.max_replicas_per_entry

    def test_distance_calculation(self, consistency_manager):
        """Test geographic distance calculation."""
        # San Francisco to New York
        sf_lat, sf_lon = 37.7749, -122.4194
        ny_lat, ny_lon = 40.7128, -74.0060

        distance = consistency_manager._calculate_distance(
            sf_lat, sf_lon, ny_lat, ny_lon
        )

        # Should be roughly 4000 km
        assert 3000 < distance < 5000

    def test_entry_key_generation(self, consistency_manager):
        """Test cache entry key generation."""
        cache_key = GlobalCacheKey(
            namespace="test_ns", identifier="test_id", version="v2.0.0"
        )

        entry_key = consistency_manager._entry_key(cache_key)
        assert entry_key == "test_ns:test_id:v2.0.0"


class TestLocationConsistencyWithLiveServers:
    """Test location consistency with live MPREG servers."""

    async def test_basic_replication_flow(
        self,
        single_server: MPREGServer,
        client_factory: Callable[[int], Any],
        server_port: int,
    ):
        """Test basic cache replication flow."""

        # Set up cache system
        cache_config = GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )

        cache_manager = GlobalCacheManager(cache_config)
        advanced_ops = AdvancedCacheOperations(cache_manager)
        topic_exchange = TopicExchange(f"ws://127.0.0.1:{server_port}", "test-cluster")

        pubsub_integration = CachePubSubIntegration(
            cache_manager=cache_manager,
            advanced_cache_ops=advanced_ops,
            topic_exchange=topic_exchange,
            cluster_id="test-cluster",
        )

        # Create location info
        location_info = LocationInfo(
            cluster_id="test-cluster",
            region="us-west",
            zone="us-west-1a",
            latitude=37.7749,
            longitude=-122.4194,
        )

        # Create location consistency manager
        consistency_manager = LocationConsistencyManager(
            cache_manager=cache_manager,
            advanced_cache_ops=advanced_ops,
            pubsub_integration=pubsub_integration,
            location_info=location_info,
        )

        # Register additional cluster locations
        east_location = LocationInfo(
            cluster_id="us-east-cluster",
            region="us-east",
            zone="us-east-1a",
            latitude=40.7128,
            longitude=-74.0060,
        )
        consistency_manager.register_cluster_location(east_location)

        # Test cache key
        cache_key = GlobalCacheKey(
            namespace="test_replication",
            identifier="replicated_key",
            version="v1.0.0",
        )

        # Replicate a cache entry
        entry = await consistency_manager.replicate_cache_entry(
            key=cache_key,
            value={"data": "test_replication_value"},
            consistency_level=ConsistencyLevel.EVENTUAL,
            target_clusters=frozenset(["us-east-cluster"]),
        )

        # Verify entry was created
        assert entry.key == cache_key
        assert entry.value == {"data": "test_replication_value"}
        assert entry.origin_cluster == "test-cluster"
        assert entry.consistency_level == ConsistencyLevel.EVENTUAL

        # Verify it's stored locally
        entry_key = consistency_manager._entry_key(cache_key)
        assert entry_key in consistency_manager.replicated_entries

        # Verify vector clock was updated
        assert consistency_manager.vector_clock.to_dict()["test-cluster"] >= 1

        # Test getting the replicated entry
        retrieved_entry = await consistency_manager.get_replicated_entry(
            key=cache_key,
            prefer_local=True,
        )

        assert retrieved_entry is not None
        assert retrieved_entry.value == {"data": "test_replication_value"}
        assert retrieved_entry.access_count >= 1

        # Test statistics
        stats = consistency_manager.get_statistics()
        assert stats["replicated_entries"] >= 1
        assert stats["known_clusters"] >= 2
        assert "test-cluster" in stats["current_vector_clock"]

        # Clean up
        await consistency_manager.shutdown()
        await pubsub_integration.shutdown()

    async def test_cache_pinning(
        self,
        single_server: MPREGServer,
        client_factory: Callable[[int], Any],
        server_port: int,
    ):
        """Test cache entry pinning to specific locations."""

        # Set up minimal components
        cache_config = GlobalCacheConfiguration(enable_l2_persistent=False)
        cache_manager = GlobalCacheManager(cache_config)
        advanced_ops = AdvancedCacheOperations(cache_manager)
        topic_exchange = TopicExchange(f"ws://127.0.0.1:{server_port}", "test-cluster")

        pubsub_integration = CachePubSubIntegration(
            cache_manager=cache_manager,
            advanced_cache_ops=advanced_ops,
            topic_exchange=topic_exchange,
            cluster_id="test-cluster",
        )

        location_info = LocationInfo(
            cluster_id="test-cluster",
            region="us-west",
            zone="us-west-1a",
        )

        consistency_manager = LocationConsistencyManager(
            cache_manager=cache_manager,
            advanced_cache_ops=advanced_ops,
            pubsub_integration=pubsub_integration,
            location_info=location_info,
        )

        # Create a cache entry first
        cache_key = GlobalCacheKey(namespace="pinning_test", identifier="hot_key")

        entry = await consistency_manager.replicate_cache_entry(
            key=cache_key,
            value="hot_data",
            target_clusters=frozenset(),  # No initial replication
        )

        # Pin the entry to specific locations
        pin_locations = frozenset(["us-east-cluster", "eu-west-cluster"])
        await consistency_manager.pin_cache_entry(
            key=cache_key,
            target_locations=pin_locations,
            duration_seconds=3600,
        )

        # Verify pinning
        entry_key = consistency_manager._entry_key(cache_key)
        updated_entry = consistency_manager.replicated_entries[entry_key]

        assert pin_locations.issubset(updated_entry.pinned_locations)
        assert consistency_manager.stats["entries_pinned"] >= 1

        # Clean up
        await consistency_manager.shutdown()
        await pubsub_integration.shutdown()

    async def test_cache_migration(
        self,
        single_server: MPREGServer,
        client_factory: Callable[[int], Any],
        server_port: int,
    ):
        """Test cache entry migration between clusters."""

        # Set up components
        cache_config = GlobalCacheConfiguration(enable_l2_persistent=False)
        cache_manager = GlobalCacheManager(cache_config)
        advanced_ops = AdvancedCacheOperations(cache_manager)
        topic_exchange = TopicExchange(f"ws://127.0.0.1:{server_port}", "test-cluster")

        pubsub_integration = CachePubSubIntegration(
            cache_manager=cache_manager,
            advanced_cache_ops=advanced_ops,
            topic_exchange=topic_exchange,
            cluster_id="test-cluster",
        )

        location_info = LocationInfo(
            cluster_id="test-cluster", region="us-west", zone="us-west-1a"
        )

        consistency_manager = LocationConsistencyManager(
            cache_manager=cache_manager,
            advanced_cache_ops=advanced_ops,
            pubsub_integration=pubsub_integration,
            location_info=location_info,
        )

        # Create a cache entry
        cache_key = GlobalCacheKey(namespace="migration_test", identifier="mobile_key")

        await consistency_manager.replicate_cache_entry(
            key=cache_key,
            value="mobile_data",
        )

        # Verify entry exists locally
        entry_key = consistency_manager._entry_key(cache_key)
        assert entry_key in consistency_manager.replicated_entries

        # Migrate to another cluster
        await consistency_manager.migrate_cache_entry(
            key=cache_key,
            target_cluster="us-east-cluster",
            remove_from_source=True,
        )

        # Verify entry was removed from source (since remove_from_source=True)
        assert entry_key not in consistency_manager.replicated_entries

        # Clean up
        await consistency_manager.shutdown()
        await pubsub_integration.shutdown()

    async def test_conflict_resolution(
        self,
        single_server: MPREGServer,
        client_factory: Callable[[int], Any],
        server_port: int,
    ):
        """Test conflict resolution between concurrent updates."""

        # Set up components
        cache_config = GlobalCacheConfiguration(enable_l2_persistent=False)
        cache_manager = GlobalCacheManager(cache_config)
        advanced_ops = AdvancedCacheOperations(cache_manager)
        topic_exchange = TopicExchange(f"ws://127.0.0.1:{server_port}", "test-cluster")

        pubsub_integration = CachePubSubIntegration(
            cache_manager=cache_manager,
            advanced_cache_ops=advanced_ops,
            topic_exchange=topic_exchange,
            cluster_id="test-cluster",
        )

        location_info = LocationInfo(
            cluster_id="test-cluster", region="us-west", zone="us-west-1a"
        )

        # Configure for vector clock conflict resolution
        config = LocationConsistencyConfig(
            conflict_resolution=ConflictResolution.VECTOR_CLOCK
        )

        consistency_manager = LocationConsistencyManager(
            cache_manager=cache_manager,
            advanced_cache_ops=advanced_ops,
            pubsub_integration=pubsub_integration,
            location_info=location_info,
            config=config,
        )

        # Create initial entry
        cache_key = GlobalCacheKey(
            namespace="conflict_test", identifier="contested_key"
        )

        entry1 = await consistency_manager.replicate_cache_entry(
            key=cache_key,
            value="initial_value",
        )

        # Simulate concurrent update from another cluster
        newer_clock = VectorClock.from_dict({"test-cluster": 1, "other-cluster": 2})

        new_entry_data = {
            "value": "updated_value",
            "version": 2,
            "vector_clock": newer_clock.to_dict(),
            "metadata": {"source": "other-cluster"},
        }

        # Test conflict resolution
        resolved_entry = await consistency_manager._resolve_conflict(
            existing_entry=entry1,
            new_entry_data=new_entry_data,
            new_vector_clock=newer_clock,
        )

        # Since newer_clock has higher version, it should win
        assert resolved_entry.value == "updated_value"
        assert resolved_entry.version == 2

        # Clean up
        await consistency_manager.shutdown()
        await pubsub_integration.shutdown()


class TestLocationConsistencyEdgeCases:
    """Test edge cases and error conditions."""

    def test_vector_clock_edge_cases(self):
        """Test vector clock edge cases."""
        # Empty clocks
        clock1 = VectorClock()
        clock2 = VectorClock()

        assert not clock1.happens_before(clock2)
        assert not clock2.happens_before(clock1)
        # Empty clocks are equal, not concurrent
        assert not clock1.concurrent_with(clock2)

        # Single cluster increment
        clock3 = clock1.increment("single-cluster")
        assert clock1.happens_before(clock3)
        assert not clock3.happens_before(clock1)

    def test_distance_calculation_edge_cases(self):
        """Test geographic distance calculation edge cases."""
        from unittest.mock import Mock

        cache_manager = Mock()
        advanced_ops = Mock()
        pubsub_integration = Mock()
        location_info = LocationInfo("test", "region", "zone")

        from unittest.mock import patch

        with (
            patch.object(LocationConsistencyManager, "_start_background_tasks"),
            patch.object(
                LocationConsistencyManager, "_setup_replication_subscriptions"
            ),
        ):
            manager = LocationConsistencyManager(
                cache_manager=cache_manager,
                advanced_cache_ops=advanced_ops,
                pubsub_integration=pubsub_integration,
                location_info=location_info,
            )

        # Same location
        distance = manager._calculate_distance(0, 0, 0, 0)
        assert distance == 0

        # Opposite sides of earth (approximately)
        distance = manager._calculate_distance(0, 0, 0, 180)
        assert distance > 10000  # Should be around 20,000 km

    def test_replication_with_no_targets(self):
        """Test replication when no target clusters are available."""
        from unittest.mock import Mock

        cache_manager = Mock()
        advanced_ops = Mock()
        pubsub_integration = Mock()
        location_info = LocationInfo("test", "region", "zone")

        from unittest.mock import patch

        with (
            patch.object(LocationConsistencyManager, "_start_background_tasks"),
            patch.object(
                LocationConsistencyManager, "_setup_replication_subscriptions"
            ),
        ):
            manager = LocationConsistencyManager(
                cache_manager=cache_manager,
                advanced_cache_ops=advanced_ops,
                pubsub_integration=pubsub_integration,
                location_info=location_info,
            )

        cache_key = GlobalCacheKey(namespace="test", identifier="key1")
        targets = manager._select_replication_targets(cache_key)

        # Should return empty set since no other clusters are registered
        assert targets == frozenset()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
