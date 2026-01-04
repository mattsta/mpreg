"""
Comprehensive tests for federation hub architecture.

This module tests the hierarchical hub system including:
- Three-tier hub hierarchy (Global → Regional → Local)
- Hub registration and cluster management
- Message routing and aggregation
- Load balancing and health monitoring
- Subscription state aggregation

Test Coverage:
- Hub creation and initialization
- Cluster registration and management
- Message routing through hierarchy
- Subscription aggregation
- Performance and scalability
"""

import time

import pytest

from mpreg.core.model import PubSubMessage
from mpreg.fabric.federation_graph import GeographicCoordinate
from mpreg.fabric.hubs import (
    AggregatedSubscriptionState,
    GlobalHub,
    HubCapabilities,
    HubLoadMetrics,
    HubTier,
    HubTopology,
    LocalHub,
    RegionalHub,
)


def create_local_hub_capabilities():
    """Create default capabilities for local hubs."""
    return HubCapabilities(
        max_clusters=100,
        max_child_hubs=0,  # Local hubs don't have child hubs
        max_subscriptions=100000,
        coverage_radius_km=50.0,
        aggregation_latency_ms=2.0,
        reliability_class=2,
        bandwidth_mbps=5000,
        cpu_capacity=50.0,
        memory_capacity_gb=16.0,
    )


def create_regional_hub_capabilities():
    """Create default capabilities for regional hubs."""
    return HubCapabilities(
        max_clusters=1000,
        max_child_hubs=50,
        max_subscriptions=500000,
        coverage_radius_km=1000.0,
        aggregation_latency_ms=5.0,
        reliability_class=1,
        bandwidth_mbps=10000,
        cpu_capacity=100.0,
        memory_capacity_gb=32.0,
    )


def create_global_hub_capabilities():
    """Create default capabilities for global hubs."""
    return HubCapabilities(
        max_clusters=10000,
        max_child_hubs=100,
        max_subscriptions=10000000,
        coverage_radius_km=20000.0,  # Global coverage
        aggregation_latency_ms=10.0,
        reliability_class=1,
        bandwidth_mbps=100000,
        cpu_capacity=1000.0,
        memory_capacity_gb=128.0,
    )


from mpreg.fabric.federation_optimized import ClusterIdentity, OptimizedClusterState


@pytest.fixture
def sample_coordinates():
    """Create sample geographic coordinates."""
    return GeographicCoordinate(40.7128, -74.0060)  # New York


@pytest.fixture
def sample_cluster_identity():
    """Create a sample cluster identity."""
    return ClusterIdentity(
        cluster_id="test_cluster",
        cluster_name="Test Cluster",
        region="us-east",
        bridge_url="ws://test.example.com",
        public_key_hash="test_hash",
        created_at=time.time(),
        geographic_coordinates=(40.7128, -74.0060),
        network_tier=1,
        max_bandwidth_mbps=1000,
        preference_weight=1.0,
    )


@pytest.fixture
def sample_message():
    """Create a sample message for testing."""
    return PubSubMessage(
        topic="test.topic",
        payload=b"test payload",
        timestamp=time.time(),
        message_id="test_msg_001",
        publisher="test_publisher",
    )


@pytest.fixture
def local_hub(sample_coordinates):
    """Create a local hub for testing."""
    return LocalHub(
        hub_id="local_hub_001",
        hub_tier=HubTier.LOCAL,
        capabilities=create_local_hub_capabilities(),
        coordinates=sample_coordinates,
        region="us-east",
    )


@pytest.fixture
def regional_hub(sample_coordinates):
    """Create a regional hub for testing."""
    return RegionalHub(
        hub_id="regional_hub_001",
        hub_tier=HubTier.REGIONAL,
        capabilities=create_regional_hub_capabilities(),
        coordinates=sample_coordinates,
        region="us-east",
    )


@pytest.fixture
def global_hub(sample_coordinates):
    """Create a global hub for testing."""
    return GlobalHub(
        hub_id="global_hub_001",
        hub_tier=HubTier.GLOBAL,
        capabilities=create_global_hub_capabilities(),
        coordinates=sample_coordinates,
        region="global",
    )


@pytest.fixture
def hub_topology():
    """Create a hub topology for testing."""
    return HubTopology()


class TestHubCapabilities:
    """Test suite for hub capabilities."""

    def test_hub_capabilities_creation(self):
        """Test hub capabilities creation."""
        capabilities = HubCapabilities(
            max_clusters=500,
            max_child_hubs=20,
            max_subscriptions=100000,
            coverage_radius_km=100.0,
        )

        assert capabilities.max_clusters == 500
        assert capabilities.max_child_hubs == 20
        assert capabilities.max_subscriptions == 100000
        assert capabilities.coverage_radius_km == 100.0
        assert capabilities.reliability_class == 1
        assert capabilities.bandwidth_mbps == 10000


class TestHubLoadMetrics:
    """Test suite for hub load metrics."""

    def test_load_metrics_initialization(self):
        """Test load metrics initialization."""
        metrics = HubLoadMetrics()

        assert metrics.active_clusters == 0
        assert metrics.active_child_hubs == 0
        assert metrics.active_subscriptions == 0
        assert metrics.current_cpu_usage == 0.0
        assert metrics.consecutive_failures == 0

    def test_utilization_score_calculation(self):
        """Test utilization score calculation."""
        metrics = HubLoadMetrics()

        # Test low utilization
        metrics.current_cpu_usage = 25.0
        metrics.current_memory_usage_gb = 8.0
        assert metrics.get_utilization_score() < 0.5

        # Test high utilization
        metrics.current_cpu_usage = 90.0
        metrics.current_memory_usage_gb = 30.0
        assert metrics.get_utilization_score() > 0.8

    def test_health_check(self):
        """Test health check functionality."""
        metrics = HubLoadMetrics()

        # Test healthy state
        assert metrics.is_healthy()

        # Test unhealthy due to failures
        metrics.consecutive_failures = 5
        assert not metrics.is_healthy()

        # Test unhealthy due to overload
        metrics.consecutive_failures = 0
        metrics.current_cpu_usage = 95.0
        assert not metrics.is_healthy()

        # Test unhealthy due to stale health check
        metrics.current_cpu_usage = 50.0
        metrics.last_health_check = time.time() - 120.0
        assert not metrics.is_healthy()


class TestAggregatedSubscriptionState:
    """Test suite for aggregated subscription state."""

    def test_subscription_state_initialization(self):
        """Test subscription state initialization."""
        state = AggregatedSubscriptionState()

        assert state.total_subscriptions == 0
        assert state.compression_ratio == 0.0
        assert len(state.subscription_counts) == 0
        assert len(state.popular_topics) == 0

    def test_subscription_aggregation(self):
        """Test subscription aggregation from clusters."""
        state = AggregatedSubscriptionState()

        # Create sample cluster states
        cluster_states = {}
        for i in range(3):
            cluster_state = OptimizedClusterState(cluster_id=f"cluster_{i}")
            cluster_state.pattern_set = {f"topic.{i}", "common.topic", f"unique.{i}"}
            cluster_states[f"cluster_{i}"] = cluster_state

        # Aggregate
        state.aggregate_from_clusters(cluster_states)

        # Check results
        assert state.total_subscriptions == 9  # 3 patterns × 3 clusters
        assert "common.topic" in state.subscription_counts
        assert state.subscription_counts["common.topic"] == 3
        assert "common.topic" in state.popular_topics
        assert state.compression_ratio > 0.0

    def test_popular_topics_identification(self):
        """Test identification of popular topics."""
        state = AggregatedSubscriptionState()

        # Create cluster states with overlapping topics
        cluster_states = {}
        for i in range(5):
            cluster_state = OptimizedClusterState(cluster_id=f"cluster_{i}")
            # Add common topics to multiple clusters
            cluster_state.pattern_set = {
                "very.popular.topic",  # In all clusters
                "somewhat.popular.topic"
                if i < 3
                else f"unique.{i}",  # In some clusters
                f"unique.{i}",  # Unique to each cluster
            }
            cluster_states[f"cluster_{i}"] = cluster_state

        state.aggregate_from_clusters(cluster_states)

        # Check popular topics are identified correctly
        assert "very.popular.topic" in state.popular_topics
        assert "somewhat.popular.topic" in state.popular_topics
        assert state.popular_topics[0] == "very.popular.topic"  # Most popular first


class TestLocalHub:
    """Test suite for local hub functionality."""

    def test_local_hub_initialization(self, sample_coordinates):
        """Test local hub initialization."""
        hub = LocalHub(
            hub_id="local_001",
            hub_tier=HubTier.LOCAL,
            capabilities=create_local_hub_capabilities(),
            coordinates=sample_coordinates,
            region="us-east",
        )

        assert hub.hub_id == "local_001"
        assert hub.hub_tier == HubTier.LOCAL
        assert hub.region == "us-east"
        assert hub.coordinates == sample_coordinates
        assert hub.capabilities.max_clusters == 100
        assert hub.capabilities.max_child_hubs == 0  # Local hubs don't have children

    @pytest.mark.asyncio
    async def test_cluster_registration(self, local_hub, sample_cluster_identity):
        """Test cluster registration with local hub."""
        # Register cluster
        result = await local_hub.register_cluster(
            "test_cluster", sample_cluster_identity
        )
        assert result

        # Check registration
        assert "test_cluster" in local_hub.registered_clusters
        assert "test_cluster" in local_hub.cluster_states
        assert local_hub.load_metrics.active_clusters == 1

        # Test duplicate registration
        result = await local_hub.register_cluster(
            "test_cluster", sample_cluster_identity
        )
        assert not result

    @pytest.mark.asyncio
    async def test_cluster_unregistration(self, local_hub, sample_cluster_identity):
        """Test cluster unregistration from local hub."""
        # Register and then unregister
        await local_hub.register_cluster("test_cluster", sample_cluster_identity)

        result = await local_hub.unregister_cluster("test_cluster")
        assert result

        # Check unregistration
        assert "test_cluster" not in local_hub.registered_clusters
        assert "test_cluster" not in local_hub.cluster_states

        # Test unregistering non-existent cluster
        result = await local_hub.unregister_cluster("non_existent")
        assert not result

    @pytest.mark.asyncio
    async def test_capacity_limits(self, local_hub):
        """Test local hub capacity limits."""
        # Create many cluster identities
        for i in range(local_hub.capabilities.max_clusters + 5):
            cluster_identity = ClusterIdentity(
                cluster_id=f"cluster_{i}",
                cluster_name=f"Cluster {i}",
                region="us-east",
                bridge_url=f"ws://cluster{i}.example.com",
                public_key_hash=f"hash_{i}",
                created_at=time.time(),
                geographic_coordinates=(40.0 + i * 0.1, -74.0 + i * 0.1),
                network_tier=1,
                max_bandwidth_mbps=1000,
                preference_weight=1.0,
            )

            result = await local_hub.register_cluster(f"cluster_{i}", cluster_identity)

            # Should succeed up to capacity
            if i < local_hub.capabilities.max_clusters:
                assert result
            else:
                assert not result

    @pytest.mark.asyncio
    async def test_message_routing(self, local_hub, sample_message):
        """Test message routing at local hub level."""
        # Test routing without registered clusters
        result = await local_hub.route_message(sample_message)
        assert isinstance(result, bool)

        # Register cluster and test routing
        cluster_identity = ClusterIdentity(
            cluster_id="test_cluster",
            cluster_name="Test Cluster",
            region="us-east",
            bridge_url="ws://test.example.com",
            public_key_hash="test_hash",
            created_at=time.time(),
            geographic_coordinates=(40.7128, -74.0060),
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        await local_hub.register_cluster("test_cluster", cluster_identity)

        # Add subscription pattern to cluster
        local_hub.cluster_states["test_cluster"].pattern_set.add("test.topic")
        local_hub.cluster_states["test_cluster"].bloom_filter.add_pattern("test.topic")

        result = await local_hub.route_message(sample_message)
        assert result

        # Check routing statistics
        assert local_hub.routing_stats["messages_routed"] > 0

    @pytest.mark.asyncio
    async def test_hub_lifecycle(self, local_hub):
        """Test hub start and stop lifecycle."""
        # Start hub
        await local_hub.start()
        assert local_hub.routing_stats["hub_started"] > 0

        # Stop hub
        await local_hub.stop()
        assert local_hub.routing_stats["hub_stopped"] > 0

    def test_hub_statistics(self, local_hub):
        """Test hub statistics collection."""
        stats = local_hub.get_comprehensive_statistics()

        assert hasattr(stats, "hub_info")
        assert stats.hub_info.hub_id == local_hub.hub_id
        assert stats.hub_info.hub_tier == "local"

        assert hasattr(stats, "capacity")
        assert stats.capacity.max_clusters == local_hub.capabilities.max_clusters

        assert hasattr(stats, "current_load")
        assert hasattr(stats, "routing_statistics")
        assert hasattr(stats, "aggregation_info")


class TestRegionalHub:
    """Test suite for regional hub functionality."""

    def test_regional_hub_initialization(self, sample_coordinates):
        """Test regional hub initialization."""
        hub = RegionalHub(
            hub_id="regional_001",
            hub_tier=HubTier.REGIONAL,
            capabilities=create_regional_hub_capabilities(),
            coordinates=sample_coordinates,
            region="us-east",
        )

        assert hub.hub_id == "regional_001"
        assert hub.hub_tier == HubTier.REGIONAL
        assert hub.region == "us-east"
        assert hub.capabilities.max_clusters == 1000
        assert hub.capabilities.max_child_hubs == 50

    @pytest.mark.asyncio
    async def test_child_hub_registration(self, regional_hub, local_hub):
        """Test child hub registration."""
        # Register local hub as child
        result = regional_hub.register_child_hub(local_hub)
        assert result

        # Check registration
        assert local_hub.hub_id in regional_hub.child_hubs
        assert local_hub.parent_hub == regional_hub

        # Test duplicate registration
        result = regional_hub.register_child_hub(local_hub)
        assert not result

    @pytest.mark.asyncio
    async def test_child_hub_unregistration(self, regional_hub, local_hub):
        """Test child hub unregistration."""
        # Register and then unregister
        regional_hub.register_child_hub(local_hub)

        result = regional_hub.unregister_child_hub(local_hub.hub_id)
        assert result

        # Check unregistration
        assert local_hub.hub_id not in regional_hub.child_hubs
        assert local_hub.parent_hub is None

    @pytest.mark.asyncio
    async def test_hierarchical_message_routing(
        self, regional_hub, local_hub, sample_message
    ):
        """Test hierarchical message routing."""
        # Set up hierarchy
        regional_hub.register_child_hub(local_hub)

        # Add subscription to local hub
        cluster_identity = ClusterIdentity(
            cluster_id="test_cluster",
            cluster_name="Test Cluster",
            region="us-east",
            bridge_url="ws://test.example.com",
            public_key_hash="test_hash",
            created_at=time.time(),
            geographic_coordinates=(40.7128, -74.0060),
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        await local_hub.register_cluster("test_cluster", cluster_identity)
        local_hub.cluster_states["test_cluster"].pattern_set.add("test.topic")
        local_hub.cluster_states["test_cluster"].bloom_filter.add_pattern("test.topic")

        # Update aggregated state
        await local_hub._update_aggregated_state()

        # Route message through regional hub
        result = await regional_hub.route_message(sample_message)
        assert isinstance(result, bool)

        # Check routing statistics
        assert regional_hub.routing_stats["messages_routed"] > 0


class TestGlobalHub:
    """Test suite for global hub functionality."""

    def test_global_hub_initialization(self, sample_coordinates):
        """Test global hub initialization."""
        hub = GlobalHub(
            hub_id="global_001",
            hub_tier=HubTier.GLOBAL,
            capabilities=create_global_hub_capabilities(),
            coordinates=sample_coordinates,
            region="global",
        )

        assert hub.hub_id == "global_001"
        assert hub.hub_tier == HubTier.GLOBAL
        assert hub.region == "global"
        assert hub.capabilities.max_clusters == 10000
        assert hub.capabilities.max_child_hubs == 100

    @pytest.mark.asyncio
    async def test_global_routing(self, global_hub, regional_hub, sample_message):
        """Test global-level message routing."""
        # Set up hierarchy
        global_hub.register_child_hub(regional_hub)

        # Add some subscription state to regional hub
        regional_hub.aggregated_state.aggregated_filter.add_pattern("test.topic")

        # Route message through global hub
        result = await global_hub.route_message(sample_message)
        assert isinstance(result, bool)

        # Check routing statistics
        assert global_hub.routing_stats["messages_routed"] > 0


class TestHubTopology:
    """Test suite for hub topology management."""

    def test_topology_initialization(self, hub_topology):
        """Test topology initialization."""
        assert len(hub_topology.global_hubs) == 0
        assert len(hub_topology.regional_hubs) == 0
        assert len(hub_topology.local_hubs) == 0
        assert len(hub_topology.hub_registry) == 0

    def test_hub_addition_to_topology(
        self, hub_topology, global_hub, regional_hub, local_hub
    ):
        """Test adding hubs to topology."""
        # Add global hub
        result = hub_topology.add_global_hub(global_hub)
        assert result
        assert global_hub.hub_id in hub_topology.global_hubs
        assert global_hub.hub_id in hub_topology.hub_registry

        # Add regional hub
        result = hub_topology.add_regional_hub(regional_hub, global_hub.hub_id)
        assert result
        assert regional_hub.hub_id in hub_topology.regional_hubs
        assert regional_hub.parent_hub == global_hub

        # Add local hub
        result = hub_topology.add_local_hub(local_hub, regional_hub.hub_id)
        assert result
        assert local_hub.hub_id in hub_topology.local_hubs
        assert local_hub.parent_hub == regional_hub

    def test_hub_hierarchy_relationships(
        self, hub_topology, global_hub, regional_hub, local_hub
    ):
        """Test hub hierarchy relationships."""
        # Build hierarchy
        hub_topology.add_global_hub(global_hub)
        hub_topology.add_regional_hub(regional_hub, global_hub.hub_id)
        hub_topology.add_local_hub(local_hub, regional_hub.hub_id)

        # Check parent-child relationships
        assert regional_hub.parent_hub == global_hub
        assert local_hub.parent_hub == regional_hub
        assert regional_hub.hub_id in global_hub.child_hubs
        assert local_hub.hub_id in regional_hub.child_hubs

    def test_best_local_hub_selection(self, hub_topology):
        """Test best local hub selection for clusters."""
        # Create local hubs at different locations
        hub1 = LocalHub(
            hub_id="local_001",
            hub_tier=HubTier.LOCAL,
            capabilities=create_local_hub_capabilities(),
            coordinates=GeographicCoordinate(40.7128, -74.0060),
            region="us-east",
        )  # NYC
        hub2 = LocalHub(
            hub_id="local_002",
            hub_tier=HubTier.LOCAL,
            capabilities=create_local_hub_capabilities(),
            coordinates=GeographicCoordinate(40.7589, -73.9851),
            region="us-east",
        )  # Manhattan
        hub3 = LocalHub(
            hub_id="local_003",
            hub_tier=HubTier.LOCAL,
            capabilities=create_local_hub_capabilities(),
            coordinates=GeographicCoordinate(34.0522, -118.2437),
            region="us-west",
        )  # LA

        # Add to topology
        regional_hub = RegionalHub(
            hub_id="regional_001",
            hub_tier=HubTier.REGIONAL,
            capabilities=create_local_hub_capabilities(),
            coordinates=GeographicCoordinate(40.0, -74.0),
            region="us-east",
        )
        hub_topology.add_global_hub(
            GlobalHub(
                hub_id="global_001",
                hub_tier=HubTier.GLOBAL,
                capabilities=create_global_hub_capabilities(),
                coordinates=GeographicCoordinate(0.0, 0.0),
                region="global",
            )
        )
        hub_topology.add_regional_hub(regional_hub, "global_001")
        hub_topology.add_local_hub(hub1, "regional_001")
        hub_topology.add_local_hub(hub2, "regional_001")

        # Test cluster in NYC area
        cluster_coords = GeographicCoordinate(40.7300, -74.0000)
        best_hub = hub_topology.find_best_local_hub(cluster_coords, "us-east")

        assert best_hub is not None
        assert best_hub.hub_id in [
            "local_001",
            "local_002",
        ]  # Should be one of the NYC hubs

        # Test cluster in different region
        best_hub = hub_topology.find_best_local_hub(cluster_coords, "us-west")
        assert best_hub is None  # No local hubs in us-west region

    def test_topology_statistics(
        self, hub_topology, global_hub, regional_hub, local_hub
    ):
        """Test topology statistics collection."""
        # Build topology
        hub_topology.add_global_hub(global_hub)
        hub_topology.add_regional_hub(regional_hub, global_hub.hub_id)
        hub_topology.add_local_hub(local_hub, regional_hub.hub_id)

        # Get statistics
        stats = hub_topology.get_topology_statistics()

        assert hasattr(stats, "hub_counts")
        assert stats.hub_counts["global_hubs"] == 1
        assert stats.hub_counts["regional_hubs"] == 1
        assert stats.hub_counts["local_hubs"] == 1

        assert hasattr(stats, "total_hubs")
        assert stats.total_hubs == 3

        assert hasattr(stats, "hierarchy_depth")
        assert stats.hierarchy_depth == 3

        assert hasattr(stats, "hub_utilization")
        assert hasattr(stats, "aggregation_ratios")

    @pytest.mark.asyncio
    async def test_topology_lifecycle(
        self, hub_topology, global_hub, regional_hub, local_hub
    ):
        """Test topology lifecycle management."""
        # Build topology
        hub_topology.add_global_hub(global_hub)
        hub_topology.add_regional_hub(regional_hub, global_hub.hub_id)
        hub_topology.add_local_hub(local_hub, regional_hub.hub_id)

        # Start all hubs
        await hub_topology.start_all_hubs()

        # Check that hubs are started
        assert global_hub.routing_stats["hub_started"] > 0
        assert regional_hub.routing_stats["hub_started"] > 0
        assert local_hub.routing_stats["hub_started"] > 0

        # Stop all hubs
        await hub_topology.stop_all_hubs()

        # Check that hubs are stopped
        assert global_hub.routing_stats["hub_stopped"] > 0
        assert regional_hub.routing_stats["hub_stopped"] > 0
        assert local_hub.routing_stats["hub_stopped"] > 0


class TestEndToEndHubFunctionality:
    """Test suite for end-to-end hub functionality."""

    @pytest.mark.asyncio
    async def test_complete_hub_hierarchy(self, hub_topology, sample_message):
        """Test complete hub hierarchy with end-to-end message routing."""
        # Create complete hierarchy
        global_hub = GlobalHub(
            hub_id="global_001",
            hub_tier=HubTier.GLOBAL,
            capabilities=create_global_hub_capabilities(),
            coordinates=GeographicCoordinate(0.0, 0.0),
            region="global",
        )
        regional_hub = RegionalHub(
            hub_id="regional_001",
            hub_tier=HubTier.REGIONAL,
            capabilities=create_local_hub_capabilities(),
            coordinates=GeographicCoordinate(40.0, -74.0),
            region="us-east",
        )
        local_hub = LocalHub(
            hub_id="local_001",
            hub_tier=HubTier.LOCAL,
            capabilities=create_local_hub_capabilities(),
            coordinates=GeographicCoordinate(40.7128, -74.0060),
            region="us-east",
        )

        # Build topology
        hub_topology.add_global_hub(global_hub)
        hub_topology.add_regional_hub(regional_hub, global_hub.hub_id)
        hub_topology.add_local_hub(local_hub, regional_hub.hub_id)

        # Register cluster with local hub
        cluster_identity = ClusterIdentity(
            cluster_id="test_cluster",
            cluster_name="Test Cluster",
            region="us-east",
            bridge_url="ws://test.example.com",
            public_key_hash="test_hash",
            created_at=time.time(),
            geographic_coordinates=(40.7128, -74.0060),
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        await local_hub.register_cluster("test_cluster", cluster_identity)
        local_hub.cluster_states["test_cluster"].pattern_set.add("test.topic")
        local_hub.cluster_states["test_cluster"].bloom_filter.add_pattern("test.topic")

        # Update aggregated states
        await local_hub._update_aggregated_state()
        await regional_hub._update_aggregated_state()
        await global_hub._update_aggregated_state()

        # Start all hubs
        await hub_topology.start_all_hubs()

        # Route message from global level
        result = await global_hub.route_message(sample_message)
        assert isinstance(result, bool)

        # Check that message was routed through hierarchy
        assert global_hub.routing_stats["messages_routed"] > 0

        # Stop all hubs
        await hub_topology.stop_all_hubs()

    @pytest.mark.asyncio
    async def test_hub_load_balancing(self, hub_topology):
        """Test hub load balancing functionality."""
        # Create multiple local hubs
        regional_hub = RegionalHub(
            hub_id="regional_001",
            hub_tier=HubTier.REGIONAL,
            capabilities=create_local_hub_capabilities(),
            coordinates=GeographicCoordinate(40.0, -74.0),
            region="us-east",
        )
        local_hubs = []

        for i in range(3):
            local_hub = LocalHub(
                hub_id=f"local_00{i}",
                hub_tier=HubTier.LOCAL,
                capabilities=create_local_hub_capabilities(),
                coordinates=GeographicCoordinate(40.0 + i, -74.0 + i),
                region="us-east",
            )
            local_hubs.append(local_hub)

        # Build topology
        hub_topology.add_global_hub(
            GlobalHub(
                hub_id="global_001",
                hub_tier=HubTier.GLOBAL,
                capabilities=create_global_hub_capabilities(),
                coordinates=GeographicCoordinate(0.0, 0.0),
                region="global",
            )
        )
        hub_topology.add_regional_hub(regional_hub, "global_001")

        for local_hub in local_hubs:
            hub_topology.add_local_hub(local_hub, "regional_001")

        # Start hubs
        await hub_topology.start_all_hubs()

        # Register clusters with different local hubs
        for i, local_hub in enumerate(local_hubs):
            for j in range(30):  # Register many clusters
                cluster_identity = ClusterIdentity(
                    cluster_id=f"cluster_{i}_{j}",
                    cluster_name=f"Cluster {i}_{j}",
                    region="us-east",
                    bridge_url=f"ws://cluster{i}{j}.example.com",
                    public_key_hash=f"hash_{i}_{j}",
                    created_at=time.time(),
                    geographic_coordinates=(40.0 + i, -74.0 + j * 0.01),
                    network_tier=1,
                    max_bandwidth_mbps=1000,
                    preference_weight=1.0,
                )

                await local_hub.register_cluster(f"cluster_{i}_{j}", cluster_identity)

        # Check load distribution
        stats = hub_topology.get_topology_statistics()
        assert stats.total_clusters == 90  # 3 hubs × 30 clusters

        # Check that load is distributed
        for local_hub in local_hubs:
            assert local_hub.load_metrics.active_clusters == 30
            assert local_hub.load_metrics.get_utilization_score() > 0.0

        # Stop hubs
        await hub_topology.stop_all_hubs()


class TestPerformanceAndScalability:
    """Test suite for performance and scalability."""

    @pytest.mark.asyncio
    async def test_large_scale_hub_topology(self):
        """Test performance with large hub topology."""
        topology = HubTopology()

        # Create large topology
        global_hub = GlobalHub(
            hub_id="global_001",
            hub_tier=HubTier.GLOBAL,
            capabilities=create_global_hub_capabilities(),
            coordinates=GeographicCoordinate(0.0, 0.0),
            region="global",
        )
        topology.add_global_hub(global_hub)

        # Create multiple regional hubs
        regional_hubs = []
        for i in range(5):
            regional_hub = RegionalHub(
                hub_id=f"regional_00{i}",
                hub_tier=HubTier.REGIONAL,
                capabilities=create_local_hub_capabilities(),
                coordinates=GeographicCoordinate(i * 10.0, i * 10.0),
                region=f"region_{i}",
            )
            regional_hubs.append(regional_hub)
            topology.add_regional_hub(regional_hub, "global_001")

        # Create many local hubs
        local_hub_count = 0
        for regional_hub in regional_hubs:
            for j in range(10):
                local_hub = LocalHub(
                    hub_id=f"local_{regional_hub.hub_id}_{j}",
                    hub_tier=HubTier.LOCAL,
                    capabilities=create_local_hub_capabilities(),
                    coordinates=GeographicCoordinate(
                        regional_hub.coordinates.latitude + j,
                        regional_hub.coordinates.longitude + j,
                    ),
                    region=regional_hub.region,
                )
                topology.add_local_hub(local_hub, regional_hub.hub_id)
                local_hub_count += 1

        # Check topology
        stats = topology.get_topology_statistics()
        assert stats.hub_counts["global_hubs"] == 1
        assert stats.hub_counts["regional_hubs"] == 5
        assert stats.hub_counts["local_hubs"] == 50
        assert stats.total_hubs == 56

        # Test performance of starting/stopping large topology
        start_time = time.time()
        await topology.start_all_hubs()
        start_duration = time.time() - start_time

        stop_time = time.time()
        await topology.stop_all_hubs()
        stop_duration = time.time() - stop_time

        # Should complete within reasonable time
        assert start_duration < 5.0
        assert stop_duration < 5.0

    @pytest.mark.asyncio
    async def test_subscription_aggregation_performance(self):
        """Test performance of subscription aggregation."""
        local_hub = LocalHub(
            hub_id="local_001",
            hub_tier=HubTier.LOCAL,
            capabilities=create_local_hub_capabilities(),
            coordinates=GeographicCoordinate(40.7128, -74.0060),
            region="us-east",
        )

        # Register many clusters with subscriptions
        for i in range(100):
            cluster_identity = ClusterIdentity(
                cluster_id=f"cluster_{i}",
                cluster_name=f"Cluster {i}",
                region="us-east",
                bridge_url=f"ws://cluster{i}.example.com",
                public_key_hash=f"hash_{i}",
                created_at=time.time(),
                geographic_coordinates=(40.0 + i * 0.01, -74.0 + i * 0.01),
                network_tier=1,
                max_bandwidth_mbps=1000,
                preference_weight=1.0,
            )

            await local_hub.register_cluster(f"cluster_{i}", cluster_identity)

            # Add subscription patterns
            for j in range(10):
                pattern = f"topic.{i}.{j}"
                local_hub.cluster_states[f"cluster_{i}"].pattern_set.add(pattern)
                local_hub.cluster_states[f"cluster_{i}"].bloom_filter.add_pattern(
                    pattern
                )

        # Test aggregation performance
        start_time = time.time()
        await local_hub._update_aggregated_state()
        aggregation_time = time.time() - start_time

        # Should complete quickly
        assert aggregation_time < 1.0

        # Check aggregation results
        assert (
            local_hub.aggregated_state.total_subscriptions == 1000
        )  # 100 clusters × 10 patterns
        assert local_hub.aggregated_state.compression_ratio > 0.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
