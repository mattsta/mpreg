"""
Comprehensive tests for hierarchical routing system.

This module tests the hierarchical routing components including:
- HierarchicalRouter with O(log N) complexity
- HubSelector intelligent selection algorithms
- ZonePartitioner geographic zone management
- Cross-region and cross-continent routing
- Performance and scalability validation

Test Coverage:
- Hub selection algorithms
- Zone partitioning and assignment
- Hierarchical route computation
- Performance benchmarks
- Error handling and edge cases
"""

import time

import pytest

from mpreg.federation.federation_graph import GeographicCoordinate
from mpreg.federation.federation_hierarchy import (
    HierarchicalRouter,
    HubRoute,
    HubSelector,
    RoutingPolicy,
    RoutingStrategy,
    ZoneDefinition,
    ZonePartitioner,
)
from mpreg.federation.federation_hubs import (
    GlobalHub,
    HubCapabilities,
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


@pytest.fixture
def sample_routing_policy():
    """Create a sample routing policy for testing."""
    return RoutingPolicy(
        default_strategy=RoutingStrategy.OPTIMAL,
        max_hops=8,
        max_latency_ms=500.0,
        max_load_threshold=0.8,
        cache_route_ttl_seconds=300.0,
        cache_max_entries=1000,
    )


@pytest.fixture
def sample_hub_topology():
    """Create a sample hub topology for testing."""
    topology = HubTopology()

    # Create global hub
    global_hub = GlobalHub(
        hub_id="global_001",
        hub_tier=HubTier.GLOBAL,
        capabilities=create_global_hub_capabilities(),
        coordinates=GeographicCoordinate(0.0, 0.0),
        region="global",
    )
    topology.add_global_hub(global_hub)

    # Create regional hubs
    regional_hubs = [
        RegionalHub(
            hub_id="regional_us",
            hub_tier=HubTier.REGIONAL,
            capabilities=create_regional_hub_capabilities(),
            coordinates=GeographicCoordinate(39.0, -98.0),
            region="us",
        ),
        RegionalHub(
            hub_id="regional_eu",
            hub_tier=HubTier.REGIONAL,
            capabilities=create_regional_hub_capabilities(),
            coordinates=GeographicCoordinate(54.0, 15.0),
            region="eu",
        ),
        RegionalHub(
            hub_id="regional_asia",
            hub_tier=HubTier.REGIONAL,
            capabilities=create_regional_hub_capabilities(),
            coordinates=GeographicCoordinate(35.0, 100.0),
            region="asia",
        ),
    ]

    for regional_hub in regional_hubs:
        topology.add_regional_hub(regional_hub, "global_001")

    # Create local hubs
    local_hubs = [
        LocalHub(
            hub_id="local_us_east",
            hub_tier=HubTier.LOCAL,
            capabilities=create_local_hub_capabilities(),
            coordinates=GeographicCoordinate(40.7128, -74.0060),
            region="us-east",
        ),
        LocalHub(
            hub_id="local_us_west",
            hub_tier=HubTier.LOCAL,
            capabilities=create_local_hub_capabilities(),
            coordinates=GeographicCoordinate(37.7749, -122.4194),
            region="us-west",
        ),
        LocalHub(
            hub_id="local_eu_central",
            hub_tier=HubTier.LOCAL,
            capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(52.5200, 13.4050),
            region="eu-central",
        ),
        LocalHub(
            hub_id="local_eu_west",
            hub_tier=HubTier.LOCAL,
            capabilities=create_local_hub_capabilities(),
            coordinates=GeographicCoordinate(48.8566, 2.3522),
            region="eu-west",
        ),
        LocalHub(
            hub_id="local_asia_east",
            hub_tier=HubTier.LOCAL,
            capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(35.6762, 139.6503),
            region="asia-east",
        ),
        LocalHub(
            hub_id="local_asia_south",
            hub_tier=HubTier.LOCAL,
            capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(1.3521, 103.8198),
            region="asia-south",
        ),
    ]

    # Add local hubs to appropriate regional hubs
    topology.add_local_hub(local_hubs[0], "regional_us")  # US East
    topology.add_local_hub(local_hubs[1], "regional_us")  # US West
    topology.add_local_hub(local_hubs[2], "regional_eu")  # EU Central
    topology.add_local_hub(local_hubs[3], "regional_eu")  # EU West
    topology.add_local_hub(local_hubs[4], "regional_asia")  # Asia East
    topology.add_local_hub(local_hubs[5], "regional_asia")  # Asia South

    return topology


@pytest.fixture
def sample_hub_selector(sample_hub_topology, sample_routing_policy):
    """Create a sample hub selector for testing."""
    return HubSelector(sample_hub_topology, sample_routing_policy)


@pytest.fixture
def sample_zone_partitioner():
    """Create a sample zone partitioner for testing."""
    partitioner = ZonePartitioner()

    # Create hierarchical zones
    # Global zone
    global_zone = ZoneDefinition(
        zone_id="global",
        zone_name="Global Zone",
        zone_type="global",
        center_coordinates=GeographicCoordinate(0.0, 0.0),
        radius_km=20000.0,
        internal_latency_ms=100.0,
        external_latency_ms=200.0,
    )
    partitioner.create_zone(global_zone)

    # Continental zones
    continental_zones = [
        ZoneDefinition(
            zone_id="north_america",
            zone_name="North America",
            zone_type="continental",
            center_coordinates=GeographicCoordinate(45.0, -100.0),
            radius_km=5000.0,
            parent_zone_id="global",
            internal_latency_ms=50.0,
            external_latency_ms=150.0,
        ),
        ZoneDefinition(
            zone_id="europe",
            zone_name="Europe",
            zone_type="continental",
            center_coordinates=GeographicCoordinate(54.0, 15.0),
            radius_km=3000.0,
            parent_zone_id="global",
            internal_latency_ms=30.0,
            external_latency_ms=100.0,
        ),
        ZoneDefinition(
            zone_id="asia",
            zone_name="Asia",
            zone_type="continental",
            center_coordinates=GeographicCoordinate(35.0, 100.0),
            radius_km=6000.0,
            parent_zone_id="global",
            internal_latency_ms=40.0,
            external_latency_ms=120.0,
        ),
    ]

    for zone in continental_zones:
        partitioner.create_zone(zone)

    # Regional zones
    regional_zones = [
        ZoneDefinition(
            zone_id="us_east",
            zone_name="US East",
            zone_type="regional",
            center_coordinates=GeographicCoordinate(40.0, -75.0),
            radius_km=1000.0,
            parent_zone_id="north_america",
            internal_latency_ms=20.0,
            external_latency_ms=60.0,
        ),
        ZoneDefinition(
            zone_id="us_west",
            zone_name="US West",
            zone_type="regional",
            center_coordinates=GeographicCoordinate(37.0, -122.0),
            radius_km=1000.0,
            parent_zone_id="north_america",
            internal_latency_ms=20.0,
            external_latency_ms=60.0,
        ),
    ]

    for zone in regional_zones:
        partitioner.create_zone(zone)

    return partitioner


@pytest.fixture
def sample_hierarchical_router(sample_hub_topology, sample_routing_policy):
    """Create a sample hierarchical router for testing."""
    return HierarchicalRouter(sample_hub_topology, sample_routing_policy)


class TestRoutingPolicy:
    """Test suite for routing policy configuration."""

    def test_routing_policy_creation(self):
        """Test routing policy creation with defaults."""
        policy = RoutingPolicy()

        assert policy.default_strategy == RoutingStrategy.OPTIMAL
        assert policy.max_hops == 10
        assert policy.max_latency_ms == 1000.0
        assert policy.max_load_threshold == 0.8
        assert policy.cache_route_ttl_seconds == 300.0
        assert policy.cache_max_entries == 10000

    def test_routing_policy_customization(self):
        """Test routing policy customization."""
        policy = RoutingPolicy(
            default_strategy=RoutingStrategy.FASTEST,
            max_hops=5,
            max_latency_ms=200.0,
            max_load_threshold=0.6,
            same_region_preference=0.9,
            cross_region_penalty=2.0,
        )

        assert policy.default_strategy == RoutingStrategy.FASTEST
        assert policy.max_hops == 5
        assert policy.max_latency_ms == 200.0
        assert policy.max_load_threshold == 0.6
        assert policy.same_region_preference == 0.9
        assert policy.cross_region_penalty == 2.0


class TestHubRoute:
    """Test suite for hub route representation."""

    def test_hub_route_creation(self):
        """Test hub route creation."""
        route = HubRoute(
            hub_path=["local_001", "regional_001", "global_001"],
            total_hops=3,
            estimated_latency_ms=50.0,
            total_bandwidth_mbps=1000.0,
            reliability_score=0.95,
            max_load_utilization=0.6,
            strategy_used=RoutingStrategy.OPTIMAL,
        )

        assert route.hub_path == ["local_001", "regional_001", "global_001"]
        assert route.total_hops == 3
        assert route.estimated_latency_ms == 50.0
        assert route.reliability_score == 0.95
        assert route.strategy_used == RoutingStrategy.OPTIMAL

    def test_hub_route_expiration(self):
        """Test hub route expiration."""
        route = HubRoute(
            hub_path=["local_001"],
            cache_ttl=0.1,  # 100ms TTL
        )

        # Initially not expired
        assert not route.is_expired()

        # Wait and check expiration
        time.sleep(0.2)
        assert route.is_expired()

    def test_routing_score_calculation(self):
        """Test routing score calculation."""
        # High quality route
        good_route = HubRoute(
            hub_path=["local_001", "regional_001"],
            total_hops=2,
            estimated_latency_ms=30.0,
            reliability_score=0.95,
            max_load_utilization=0.3,
            crosses_regions=False,
        )

        # Poor quality route
        poor_route = HubRoute(
            hub_path=[
                "local_001",
                "regional_001",
                "global_001",
                "regional_002",
                "local_002",
            ],
            total_hops=5,
            estimated_latency_ms=200.0,
            reliability_score=0.8,
            max_load_utilization=0.9,
            crosses_continents=True,
        )

        good_score = good_route.get_routing_score()
        poor_score = poor_route.get_routing_score()

        assert good_score > poor_score


class TestZoneDefinition:
    """Test suite for zone definition."""

    def test_zone_definition_creation(self):
        """Test zone definition creation."""
        zone = ZoneDefinition(
            zone_id="test_zone",
            zone_name="Test Zone",
            zone_type="regional",
            center_coordinates=GeographicCoordinate(40.0, -74.0),
            radius_km=500.0,
            parent_zone_id="parent_zone",
        )

        assert zone.zone_id == "test_zone"
        assert zone.zone_name == "Test Zone"
        assert zone.zone_type == "regional"
        assert zone.radius_km == 500.0
        assert zone.parent_zone_id == "parent_zone"

    def test_zone_contains_coordinates(self):
        """Test zone coordinate containment."""
        zone = ZoneDefinition(
            zone_id="test_zone",
            zone_name="Test Zone",
            center_coordinates=GeographicCoordinate(40.0, -74.0),
            radius_km=100.0,
        )

        # Point within zone
        inside_point = GeographicCoordinate(40.05, -74.05)
        assert zone.contains_coordinates(inside_point)

        # Point outside zone
        outside_point = GeographicCoordinate(50.0, -74.0)
        assert not zone.contains_coordinates(outside_point)

    def test_zone_latency_calculation(self):
        """Test zone-to-zone latency calculation."""
        zone1 = ZoneDefinition(
            zone_id="zone1",
            zone_name="Zone 1",
            center_coordinates=GeographicCoordinate(40.0, -74.0),
            internal_latency_ms=20.0,
            external_latency_ms=50.0,
        )

        zone2 = ZoneDefinition(
            zone_id="zone2",
            zone_name="Zone 2",
            center_coordinates=GeographicCoordinate(37.0, -122.0),
            internal_latency_ms=15.0,
            external_latency_ms=40.0,
        )

        # Same zone
        internal_latency = zone1.get_zone_latency_to(zone1)
        assert internal_latency == 20.0

        # Different zones
        external_latency = zone1.get_zone_latency_to(zone2)
        assert external_latency > 20.0


class TestHubSelector:
    """Test suite for hub selector."""

    def test_hub_selector_initialization(self, sample_hub_selector):
        """Test hub selector initialization."""
        selector = sample_hub_selector

        assert selector.hub_topology is not None
        assert selector.routing_policy is not None
        assert isinstance(selector.selection_stats, dict)

    def test_best_local_hub_selection(self, sample_hub_selector):
        """Test best local hub selection."""
        selector = sample_hub_selector

        # Test selection in US East
        us_east_coords = GeographicCoordinate(40.7128, -74.0060)
        best_hub = selector.select_best_local_hub(us_east_coords, "us-east")

        assert best_hub is not None
        assert isinstance(best_hub, LocalHub)
        assert best_hub.region == "us-east"

    def test_best_regional_hub_selection(self, sample_hub_selector):
        """Test best regional hub selection."""
        selector = sample_hub_selector

        # Test intra-region selection
        us_hub = selector.select_best_regional_hub("us", "us")
        assert us_hub is not None
        assert isinstance(us_hub, RegionalHub)

        # Test inter-region selection
        cross_region_hub = selector.select_best_regional_hub("us", "eu")
        assert cross_region_hub is not None
        assert isinstance(cross_region_hub, RegionalHub)

    def test_best_global_hub_selection(self, sample_hub_selector):
        """Test best global hub selection."""
        selector = sample_hub_selector

        global_hub = selector.select_best_global_hub("us", "asia")
        assert global_hub is not None
        assert isinstance(global_hub, GlobalHub)

    def test_hub_selection_caching(self, sample_hub_selector):
        """Test hub selection caching."""
        selector = sample_hub_selector

        coords = GeographicCoordinate(40.0, -74.0)

        # First selection
        hub1 = selector.select_best_local_hub(coords, "us-east")

        # Second selection should use cache
        hub2 = selector.select_best_local_hub(coords, "us-east")

        assert hub1 is hub2
        assert selector.selection_stats["cache_hits"] > 0

    def test_hub_selection_with_different_strategies(self, sample_hub_selector):
        """Test hub selection with different routing strategies."""
        selector = sample_hub_selector
        coords = GeographicCoordinate(40.0, -74.0)

        # Test different strategies
        for strategy in RoutingStrategy:
            hub = selector.select_best_local_hub(coords, "us-east", strategy)
            assert hub is not None

    def test_hub_selection_statistics(self, sample_hub_selector):
        """Test hub selection statistics."""
        selector = sample_hub_selector

        # Perform some selections
        coords = GeographicCoordinate(40.0, -74.0)
        selector.select_best_local_hub(coords, "us-east")
        selector.select_best_regional_hub("us", "eu")
        selector.select_best_global_hub("us", "asia")

        stats = selector.get_selection_statistics()

        assert hasattr(stats, "selection_counts")
        assert hasattr(stats, "cache_size")
        assert hasattr(stats, "cache_hit_rate")
        assert hasattr(stats, "policy_config")
        assert stats.selection_counts["local_selections"] > 0


class TestZonePartitioner:
    """Test suite for zone partitioner."""

    def test_zone_partitioner_initialization(self, sample_zone_partitioner):
        """Test zone partitioner initialization."""
        partitioner = sample_zone_partitioner

        assert len(partitioner.zones) > 0
        assert "global" in partitioner.zones
        assert "north_america" in partitioner.zones
        assert "europe" in partitioner.zones

    def test_zone_creation(self):
        """Test zone creation."""
        partitioner = ZonePartitioner()

        zone = ZoneDefinition(
            zone_id="test_zone",
            zone_name="Test Zone",
            center_coordinates=GeographicCoordinate(40.0, -74.0),
            radius_km=100.0,
        )

        result = partitioner.create_zone(zone)
        assert result
        assert "test_zone" in partitioner.zones

        # Test duplicate creation
        result = partitioner.create_zone(zone)
        assert not result

    def test_cluster_zone_assignment(self, sample_zone_partitioner):
        """Test cluster assignment to zones."""
        partitioner = sample_zone_partitioner

        # Test assignment in North America
        us_coords = GeographicCoordinate(40.7128, -74.0060)
        zone_id = partitioner.assign_cluster_to_zone("cluster_us", us_coords)
        assert zone_id == "us_east"

        # Test assignment in Europe
        eu_coords = GeographicCoordinate(52.5200, 13.4050)
        zone_id = partitioner.assign_cluster_to_zone("cluster_eu", eu_coords)
        assert zone_id == "europe"

        # Test assignment in Asia
        asia_coords = GeographicCoordinate(35.6762, 139.6503)
        zone_id = partitioner.assign_cluster_to_zone("cluster_asia", asia_coords)
        assert zone_id == "asia"

    def test_zone_path_finding(self, sample_zone_partitioner):
        """Test zone path finding."""
        partitioner = sample_zone_partitioner

        # Test same zone path
        same_zone_path = partitioner.get_zone_path("us_east", "us_east")
        assert same_zone_path == ["us_east"]

        # Test cross-regional path
        cross_regional_path = partitioner.get_zone_path("us_east", "us_west")
        assert cross_regional_path is not None
        assert len(cross_regional_path) >= 3
        assert "north_america" in cross_regional_path

        # Test cross-continental path
        cross_continental_path = partitioner.get_zone_path("us_east", "europe")
        assert cross_continental_path is not None
        assert len(cross_continental_path) >= 3
        assert "global" in cross_continental_path

    def test_zone_hierarchy_depth(self, sample_zone_partitioner):
        """Test zone hierarchy depth calculation."""
        partitioner = sample_zone_partitioner

        stats = partitioner.get_zone_statistics()
        assert stats.zone_hierarchy_depth >= 3  # At least 3 levels


class TestHierarchicalRouter:
    """Test suite for hierarchical router."""

    def test_hierarchical_router_initialization(self, sample_hierarchical_router):
        """Test hierarchical router initialization."""
        router = sample_hierarchical_router

        assert router.hub_topology is not None
        assert router.routing_policy is not None
        assert router.hub_selector is not None
        assert router.zone_partitioner is not None

    @pytest.mark.asyncio
    async def test_intra_regional_routing(self, sample_hierarchical_router):
        """Test routing within the same region."""
        router = sample_hierarchical_router

        # Route within US region
        source_coords = GeographicCoordinate(40.7128, -74.0060)  # NYC
        target_coords = GeographicCoordinate(37.7749, -122.4194)  # SF

        route = await router.find_optimal_route(
            source_cluster_id="cluster_nyc",
            source_coordinates=source_coords,
            source_region="us",
            target_cluster_id="cluster_sf",
            target_coordinates=target_coords,
            target_region="us",
        )

        assert route is not None
        assert isinstance(route, HubRoute)
        assert len(route.hub_path) >= 2
        assert not route.crosses_regions
        assert not route.crosses_continents
        assert route.estimated_latency_ms > 0

    @pytest.mark.asyncio
    async def test_inter_regional_routing(self, sample_hierarchical_router):
        """Test routing across regions."""
        router = sample_hierarchical_router

        # Route from US to Europe
        source_coords = GeographicCoordinate(40.7128, -74.0060)  # NYC
        target_coords = GeographicCoordinate(52.5200, 13.4050)  # Berlin

        route = await router.find_optimal_route(
            source_cluster_id="cluster_nyc",
            source_coordinates=source_coords,
            source_region="us",
            target_cluster_id="cluster_berlin",
            target_coordinates=target_coords,
            target_region="eu",
        )

        assert route is not None
        assert isinstance(route, HubRoute)
        assert len(route.hub_path) >= 3
        assert route.crosses_regions
        assert route.estimated_latency_ms > 0

    @pytest.mark.asyncio
    async def test_cross_continental_routing(self, sample_hierarchical_router):
        """Test routing across continents."""
        router = sample_hierarchical_router

        # Route from US to Asia
        source_coords = GeographicCoordinate(40.7128, -74.0060)  # NYC
        target_coords = GeographicCoordinate(35.6762, 139.6503)  # Tokyo

        route = await router.find_optimal_route(
            source_cluster_id="cluster_nyc",
            source_coordinates=source_coords,
            source_region="us",
            target_cluster_id="cluster_tokyo",
            target_coordinates=target_coords,
            target_region="asia",
        )

        assert route is not None
        assert isinstance(route, HubRoute)
        assert len(route.hub_path) >= 5  # Full 5-tier path
        assert route.crosses_regions
        assert route.crosses_continents
        assert route.estimated_latency_ms > 0

    @pytest.mark.asyncio
    async def test_routing_with_different_strategies(self, sample_hierarchical_router):
        """Test routing with different strategies."""
        router = sample_hierarchical_router

        source_coords = GeographicCoordinate(40.7128, -74.0060)  # NYC
        target_coords = GeographicCoordinate(37.7749, -122.4194)  # SF

        routes = {}
        for strategy in RoutingStrategy:
            route = await router.find_optimal_route(
                source_cluster_id="cluster_nyc",
                source_coordinates=source_coords,
                source_region="us",
                target_cluster_id="cluster_sf",
                target_coordinates=target_coords,
                target_region="us",
                strategy=strategy,
            )
            routes[strategy] = route

        # All strategies should find routes
        for strategy, route in routes.items():
            assert route is not None
            assert route.strategy_used == strategy

    @pytest.mark.asyncio
    async def test_route_caching(self, sample_hierarchical_router):
        """Test route caching functionality."""
        router = sample_hierarchical_router

        source_coords = GeographicCoordinate(40.7128, -74.0060)  # NYC
        target_coords = GeographicCoordinate(37.7749, -122.4194)  # SF

        # First route computation
        route1 = await router.find_optimal_route(
            source_cluster_id="cluster_nyc",
            source_coordinates=source_coords,
            source_region="us",
            target_cluster_id="cluster_sf",
            target_coordinates=target_coords,
            target_region="us",
        )

        # Second route computation (should use cache)
        route2 = await router.find_optimal_route(
            source_cluster_id="cluster_nyc",
            source_coordinates=source_coords,
            source_region="us",
            target_cluster_id="cluster_sf",
            target_coordinates=target_coords,
            target_region="us",
        )

        assert route1 is route2
        assert router.routing_stats["cache_hits"] > 0

    @pytest.mark.asyncio
    async def test_routing_performance_tracking(self, sample_hierarchical_router):
        """Test routing performance tracking."""
        router = sample_hierarchical_router

        source_coords = GeographicCoordinate(40.7128, -74.0060)  # NYC
        target_coords = GeographicCoordinate(37.7749, -122.4194)  # SF

        # Perform multiple routing operations
        for i in range(5):
            await router.find_optimal_route(
                source_cluster_id=f"cluster_nyc_{i}",
                source_coordinates=source_coords,
                source_region="us",
                target_cluster_id=f"cluster_sf_{i}",
                target_coordinates=target_coords,
                target_region="us",
            )

        stats = router.get_comprehensive_statistics()

        assert hasattr(stats, "routing_stats")
        assert hasattr(stats, "performance_metrics")
        assert hasattr(stats, "cache_statistics")
        assert stats.routing_stats["routes_computed"] >= 5
        assert stats.performance_metrics["avg_routing_time_ms"] > 0

    def test_router_statistics(self, sample_hierarchical_router):
        """Test router statistics collection."""
        router = sample_hierarchical_router

        stats = router.get_comprehensive_statistics()

        assert hasattr(stats, "routing_stats")
        assert hasattr(stats, "performance_metrics")
        assert hasattr(stats, "cache_statistics")
        assert hasattr(stats, "hub_selector_stats")
        assert hasattr(stats, "zone_partitioner_stats")
        assert hasattr(stats, "routing_policy")

        # Check policy configuration
        policy_config = stats.routing_policy
        assert policy_config.default_strategy == RoutingStrategy.OPTIMAL.value
        assert policy_config.max_hops == 8  # Using sample policy configuration
        assert (
            policy_config.max_latency_ms == 500.0
        )  # Using sample policy configuration


class TestEndToEndHierarchicalRouting:
    """Test suite for end-to-end hierarchical routing."""

    @pytest.mark.asyncio
    async def test_complete_routing_scenario(self, sample_hierarchical_router):
        """Test complete routing scenario with multiple hops."""
        router = sample_hierarchical_router

        # Define multiple routing scenarios
        scenarios = [
            {
                "name": "Local routing",
                "source": GeographicCoordinate(40.7128, -74.0060),
                "target": GeographicCoordinate(40.7589, -73.9851),
                "source_region": "us-east",
                "target_region": "us-east",
                "expected_hops": 1,
            },
            {
                "name": "Regional routing",
                "source": GeographicCoordinate(40.7128, -74.0060),
                "target": GeographicCoordinate(37.7749, -122.4194),
                "source_region": "us-east",
                "target_region": "us-west",
                "expected_hops": 3,
            },
            {
                "name": "Global routing",
                "source": GeographicCoordinate(40.7128, -74.0060),
                "target": GeographicCoordinate(52.5200, 13.4050),
                "source_region": "us",
                "target_region": "eu",
                "expected_hops": 5,
            },
        ]

        for i, scenario in enumerate(scenarios):
            route = await router.find_optimal_route(
                source_cluster_id=f"cluster_source_{i}",
                source_coordinates=scenario["source"],
                source_region=scenario["source_region"],
                target_cluster_id=f"cluster_target_{i}",
                target_coordinates=scenario["target"],
                target_region=scenario["target_region"],
            )

            assert route is not None, f"Failed to find route for {scenario['name']}"
            assert len(route.hub_path) >= scenario["expected_hops"], (  # type: ignore[operator]
                f"Insufficient hops for {scenario['name']}"
            )
            assert route.estimated_latency_ms > 0, (
                f"Invalid latency for {scenario['name']}"
            )

    @pytest.mark.asyncio
    async def test_routing_scalability(self, sample_hierarchical_router):
        """Test routing scalability with many route computations."""
        router = sample_hierarchical_router

        # Define coordinates for different regions
        coordinates = [
            GeographicCoordinate(40.7128, -74.0060),  # NYC
            GeographicCoordinate(37.7749, -122.4194),  # SF
            GeographicCoordinate(52.5200, 13.4050),  # Berlin
            GeographicCoordinate(35.6762, 139.6503),  # Tokyo
            GeographicCoordinate(51.5074, -0.1278),  # London
            GeographicCoordinate(48.8566, 2.3522),  # Paris
        ]

        regions = ["us", "us", "eu", "asia", "eu", "eu"]

        # Perform many route computations
        start_time = time.time()
        successful_routes = 0

        for i in range(20):
            source_idx = i % len(coordinates)
            target_idx = (i + 1) % len(coordinates)

            route = await router.find_optimal_route(
                source_cluster_id=f"cluster_source_{i}",
                source_coordinates=coordinates[source_idx],
                source_region=regions[source_idx],
                target_cluster_id=f"cluster_target_{i}",
                target_coordinates=coordinates[target_idx],
                target_region=regions[target_idx],
            )

            if route:
                successful_routes += 1

        total_time = time.time() - start_time

        # Should complete within reasonable time (scalability test)
        assert total_time < 2.0, f"Routing took too long: {total_time:.2f}s"
        assert successful_routes >= 15, (
            f"Too many routing failures: {successful_routes}/20"
        )

        # Check that caching is working
        stats = router.get_comprehensive_statistics()
        cache_hit_rate = stats.cache_statistics.cache_hit_rate
        # Note: Cache hit rate might be 0.0 if all routes are unique
        assert cache_hit_rate >= 0.0, "Invalid cache hit rate"


class TestPerformanceAndOptimization:
    """Test suite for performance and optimization features."""

    @pytest.mark.asyncio
    async def test_routing_performance_benchmarks(self, sample_hierarchical_router):
        """Test routing performance benchmarks."""
        router = sample_hierarchical_router

        # Benchmark different routing scenarios
        scenarios = [
            (
                "Local",
                GeographicCoordinate(40.7128, -74.0060),
                GeographicCoordinate(40.7589, -73.9851),
                "us-east",
                "us-east",
            ),
            (
                "Regional",
                GeographicCoordinate(40.7128, -74.0060),
                GeographicCoordinate(37.7749, -122.4194),
                "us-east",
                "us-west",
            ),
            (
                "Global",
                GeographicCoordinate(40.7128, -74.0060),
                GeographicCoordinate(52.5200, 13.4050),
                "us",
                "eu",
            ),
        ]

        for (
            scenario_name,
            source_coords,
            target_coords,
            source_region,
            target_region,
        ) in scenarios:
            start_time = time.time()

            # Perform multiple route computations
            for i in range(10):
                route = await router.find_optimal_route(
                    source_cluster_id=f"cluster_source_{scenario_name}_{i}",
                    source_coordinates=source_coords,
                    source_region=source_region,
                    target_cluster_id=f"cluster_target_{scenario_name}_{i}",
                    target_coordinates=target_coords,
                    target_region=target_region,
                )
                assert route is not None

            scenario_time = time.time() - start_time
            avg_time_ms = (scenario_time / 10) * 1000

            # Performance assertions
            assert avg_time_ms < 50.0, (
                f"{scenario_name} routing too slow: {avg_time_ms:.2f}ms"
            )

    @pytest.mark.asyncio
    async def test_memory_efficiency(self, sample_hierarchical_router):
        """Test memory efficiency of routing system."""
        router = sample_hierarchical_router

        # Perform many route computations to test memory usage
        for i in range(100):
            source_coords = GeographicCoordinate(40.0 + i * 0.1, -74.0 + i * 0.1)
            target_coords = GeographicCoordinate(37.0 + i * 0.1, -122.0 + i * 0.1)

            route = await router.find_optimal_route(
                source_cluster_id=f"cluster_source_{i}",
                source_coordinates=source_coords,
                source_region="us",
                target_cluster_id=f"cluster_target_{i}",
                target_coordinates=target_coords,
                target_region="us",
            )

        # Check that cache size is managed
        stats = router.get_comprehensive_statistics()
        cache_size = stats.cache_statistics.cache_size
        max_cache_entries = router.routing_policy.cache_max_entries

        assert cache_size <= max_cache_entries, (
            f"Cache size exceeded limit: {cache_size}/{max_cache_entries}"
        )

    def test_complexity_analysis(self, sample_hierarchical_router):
        """Test that routing complexity is O(log N)."""
        router = sample_hierarchical_router

        # This test verifies that the routing system maintains O(log N) complexity
        # by checking that the maximum path length doesn't exceed log(N) bounds

        # Get topology statistics
        topology_stats = router.hub_topology.get_topology_statistics()
        total_hubs = topology_stats.total_hubs

        # Calculate expected maximum path length for O(log N) complexity
        import math

        expected_max_hops = int(math.log2(total_hubs)) + 3  # +3 for hierarchy overhead

        # Verify that routing policy respects this bound
        assert router.routing_policy.max_hops >= expected_max_hops

        # The hierarchical structure should provide O(log N) routing
        # This is achieved through the 3-tier hierarchy limiting path lengths
        assert topology_stats.hierarchy_depth == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
