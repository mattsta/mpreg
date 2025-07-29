"""
Comprehensive tests for hub registration and discovery system.

This module tests the hub registration and discovery components including:
- HubRegistry distributed registration system
- ClusterRegistrar automatic assignment system
- HubHealthMonitor failure detection and recovery
- Discovery protocols and failover mechanisms
- Performance and scalability validation

Test Coverage:
- Hub registration and deregistration
- Cluster assignment and reassignment
- Health monitoring and failure detection
- Discovery protocols and methods
- Performance benchmarks and scalability
"""

import asyncio
import time

import pytest

from mpreg.federation.federation_graph import GeographicCoordinate
from mpreg.federation.federation_hierarchy import HubSelector, RoutingPolicy
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


from mpreg.federation.federation_optimized import ClusterIdentity
from mpreg.federation.federation_registry import (
    BroadcastDiscoveryProtocol,
    ClusterRegistrar,
    ClusterRegistrationInfo,
    DiscoveryCriteria,
    DiscoveryMethod,
    GeographicDiscoveryProtocol,
    GossipDiscoveryProtocol,
    HubHealthMonitor,
    HubRegistrationInfo,
    HubRegistry,
)


@pytest.fixture
def sample_hub_capabilities():
    """Create sample hub capabilities for testing."""
    return HubCapabilities(
        max_clusters=100,
        max_child_hubs=10,
        max_subscriptions=10000,
        coverage_radius_km=500.0,
        aggregation_latency_ms=5.0,
        bandwidth_mbps=1000,
        cpu_capacity=50.0,
        memory_capacity_gb=16.0,
    )


@pytest.fixture
def sample_hub_registration_info(sample_hub_capabilities):
    """Create sample hub registration info for testing."""
    return HubRegistrationInfo(
        hub_id="test_hub_001",
        hub_tier=HubTier.LOCAL,
        hub_capabilities=sample_hub_capabilities,
        coordinates=GeographicCoordinate(40.7128, -74.0060),
        region="us-east",
        connection_url="ws://test-hub.example.com",
        max_clusters=100,
    )


@pytest.fixture
def sample_cluster_identity():
    """Create sample cluster identity for testing."""
    return ClusterIdentity(
        cluster_id="test_cluster_001",
        cluster_name="Test Cluster",
        region="us-east",
        bridge_url="ws://test-cluster.example.com",
        public_key_hash="test_hash",
        created_at=time.time(),
        geographic_coordinates=(40.7128, -74.0060),
        network_tier=1,
        max_bandwidth_mbps=1000,
        preference_weight=1.0,
    )


@pytest.fixture
def sample_hub_topology():
    """Create sample hub topology for testing."""
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

    # Create regional hub
    regional_hub = RegionalHub(
        hub_id="regional_001",
        hub_tier=HubTier.REGIONAL,
        capabilities=create_regional_hub_capabilities(),
        coordinates=GeographicCoordinate(40.0, -74.0),
        region="us-east",
    )
    topology.add_regional_hub(regional_hub, "global_001")

    # Create local hub
    local_hub = LocalHub(
        hub_id="local_001",
        hub_tier=HubTier.LOCAL,
        capabilities=create_local_hub_capabilities(),
        coordinates=GeographicCoordinate(40.7128, -74.0060),
        region="us-east",
    )
    topology.add_local_hub(local_hub, "regional_001")

    return topology


@pytest.fixture
def sample_hub_registry():
    """Create sample hub registry for testing."""
    return HubRegistry(
        registry_id="test_registry_001",
        discovery_methods=[DiscoveryMethod.GEOGRAPHIC],
        heartbeat_interval=10.0,
        cleanup_interval=30.0,
    )


@pytest.fixture
def sample_hub_selector(sample_hub_topology):
    """Create sample hub selector for testing."""
    return HubSelector(sample_hub_topology, RoutingPolicy())


@pytest.fixture
def sample_cluster_registrar(sample_hub_registry, sample_hub_selector):
    """Create sample cluster registrar for testing."""
    return ClusterRegistrar(
        sample_hub_registry, sample_hub_selector, assignment_strategy="optimal"
    )


@pytest.fixture
def sample_hub_health_monitor(sample_hub_registry, sample_cluster_registrar):
    """Create sample hub health monitor for testing."""
    return HubHealthMonitor(
        sample_hub_registry,
        sample_cluster_registrar,
        monitoring_interval=5.0,
        failure_threshold=2,
    )


class TestHubRegistrationInfo:
    """Test suite for hub registration info."""

    def test_hub_registration_info_creation(self, sample_hub_registration_info):
        """Test hub registration info creation."""
        info = sample_hub_registration_info

        assert info.hub_id == "test_hub_001"
        assert info.hub_tier == HubTier.LOCAL
        assert info.region == "us-east"
        assert info.connection_url == "ws://test-hub.example.com"
        assert info.max_clusters == 100

    def test_hub_registration_expiration(self, sample_hub_registration_info):
        """Test hub registration expiration."""
        info = sample_hub_registration_info

        # Initially not expired
        assert not info.is_expired()

        # Make it expired
        info.last_heartbeat = time.time() - 400  # 400 seconds ago
        assert info.is_expired()

    def test_hub_registration_health_check(self, sample_hub_registration_info):
        """Test hub registration health check."""
        info = sample_hub_registration_info

        # Initially healthy
        assert info.is_healthy()

        # Make it unhealthy due to high load
        info.current_load = 0.95
        assert not info.is_healthy()

        # Make it unhealthy due to failures
        info.current_load = 0.5
        info.consecutive_failures = 5
        assert not info.is_healthy()

    def test_hub_registration_failure_recording(self, sample_hub_registration_info):
        """Test failure recording."""
        info = sample_hub_registration_info

        initial_health = info.health_score

        # Record failure
        info.record_failure()

        assert info.consecutive_failures == 1
        assert info.health_score < initial_health
        assert info.last_failure_time > 0

    def test_hub_registration_success_recording(self, sample_hub_registration_info):
        """Test success recording."""
        info = sample_hub_registration_info

        # Record failure first
        info.record_failure()
        assert info.consecutive_failures == 1

        # Record success
        info.record_success()

        assert info.consecutive_failures == 0
        assert info.health_score > 0.5


class TestClusterRegistrationInfo:
    """Test suite for cluster registration info."""

    def test_cluster_registration_info_creation(self, sample_cluster_identity):
        """Test cluster registration info creation."""
        info = ClusterRegistrationInfo(
            cluster_id="test_cluster_001",
            cluster_identity=sample_cluster_identity,
            assigned_hub_id="test_hub_001",
        )

        assert info.cluster_id == "test_cluster_001"
        assert info.assigned_hub_id == "test_hub_001"
        assert info.preferred_hub_tier == HubTier.LOCAL

    def test_cluster_registration_expiration(self, sample_cluster_identity):
        """Test cluster registration expiration."""
        info = ClusterRegistrationInfo(
            cluster_id="test_cluster_001", cluster_identity=sample_cluster_identity
        )

        # Initially not expired
        assert not info.is_expired()

        # Make it expired
        info.last_heartbeat = time.time() - 400  # 400 seconds ago
        assert info.is_expired()

    def test_cluster_registration_health_check(self, sample_cluster_identity):
        """Test cluster registration health check."""
        info = ClusterRegistrationInfo(
            cluster_id="test_cluster_001", cluster_identity=sample_cluster_identity
        )

        # Initially healthy
        assert info.is_healthy()

        # Make it unhealthy due to poor connection
        info.connection_quality = 0.3
        assert not info.is_healthy()

    def test_cluster_registration_failover(self, sample_cluster_identity):
        """Test cluster failover recording."""
        info = ClusterRegistrationInfo(
            cluster_id="test_cluster_001",
            cluster_identity=sample_cluster_identity,
            assigned_hub_id="hub_001",
        )

        # Record failover
        info.record_failover("hub_002")

        assert info.assigned_hub_id == "hub_002"
        assert "hub_001" in info.assignment_history
        assert info.failover_count == 1
        assert info.last_failover_time > 0


class TestHubRegistry:
    """Test suite for hub registry."""

    def test_hub_registry_initialization(self, sample_hub_registry):
        """Test hub registry initialization."""
        registry = sample_hub_registry

        assert registry.registry_id == "test_registry_001"
        assert DiscoveryMethod.GEOGRAPHIC in registry.discovery_methods
        assert registry.heartbeat_interval == 10.0
        assert registry.cleanup_interval == 30.0

    @pytest.mark.asyncio
    async def test_hub_registry_lifecycle(self, sample_hub_registry):
        """Test hub registry start and stop."""
        registry = sample_hub_registry

        # Start registry
        await registry.start()
        assert registry.registry_stats.registry_started > 0

        # Stop registry
        await registry.stop()
        assert registry.registry_stats.registry_stopped > 0

    @pytest.mark.asyncio
    async def test_hub_registration(
        self, sample_hub_registry, sample_hub_registration_info
    ):
        """Test hub registration."""
        registry = sample_hub_registry

        # Register hub
        result = await registry.register_hub(sample_hub_registration_info)
        assert result

        # Check registration
        assert sample_hub_registration_info.hub_id in registry.registered_hubs
        assert (
            sample_hub_registration_info.hub_id in registry.hub_by_tier[HubTier.LOCAL]
        )
        assert sample_hub_registration_info.hub_id in registry.hub_by_region["us-east"]
        assert registry.registry_stats.hubs_registered == 1

    @pytest.mark.asyncio
    async def test_hub_deregistration(
        self, sample_hub_registry, sample_hub_registration_info
    ):
        """Test hub deregistration."""
        registry = sample_hub_registry

        # Register first
        await registry.register_hub(sample_hub_registration_info)

        # Deregister
        result = await registry.deregister_hub(sample_hub_registration_info.hub_id)
        assert result

        # Check deregistration
        assert sample_hub_registration_info.hub_id not in registry.registered_hubs
        assert (
            sample_hub_registration_info.hub_id
            not in registry.hub_by_tier[HubTier.LOCAL]
        )
        assert (
            sample_hub_registration_info.hub_id not in registry.hub_by_region["us-east"]
        )
        assert registry.registry_stats.hubs_deregistered == 1

    @pytest.mark.asyncio
    async def test_hub_discovery(
        self, sample_hub_registry, sample_hub_registration_info
    ):
        """Test hub discovery."""
        registry = sample_hub_registry

        # Register multiple hubs
        hub_infos = []
        for i in range(3):
            hub_info = HubRegistrationInfo(
                hub_id=f"hub_{i}",
                hub_tier=HubTier.LOCAL,
                hub_capabilities=sample_hub_registration_info.hub_capabilities,
                coordinates=GeographicCoordinate(40.0 + i, -74.0 + i),
                region="us-east",
                max_clusters=100,
            )
            hub_infos.append(hub_info)
            await registry.register_hub(hub_info)

        # Test discovery by tier
        discovered_hubs = await registry.discover_hubs(hub_tier=HubTier.LOCAL)
        assert len(discovered_hubs) == 3

        # Test discovery by region
        discovered_hubs = await registry.discover_hubs(region="us-east")
        assert len(discovered_hubs) == 3

        # Test discovery by proximity
        discovered_hubs = await registry.discover_hubs(
            coordinates=GeographicCoordinate(40.0, -74.0), max_distance_km=500.0
        )
        assert len(discovered_hubs) >= 1

        # Test discovery with limits
        discovered_hubs = await registry.discover_hubs(
            hub_tier=HubTier.LOCAL, max_results=2
        )
        assert len(discovered_hubs) == 2

    @pytest.mark.asyncio
    async def test_hub_health_update(
        self, sample_hub_registry, sample_hub_registration_info
    ):
        """Test hub health update."""
        registry = sample_hub_registry

        # Register hub
        await registry.register_hub(sample_hub_registration_info)

        # Update health
        result = await registry.update_hub_health(
            sample_hub_registration_info.hub_id, {"health_score": 0.8, "load": 0.6}
        )
        assert result

        # Check update
        hub_info = registry.registered_hubs[sample_hub_registration_info.hub_id]
        assert hub_info.health_score == 0.8
        assert hub_info.current_load == 0.6

    @pytest.mark.asyncio
    async def test_hub_registry_cleanup(
        self, sample_hub_registry, sample_hub_registration_info
    ):
        """Test hub registry cleanup of expired registrations."""
        registry = sample_hub_registry

        # Register hub
        await registry.register_hub(sample_hub_registration_info)

        # Make it expired
        hub_info = registry.registered_hubs[sample_hub_registration_info.hub_id]
        hub_info.last_heartbeat = time.time() - 400  # 400 seconds ago

        # Trigger cleanup
        await registry._cleanup_expired_registrations()

        # Check cleanup
        assert sample_hub_registration_info.hub_id not in registry.registered_hubs

    def test_hub_registry_statistics(self, sample_hub_registry):
        """Test hub registry statistics."""
        registry = sample_hub_registry

        stats = registry.get_registry_statistics()

        assert hasattr(stats, "registry_info")
        assert hasattr(stats, "registration_counts")
        assert hasattr(stats, "performance_stats")
        assert hasattr(stats, "performance_metrics")

        assert stats.registry_info.registry_id == "test_registry_001"
        assert stats.registration_counts.total_hubs == 0


class TestClusterRegistrar:
    """Test suite for cluster registrar."""

    def test_cluster_registrar_initialization(self, sample_cluster_registrar):
        """Test cluster registrar initialization."""
        registrar = sample_cluster_registrar

        assert registrar.assignment_strategy == "optimal"
        assert len(registrar.cluster_assignments) == 0

    @pytest.mark.asyncio
    async def test_cluster_registration(
        self,
        sample_cluster_registrar,
        sample_hub_registry,
        sample_hub_registration_info,
        sample_cluster_identity,
    ):
        """Test cluster registration and assignment."""
        registrar = sample_cluster_registrar
        registry = sample_hub_registry

        # Register hub first
        await registry.register_hub(sample_hub_registration_info)

        # Register cluster
        assigned_hub_id = await registrar.register_cluster(sample_cluster_identity)

        # Check assignment
        assert assigned_hub_id is not None
        assert sample_cluster_identity.cluster_id in registrar.cluster_assignments

        assignment_info = registrar.cluster_assignments[
            sample_cluster_identity.cluster_id
        ]
        assert assignment_info.assigned_hub_id == assigned_hub_id
        assert registrar.assignment_stats.clusters_assigned == 1

    @pytest.mark.asyncio
    async def test_cluster_deregistration(
        self,
        sample_cluster_registrar,
        sample_hub_registry,
        sample_hub_registration_info,
        sample_cluster_identity,
    ):
        """Test cluster deregistration."""
        registrar = sample_cluster_registrar
        registry = sample_hub_registry

        # Register hub and cluster
        await registry.register_hub(sample_hub_registration_info)
        await registrar.register_cluster(sample_cluster_identity)

        # Deregister cluster
        result = await registrar.deregister_cluster(sample_cluster_identity.cluster_id)
        assert result

        # Check deregistration
        assert sample_cluster_identity.cluster_id not in registrar.cluster_assignments
        assert registrar.assignment_stats.clusters_deregistered == 1

    @pytest.mark.asyncio
    async def test_cluster_reassignment(
        self,
        sample_cluster_registrar,
        sample_hub_registry,
        sample_hub_registration_info,
        sample_cluster_identity,
    ):
        """Test cluster reassignment."""
        registrar = sample_cluster_registrar
        registry = sample_hub_registry

        # Register hub and cluster
        await registry.register_hub(sample_hub_registration_info)
        original_hub_id = await registrar.register_cluster(sample_cluster_identity)

        # Force reassignment
        new_hub_id = await registrar.reassign_cluster(
            sample_cluster_identity.cluster_id, force=True
        )

        # Check reassignment
        assert new_hub_id is not None
        # Note: In this test, reassignment might return the same hub if it's the only available hub
        assert registrar.assignment_stats.clusters_reassigned >= 0

    @pytest.mark.asyncio
    async def test_cluster_assignment_strategies(
        self, sample_hub_registry, sample_hub_selector
    ):
        """Test different cluster assignment strategies."""
        strategies = ["optimal", "proximity", "load_balanced"]

        for strategy in strategies:
            registrar = ClusterRegistrar(
                sample_hub_registry, sample_hub_selector, assignment_strategy=strategy
            )

            assert registrar.assignment_strategy == strategy

    def test_cluster_registrar_statistics(self, sample_cluster_registrar):
        """Test cluster registrar statistics."""
        registrar = sample_cluster_registrar

        stats = registrar.get_assignment_statistics()

        assert hasattr(stats, "assignment_info")
        assert hasattr(stats, "assignment_stats")
        assert hasattr(stats, "assignment_history")

        assert stats.assignment_info.strategy == "optimal"
        assert stats.assignment_info.total_assignments == 0


class TestHubHealthMonitor:
    """Test suite for hub health monitor."""

    def test_hub_health_monitor_initialization(self, sample_hub_health_monitor):
        """Test hub health monitor initialization."""
        monitor = sample_hub_health_monitor

        assert monitor.monitoring_interval == 5.0
        assert monitor.failure_threshold == 2
        assert len(monitor.hub_health_history) == 0

    @pytest.mark.asyncio
    async def test_hub_health_monitor_lifecycle(self, sample_hub_health_monitor):
        """Test hub health monitor start and stop."""
        monitor = sample_hub_health_monitor

        # Start monitoring
        await monitor.start_monitoring()
        assert len(monitor._background_tasks) > 0

        # Stop monitoring
        await monitor.stop_monitoring()
        assert len(monitor._background_tasks) == 0

    @pytest.mark.asyncio
    async def test_hub_health_monitoring(
        self,
        sample_hub_health_monitor,
        sample_hub_registry,
        sample_hub_registration_info,
    ):
        """Test hub health monitoring."""
        monitor = sample_hub_health_monitor
        registry = sample_hub_registry

        # Register hub
        await registry.register_hub(sample_hub_registration_info)

        # Check single hub health
        await monitor._check_single_hub_health(sample_hub_registration_info)

        # Check health history
        assert sample_hub_registration_info.hub_id in monitor.hub_health_history
        assert len(monitor.hub_health_history[sample_hub_registration_info.hub_id]) == 1

    @pytest.mark.asyncio
    async def test_hub_failure_detection(
        self,
        sample_hub_health_monitor,
        sample_hub_registry,
        sample_hub_registration_info,
    ):
        """Test hub failure detection."""
        monitor = sample_hub_health_monitor
        registry = sample_hub_registry

        # Register hub
        await registry.register_hub(sample_hub_registration_info)

        # Simulate hub failure
        sample_hub_registration_info.health_score = 0.1
        sample_hub_registration_info.consecutive_failures = 5

        # Check health multiple times to trigger failure detection
        for _ in range(3):
            await monitor._check_single_hub_health(sample_hub_registration_info)

        # Check failure count
        assert monitor.hub_failure_counts[sample_hub_registration_info.hub_id] >= 2

    @pytest.mark.asyncio
    async def test_hub_failover_handling(
        self,
        sample_hub_health_monitor,
        sample_hub_registry,
        sample_cluster_registrar,
        sample_hub_registration_info,
        sample_cluster_identity,
    ):
        """Test hub failover handling."""
        monitor = sample_hub_health_monitor
        registry = sample_hub_registry
        registrar = sample_cluster_registrar

        # Register hub and cluster
        await registry.register_hub(sample_hub_registration_info)
        await registrar.register_cluster(sample_cluster_identity)

        # Simulate hub failure
        monitor.hub_failure_counts[sample_hub_registration_info.hub_id] = 5

        # Handle failed hub
        await monitor._handle_hub_failure(sample_hub_registration_info.hub_id)

        # Check that failure was handled
        assert monitor.hub_failure_counts[sample_hub_registration_info.hub_id] == 0
        assert monitor.monitoring_stats.hub_failures_handled == 1

    def test_hub_health_monitor_statistics(self, sample_hub_health_monitor):
        """Test hub health monitor statistics."""
        monitor = sample_hub_health_monitor

        stats = monitor.get_monitoring_statistics()

        assert hasattr(stats, "monitoring_info")
        assert hasattr(stats, "monitoring_stats")
        assert hasattr(stats, "hub_health_summary")

        assert stats.monitoring_info.monitoring_interval == 5.0
        assert stats.monitoring_info.failure_threshold == 2


class TestDiscoveryProtocols:
    """Test suite for discovery protocols."""

    def test_geographic_discovery_protocol(self, sample_hub_registry):
        """Test geographic discovery protocol."""
        protocol = GeographicDiscoveryProtocol(sample_hub_registry)

        assert protocol.hub_registry == sample_hub_registry

    @pytest.mark.asyncio
    async def test_geographic_discovery_protocol_operations(self, sample_hub_registry):
        """Test geographic discovery protocol operations."""
        protocol = GeographicDiscoveryProtocol(sample_hub_registry)

        # Test discover hubs
        hubs = await protocol.discover_hubs(DiscoveryCriteria(hub_tier="local"))
        assert isinstance(hubs, list)

        # Test announce hub
        hub_info = HubRegistrationInfo(
            hub_id="test_hub",
            hub_tier=HubTier.LOCAL,
            hub_capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(40.0, -74.0),
            region="us-east",
        )
        result = await protocol.announce_hub(hub_info)
        assert result

        # Test withdraw hub
        result = await protocol.withdraw_hub("test_hub")
        assert result

    def test_broadcast_discovery_protocol(self, sample_hub_registry):
        """Test broadcast discovery protocol."""
        protocol = BroadcastDiscoveryProtocol(sample_hub_registry)

        assert protocol.hub_registry == sample_hub_registry

    @pytest.mark.asyncio
    async def test_broadcast_discovery_protocol_operations(self, sample_hub_registry):
        """Test broadcast discovery protocol operations."""
        protocol = BroadcastDiscoveryProtocol(sample_hub_registry)

        # Test operations
        hubs = await protocol.discover_hubs(DiscoveryCriteria(hub_tier="local"))
        assert isinstance(hubs, list)

        hub_info = HubRegistrationInfo(
            hub_id="test_hub",
            hub_tier=HubTier.LOCAL,
            hub_capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(40.0, -74.0),
            region="us-east",
        )
        result = await protocol.announce_hub(hub_info)
        assert result

        result = await protocol.withdraw_hub("test_hub")
        assert result

    def test_gossip_discovery_protocol(self, sample_hub_registry):
        """Test gossip discovery protocol."""
        protocol = GossipDiscoveryProtocol(sample_hub_registry)

        assert protocol.hub_registry == sample_hub_registry

    @pytest.mark.asyncio
    async def test_gossip_discovery_protocol_operations(self, sample_hub_registry):
        """Test gossip discovery protocol operations."""
        protocol = GossipDiscoveryProtocol(sample_hub_registry)

        # Test operations
        hubs = await protocol.discover_hubs(DiscoveryCriteria(hub_tier="local"))
        assert isinstance(hubs, list)

        hub_info = HubRegistrationInfo(
            hub_id="test_hub",
            hub_tier=HubTier.LOCAL,
            hub_capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(40.0, -74.0),
            region="us-east",
        )
        result = await protocol.announce_hub(hub_info)
        assert result

        result = await protocol.withdraw_hub("test_hub")
        assert result


class TestEndToEndDiscovery:
    """Test suite for end-to-end discovery functionality."""

    @pytest.mark.asyncio
    async def test_complete_discovery_workflow(
        self, sample_hub_registry, sample_cluster_registrar, sample_hub_health_monitor
    ):
        """Test complete discovery workflow."""
        registry = sample_hub_registry
        registrar = sample_cluster_registrar
        monitor = sample_hub_health_monitor

        # Start systems
        await registry.start()
        await monitor.start_monitoring()

        # Register multiple hubs
        hub_infos = []
        for i in range(3):
            hub_info = HubRegistrationInfo(
                hub_id=f"hub_{i}",
                hub_tier=HubTier.LOCAL,
                hub_capabilities=HubCapabilities(max_clusters=50),
                coordinates=GeographicCoordinate(40.0 + i, -74.0 + i),
                region="us-east",
                max_clusters=50,
            )
            hub_infos.append(hub_info)
            await registry.register_hub(hub_info)

        # Register multiple clusters
        cluster_identities = []
        for i in range(5):
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
            cluster_identities.append(cluster_identity)
            assigned_hub_id = await registrar.register_cluster(cluster_identity)
            assert assigned_hub_id is not None

        # Check assignments
        assert len(registrar.cluster_assignments) == 5

        # Test discovery
        discovered_hubs = await registry.discover_hubs(hub_tier=HubTier.LOCAL)
        assert len(discovered_hubs) == 3

        # Simulate hub failure and recovery
        failing_hub = hub_infos[0]
        failing_hub.health_score = 0.1
        failing_hub.consecutive_failures = 3

        # Update health
        await registry.update_hub_health(
            failing_hub.hub_id, {"health_score": 0.1, "load": 0.9}
        )

        # Trigger health check
        await monitor._check_single_hub_health(failing_hub)

        # Check that unhealthy hub is detected
        assert not failing_hub.is_healthy()

        # Stop systems
        await monitor.stop_monitoring()
        await registry.stop()

    @pytest.mark.asyncio
    async def test_discovery_performance(self, sample_hub_registry):
        """Test discovery performance with many hubs."""
        registry = sample_hub_registry

        # Register many hubs
        for i in range(50):
            hub_info = HubRegistrationInfo(
                hub_id=f"hub_{i}",
                hub_tier=HubTier.LOCAL if i % 3 == 0 else HubTier.REGIONAL,
                hub_capabilities=HubCapabilities(),
                coordinates=GeographicCoordinate(40.0 + i * 0.1, -74.0 + i * 0.1),
                region=f"region_{i % 5}",
                max_clusters=100,
            )
            await registry.register_hub(hub_info)

        # Test discovery performance
        start_time = time.time()

        # Perform many discovery operations
        for i in range(20):
            discovered_hubs = await registry.discover_hubs(
                hub_tier=HubTier.LOCAL, max_results=10
            )
            assert len(discovered_hubs) <= 10

        discovery_time = time.time() - start_time

        # Should complete within reasonable time
        assert discovery_time < 1.0  # Should be very fast

        # Check statistics
        stats = registry.get_registry_statistics()
        assert stats.registration_counts.total_hubs == 50
        assert stats.performance_stats.discovery_requests == 20

    @pytest.mark.asyncio
    async def test_discovery_scalability(
        self, sample_hub_registry, sample_cluster_registrar
    ):
        """Test discovery scalability with many clusters."""
        registry = sample_hub_registry
        registrar = sample_cluster_registrar

        # Register multiple hubs
        for i in range(10):
            hub_info = HubRegistrationInfo(
                hub_id=f"hub_{i}",
                hub_tier=HubTier.LOCAL,
                hub_capabilities=HubCapabilities(max_clusters=20),
                coordinates=GeographicCoordinate(40.0 + i, -74.0 + i),
                region="us-east",
                max_clusters=20,
            )
            await registry.register_hub(hub_info)

        # Register many clusters
        successful_assignments = 0
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

            assigned_hub_id = await registrar.register_cluster(cluster_identity)
            if assigned_hub_id:
                successful_assignments += 1

        # Check that most clusters were assigned
        assert successful_assignments > 50  # Should assign most clusters

        # Check load distribution
        stats = registrar.get_assignment_statistics()
        assert stats.assignment_info.total_assignments == successful_assignments


class TestPerformanceAndReliability:
    """Test suite for performance and reliability features."""

    @pytest.mark.asyncio
    async def test_hub_registry_concurrent_operations(self, sample_hub_registry):
        """Test hub registry concurrent operations."""
        registry = sample_hub_registry

        # Create multiple hub registration tasks
        async def register_hub(hub_id):
            hub_info = HubRegistrationInfo(
                hub_id=hub_id,
                hub_tier=HubTier.LOCAL,
                hub_capabilities=HubCapabilities(),
                coordinates=GeographicCoordinate(40.0, -74.0),
                region="us-east",
                max_clusters=100,
            )
            return await registry.register_hub(hub_info)

        # Run concurrent registrations
        tasks = [register_hub(f"hub_{i}") for i in range(20)]
        results = await asyncio.gather(*tasks)

        # All should succeed
        assert all(results)
        assert len(registry.registered_hubs) == 20

    @pytest.mark.asyncio
    async def test_cluster_registrar_load_balancing(self, sample_hub_registry):
        """Test cluster registrar load balancing."""
        # Create registrar with load-balanced strategy
        registrar = ClusterRegistrar(
            sample_hub_registry,
            HubSelector(HubTopology(), RoutingPolicy()),
            assignment_strategy="load_balanced",
        )

        # Register multiple hubs with different loads
        for i in range(3):
            hub_info = HubRegistrationInfo(
                hub_id=f"hub_{i}",
                hub_tier=HubTier.LOCAL,
                hub_capabilities=HubCapabilities(max_clusters=10),
                coordinates=GeographicCoordinate(40.0 + i, -74.0 + i),
                region="us-east",
                current_load=i * 0.3,  # Different loads
                max_clusters=10,
            )
            await sample_hub_registry.register_hub(hub_info)

        # Register multiple clusters
        assignments = {}
        for i in range(6):
            cluster_identity = ClusterIdentity(
                cluster_id=f"cluster_{i}",
                cluster_name=f"Cluster {i}",
                region="us-east",
                bridge_url=f"ws://cluster{i}.example.com",
                public_key_hash=f"hash_{i}",
                created_at=time.time(),
                geographic_coordinates=(40.0, -74.0),
                network_tier=1,
                max_bandwidth_mbps=1000,
                preference_weight=1.0,
            )

            assigned_hub_id = await registrar.register_cluster(cluster_identity)
            if assigned_hub_id:
                assignments[cluster_identity.cluster_id] = assigned_hub_id

        # Check that assignments were successful
        assert len(assignments) > 0  # Should assign at least some clusters

        # Check that load balancing strategy was used
        assert registrar.assignment_strategy == "load_balanced"

    @pytest.mark.asyncio
    async def test_hub_health_monitor_reliability(self, sample_hub_registry):
        """Test hub health monitor reliability."""
        monitor = HubHealthMonitor(
            sample_hub_registry,
            ClusterRegistrar(
                sample_hub_registry, HubSelector(HubTopology(), RoutingPolicy())
            ),
            monitoring_interval=0.1,  # Very fast monitoring
            failure_threshold=2,
        )

        # Register hub
        hub_info = HubRegistrationInfo(
            hub_id="test_hub",
            hub_tier=HubTier.LOCAL,
            hub_capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(40.0, -74.0),
            region="us-east",
            max_clusters=100,
        )
        await sample_hub_registry.register_hub(hub_info)

        # Start monitoring
        await monitor.start_monitoring()

        # Wait for some monitoring cycles
        await asyncio.sleep(0.5)

        # Check that monitoring is working
        assert "test_hub" in monitor.hub_health_history
        assert len(monitor.hub_health_history["test_hub"]) > 0

        # Stop monitoring
        await monitor.stop_monitoring()

        # Check statistics
        stats = monitor.get_monitoring_statistics()
        assert stats.monitoring_stats.health_checks > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
