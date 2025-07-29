"""
Tests for Auto-Discovery and Registration System.

This module tests the comprehensive auto-discovery functionality including:
- Multiple discovery protocol backends
- Health-aware filtering and registration
- Background discovery and registration loops
- Event-driven notifications and callbacks
- Configuration management and validation
"""

import asyncio
import json
import tempfile
import time
from pathlib import Path

import pytest

from mpreg.federation.auto_discovery import (
    AutoDiscoveryService,
    DiscoveredCluster,
    DiscoveryConfiguration,
    DiscoveryProtocol,
    DNSDiscoveryBackend,
    HTTPDiscoveryBackend,
    StaticConfigDiscoveryBackend,
    create_auto_discovery_service,
    create_consul_discovery_config,
    create_dns_discovery_config,
    create_http_discovery_config,
    create_static_discovery_config,
)
from mpreg.federation.federation_resilience import HealthStatus


@pytest.fixture
def sample_discovered_cluster():
    """Create a sample discovered cluster for testing."""
    return DiscoveredCluster(
        cluster_id="test-cluster-1",
        cluster_name="Test Cluster 1",
        region="us-west-2",
        server_url="ws://test1.example.com:8000",
        bridge_url="ws://test1.example.com:9000",
        health_status=HealthStatus.HEALTHY,
        health_score=95.0,
        discovery_source="test",
        metadata={"environment": "testing", "priority": "high"},
    )


@pytest.fixture
def static_config_file():
    """Create a temporary static configuration file."""
    config_data = {
        "clusters": [
            {
                "cluster_id": "static-cluster-1",
                "cluster_name": "Static Cluster 1",
                "region": "us-east-1",
                "server_url": "ws://static1.example.com:8000",
                "bridge_url": "ws://static1.example.com:9000",
                "metadata": {"tier": "production"},
            },
            {
                "cluster_id": "static-cluster-2",
                "cluster_name": "Static Cluster 2",
                "region": "eu-central-1",
                "server_url": "ws://static2.example.com:8000",
                "bridge_url": "ws://static2.example.com:9000",
                "metadata": {"tier": "staging"},
            },
        ]
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config_data, f)
        temp_path = f.name

    yield temp_path

    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


class TestDiscoveredCluster:
    """Test DiscoveredCluster dataclass functionality."""

    def test_cluster_creation(self, sample_discovered_cluster):
        """Test basic cluster creation and attributes."""
        cluster = sample_discovered_cluster

        assert cluster.cluster_id == "test-cluster-1"
        assert cluster.cluster_name == "Test Cluster 1"
        assert cluster.region == "us-west-2"
        assert cluster.server_url == "ws://test1.example.com:8000"
        assert cluster.bridge_url == "ws://test1.example.com:9000"
        assert cluster.health_status == HealthStatus.HEALTHY
        assert cluster.health_score == 95.0
        assert cluster.discovery_source == "test"
        assert cluster.metadata["environment"] == "testing"

    def test_to_cluster_identity(self, sample_discovered_cluster):
        """Test conversion to ClusterIdentity."""
        cluster = sample_discovered_cluster
        identity = cluster.to_cluster_identity()

        assert identity.cluster_id == cluster.cluster_id
        assert identity.cluster_name == cluster.cluster_name
        assert identity.region == cluster.region
        assert identity.bridge_url == cluster.bridge_url
        assert identity.public_key_hash == f"auto_discovery_{cluster.cluster_id}"
        assert identity.created_at == cluster.last_seen


class TestDiscoveryConfiguration:
    """Test discovery configuration creation and validation."""

    def test_default_configuration(self):
        """Test default configuration values."""
        config = DiscoveryConfiguration(protocol=DiscoveryProtocol.STATIC_CONFIG)

        assert config.protocol == DiscoveryProtocol.STATIC_CONFIG
        assert config.enabled is True
        assert config.filter_unhealthy is True
        assert config.min_health_score == 50.0
        assert config.discovery_interval == 30.0

    def test_consul_configuration(self):
        """Test Consul-specific configuration."""
        config = create_consul_discovery_config(
            consul_host="consul.example.com",
            consul_port=8501,
            service_name="custom-mpreg",
            datacenter="dc2",
        )

        assert config.protocol == DiscoveryProtocol.CONSUL
        assert config.consul_host == "consul.example.com"
        assert config.consul_port == 8501
        assert config.consul_service == "custom-mpreg"
        assert config.consul_datacenter == "dc2"

    def test_static_configuration(self):
        """Test static file configuration."""
        config = create_static_discovery_config("/path/to/config.json")

        assert config.protocol == DiscoveryProtocol.STATIC_CONFIG
        assert config.static_config_path == "/path/to/config.json"

    def test_http_configuration(self):
        """Test HTTP endpoint configuration."""
        config = create_http_discovery_config(
            "https://discovery.example.com/clusters",
            discovery_interval=45.0,
            registration_ttl=90.0,
        )

        assert config.protocol == DiscoveryProtocol.HTTP_ENDPOINT
        assert config.http_discovery_url == "https://discovery.example.com/clusters"
        assert config.discovery_interval == 45.0
        assert config.registration_ttl == 90.0

    def test_dns_configuration(self):
        """Test DNS SRV configuration."""
        config = create_dns_discovery_config("example.com", "_custom._tcp")

        assert config.protocol == DiscoveryProtocol.DNS_SRV
        assert config.dns_domain == "example.com"
        assert config.dns_service == "_custom._tcp"


class TestStaticConfigDiscoveryBackend:
    """Test static configuration file discovery backend."""

    def test_backend_initialization(self):
        """Test backend initialization."""
        config = create_static_discovery_config("/tmp/test.json")
        backend = StaticConfigDiscoveryBackend(config)

        assert backend.config == config
        assert hasattr(backend, "logger")

    @pytest.mark.asyncio
    async def test_discover_clusters_success(self, static_config_file):
        """Test successful cluster discovery from static config."""
        config = create_static_discovery_config(static_config_file)
        backend = StaticConfigDiscoveryBackend(config)

        clusters = await backend.discover_clusters()

        assert len(clusters) == 2
        assert clusters[0].cluster_id == "static-cluster-1"
        assert clusters[0].cluster_name == "Static Cluster 1"
        assert clusters[0].region == "us-east-1"
        assert clusters[0].discovery_source == "static_config"
        assert clusters[0].metadata["tier"] == "production"

        assert clusters[1].cluster_id == "static-cluster-2"
        assert clusters[1].region == "eu-central-1"
        assert clusters[1].metadata["tier"] == "staging"

    @pytest.mark.asyncio
    async def test_discover_clusters_missing_file(self):
        """Test discovery with missing configuration file."""
        config = create_static_discovery_config("/nonexistent/config.json")
        backend = StaticConfigDiscoveryBackend(config)

        clusters = await backend.discover_clusters()
        assert len(clusters) == 0

    @pytest.mark.asyncio
    async def test_discover_clusters_no_config_path(self):
        """Test discovery with no config path specified."""
        config = DiscoveryConfiguration(
            protocol=DiscoveryProtocol.STATIC_CONFIG, static_config_path=""
        )
        backend = StaticConfigDiscoveryBackend(config)

        clusters = await backend.discover_clusters()
        assert len(clusters) == 0

    @pytest.mark.asyncio
    async def test_register_cluster_not_supported(self, sample_discovered_cluster):
        """Test that registration is not supported."""
        config = create_static_discovery_config("/tmp/test.json")
        backend = StaticConfigDiscoveryBackend(config)

        result = await backend.register_cluster(sample_discovered_cluster)
        assert result is False

    @pytest.mark.asyncio
    async def test_unregister_cluster_not_supported(self):
        """Test that unregistration is not supported."""
        config = create_static_discovery_config("/tmp/test.json")
        backend = StaticConfigDiscoveryBackend(config)

        result = await backend.unregister_cluster("test-cluster")
        assert result is False

    @pytest.mark.asyncio
    async def test_health_check(self, static_config_file):
        """Test health check functionality."""
        # Test with existing file
        config = create_static_discovery_config(static_config_file)
        backend = StaticConfigDiscoveryBackend(config)

        assert await backend.health_check() is True

        # Test with missing file
        config_missing = create_static_discovery_config("/nonexistent/config.json")
        backend_missing = StaticConfigDiscoveryBackend(config_missing)

        assert await backend_missing.health_check() is False

        # Test with no path
        config_no_path = DiscoveryConfiguration(
            protocol=DiscoveryProtocol.STATIC_CONFIG, static_config_path=""
        )
        backend_no_path = StaticConfigDiscoveryBackend(config_no_path)

        assert await backend_no_path.health_check() is False


class TestDNSDiscoveryBackend:
    """Test DNS SRV discovery backend."""

    def test_backend_initialization(self):
        """Test DNS backend initialization."""
        config = create_dns_discovery_config("example.com")
        backend = DNSDiscoveryBackend(config)

        assert backend.config == config
        assert hasattr(backend, "logger")

    @pytest.mark.asyncio
    async def test_discover_clusters_simulation(self):
        """Test simulated DNS discovery."""
        config = create_dns_discovery_config("example.com", "_mpreg._tcp")
        backend = DNSDiscoveryBackend(config)

        clusters = await backend.discover_clusters()

        # Should return simulated clusters
        assert len(clusters) == 2
        assert clusters[0].cluster_id == "dns-cluster-1"
        assert clusters[0].discovery_source == "dns_srv"
        assert clusters[0].metadata["dns_host"] == "cluster1.example.com"

        assert clusters[1].cluster_id == "dns-cluster-2"
        assert clusters[1].region == "us-east-1"

    @pytest.mark.asyncio
    async def test_register_not_supported(self, sample_discovered_cluster):
        """Test that DNS registration is not supported."""
        config = create_dns_discovery_config("example.com")
        backend = DNSDiscoveryBackend(config)

        result = await backend.register_cluster(sample_discovered_cluster)
        assert result is False

    @pytest.mark.asyncio
    async def test_health_check(self):
        """Test DNS health check."""
        config = create_dns_discovery_config("example.com")
        backend = DNSDiscoveryBackend(config)

        # This tests basic DNS resolution capability
        result = await backend.health_check()
        assert isinstance(result, bool)


class TestHTTPDiscoveryBackend:
    """Test HTTP endpoint discovery backend."""

    def test_backend_initialization(self):
        """Test HTTP backend initialization."""
        config = create_http_discovery_config("https://discovery.example.com/clusters")
        backend = HTTPDiscoveryBackend(config)

        assert backend.config == config
        assert hasattr(backend, "logger")

    @pytest.mark.asyncio
    async def test_discover_clusters_no_url(self):
        """Test discovery with no URL configured."""
        config = DiscoveryConfiguration(
            protocol=DiscoveryProtocol.HTTP_ENDPOINT, http_discovery_url=""
        )
        backend = HTTPDiscoveryBackend(config)

        clusters = await backend.discover_clusters()
        assert len(clusters) == 0

    @pytest.mark.asyncio
    async def test_register_cluster_no_url(self, sample_discovered_cluster):
        """Test registration with no URL configured."""
        config = DiscoveryConfiguration(
            protocol=DiscoveryProtocol.HTTP_ENDPOINT, http_discovery_url=""
        )
        backend = HTTPDiscoveryBackend(config)

        result = await backend.register_cluster(sample_discovered_cluster)
        assert result is False

    @pytest.mark.asyncio
    async def test_health_check_no_url(self):
        """Test health check with no URL configured."""
        config = DiscoveryConfiguration(
            protocol=DiscoveryProtocol.HTTP_ENDPOINT, http_discovery_url=""
        )
        backend = HTTPDiscoveryBackend(config)

        result = await backend.health_check()
        assert result is False


class TestAutoDiscoveryService:
    """Test the main auto-discovery service."""

    @pytest.fixture
    def discovery_configs(self, static_config_file):
        """Create test discovery configurations."""
        return [
            create_static_discovery_config(static_config_file, discovery_interval=1.0),
            create_dns_discovery_config("example.com", discovery_interval=2.0),
        ]

    @pytest.mark.asyncio
    async def test_service_creation(self, discovery_configs, sample_discovered_cluster):
        """Test auto-discovery service creation."""
        service = await create_auto_discovery_service(
            discovery_configs=discovery_configs, local_cluster=sample_discovered_cluster
        )

        assert service.local_cluster == sample_discovered_cluster
        assert len(service.discovery_configs) == 2
        assert len(service.discovery_backends) == 2
        assert DiscoveryProtocol.STATIC_CONFIG in service.discovery_backends
        assert DiscoveryProtocol.DNS_SRV in service.discovery_backends

    @pytest.mark.asyncio
    async def test_service_lifecycle(self, discovery_configs):
        """Test service start and stop lifecycle."""
        service = await create_auto_discovery_service(discovery_configs)

        # Start discovery
        await service.start_discovery()
        assert len(service._discovery_tasks) == 2

        # Let it run briefly
        await asyncio.sleep(0.1)

        # Stop discovery
        await service.stop_discovery()
        assert len(service._discovery_tasks) == 0
        assert len(service._registration_tasks) == 0

    @pytest.mark.asyncio
    async def test_cluster_discovery_and_callbacks(self, discovery_configs):
        """Test cluster discovery with callbacks."""
        service = await create_auto_discovery_service(discovery_configs)

        # Track callback invocations
        discovered_clusters = []
        lost_clusters = []
        discovery_errors = []

        def on_cluster_discovered(cluster: DiscoveredCluster):
            discovered_clusters.append(cluster)

        def on_cluster_lost(cluster_id: str):
            lost_clusters.append(cluster_id)

        def on_discovery_error(error: Exception):
            discovery_errors.append(error)

        service.add_cluster_discovered_callback(on_cluster_discovered)
        service.add_cluster_lost_callback(on_cluster_lost)
        service.add_discovery_error_callback(on_discovery_error)

        # Start discovery
        await service.start_discovery()

        # Wait for discovery to run
        await asyncio.sleep(2.5)  # Let both backends run at least once

        # Stop discovery
        await service.stop_discovery()

        # Verify clusters were discovered
        discovered = service.get_discovered_clusters()
        assert len(discovered) > 0

        # Verify callbacks were called
        assert len(discovered_clusters) > 0

    def test_cluster_filtering_and_queries(self, discovery_configs):
        """Test cluster filtering and query methods."""
        service = AutoDiscoveryService(discovery_configs=discovery_configs)

        # Add test clusters
        cluster1 = DiscoveredCluster(
            cluster_id="cluster-1",
            cluster_name="Cluster 1",
            region="us-west-2",
            server_url="ws://cluster1.example.com:8000",
            bridge_url="ws://cluster1.example.com:9000",
            health_status=HealthStatus.HEALTHY,
            health_score=95.0,
        )

        cluster2 = DiscoveredCluster(
            cluster_id="cluster-2",
            cluster_name="Cluster 2",
            region="us-east-1",
            server_url="ws://cluster2.example.com:8000",
            bridge_url="ws://cluster2.example.com:9000",
            health_status=HealthStatus.DEGRADED,
            health_score=60.0,
        )

        cluster3 = DiscoveredCluster(
            cluster_id="cluster-3",
            cluster_name="Cluster 3",
            region="us-west-2",
            server_url="ws://cluster3.example.com:8000",
            bridge_url="ws://cluster3.example.com:9000",
            health_status=HealthStatus.UNHEALTHY,
            health_score=20.0,
        )

        service.discovered_clusters = {
            cluster1.cluster_id: cluster1,
            cluster2.cluster_id: cluster2,
            cluster3.cluster_id: cluster3,
        }

        # Test get all clusters
        all_clusters = service.get_discovered_clusters()
        assert len(all_clusters) == 3

        # Test get by ID
        found_cluster = service.get_cluster_by_id("cluster-1")
        assert found_cluster == cluster1

        # Test get by region
        west_clusters = service.get_clusters_by_region("us-west-2")
        assert len(west_clusters) == 2
        assert all(c.region == "us-west-2" for c in west_clusters)

        # Test get healthy clusters
        healthy_clusters = service.get_healthy_clusters()
        assert len(healthy_clusters) == 1
        assert healthy_clusters[0] == cluster1

    def test_discovery_statistics(self, discovery_configs):
        """Test discovery statistics collection."""
        service = AutoDiscoveryService(discovery_configs=discovery_configs)

        # Simulate some statistics
        service.discovery_stats["static_config_discoveries"] = 5
        service.discovery_stats["static_config_new"] = 2
        service.discovery_stats["dns_srv_errors"] = 1
        service.last_discovery[DiscoveryProtocol.STATIC_CONFIG] = time.time()

        # Add test clusters
        cluster1 = DiscoveredCluster(
            cluster_id="cluster-1",
            cluster_name="Cluster 1",
            region="us-west-2",
            server_url="ws://cluster1.example.com:8000",
            bridge_url="ws://cluster1.example.com:9000",
            health_status=HealthStatus.HEALTHY,
        )

        cluster2 = DiscoveredCluster(
            cluster_id="cluster-2",
            cluster_name="Cluster 2",
            region="us-east-1",
            server_url="ws://cluster2.example.com:8000",
            bridge_url="ws://cluster2.example.com:9000",
            health_status=HealthStatus.DEGRADED,
        )

        service.discovered_clusters = {
            cluster1.cluster_id: cluster1,
            cluster2.cluster_id: cluster2,
        }

        stats = service.get_discovery_statistics()

        assert stats.total_clusters == 2
        assert stats.clusters_by_region.region_counts["us-west-2"] == 1
        assert stats.clusters_by_region.region_counts["us-east-1"] == 1
        assert stats.clusters_by_health.healthy == 1
        assert stats.clusters_by_health.degraded == 1
        assert stats.discovery_backends == 2
        assert "static_config" in stats.backend_statistics
        assert stats.backend_statistics["static_config"].discoveries == 5
        assert stats.local_cluster_registered is False

    def test_dynamic_configuration(self):
        """Test dynamic configuration management."""
        service = AutoDiscoveryService()

        assert len(service.discovery_configs) == 0
        assert len(service.discovery_backends) == 0

        # Add configuration
        config = create_static_discovery_config("/tmp/test.json")
        service.add_discovery_config(config)

        assert len(service.discovery_configs) == 1
        assert len(service.discovery_backends) == 1
        assert DiscoveryProtocol.STATIC_CONFIG in service.discovery_backends

    def test_local_cluster_management(self, sample_discovered_cluster):
        """Test local cluster management."""
        service = AutoDiscoveryService()

        assert service.local_cluster is None

        service.set_local_cluster(sample_discovered_cluster)
        assert service.local_cluster == sample_discovered_cluster


@pytest.mark.asyncio
async def test_integration_multiple_backends(static_config_file):
    """Test integration with multiple discovery backends."""
    # Create configurations for multiple backends
    configs = [
        create_static_discovery_config(static_config_file, discovery_interval=0.5),
        create_dns_discovery_config("example.com", discovery_interval=0.5),
        create_http_discovery_config(
            "https://nonexistent.example.com/clusters", discovery_interval=0.5
        ),
    ]

    service = await create_auto_discovery_service(configs)

    # Track events
    events = []

    def track_discovered(cluster):
        events.append(("discovered", cluster.cluster_id))

    def track_lost(cluster_id):
        events.append(("lost", cluster_id))

    def track_error(error):
        events.append(("error", str(error)))

    service.add_cluster_discovered_callback(track_discovered)
    service.add_cluster_lost_callback(track_lost)
    service.add_discovery_error_callback(track_error)

    # Start discovery
    await service.start_discovery()

    # Let it run for discovery cycles
    await asyncio.sleep(1.5)

    # Check discovered clusters
    clusters = service.get_discovered_clusters()
    assert len(clusters) > 0  # Should find clusters from static config and DNS

    # Verify multiple discovery sources
    sources = {c.discovery_source for c in clusters}
    assert "static_config" in sources
    assert "dns_srv" in sources

    # Get statistics
    stats = service.get_discovery_statistics()
    assert stats.total_clusters > 0
    assert stats.discovery_backends == 3

    # Stop discovery
    await service.stop_discovery()

    # Verify events were tracked
    assert len(events) > 0
    discovered_events = [e for e in events if e[0] == "discovered"]
    assert len(discovered_events) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
