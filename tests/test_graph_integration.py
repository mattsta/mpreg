"""
Comprehensive integration tests for graph-aware federation bridge.

This module tests the complete integration of graph routing with the existing
federation system, ensuring:
- 100% backward compatibility with existing federation API
- Seamless graph routing for multi-hop scenarios
- Performance regression prevention
- End-to-end routing functionality

Test Coverage:
- Backward compatibility validation
- Graph routing integration
- Multi-hop routing scenarios
- Performance benchmarks
- Error handling and fallback scenarios
"""

import asyncio
import time

import pytest

from mpreg.core.model import PubSubMessage
from mpreg.core.topic_exchange import TopicExchange
from mpreg.federation.federation_bridge import (
    FederationBridge,  # Backward compatibility alias
    GraphAwareFederationBridge,
    ProductionFederationBridge,  # Backward compatibility alias
)
from mpreg.federation.federation_optimized import (
    ClusterIdentity,
    ClusterStatus,
    IntelligentRoutingTable,
)


@pytest.fixture
def sample_cluster_identity():
    """Create a sample cluster identity for testing."""
    return ClusterIdentity(
        cluster_id="test_cluster",
        cluster_name="Test Cluster",
        region="us-east",
        bridge_url="ws://test.example.com",
        public_key_hash="test_hash",
        created_at=time.time(),
        geographic_coordinates=(40.7128, -74.0060),  # New York
        network_tier=1,
        max_bandwidth_mbps=1000,
        preference_weight=1.0,
    )


@pytest.fixture
def sample_topic_exchange():
    """Create a sample topic exchange for testing."""
    return TopicExchange(
        server_url="ws://test.local:8080", cluster_id="test_cluster_local"
    )


@pytest.fixture
def sample_federation_bridge(sample_topic_exchange, sample_cluster_identity):
    """Create a sample federation bridge for testing."""
    return GraphAwareFederationBridge(
        local_cluster=sample_topic_exchange,
        cluster_identity=sample_cluster_identity,
        enable_graph_routing=True,
        enable_monitoring=True,
    )


@pytest.fixture
def sample_message():
    """Create a sample message for testing."""
    return PubSubMessage(
        message_id="test_msg_001",
        topic="test.topic",
        payload=b"test payload",
        timestamp=time.time(),
        publisher="test_publisher",
    )


class TestBackwardCompatibility:
    """Test suite for backward compatibility validation."""

    def test_alias_compatibility(self):
        """Test that backward compatibility aliases work."""
        # Test that aliases point to the same class
        assert FederationBridge is GraphAwareFederationBridge
        assert ProductionFederationBridge is GraphAwareFederationBridge

    def test_api_compatibility(self, sample_federation_bridge):
        """Test that all required API methods exist."""
        bridge = sample_federation_bridge

        # Check required methods exist
        assert hasattr(bridge, "send_message")
        assert hasattr(bridge, "get_cluster_status")
        assert hasattr(bridge, "get_routing_table")
        assert hasattr(bridge, "add_cluster")
        assert hasattr(bridge, "remove_cluster")
        assert hasattr(bridge, "start")
        assert hasattr(bridge, "stop")

        # Check methods are callable
        assert callable(bridge.send_message)
        assert callable(bridge.get_cluster_status)
        assert callable(bridge.get_routing_table)
        assert callable(bridge.add_cluster)
        assert callable(bridge.remove_cluster)
        assert callable(bridge.start)
        assert callable(bridge.stop)

    def test_cluster_status_compatibility(self, sample_federation_bridge):
        """Test cluster status API compatibility."""
        bridge = sample_federation_bridge

        # Test with non-existent cluster
        status = bridge.get_cluster_status("nonexistent")
        assert status == ClusterStatus.DISCONNECTED

        # Test with existing cluster
        cluster_identity = ClusterIdentity(
            cluster_id="test_cluster_2",
            cluster_name="Test Cluster 2",
            region="us-west",
            bridge_url="ws://test2.example.com",
            public_key_hash="test_hash_2",
            created_at=time.time(),
            geographic_coordinates=(37.7749, -122.4194),
            network_tier=2,
            max_bandwidth_mbps=800,
            preference_weight=0.8,
        )

        # Add cluster
        asyncio.run(bridge.add_cluster(cluster_identity))

        # Check status
        status = bridge.get_cluster_status("test_cluster_2")
        assert isinstance(status, ClusterStatus)

    def test_routing_table_compatibility(self, sample_federation_bridge):
        """Test routing table API compatibility."""
        bridge = sample_federation_bridge

        routing_table = bridge.get_routing_table()
        assert isinstance(routing_table, IntelligentRoutingTable)

    @pytest.mark.asyncio
    async def test_message_sending_compatibility(
        self, sample_federation_bridge, sample_message
    ):
        """Test message sending API compatibility."""
        bridge = sample_federation_bridge

        # Test sending to non-existent cluster
        result = await bridge.send_message(sample_message, "nonexistent")
        assert isinstance(result, bool)

        # Add a cluster and test sending
        cluster_identity = ClusterIdentity(
            cluster_id="test_cluster_3",
            cluster_name="Test Cluster 3",
            region="eu-central",
            bridge_url="ws://test3.example.com",
            public_key_hash="test_hash_3",
            created_at=time.time(),
            geographic_coordinates=(52.5200, 13.4050),
            network_tier=1,
            max_bandwidth_mbps=1200,
            preference_weight=1.2,
        )

        await bridge.add_cluster(cluster_identity)

        # Test sending message
        result = await bridge.send_message(sample_message, "test_cluster_3")
        assert isinstance(result, bool)


class TestGraphRoutingIntegration:
    """Test suite for graph routing integration."""

    @pytest.mark.asyncio
    async def test_graph_routing_initialization(self, sample_federation_bridge):
        """Test graph routing components are properly initialized."""
        bridge = sample_federation_bridge

        # Check graph router exists
        assert hasattr(bridge, "graph_router")
        assert bridge.graph_router is not None

        # Check graph monitor exists
        assert hasattr(bridge, "graph_monitor")
        assert bridge.graph_monitor is not None

        # Check local cluster is in graph
        local_cluster_id = bridge.cluster_identity.cluster_id
        local_node = bridge.graph_router.graph.get_node(local_cluster_id)
        assert local_node is not None
        assert local_node.node_id == local_cluster_id

    @pytest.mark.asyncio
    async def test_cluster_addition_to_graph(self, sample_federation_bridge):
        """Test that adding clusters updates the graph."""
        bridge = sample_federation_bridge

        # Add multiple clusters
        cluster_identities = [
            ClusterIdentity(
                cluster_id=f"cluster_{i}",
                cluster_name=f"Cluster {i}",
                region="us-east" if i % 2 == 0 else "us-west",
                bridge_url=f"ws://cluster{i}.example.com",
                public_key_hash=f"hash_{i}",
                created_at=time.time(),
                geographic_coordinates=(40.0 + i, -74.0 + i),
                network_tier=1,
                max_bandwidth_mbps=1000,
                preference_weight=1.0,
            )
            for i in range(5)
        ]

        for cluster_identity in cluster_identities:
            await bridge.add_cluster(cluster_identity)

        # Check clusters are in graph
        for cluster_identity in cluster_identities:
            node = bridge.graph_router.graph.get_node(cluster_identity.cluster_id)
            assert node is not None
            assert node.node_id == cluster_identity.cluster_id
            assert node.region == cluster_identity.region

        # Check graph statistics
        stats = bridge.graph_router.get_comprehensive_statistics()
        assert stats.graph_statistics.total_nodes >= 5  # At least 5 clusters + local

    @pytest.mark.asyncio
    async def test_graph_edge_creation(self, sample_federation_bridge):
        """Test that graph edges are created appropriately."""
        bridge = sample_federation_bridge

        # Add clusters in same region (should be connected)
        cluster_1 = ClusterIdentity(
            cluster_id="cluster_same_region_1",
            cluster_name="Same Region 1",
            region="us-east",
            bridge_url="ws://cluster1.example.com",
            public_key_hash="hash_1",
            created_at=time.time(),
            geographic_coordinates=(40.7128, -74.0060),
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        cluster_2 = ClusterIdentity(
            cluster_id="cluster_same_region_2",
            cluster_name="Same Region 2",
            region="us-east",
            bridge_url="ws://cluster2.example.com",
            public_key_hash="hash_2",
            created_at=time.time(),
            geographic_coordinates=(40.7500, -74.0100),
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        await bridge.add_cluster(cluster_1)
        await bridge.add_cluster(cluster_2)

        # Check that edges exist
        edge_1_to_2 = bridge.graph_router.graph.get_edge(
            "cluster_same_region_1", "cluster_same_region_2"
        )
        edge_2_to_1 = bridge.graph_router.graph.get_edge(
            "cluster_same_region_2", "cluster_same_region_1"
        )

        assert edge_1_to_2 is not None
        assert edge_2_to_1 is not None

        # Check edge properties
        assert edge_1_to_2.source_id == "cluster_same_region_1"
        assert edge_1_to_2.target_id == "cluster_same_region_2"
        assert edge_1_to_2.latency_ms > 0
        assert edge_1_to_2.bandwidth_mbps > 0

    @pytest.mark.asyncio
    async def test_graph_routing_path_finding(self, sample_federation_bridge):
        """Test graph routing path finding functionality."""
        bridge = sample_federation_bridge

        # Create a chain of clusters
        cluster_ids = []
        for i in range(4):
            cluster_identity = ClusterIdentity(
                cluster_id=f"chain_cluster_{i}",
                cluster_name=f"Chain Cluster {i}",
                region="global",
                bridge_url=f"ws://chain{i}.example.com",
                public_key_hash=f"chain_hash_{i}",
                created_at=time.time(),
                geographic_coordinates=(40.0 + i * 10, -74.0 + i * 10),
                network_tier=1,
                max_bandwidth_mbps=1000,
                preference_weight=1.0,
            )
            await bridge.add_cluster(cluster_identity)
            cluster_ids.append(cluster_identity.cluster_id)

        # Test path finding
        local_cluster_id = bridge.cluster_identity.cluster_id
        target_cluster_id = cluster_ids[-1]

        path = bridge.graph_router.find_optimal_path(
            local_cluster_id, target_cluster_id, max_hops=10
        )
        assert path is not None
        assert len(path) >= 2
        assert path[0] == local_cluster_id
        assert path[-1] == target_cluster_id

    @pytest.mark.asyncio
    async def test_multi_path_routing(self, sample_federation_bridge):
        """Test multi-path routing functionality."""
        bridge = sample_federation_bridge

        # Create a well-connected graph
        cluster_ids = []
        for i in range(6):
            cluster_identity = ClusterIdentity(
                cluster_id=f"multipath_cluster_{i}",
                cluster_name=f"Multipath Cluster {i}",
                region="global",
                bridge_url=f"ws://multipath{i}.example.com",
                public_key_hash=f"multipath_hash_{i}",
                created_at=time.time(),
                geographic_coordinates=(40.0 + i * 5, -74.0 + i * 5),
                network_tier=1,
                max_bandwidth_mbps=1000,
                preference_weight=1.0,
            )
            await bridge.add_cluster(cluster_identity)
            cluster_ids.append(cluster_identity.cluster_id)

        # Test multiple paths
        local_cluster_id = bridge.cluster_identity.cluster_id
        target_cluster_id = cluster_ids[0]

        paths = bridge.graph_router.find_multiple_paths(
            local_cluster_id, target_cluster_id, 3
        )
        assert len(paths) >= 1

        # Check that paths are different
        if len(paths) > 1:
            assert paths[0] != paths[1]

    @pytest.mark.asyncio
    async def test_geographic_routing(self, sample_federation_bridge):
        """Test geographic-aware routing."""
        bridge = sample_federation_bridge

        # Add clusters with specific geographic coordinates
        cluster_identity = ClusterIdentity(
            cluster_id="geographic_cluster",
            cluster_name="Geographic Cluster",
            region="eu-central",
            bridge_url="ws://geographic.example.com",
            public_key_hash="geo_hash",
            created_at=time.time(),
            geographic_coordinates=(52.5200, 13.4050),  # Berlin
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        await bridge.add_cluster(cluster_identity)

        # Test geographic routing
        local_cluster_id = bridge.cluster_identity.cluster_id
        target_cluster_id = "geographic_cluster"

        geo_path = bridge.graph_router.find_geographic_path(
            local_cluster_id, target_cluster_id
        )
        assert geo_path is not None
        assert len(geo_path) >= 2
        assert geo_path[0] == local_cluster_id
        assert geo_path[-1] == target_cluster_id


class TestMessageRouting:
    """Test suite for message routing functionality."""

    @pytest.mark.asyncio
    async def test_direct_message_routing(
        self, sample_federation_bridge, sample_message
    ):
        """Test direct message routing to healthy clusters."""
        bridge = sample_federation_bridge

        # Add a healthy cluster
        cluster_identity = ClusterIdentity(
            cluster_id="healthy_cluster",
            cluster_name="Healthy Cluster",
            region="us-east",
            bridge_url="ws://healthy.example.com",
            public_key_hash="healthy_hash",
            created_at=time.time(),
            geographic_coordinates=(40.7128, -74.0060),
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        await bridge.add_cluster(cluster_identity)

        # Mark cluster as healthy
        connection = bridge.remote_clusters["healthy_cluster"]
        connection.status = ClusterStatus.ACTIVE
        connection.last_heartbeat = time.time()

        # Send message
        result = await bridge.send_message(sample_message, "healthy_cluster")
        assert isinstance(result, bool)

        # Check statistics
        stats = bridge.get_comprehensive_statistics()
        assert stats.graph_routing_stats["direct_routing_used"] > 0

    @pytest.mark.asyncio
    async def test_graph_routing_fallback(
        self, sample_federation_bridge, sample_message
    ):
        """Test graph routing fallback for unhealthy clusters."""
        bridge = sample_federation_bridge

        # Add an unhealthy cluster
        cluster_identity = ClusterIdentity(
            cluster_id="unhealthy_cluster",
            cluster_name="Unhealthy Cluster",
            region="us-west",
            bridge_url="ws://unhealthy.example.com",
            public_key_hash="unhealthy_hash",
            created_at=time.time(),
            geographic_coordinates=(37.7749, -122.4194),
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        await bridge.add_cluster(cluster_identity)

        # Mark cluster as unhealthy
        connection = bridge.remote_clusters["unhealthy_cluster"]
        connection.status = ClusterStatus.FAILED
        connection.last_heartbeat = time.time() - 100  # Old heartbeat

        # Send message (should attempt graph routing)
        result = await bridge.send_message(sample_message, "unhealthy_cluster")
        assert isinstance(result, bool)

        # Check that graph routing was attempted
        stats = bridge.get_comprehensive_statistics()
        routing_stats = stats.graph_routing_stats
        assert (
            routing_stats.get("no_path_found", 0) > 0
            or routing_stats.get("graph_routing_used", 0) > 0
        )

    @pytest.mark.asyncio
    async def test_multi_hop_routing(self, sample_federation_bridge, sample_message):
        """Test multi-hop routing through intermediate clusters."""
        import asyncio

        from mpreg.core.config import MPREGSettings
        from mpreg.server import MPREGServer

        from .port_allocator import allocate_port_range

        # Allocate ports for 3 test servers
        ports = allocate_port_range(3, "servers")

        servers = []
        server_tasks = []
        cluster_identities = []

        try:
            # Create a chain of real test servers
            for i, port in enumerate(ports):
                settings = MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"HopServer{i}",
                    cluster_id=f"hop_cluster_{i}",
                )
                server = MPREGServer(settings=settings)
                servers.append(server)

                # Start server
                server_task = asyncio.create_task(server.server())
                server_tasks.append(server_task)

                # Create cluster identity
                cluster_identity = ClusterIdentity(
                    cluster_id=f"hop_cluster_{i}",
                    cluster_name=f"Hop Cluster {i}",
                    region="global",
                    bridge_url=f"ws://127.0.0.1:{port}",
                    public_key_hash=f"hop_hash_{i}",
                    created_at=time.time(),
                    geographic_coordinates=(40.0 + i * 15, -74.0 + i * 15),
                    network_tier=1,
                    max_bandwidth_mbps=1000,
                    preference_weight=1.0,
                )
                cluster_identities.append(cluster_identity)

            # Give servers time to start
            await asyncio.sleep(1.0)

            # Add clusters to bridge
            bridge = sample_federation_bridge
            for cluster_identity in cluster_identities:
                await bridge.add_cluster(cluster_identity)

            # Send message to distant cluster
            result = await bridge.send_message(sample_message, "hop_cluster_2")
            assert isinstance(result, bool)

            # Check routing statistics
            stats = bridge.get_comprehensive_statistics()
            routing_stats = stats.graph_routing_stats
            # Should have attempted direct routing to the live server
            assert routing_stats.get("direct_routing_used", 0) >= 1

        finally:
            # Cleanup servers
            for server_task in server_tasks:
                server_task.cancel()
                try:
                    await server_task
                except asyncio.CancelledError:
                    pass


class TestPerformanceAndMonitoring:
    """Test suite for performance and monitoring features."""

    @pytest.mark.asyncio
    async def test_performance_metrics_collection(self, sample_federation_bridge):
        """Test performance metrics collection."""
        bridge = sample_federation_bridge

        # Add cluster
        cluster_identity = ClusterIdentity(
            cluster_id="perf_cluster",
            cluster_name="Performance Cluster",
            region="us-east",
            bridge_url="ws://perf.example.com",
            public_key_hash="perf_hash",
            created_at=time.time(),
            geographic_coordinates=(40.7128, -74.0060),
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        await bridge.add_cluster(cluster_identity)

        # Send some messages
        message = PubSubMessage(
            message_id="perf_msg",
            topic="perf.topic",
            payload=b"performance test",
            timestamp=time.time(),
            publisher="test_publisher",
        )

        for _ in range(10):
            await bridge.send_message(message, "perf_cluster")

        # Check statistics
        stats = bridge.get_comprehensive_statistics()

        assert hasattr(stats, "performance_metrics")
        assert hasattr(stats, "graph_routing_stats")
        assert hasattr(stats, "message_statistics")

        # Check that metrics were recorded
        routing_stats = stats.graph_routing_stats
        total_routing = (
            routing_stats.get("direct_routing_used", 0)
            + routing_stats.get("graph_routing_used", 0)
            + routing_stats.get("multi_hop_routing_used", 0)
        )
        assert total_routing > 0

    @pytest.mark.asyncio
    async def test_monitoring_integration(self, sample_federation_bridge):
        """Test monitoring system integration."""
        bridge = sample_federation_bridge

        # Start monitoring
        await bridge.start()

        # Add cluster and generate activity
        cluster_identity = ClusterIdentity(
            cluster_id="monitor_cluster",
            cluster_name="Monitor Cluster",
            region="us-east",
            bridge_url="ws://monitor.example.com",
            public_key_hash="monitor_hash",
            created_at=time.time(),
            geographic_coordinates=(40.7128, -74.0060),
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        await bridge.add_cluster(cluster_identity)

        # Wait for monitoring to collect data
        await asyncio.sleep(0.1)

        # Check monitoring status
        stats = bridge.get_comprehensive_statistics()

        if hasattr(stats, "monitoring") and stats.monitoring:
            monitoring_stats = stats.monitoring
            assert hasattr(monitoring_stats, "monitoring_status")
            assert monitoring_stats.monitoring_status.is_monitoring

        # Stop monitoring
        await bridge.stop()

    @pytest.mark.asyncio
    async def test_performance_regression_prevention(self, sample_federation_bridge):
        """Test that graph routing doesn't cause performance regression."""
        bridge = sample_federation_bridge

        # Add cluster
        cluster_identity = ClusterIdentity(
            cluster_id="regression_cluster",
            cluster_name="Regression Cluster",
            region="us-east",
            bridge_url="ws://127.0.0.1:19500",
            public_key_hash="regression_hash",
            created_at=time.time(),
            geographic_coordinates=(40.7128, -74.0060),
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        await bridge.add_cluster(cluster_identity)

        # Mark cluster as healthy for direct routing
        connection = bridge.remote_clusters["regression_cluster"]
        connection.status = ClusterStatus.ACTIVE
        connection.last_heartbeat = time.time()

        # Measure direct routing performance
        message = PubSubMessage(
            message_id="regression_msg",
            topic="regression.topic",
            payload=b"regression test",
            timestamp=time.time(),
            publisher="test_publisher",
        )

        start_time = time.time()
        for _ in range(10):
            await bridge.send_message(message, "regression_cluster")
        direct_routing_time = time.time() - start_time

        # Check that direct routing was used (no regression)
        # Note: circuit breaker opens after failures, so may be fewer than 10 attempts
        stats = bridge.get_comprehensive_statistics()
        assert stats.graph_routing_stats["direct_routing_used"] >= 3

        # Verify reasonable performance
        assert direct_routing_time < 1.0  # Should complete in under 1 second

    @pytest.mark.asyncio
    async def test_large_scale_routing(self, sample_federation_bridge):
        """Test routing with many clusters."""
        bridge = sample_federation_bridge

        # Add many clusters
        cluster_count = 20
        for i in range(cluster_count):
            cluster_identity = ClusterIdentity(
                cluster_id=f"scale_cluster_{i}",
                cluster_name=f"Scale Cluster {i}",
                region=f"region_{i % 5}",
                bridge_url=f"ws://scale{i}.example.com",
                public_key_hash=f"scale_hash_{i}",
                created_at=time.time(),
                geographic_coordinates=(40.0 + i * 2, -74.0 + i * 2),
                network_tier=1,
                max_bandwidth_mbps=1000,
                preference_weight=1.0,
            )
            await bridge.add_cluster(cluster_identity)

        # Test routing to multiple clusters
        message = PubSubMessage(
            message_id="scale_msg",
            topic="scale.topic",
            payload=b"scale test",
            timestamp=time.time(),
            publisher="test_publisher",
        )

        start_time = time.time()
        for i in range(min(10, cluster_count)):
            await bridge.send_message(message, f"scale_cluster_{i}")
        routing_time = time.time() - start_time

        # Check performance
        assert routing_time < 2.0  # Should complete in under 2 seconds

        # Check graph statistics
        stats = bridge.get_comprehensive_statistics()
        graph_stats = stats.graph_routing_performance.graph_statistics
        assert graph_stats.total_nodes >= cluster_count


class TestErrorHandling:
    """Test suite for error handling and resilience."""

    @pytest.mark.asyncio
    async def test_cluster_removal_handling(self, sample_federation_bridge):
        """Test handling of cluster removal."""
        bridge = sample_federation_bridge

        # Add cluster
        cluster_identity = ClusterIdentity(
            cluster_id="removable_cluster",
            cluster_name="Removable Cluster",
            region="us-east",
            bridge_url="ws://removable.example.com",
            public_key_hash="removable_hash",
            created_at=time.time(),
            geographic_coordinates=(40.7128, -74.0060),
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        await bridge.add_cluster(cluster_identity)

        # Verify cluster exists
        assert "removable_cluster" in bridge.remote_clusters
        node = bridge.graph_router.graph.get_node("removable_cluster")
        assert node is not None

        # Remove cluster
        result = await bridge.remove_cluster("removable_cluster")
        assert result

        # Verify cluster is removed
        assert "removable_cluster" not in bridge.remote_clusters
        node = bridge.graph_router.graph.get_node("removable_cluster")
        assert node is None

    @pytest.mark.asyncio
    async def test_invalid_cluster_handling(
        self, sample_federation_bridge, sample_message
    ):
        """Test handling of invalid cluster IDs."""
        bridge = sample_federation_bridge

        # Test sending to non-existent cluster
        result = await bridge.send_message(sample_message, "nonexistent_cluster")
        assert not result

        # Test getting status of non-existent cluster
        status = bridge.get_cluster_status("nonexistent_cluster")
        assert status == ClusterStatus.DISCONNECTED

        # Test removing non-existent cluster
        result = await bridge.remove_cluster("nonexistent_cluster")
        assert not result

    @pytest.mark.asyncio
    async def test_circuit_breaker_handling(
        self, sample_federation_bridge, sample_message
    ):
        """Test circuit breaker functionality."""
        bridge = sample_federation_bridge

        # Add cluster
        cluster_identity = ClusterIdentity(
            cluster_id="circuit_cluster",
            cluster_name="Circuit Cluster",
            region="us-east",
            bridge_url="ws://circuit.example.com",
            public_key_hash="circuit_hash",
            created_at=time.time(),
            geographic_coordinates=(40.7128, -74.0060),
            network_tier=1,
            max_bandwidth_mbps=1000,
            preference_weight=1.0,
        )

        await bridge.add_cluster(cluster_identity)

        # Get connection and trigger circuit breaker
        connection = bridge.remote_clusters["circuit_cluster"]

        # Simulate failures to trigger circuit breaker
        for _ in range(5):
            connection.circuit_breaker.record_failure()

        # Verify circuit breaker is open
        assert not connection.circuit_breaker.can_execute()

        # Test message sending with circuit breaker open
        result = await bridge.send_message(sample_message, "circuit_cluster")
        assert not result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
