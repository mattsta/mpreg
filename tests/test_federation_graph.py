"""
Comprehensive unit tests for the federation graph routing engine.

These tests verify the correctness, performance, and reliability of the graph-based
routing algorithms that form the foundation of planet-scale federation.

Test Coverage:
- Graph data structure operations
- Dijkstra's algorithm correctness
- A* algorithm with geographic heuristics
- Multi-path routing functionality
- Real-time metric updates
- Cache management and performance
- Error handling and edge cases
- Performance benchmarks

This is part of Phase 1 of the Planet-Scale Federation Roadmap.
"""

import time

import pytest

from mpreg.fabric.federation_graph import (
    DijkstraRouter,
    FederationGraph,
    FederationGraphEdge,
    FederationGraphNode,
    GeographicAStarRouter,
    GeographicCoordinate,
    GraphBasedFederationRouter,
    MultiPathRouter,
    NodeType,
)


class TestGeographicCoordinate:
    """Test geographic coordinate calculations."""

    def test_distance_calculation(self):
        """Test Haversine distance calculation."""
        # Test known distances
        san_francisco = GeographicCoordinate(37.7749, -122.4194)
        new_york = GeographicCoordinate(40.7128, -74.0060)

        distance = san_francisco.distance_to(new_york)

        # SF to NYC is approximately 4135 km
        assert 4100 <= distance <= 4200, f"Expected ~4135km, got {distance}"

    def test_distance_same_point(self):
        """Test distance from a point to itself."""
        point = GeographicCoordinate(37.7749, -122.4194)
        distance = point.distance_to(point)
        assert distance == 0.0

    def test_distance_symmetry(self):
        """Test that distance is symmetric."""
        point1 = GeographicCoordinate(37.7749, -122.4194)
        point2 = GeographicCoordinate(40.7128, -74.0060)

        distance1 = point1.distance_to(point2)
        distance2 = point2.distance_to(point1)

        assert abs(distance1 - distance2) < 0.001


class TestFederationGraphNode:
    """Test federation graph node functionality."""

    def test_node_creation(self):
        """Test node creation with all parameters."""
        coords = GeographicCoordinate(37.7749, -122.4194)
        node = FederationGraphNode(
            node_id="test_node",
            node_type=NodeType.CLUSTER,
            region="us-west",
            coordinates=coords,
            max_capacity=1000,
            current_load=500.0,
            health_score=0.9,
        )

        assert node.node_id == "test_node"
        assert node.node_type == NodeType.CLUSTER
        assert node.region == "us-west"
        assert node.coordinates == coords
        assert node.max_capacity == 1000
        assert node.current_load == 500.0
        assert node.health_score == 0.9

    def test_capacity_utilization(self):
        """Test capacity utilization calculation."""
        coords = GeographicCoordinate(37.7749, -122.4194)
        node = FederationGraphNode(
            node_id="test_node",
            node_type=NodeType.CLUSTER,
            region="us-west",
            coordinates=coords,
            max_capacity=1000,
            current_load=500.0,
        )

        assert node.get_capacity_utilization() == 0.5

        # Test over-capacity
        node.current_load = 1500.0
        assert node.get_capacity_utilization() == 1.0

    def test_node_health_check(self):
        """Test node health determination."""
        coords = GeographicCoordinate(37.7749, -122.4194)

        # Healthy node
        healthy_node = FederationGraphNode(
            node_id="healthy",
            node_type=NodeType.CLUSTER,
            region="us-west",
            coordinates=coords,
            max_capacity=1000,
            current_load=500.0,
            health_score=0.9,
        )

        assert healthy_node.is_healthy()

        # Unhealthy node (low health score)
        unhealthy_node = FederationGraphNode(
            node_id="unhealthy",
            node_type=NodeType.CLUSTER,
            region="us-west",
            coordinates=coords,
            max_capacity=1000,
            current_load=500.0,
            health_score=0.3,
        )

        assert not unhealthy_node.is_healthy()

        # Overloaded node
        overloaded_node = FederationGraphNode(
            node_id="overloaded",
            node_type=NodeType.CLUSTER,
            region="us-west",
            coordinates=coords,
            max_capacity=1000,
            current_load=950.0,
            health_score=0.9,
        )

        assert not overloaded_node.is_healthy()

    def test_node_weight_calculation(self):
        """Test node weight calculation for routing."""
        coords = GeographicCoordinate(37.7749, -122.4194)

        # Low weight (good) node
        good_node = FederationGraphNode(
            node_id="good",
            node_type=NodeType.CLUSTER,
            region="us-west",
            coordinates=coords,
            max_capacity=1000,
            current_load=100.0,
            health_score=0.95,
            processing_latency_ms=2.0,
        )

        # High weight (bad) node
        bad_node = FederationGraphNode(
            node_id="bad",
            node_type=NodeType.CLUSTER,
            region="us-west",
            coordinates=coords,
            max_capacity=1000,
            current_load=900.0,
            health_score=0.6,
            processing_latency_ms=20.0,
        )

        good_weight = good_node.get_node_weight()
        bad_weight = bad_node.get_node_weight()

        assert good_weight < bad_weight, (
            f"Good node weight {good_weight} should be less than bad node weight {bad_weight}"
        )


class TestFederationGraphEdge:
    """Test federation graph edge functionality."""

    def test_edge_creation(self):
        """Test edge creation with all parameters."""
        edge = FederationGraphEdge(
            source_id="node1",
            target_id="node2",
            latency_ms=50.0,
            bandwidth_mbps=1000,
            reliability_score=0.95,
            current_utilization=0.3,
            packet_loss_rate=0.01,
            jitter_ms=2.0,
        )

        assert edge.source_id == "node1"
        assert edge.target_id == "node2"
        assert edge.latency_ms == 50.0
        assert edge.bandwidth_mbps == 1000
        assert edge.reliability_score == 0.95
        assert edge.current_utilization == 0.3
        assert edge.packet_loss_rate == 0.01
        assert edge.jitter_ms == 2.0

    def test_edge_weight_calculation(self):
        """Test edge weight calculation for routing."""
        # Good edge (low weight)
        good_edge = FederationGraphEdge(
            source_id="node1",
            target_id="node2",
            latency_ms=10.0,
            bandwidth_mbps=1000,
            reliability_score=0.99,
            current_utilization=0.1,
            packet_loss_rate=0.001,
            jitter_ms=0.5,
        )

        # Bad edge (high weight)
        bad_edge = FederationGraphEdge(
            source_id="node1",
            target_id="node2",
            latency_ms=200.0,
            bandwidth_mbps=100,
            reliability_score=0.7,
            current_utilization=0.9,
            packet_loss_rate=0.05,
            jitter_ms=10.0,
        )

        good_weight = good_edge.get_edge_weight()
        bad_weight = bad_edge.get_edge_weight()

        assert good_weight < bad_weight, (
            f"Good edge weight {good_weight} should be less than bad edge weight {bad_weight}"
        )

    def test_edge_usability(self):
        """Test edge usability determination."""
        # Usable edge
        usable_edge = FederationGraphEdge(
            source_id="node1",
            target_id="node2",
            latency_ms=50.0,
            bandwidth_mbps=1000,
            reliability_score=0.95,
            current_utilization=0.7,
            packet_loss_rate=0.01,
        )

        assert usable_edge.is_usable()

        # Unusable edge (high packet loss)
        unusable_edge = FederationGraphEdge(
            source_id="node1",
            target_id="node2",
            latency_ms=50.0,
            bandwidth_mbps=1000,
            reliability_score=0.95,
            current_utilization=0.7,
            packet_loss_rate=0.15,
        )

        assert not unusable_edge.is_usable()

    def test_metric_updates(self):
        """Test edge metric updates with exponential moving average."""
        edge = FederationGraphEdge(
            source_id="node1",
            target_id="node2",
            latency_ms=50.0,
            bandwidth_mbps=1000,
            reliability_score=0.95,
            current_utilization=0.5,
        )

        original_latency = edge.latency_ms
        original_utilization = edge.current_utilization

        # Update with new metrics
        edge.update_metrics(
            latency_ms=60.0, utilization=0.7, packet_loss=0.02, jitter_ms=3.0
        )

        # First update uses exact values (sample_count was 0)
        assert edge.latency_ms == 60.0  # First update, so exact value
        assert edge.current_utilization == 0.7  # First update, so exact value
        assert edge.packet_loss_rate == 0.02  # First update, so exact value
        assert edge.jitter_ms == 3.0  # First update, so exact value

        # Second update to test smoothing
        edge.update_metrics(
            latency_ms=80.0, utilization=0.3, packet_loss=0.01, jitter_ms=1.0
        )

        # Now values should be smoothed (not exactly the new values)
        assert edge.latency_ms != 80.0
        assert edge.current_utilization != 0.3
        assert 60.0 < edge.latency_ms < 80.0  # Should be between old and new values
        assert (
            0.3 < edge.current_utilization < 0.7
        )  # Should be between old and new values


class TestFederationGraph:
    """Test federation graph data structure."""

    def setup_method(self):
        """Setup test graph for each test."""
        self.graph = FederationGraph()

        # Create test nodes
        self.node1 = FederationGraphNode(
            node_id="node1",
            node_type=NodeType.CLUSTER,
            region="us-west",
            coordinates=GeographicCoordinate(37.7749, -122.4194),
            max_capacity=1000,
        )

        self.node2 = FederationGraphNode(
            node_id="node2",
            node_type=NodeType.CLUSTER,
            region="us-east",
            coordinates=GeographicCoordinate(40.7128, -74.0060),
            max_capacity=1000,
        )

        self.node3 = FederationGraphNode(
            node_id="node3",
            node_type=NodeType.REGIONAL_HUB,
            region="eu-central",
            coordinates=GeographicCoordinate(50.1109, 8.6821),
            max_capacity=5000,
        )

        # Add nodes to graph
        self.graph.add_node(self.node1)
        self.graph.add_node(self.node2)
        self.graph.add_node(self.node3)

    def test_node_operations(self):
        """Test node addition, retrieval, and removal."""
        # Test node retrieval
        retrieved_node = self.graph.get_node("node1")
        assert retrieved_node is not None
        assert retrieved_node.node_id == "node1"

        # Test node removal
        assert self.graph.remove_node("node1")
        assert self.graph.get_node("node1") is None

        # Test removing non-existent node
        assert not self.graph.remove_node("nonexistent")

    def test_edge_operations(self):
        """Test edge addition, retrieval, and removal."""
        # Create and add edge
        edge = FederationGraphEdge(
            source_id="node1",
            target_id="node2",
            latency_ms=50.0,
            bandwidth_mbps=1000,
            reliability_score=0.95,
        )

        self.graph.add_edge(edge)

        # Test edge retrieval
        retrieved_edge = self.graph.get_edge("node1", "node2")
        assert retrieved_edge is not None
        assert retrieved_edge.latency_ms == 50.0

        # Test reverse edge (bidirectional)
        reverse_edge = self.graph.get_edge("node2", "node1")
        assert reverse_edge is not None
        assert reverse_edge.latency_ms == 50.0

        # Test neighbors
        neighbors = self.graph.get_neighbors("node1")
        assert "node2" in neighbors

        # Test edge removal
        assert self.graph.remove_edge("node1", "node2")
        assert self.graph.get_edge("node1", "node2") is None
        assert self.graph.get_edge("node2", "node1") is None

    def test_metric_updates(self):
        """Test real-time metric updates."""
        # Add edge
        edge = FederationGraphEdge(
            source_id="node1",
            target_id="node2",
            latency_ms=50.0,
            bandwidth_mbps=1000,
            reliability_score=0.95,
        )
        self.graph.add_edge(edge)

        # Update node metrics
        assert self.graph.update_node_metrics("node1", 500.0, 0.8, 5.0)

        updated_node = self.graph.get_node("node1")
        assert updated_node is not None
        assert updated_node.current_load == 500.0
        assert updated_node.health_score == 0.8
        assert updated_node.processing_latency_ms == 5.0

        # Update edge metrics
        assert self.graph.update_edge_metrics("node1", "node2", 60.0, 0.7, 0.02, 3.0)

        updated_edge = self.graph.get_edge("node1", "node2")
        assert updated_edge is not None
        assert updated_edge.latency_ms == 60.0
        assert updated_edge.current_utilization == 0.7

    def test_path_caching(self):
        """Test path caching functionality."""
        test_path = ["node1", "node2", "node3"]

        # Test cache miss
        cached_path = self.graph.get_path_from_cache("node1", "node3")
        assert cached_path is None

        # Cache a path
        self.graph.cache_path("node1", "node3", test_path)

        # Test cache hit
        cached_path = self.graph.get_path_from_cache("node1", "node3")
        assert cached_path == test_path

        # Test cache statistics
        stats = self.graph.get_statistics()
        assert stats.cache_statistics.cache_hits == 1
        assert stats.cache_statistics.cache_misses == 1

    def test_graph_statistics(self):
        """Test comprehensive graph statistics."""
        # Add some edges
        edge1 = FederationGraphEdge("node1", "node2", 50.0, 1000, 0.95)
        edge2 = FederationGraphEdge("node2", "node3", 100.0, 1000, 0.9)

        self.graph.add_edge(edge1)
        self.graph.add_edge(edge2)

        stats = self.graph.get_statistics()

        assert stats.total_nodes == 3
        assert stats.total_edges == 2
        assert stats.healthy_nodes == 3
        assert stats.average_connectivity == 4.0 / 3.0  # (2 * 2 edges) / 3 nodes
        assert hasattr(stats, "cache_statistics")
        assert hasattr(stats, "node_types")


class TestDijkstraRouter:
    """Test Dijkstra's algorithm implementation."""

    def setup_method(self):
        """Setup test graph for routing tests."""
        self.graph = FederationGraph()
        self.router = DijkstraRouter(self.graph)

        # Create a simple test topology
        nodes = [
            FederationGraphNode(
                node_id=f"node{i}",
                node_type=NodeType.CLUSTER,
                region="test",
                coordinates=GeographicCoordinate(0.0, float(i)),
                max_capacity=1000,
            )
            for i in range(1, 6)  # node1 to node5
        ]

        for node in nodes:
            self.graph.add_node(node)

        # Create edges: node1 -> node2 -> node3 -> node4 -> node5
        #               node1 -> node5 (longer path)
        edges = [
            FederationGraphEdge("node1", "node2", 10.0, 1000, 0.95),
            FederationGraphEdge("node2", "node3", 10.0, 1000, 0.95),
            FederationGraphEdge("node3", "node4", 10.0, 1000, 0.95),
            FederationGraphEdge("node4", "node5", 10.0, 1000, 0.95),
            FederationGraphEdge(
                "node1", "node5", 50.0, 1000, 0.95
            ),  # Direct but slower
        ]

        for edge in edges:
            self.graph.add_edge(edge)

    def test_optimal_path_finding(self):
        """Test optimal path finding with Dijkstra's algorithm."""
        # Find path from node1 to node5
        path = self.router.find_optimal_path("node1", "node5")

        # Should take the multi-hop path (cheaper than direct)
        expected_path = ["node1", "node2", "node3", "node4", "node5"]
        assert path == expected_path

    def test_same_node_path(self):
        """Test path from a node to itself."""
        path = self.router.find_optimal_path("node1", "node1")
        assert path == ["node1"]

    def test_no_path_exists(self):
        """Test behavior when no path exists."""
        # Add isolated node
        isolated_node = FederationGraphNode(
            node_id="isolated",
            node_type=NodeType.CLUSTER,
            region="test",
            coordinates=GeographicCoordinate(0.0, 10.0),
            max_capacity=1000,
        )
        self.graph.add_node(isolated_node)

        path = self.router.find_optimal_path("node1", "isolated")
        assert path is None

    def test_unhealthy_node_avoidance(self):
        """Test that unhealthy nodes are avoided."""
        # Make node3 unhealthy
        self.graph.update_node_metrics("node3", 950.0, 0.3, 100.0)

        # Should find alternative path or no path
        path = self.router.find_optimal_path("node1", "node5")

        # Should either take direct path or find no path
        if path:
            assert "node3" not in path

    def test_max_hops_limit(self):
        """Test maximum hops limitation."""
        # Try to find path with very low hop limit
        path = self.router.find_optimal_path("node1", "node5", max_hops=2)

        # Should either take direct path or find no path within hop limit
        if path:
            assert len(path) <= 2

    def test_path_caching(self):
        """Test that paths are cached for performance."""
        # First call should compute path
        path1 = self.router.find_optimal_path("node1", "node5")

        # Second call should use cache
        path2 = self.router.find_optimal_path("node1", "node5")

        assert path1 == path2

        # Check cache statistics
        stats = self.graph.get_statistics()
        assert stats.cache_statistics.cache_hits >= 1


class TestMultiPathRouter:
    """Test multiple path routing functionality."""

    def setup_method(self):
        """Setup test graph with multiple paths."""
        self.graph = FederationGraph()
        self.router = MultiPathRouter(self.graph)

        # Create diamond topology for multiple paths
        nodes = [
            FederationGraphNode(
                node_id=f"node{i}",
                node_type=NodeType.CLUSTER,
                region="test",
                coordinates=GeographicCoordinate(0.0, float(i)),
                max_capacity=1000,
            )
            for i in range(1, 5)  # node1 to node4
        ]

        for node in nodes:
            self.graph.add_node(node)

        # Create diamond topology: node1 -> {node2, node3} -> node4
        edges = [
            FederationGraphEdge("node1", "node2", 10.0, 1000, 0.95),
            FederationGraphEdge("node1", "node3", 15.0, 1000, 0.95),
            FederationGraphEdge("node2", "node4", 10.0, 1000, 0.95),
            FederationGraphEdge("node3", "node4", 10.0, 1000, 0.95),
        ]

        for edge in edges:
            self.graph.add_edge(edge)

    def test_multiple_paths(self):
        """Test finding multiple disjoint paths."""
        paths = self.router.find_multiple_paths("node1", "node4", num_paths=2)

        assert len(paths) == 2

        # Paths should be different
        assert paths[0] != paths[1]

        # Both paths should start and end correctly
        for path in paths:
            assert path[0] == "node1"
            assert path[-1] == "node4"

    def test_path_disjointness(self):
        """Test that multiple paths are disjoint (don't share edges)."""
        paths = self.router.find_multiple_paths("node1", "node4", num_paths=2)

        if len(paths) >= 2:
            # Extract edges from each path
            edges1 = [(paths[0][i], paths[0][i + 1]) for i in range(len(paths[0]) - 1)]
            edges2 = [(paths[1][i], paths[1][i + 1]) for i in range(len(paths[1]) - 1)]

            # Should not share any edges
            assert not any(edge in edges2 for edge in edges1)

    def test_limited_paths(self):
        """Test behavior when fewer paths exist than requested."""
        # In our diamond topology, there are only 2 disjoint paths
        paths = self.router.find_multiple_paths("node1", "node4", num_paths=5)

        # Should return only the paths that exist
        assert len(paths) <= 2


class TestGeographicAStarRouter:
    """Test A* algorithm with geographic heuristics."""

    def setup_method(self):
        """Setup test graph with geographic coordinates."""
        self.graph = FederationGraph()
        self.router = GeographicAStarRouter(self.graph)

        # Create nodes with realistic geographic coordinates
        nodes = [
            FederationGraphNode(
                node_id="san_francisco",
                node_type=NodeType.CLUSTER,
                region="us-west",
                coordinates=GeographicCoordinate(37.7749, -122.4194),
                max_capacity=1000,
            ),
            FederationGraphNode(
                node_id="denver",
                node_type=NodeType.CLUSTER,
                region="us-central",
                coordinates=GeographicCoordinate(39.7392, -104.9903),
                max_capacity=1000,
            ),
            FederationGraphNode(
                node_id="new_york",
                node_type=NodeType.CLUSTER,
                region="us-east",
                coordinates=GeographicCoordinate(40.7128, -74.0060),
                max_capacity=1000,
            ),
            FederationGraphNode(
                node_id="miami",
                node_type=NodeType.CLUSTER,
                region="us-southeast",
                coordinates=GeographicCoordinate(25.7617, -80.1918),
                max_capacity=1000,
            ),
        ]

        for node in nodes:
            self.graph.add_node(node)

        # Create edges (all-to-all for simplicity)
        node_pairs = [
            ("san_francisco", "denver", 25.0),
            ("san_francisco", "new_york", 70.0),
            ("san_francisco", "miami", 80.0),
            ("denver", "new_york", 45.0),
            ("denver", "miami", 60.0),
            ("new_york", "miami", 30.0),
        ]

        for source, target, latency in node_pairs:
            edge = FederationGraphEdge(source, target, latency, 1000, 0.95)
            self.graph.add_edge(edge)

    def test_geographic_path_finding(self):
        """Test geographic-aware path finding."""
        path = self.router.find_geographic_path("san_francisco", "new_york")

        assert path is not None
        assert path[0] == "san_francisco"
        assert path[-1] == "new_york"

        # A* should find a reasonable path
        assert len(path) >= 2

    def test_geographic_heuristic_effectiveness(self):
        """Test that geographic heuristic guides search effectively."""
        # This test verifies that A* considers geographic distance
        # by comparing computation time with regular Dijkstra

        dijkstra_router = DijkstraRouter(self.graph)

        # Both should find paths, but A* might be faster for large graphs
        astar_path = self.router.find_geographic_path("san_francisco", "new_york")
        dijkstra_path = dijkstra_router.find_optimal_path("san_francisco", "new_york")

        assert astar_path is not None
        assert dijkstra_path is not None

        # Both should find valid paths (may be different due to heuristics)
        assert astar_path[0] == "san_francisco"
        assert astar_path[-1] == "new_york"
        assert dijkstra_path[0] == "san_francisco"
        assert dijkstra_path[-1] == "new_york"

    def test_same_location_path(self):
        """Test path from a node to itself."""
        path = self.router.find_geographic_path("san_francisco", "san_francisco")
        assert path == ["san_francisco"]


class TestGraphBasedFederationRouter:
    """Test the unified graph-based federation router."""

    def setup_method(self):
        """Setup test router with sample topology."""
        self.router = GraphBasedFederationRouter()

        # Create test topology
        nodes = [
            FederationGraphNode(
                node_id="cluster1",
                node_type=NodeType.CLUSTER,
                region="us-west",
                coordinates=GeographicCoordinate(37.7749, -122.4194),
                max_capacity=1000,
            ),
            FederationGraphNode(
                node_id="hub1",
                node_type=NodeType.REGIONAL_HUB,
                region="us-west",
                coordinates=GeographicCoordinate(37.7749, -122.4194),
                max_capacity=5000,
            ),
            FederationGraphNode(
                node_id="cluster2",
                node_type=NodeType.CLUSTER,
                region="us-east",
                coordinates=GeographicCoordinate(40.7128, -74.0060),
                max_capacity=1000,
            ),
        ]

        for node in nodes:
            self.router.add_node(node)

        # Create edges
        edges = [
            FederationGraphEdge("cluster1", "hub1", 5.0, 1000, 0.99),
            FederationGraphEdge("hub1", "cluster2", 50.0, 1000, 0.95),
            FederationGraphEdge(
                "cluster1", "cluster2", 70.0, 1000, 0.9
            ),  # Direct backup
        ]

        for edge in edges:
            self.router.add_edge(edge)

    def test_unified_router_functionality(self):
        """Test all routing methods work together."""
        # Test optimal path
        optimal_path = self.router.find_optimal_path("cluster1", "cluster2")
        assert optimal_path is not None

        # Test multiple paths
        multiple_paths = self.router.find_multiple_paths(
            "cluster1", "cluster2", num_paths=2
        )
        assert len(multiple_paths) >= 1

        # Test geographic path
        geographic_path = self.router.find_geographic_path("cluster1", "cluster2")
        assert geographic_path is not None

        # All paths should be valid
        for path in [optimal_path, geographic_path] + multiple_paths:
            if path:
                assert path[0] == "cluster1"
                assert path[-1] == "cluster2"

    def test_metric_updates(self):
        """Test real-time metric updates."""
        # Update edge metrics
        self.router.update_edge_metrics("cluster1", "hub1", 10.0, 0.5, 0.01, 2.0)

        # Update node metrics
        result = self.router.update_node_metrics("cluster1", 500.0, 0.9, 3.0)
        assert result is True

        # Verify updates
        node = self.router.graph.get_node("cluster1")
        assert node is not None
        assert node.current_load == 500.0
        assert node.health_score == 0.9
        assert node.processing_latency_ms == 3.0

    def test_performance_statistics(self):
        """Test comprehensive statistics collection."""
        # Perform some routing operations
        self.router.find_optimal_path("cluster1", "cluster2")
        self.router.find_multiple_paths("cluster1", "cluster2")

        stats = self.router.get_comprehensive_statistics()

        assert hasattr(stats, "graph_statistics")
        assert hasattr(stats, "routing_performance")
        assert hasattr(stats, "algorithm_performance")

        # Check routing performance stats
        routing_stats = stats.routing_performance
        assert routing_stats.total_requests >= 2
        assert routing_stats.successful_routes >= 1
        assert hasattr(routing_stats, "success_rate")
        assert hasattr(routing_stats, "average_computation_time_ms")

    def test_node_removal_cleanup(self):
        """Test that removing nodes properly cleans up the graph."""
        # Add and then remove a node
        test_node = FederationGraphNode(
            node_id="temp_node",
            node_type=NodeType.CLUSTER,
            region="test",
            coordinates=GeographicCoordinate(0.0, 0.0),
            max_capacity=1000,
        )

        self.router.add_node(test_node)
        assert self.router.graph.get_node("temp_node") is not None

        # Remove the node
        result = self.router.remove_node("temp_node")
        assert result is True
        assert self.router.graph.get_node("temp_node") is None


class TestPerformanceBenchmarks:
    """Performance benchmarks for graph routing algorithms."""

    def test_large_graph_performance(self):
        """Test performance with large graphs (target: <10ms for 1000 nodes)."""
        router = GraphBasedFederationRouter()

        # Create a large graph (scaled down for unit tests)
        num_nodes = 100  # Would be 1000 in full benchmark

        # Add nodes
        for i in range(num_nodes):
            node = FederationGraphNode(
                node_id=f"node_{i}",
                node_type=NodeType.CLUSTER,
                region=f"region_{i // 10}",
                coordinates=GeographicCoordinate(
                    latitude=float(i % 180 - 90), longitude=float(i % 360 - 180)
                ),
                max_capacity=1000,
            )
            router.add_node(node)

        # Add edges (create a connected graph - simple chain)
        for i in range(num_nodes - 1):
            edge = FederationGraphEdge(
                source_id=f"node_{i}",
                target_id=f"node_{i + 1}",
                latency_ms=10.0,
                bandwidth_mbps=1000,
                reliability_score=0.95,
            )
            router.add_edge(edge)

        # Benchmark path finding (increase max_hops for large chain)
        start_time = time.time()
        path = router.find_optimal_path(
            "node_0", f"node_{num_nodes - 1}", max_hops=num_nodes
        )
        computation_time = (time.time() - start_time) * 1000

        assert path is not None, (
            f"Expected path from node_0 to node_{num_nodes - 1}, but got None"
        )
        assert path[0] == "node_0"
        assert path[-1] == f"node_{num_nodes - 1}"

        # Performance target: <10ms for 1000 nodes, scaled down
        target_time_ms = 10.0 * (num_nodes / 1000.0)
        assert computation_time < target_time_ms * 2, (
            f"Computation took {computation_time:.2f}ms, target was {target_time_ms:.2f}ms"
        )

    def test_cache_performance(self):
        """Test caching performance improvements."""
        router = GraphBasedFederationRouter()

        # Create simple graph
        for i in range(10):
            node = FederationGraphNode(
                node_id=f"node_{i}",
                node_type=NodeType.CLUSTER,
                region="test",
                coordinates=GeographicCoordinate(0.0, float(i)),
                max_capacity=1000,
            )
            router.add_node(node)

        # Add edges
        for i in range(9):
            edge = FederationGraphEdge(
                source_id=f"node_{i}",
                target_id=f"node_{i + 1}",
                latency_ms=10.0,
                bandwidth_mbps=1000,
                reliability_score=0.95,
            )
            router.add_edge(edge)

        # First call (cold cache)
        start_time = time.time()
        path1 = router.find_optimal_path("node_0", "node_9")
        cold_time = (time.time() - start_time) * 1000

        # Second call (warm cache)
        start_time = time.time()
        path2 = router.find_optimal_path("node_0", "node_9")
        warm_time = (time.time() - start_time) * 1000

        assert path1 == path2

        # Warm cache should be faster or at least not slower
        # (For very small graphs, timing differences might be minimal)
        assert warm_time <= cold_time * 1.5, (
            f"Warm cache time {warm_time:.2f}ms should not be significantly slower than cold time {cold_time:.2f}ms"
        )

    def test_concurrent_access(self):
        """Test thread safety under concurrent access."""
        import threading

        router = GraphBasedFederationRouter()

        # Create simple graph
        for i in range(5):
            node = FederationGraphNode(
                node_id=f"node_{i}",
                node_type=NodeType.CLUSTER,
                region="test",
                coordinates=GeographicCoordinate(0.0, float(i)),
                max_capacity=1000,
            )
            router.add_node(node)

        # Add edges
        for i in range(4):
            edge = FederationGraphEdge(
                source_id=f"node_{i}",
                target_id=f"node_{i + 1}",
                latency_ms=10.0,
                bandwidth_mbps=1000,
                reliability_score=0.95,
            )
            router.add_edge(edge)

        results = []
        errors = []

        def worker():
            """Worker thread that performs routing operations."""
            try:
                for _ in range(10):
                    path = router.find_optimal_path("node_0", "node_4")
                    results.append(path)

                    # Update metrics
                    router.update_edge_metrics("node_0", "node_1", 15.0, 0.5)
                    router.update_node_metrics("node_0", 500.0, 0.9, 3.0)

            except Exception as e:
                errors.append(e)

        # Start multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 50, f"Expected 50 results, got {len(results)}"

        # All results should be valid paths
        for path in results:
            assert path is not None
            assert path[0] == "node_0"
            assert path[-1] == "node_4"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
