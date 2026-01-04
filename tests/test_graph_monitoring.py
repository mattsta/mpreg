"""
Comprehensive tests for federation graph monitoring system.

This module tests the real-time monitoring capabilities including:
- Metrics collection from various sources
- Intelligent path cache invalidation
- Dynamic graph optimization
- Performance monitoring under load

Test Coverage:
- MetricsCollector accuracy and performance
- PathCacheManager invalidation strategies
- GraphOptimizer suggestion generation
- Full integration testing
"""

import asyncio
import time

import pytest

from mpreg.fabric.federation_graph import (
    FederationGraph,
    FederationGraphEdge,
    FederationGraphNode,
    GeographicCoordinate,
    GraphBasedFederationRouter,
    NodeType,
)
from mpreg.fabric.federation_graph_monitor import (
    FederationGraphMonitor,
    GraphMetric,
    GraphMetricsCollector,
    GraphOptimizer,
    MetricType,
    OptimizationSuggestion,
    PathCacheManager,
)


class MockMetricsCollector:
    """Mock metrics collector for testing."""

    def __init__(self, collection_interval: float = 1.0):
        self.collection_interval = collection_interval
        self.collected_metrics: list[GraphMetric] = []

    def collect_node_metrics(self, node_id: str) -> list[GraphMetric]:
        """Collect mock node metrics."""
        metrics = [
            GraphMetric(
                metric_type=MetricType.HEALTH,
                source_node=node_id,
                target_node=None,
                value=0.8 + (hash(node_id) % 20) / 100,  # 0.8-1.0
                timestamp=time.time(),
                confidence=0.9,
            )
        ]
        self.collected_metrics.extend(metrics)
        return metrics

    def collect_edge_metrics(self, source: str, target: str) -> list[GraphMetric]:
        """Collect mock edge metrics."""
        metrics = [
            GraphMetric(
                metric_type=MetricType.LATENCY,
                source_node=source,
                target_node=target,
                value=10.0 + (hash(source + target) % 50),  # 10-60ms
                timestamp=time.time(),
                confidence=0.95,
            ),
            GraphMetric(
                metric_type=MetricType.UTILIZATION,
                source_node=source,
                target_node=target,
                value=(hash(source + target) % 80) / 100,  # 0.0-0.8
                timestamp=time.time(),
                confidence=0.9,
            ),
        ]
        self.collected_metrics.extend(metrics)
        return metrics

    def get_collection_interval(self) -> float:
        """Get collection interval."""
        return self.collection_interval


@pytest.fixture
def sample_graph():
    """Create a sample federation graph for testing."""
    graph = FederationGraph(cache_ttl_seconds=30.0)

    # Add nodes
    nodes = [
        FederationGraphNode(
            node_id="node1",
            node_type=NodeType.CLUSTER,
            region="us-east",
            coordinates=GeographicCoordinate(40.7128, -74.0060),  # New York
            max_capacity=1000,
            current_load=200.0,
            health_score=0.9,
        ),
        FederationGraphNode(
            node_id="node2",
            node_type=NodeType.CLUSTER,
            region="us-west",
            coordinates=GeographicCoordinate(37.7749, -122.4194),  # San Francisco
            max_capacity=1000,
            current_load=300.0,
            health_score=0.8,
        ),
        FederationGraphNode(
            node_id="node3",
            node_type=NodeType.REGIONAL_HUB,
            region="eu-central",
            coordinates=GeographicCoordinate(52.5200, 13.4050),  # Berlin
            max_capacity=2000,
            current_load=400.0,
            health_score=0.95,
        ),
    ]

    for node in nodes:
        graph.add_node(node)

    # Add edges
    edges = [
        FederationGraphEdge(
            source_id="node1",
            target_id="node2",
            latency_ms=50.0,
            bandwidth_mbps=1000,
            reliability_score=0.9,
            current_utilization=0.3,
        ),
        FederationGraphEdge(
            source_id="node1",
            target_id="node3",
            latency_ms=100.0,
            bandwidth_mbps=500,
            reliability_score=0.8,
            current_utilization=0.2,
        ),
        FederationGraphEdge(
            source_id="node2",
            target_id="node3",
            latency_ms=120.0,
            bandwidth_mbps=800,
            reliability_score=0.85,
            current_utilization=0.4,
        ),
    ]

    for edge in edges:
        graph.add_edge(edge)

    return graph


class TestGraphMetricsCollector:
    """Test suite for GraphMetricsCollector."""

    @pytest.mark.asyncio
    async def test_collector_initialization(self, sample_graph):
        """Test metrics collector initialization."""
        collector = GraphMetricsCollector(sample_graph, collection_interval=1.0)

        assert collector.graph == sample_graph
        assert collector.collection_interval == 1.0
        assert not collector.is_collecting
        assert len(collector.collectors) == 0
        assert collector.metrics_collected == 0

    @pytest.mark.asyncio
    async def test_add_remove_collectors(self, sample_graph):
        """Test adding and removing metrics collectors."""
        collector = GraphMetricsCollector(sample_graph)
        mock_collector = MockMetricsCollector()

        # Add collector
        collector.add_collector(mock_collector)
        assert len(collector.collectors) == 1
        assert mock_collector in collector.collectors

        # Remove collector
        success = collector.remove_collector(mock_collector)
        assert success
        assert len(collector.collectors) == 0
        assert mock_collector not in collector.collectors

        # Remove non-existent collector
        success = collector.remove_collector(mock_collector)
        assert not success

    @pytest.mark.asyncio
    async def test_metrics_collection(self, sample_graph):
        """Test metrics collection from mock sources."""
        collector = GraphMetricsCollector(sample_graph, collection_interval=0.1)
        mock_collector = MockMetricsCollector()

        collector.add_collector(mock_collector)

        # Start collection
        await collector.start_collection()

        # Wait for some collections
        await asyncio.sleep(0.3)

        # Stop collection
        await collector.stop_collection()

        # Check that metrics were collected
        assert collector.metrics_collected > 0
        assert len(mock_collector.collected_metrics) > 0

        # Check statistics
        stats = collector.get_statistics()
        assert not stats.collection_status.is_collecting
        assert stats.metrics_statistics.total_collected > 0
        assert stats.metrics_statistics.success_rate >= 0

    @pytest.mark.asyncio
    async def test_metric_storage_and_retrieval(self, sample_graph):
        """Test metric storage and retrieval."""
        collector = GraphMetricsCollector(sample_graph)

        # Create test metric
        metric = GraphMetric(
            metric_type=MetricType.LATENCY,
            source_node="node1",
            target_node="node2",
            value=25.0,
            timestamp=time.time(),
            confidence=0.9,
        )

        # Store metric
        collector._store_metric(metric)

        # Retrieve metric
        retrieved = collector.get_recent_metrics("node1", "node2", MetricType.LATENCY)
        assert len(retrieved) == 1
        assert retrieved[0].value == 25.0
        assert retrieved[0].metric_type == MetricType.LATENCY

    @pytest.mark.asyncio
    async def test_edge_metric_application(self, sample_graph):
        """Test application of edge metrics to graph."""
        collector = GraphMetricsCollector(sample_graph)

        # Get original edge
        original_edge = sample_graph.get_edge("node1", "node2")
        assert original_edge is not None
        original_latency = original_edge.latency_ms

        # Create and apply latency metric
        latency_metric = GraphMetric(
            metric_type=MetricType.LATENCY,
            source_node="node1",
            target_node="node2",
            value=75.0,
            timestamp=time.time(),
            confidence=0.9,
        )

        collector._apply_edge_metric(latency_metric)

        # Check that edge was updated
        updated_edge = sample_graph.get_edge("node1", "node2")
        assert updated_edge is not None

        # First update should use exact value
        if updated_edge.sample_count == 1:
            assert updated_edge.latency_ms == 75.0
        else:
            # Subsequent updates use exponential moving average
            assert updated_edge.latency_ms != original_latency

    @pytest.mark.asyncio
    async def test_node_metric_application(self, sample_graph):
        """Test application of node metrics to graph."""
        collector = GraphMetricsCollector(sample_graph)

        # Get original node
        original_node = sample_graph.get_node("node1")
        assert original_node is not None
        original_health = original_node.health_score

        # Create and apply health metric
        health_metric = GraphMetric(
            metric_type=MetricType.HEALTH,
            source_node="node1",
            target_node=None,
            value=0.75,
            timestamp=time.time(),
            confidence=0.9,
        )

        collector._apply_node_metric(health_metric)

        # Check that node was updated
        updated_node = sample_graph.get_node("node1")
        assert updated_node is not None
        assert updated_node.health_score == 0.75

    @pytest.mark.asyncio
    async def test_concurrent_collection(self, sample_graph):
        """Test concurrent metrics collection."""
        collector = GraphMetricsCollector(sample_graph, collection_interval=0.1)

        # Add multiple mock collectors
        mock_collectors = [MockMetricsCollector() for _ in range(3)]
        for mock_collector in mock_collectors:
            collector.add_collector(mock_collector)

        # Start collection
        await collector.start_collection()

        # Wait for collections
        await asyncio.sleep(0.3)

        # Stop collection
        await collector.stop_collection()

        # Check that all collectors were used
        total_collected = sum(len(mc.collected_metrics) for mc in mock_collectors)
        assert total_collected > 0
        assert collector.metrics_collected > 0


class TestPathCacheManager:
    """Test suite for PathCacheManager."""

    def test_cache_manager_initialization(self, sample_graph):
        """Test cache manager initialization."""
        manager = PathCacheManager(sample_graph, "conservative")

        assert manager.graph == sample_graph
        assert manager.invalidation_strategy == "conservative"
        assert manager.invalidations_triggered == 0

    def test_edge_change_detection(self, sample_graph):
        """Test edge change detection for cache invalidation."""
        manager = PathCacheManager(sample_graph, "conservative")

        # Create old and new edges
        old_edge = FederationGraphEdge(
            source_id="node1",
            target_id="node2",
            latency_ms=50.0,
            bandwidth_mbps=1000,
            reliability_score=0.9,
            current_utilization=0.3,
        )

        new_edge = FederationGraphEdge(
            source_id="node1",
            target_id="node2",
            latency_ms=80.0,  # 60% increase
            bandwidth_mbps=1000,
            reliability_score=0.9,
            current_utilization=0.3,
        )

        # Test latency change detection
        should_invalidate, reason = manager._should_invalidate_for_edge(
            old_edge, new_edge
        )
        assert should_invalidate
        assert "latency_change" in reason

        # Test small change (should not invalidate)
        new_edge.latency_ms = 52.0  # 4% increase
        should_invalidate, reason = manager._should_invalidate_for_edge(
            old_edge, new_edge
        )
        assert not should_invalidate

    def test_node_change_detection(self, sample_graph):
        """Test node change detection for cache invalidation."""
        manager = PathCacheManager(sample_graph, "conservative")

        # Create old and new nodes
        old_node = sample_graph.get_node("node1")
        assert old_node is not None

        new_node = FederationGraphNode(
            node_id="node1",
            node_type=NodeType.CLUSTER,
            region="us-east",
            coordinates=GeographicCoordinate(40.7128, -74.0060),
            max_capacity=1000,
            current_load=200.0,
            health_score=0.6,  # Significant health decrease
        )

        # Test health change detection
        should_invalidate, reason = manager._should_invalidate_for_node(
            old_node, new_node
        )
        assert should_invalidate
        assert "health_change" in reason

    def test_cache_invalidation_strategies(self, sample_graph):
        """Test different cache invalidation strategies."""
        # Test aggressive strategy
        manager_aggressive = PathCacheManager(sample_graph, "aggressive")

        # Add some cache entries
        sample_graph.cache_path("node1", "node2", ["node1", "node2"])
        sample_graph.cache_path("node1", "node3", ["node1", "node3"])

        initial_cache_size = len(sample_graph.path_cache)
        assert initial_cache_size > 0

        # Create edge change
        old_edge = sample_graph.get_edge("node1", "node2")
        assert old_edge is not None

        new_edge = FederationGraphEdge(
            source_id="node1",
            target_id="node2",
            latency_ms=old_edge.latency_ms * 2,  # Double latency
            bandwidth_mbps=old_edge.bandwidth_mbps,
            reliability_score=old_edge.reliability_score,
            current_utilization=old_edge.current_utilization,
        )

        # Monitor edge update (aggressive should clear all cache)
        manager_aggressive.monitor_edge_update("node1", "node2", old_edge, new_edge)

        assert len(sample_graph.path_cache) == 0
        assert manager_aggressive.invalidations_triggered == 1

    def test_cache_manager_statistics(self, sample_graph):
        """Test cache manager statistics."""
        manager = PathCacheManager(sample_graph, "adaptive")

        # Trigger some invalidations
        old_edge = sample_graph.get_edge("node1", "node2")
        assert old_edge is not None

        new_edge = FederationGraphEdge(
            source_id="node1",
            target_id="node2",
            latency_ms=old_edge.latency_ms * 2,
            bandwidth_mbps=old_edge.bandwidth_mbps,
            reliability_score=old_edge.reliability_score,
            current_utilization=old_edge.current_utilization,
        )

        manager.monitor_edge_update("node1", "node2", old_edge, new_edge)

        # Check statistics
        stats = manager.get_statistics()
        assert stats.invalidation_strategy == "adaptive"
        assert stats.invalidations_triggered > 0
        assert hasattr(stats, "invalidations_by_reason")
        assert hasattr(stats, "thresholds")


class TestGraphOptimizer:
    """Test suite for GraphOptimizer."""

    @pytest.mark.asyncio
    async def test_optimizer_initialization(self, sample_graph):
        """Test optimizer initialization."""
        optimizer = GraphOptimizer(sample_graph, optimization_interval=10.0)

        assert optimizer.graph == sample_graph
        assert optimizer.optimization_interval == 10.0
        assert not optimizer.is_optimizing
        assert optimizer.optimizations_performed == 0

    @pytest.mark.asyncio
    async def test_optimization_lifecycle(self, sample_graph):
        """Test optimization start/stop lifecycle."""
        optimizer = GraphOptimizer(sample_graph, optimization_interval=0.1)

        # Start optimization
        await optimizer.start_optimization()
        assert optimizer.is_optimizing
        assert optimizer.optimization_task is not None

        # Wait for some optimization cycles
        await asyncio.sleep(0.3)

        # Stop optimization
        await optimizer.stop_optimization()
        assert not optimizer.is_optimizing
        assert optimizer.optimization_task is None

    def test_connectivity_analysis(self, sample_graph):
        """Test connectivity analysis for optimization suggestions."""
        optimizer = GraphOptimizer(sample_graph)

        # Add an isolated node
        isolated_node = FederationGraphNode(
            node_id="isolated",
            node_type=NodeType.CLUSTER,
            region="us-central",
            coordinates=GeographicCoordinate(39.7392, -104.9903),  # Denver
            max_capacity=1000,
            current_load=100.0,
            health_score=0.9,
        )
        sample_graph.add_node(isolated_node)

        # Generate suggestions
        suggestions = optimizer._analyze_connectivity()

        # Should suggest adding connections for isolated node
        isolated_suggestions = [
            s for s in suggestions if "isolated" in s.affected_nodes
        ]
        assert len(isolated_suggestions) > 0
        assert any(
            "redundant_connection" in s.optimization_type for s in isolated_suggestions
        )

    def test_bottleneck_analysis(self, sample_graph):
        """Test bottleneck analysis for optimization suggestions."""
        optimizer = GraphOptimizer(sample_graph)

        # Create high-utilization edge
        high_util_edge = FederationGraphEdge(
            source_id="node1",
            target_id="bottleneck",
            latency_ms=20.0,
            bandwidth_mbps=1000,
            reliability_score=0.9,
            current_utilization=0.9,  # Very high utilization
        )

        # Add bottleneck node
        bottleneck_node = FederationGraphNode(
            node_id="bottleneck",
            node_type=NodeType.CLUSTER,
            region="us-central",
            coordinates=GeographicCoordinate(39.7392, -104.9903),
            max_capacity=100,  # Low capacity
            current_load=95.0,  # High load
            health_score=0.8,
        )

        sample_graph.add_node(bottleneck_node)
        sample_graph.add_edge(high_util_edge)

        # Generate suggestions
        suggestions = optimizer._analyze_bottlenecks()

        # Should suggest load balancing for high-load node
        load_suggestions = [
            s for s in suggestions if "load_balancing" in s.optimization_type
        ]
        assert len(load_suggestions) > 0
        assert any("bottleneck" in s.affected_nodes for s in load_suggestions)

    def test_redundancy_analysis(self, sample_graph):
        """Test redundancy analysis for optimization suggestions."""
        optimizer = GraphOptimizer(sample_graph)

        # Current graph has reasonable connectivity, so add a bridge node
        bridge_node = FederationGraphNode(
            node_id="bridge",
            node_type=NodeType.REGIONAL_HUB,
            region="asia-pacific",
            coordinates=GeographicCoordinate(35.6762, 139.6503),  # Tokyo
            max_capacity=2000,
            current_load=500.0,
            health_score=0.9,
        )

        sample_graph.add_node(bridge_node)

        # Connect bridge to multiple nodes (making it potentially critical)
        bridge_edges = [
            FederationGraphEdge(
                source_id="bridge",
                target_id="node1",
                latency_ms=150.0,
                bandwidth_mbps=800,
                reliability_score=0.8,
                current_utilization=0.3,
            ),
            FederationGraphEdge(
                source_id="bridge",
                target_id="node2",
                latency_ms=140.0,
                bandwidth_mbps=800,
                reliability_score=0.8,
                current_utilization=0.3,
            ),
        ]

        for edge in bridge_edges:
            sample_graph.add_edge(edge)

        # Generate suggestions
        suggestions = optimizer._analyze_redundancy()

        # May or may not suggest redundancy depending on graph structure
        # This tests that the analysis runs without errors
        assert isinstance(suggestions, list)

    def test_optimization_suggestions_prioritization(self, sample_graph):
        """Test optimization suggestion prioritization."""
        optimizer = GraphOptimizer(sample_graph)

        # Create suggestions with different priorities
        suggestions = [
            OptimizationSuggestion(
                optimization_type="low_priority",
                priority=3,
                description="Low priority optimization",
                affected_nodes=["node1"],
                estimated_improvement=0.1,
                implementation_cost=0.1,
            ),
            OptimizationSuggestion(
                optimization_type="high_priority",
                priority=1,
                description="High priority optimization",
                affected_nodes=["node2"],
                estimated_improvement=0.5,
                implementation_cost=0.2,
            ),
            OptimizationSuggestion(
                optimization_type="medium_priority",
                priority=2,
                description="Medium priority optimization",
                affected_nodes=["node3"],
                estimated_improvement=0.3,
                implementation_cost=0.15,
            ),
        ]

        # Sort suggestions
        suggestions.sort()

        # Check that high priority comes first
        assert suggestions[0].priority == 1
        assert suggestions[1].priority == 2
        assert suggestions[2].priority == 3

    def test_optimizer_statistics(self, sample_graph):
        """Test optimizer statistics collection."""
        optimizer = GraphOptimizer(sample_graph)

        # Generate some suggestions
        suggestions = optimizer._generate_optimization_suggestions()

        # Get statistics
        stats = optimizer.get_statistics()

        assert hasattr(stats, "optimization_status")
        assert hasattr(stats, "optimization_history")
        assert hasattr(stats, "current_suggestions")
        assert hasattr(stats, "suggestions_by_priority")

        assert not stats.optimization_status.is_optimizing
        assert stats.optimization_history.total_optimizations == 0
        assert stats.current_suggestions == len(suggestions)


class TestFederationGraphMonitor:
    """Test suite for FederationGraphMonitor (integration tests)."""

    @pytest.mark.asyncio
    async def test_monitor_initialization(self, sample_graph):
        """Test monitor initialization."""
        monitor = FederationGraphMonitor(
            sample_graph, collection_interval=1.0, optimization_interval=10.0
        )

        assert monitor.graph == sample_graph
        assert monitor.metrics_collector is not None
        assert monitor.cache_manager is not None
        assert monitor.optimizer is not None
        assert not monitor.is_monitoring

    @pytest.mark.asyncio
    async def test_monitor_lifecycle(self, sample_graph):
        """Test monitor start/stop lifecycle."""
        monitor = FederationGraphMonitor(
            sample_graph, collection_interval=0.1, optimization_interval=0.5
        )

        # Add mock collector
        mock_collector = MockMetricsCollector()
        monitor.add_metrics_collector(mock_collector)

        # Start monitoring
        await monitor.start_monitoring()
        assert monitor.is_monitoring
        assert monitor.start_time > 0

        # Wait for some monitoring cycles
        await asyncio.sleep(0.3)

        # Stop monitoring
        await monitor.stop_monitoring()
        assert not monitor.is_monitoring

    @pytest.mark.asyncio
    async def test_comprehensive_status(self, sample_graph):
        """Test comprehensive status reporting."""
        monitor = FederationGraphMonitor(sample_graph)

        # Add mock collector
        mock_collector = MockMetricsCollector()
        monitor.add_metrics_collector(mock_collector)

        # Get status
        status = monitor.get_comprehensive_status()

        # Check status structure
        assert hasattr(status, "monitoring_status")
        assert hasattr(status, "metrics_collector")
        assert hasattr(status, "cache_manager")
        assert hasattr(status, "optimizer")
        assert hasattr(status, "graph_statistics")

        # Check monitoring status
        assert not status.monitoring_status.is_monitoring
        assert status.monitoring_status.uptime_seconds >= 0

        # Check component statistics
        assert hasattr(status.metrics_collector, "collection_status")
        assert hasattr(status.cache_manager, "invalidation_strategy")
        assert hasattr(status.optimizer, "optimization_status")
        assert hasattr(status.graph_statistics, "total_nodes")

    @pytest.mark.asyncio
    async def test_end_to_end_monitoring(self, sample_graph):
        """Test end-to-end monitoring with real graph operations."""
        monitor = FederationGraphMonitor(
            sample_graph, collection_interval=0.1, optimization_interval=0.3
        )

        # Add mock collector
        mock_collector = MockMetricsCollector()
        monitor.add_metrics_collector(mock_collector)

        # Start monitoring
        await monitor.start_monitoring()

        # Perform some graph operations
        router = GraphBasedFederationRouter()
        router.graph = sample_graph

        # Add some routing activity
        for _ in range(5):
            path = router.find_optimal_path("node1", "node3")
            if path:
                # Simulate metric updates
                monitor.graph.update_edge_metrics("node1", "node2", 45.0, 0.4)
                monitor.graph.update_node_metrics("node1", 250.0, 0.85, 2.0)

        # Wait for monitoring to process
        await asyncio.sleep(0.5)

        # Stop monitoring
        await monitor.stop_monitoring()

        # Check that monitoring detected activity
        status = monitor.get_comprehensive_status()

        # Should have collected some metrics
        assert status.metrics_collector.metrics_statistics.total_collected > 0

        # Should have some graph statistics
        assert status.graph_statistics.total_nodes == 3
        assert status.graph_statistics.total_edges == 3


class TestPerformanceAndScalability:
    """Test suite for performance and scalability."""

    @pytest.mark.asyncio
    async def test_large_graph_monitoring(self):
        """Test monitoring performance with large graphs."""
        # Create large graph
        graph = FederationGraph(cache_ttl_seconds=30.0)

        # Add many nodes
        for i in range(100):
            node = FederationGraphNode(
                node_id=f"node{i}",
                node_type=NodeType.CLUSTER,
                region=f"region{i % 10}",
                coordinates=GeographicCoordinate(
                    latitude=40.0 + (i % 10) * 0.1, longitude=-74.0 + (i % 10) * 0.1
                ),
                max_capacity=1000,
                current_load=float(i % 500),
                health_score=0.8 + (i % 20) / 100,
            )
            graph.add_node(node)

        # Add edges (create a connected graph)
        for i in range(99):
            edge = FederationGraphEdge(
                source_id=f"node{i}",
                target_id=f"node{i + 1}",
                latency_ms=10.0 + (i % 50),
                bandwidth_mbps=1000,
                reliability_score=0.9,
                current_utilization=(i % 80) / 100,
            )
            graph.add_edge(edge)

        # Test monitoring with large graph
        monitor = FederationGraphMonitor(
            graph, collection_interval=0.1, optimization_interval=0.5
        )

        # Add mock collector
        mock_collector = MockMetricsCollector()
        monitor.add_metrics_collector(mock_collector)

        # Measure monitoring performance
        start_time = time.time()

        await monitor.start_monitoring()
        await asyncio.sleep(0.3)  # Let it run for a bit
        await monitor.stop_monitoring()

        elapsed = time.time() - start_time

        # Should handle large graph efficiently
        assert elapsed < 2.0  # Should complete within 2 seconds

        # Check status
        status = monitor.get_comprehensive_status()
        assert status.graph_statistics.total_nodes == 100
        assert status.graph_statistics.total_edges == 99

    @pytest.mark.asyncio
    async def test_concurrent_monitoring_operations(self, sample_graph):
        """Test concurrent monitoring operations."""
        monitor = FederationGraphMonitor(
            sample_graph, collection_interval=0.1, optimization_interval=0.2
        )

        # Add multiple mock collectors
        mock_collectors = [MockMetricsCollector() for _ in range(3)]
        for collector in mock_collectors:
            monitor.add_metrics_collector(collector)

        # Start monitoring
        await monitor.start_monitoring()

        # Simulate concurrent graph operations
        async def simulate_routing():
            router = GraphBasedFederationRouter()
            router.graph = sample_graph

            for _ in range(10):
                path = router.find_optimal_path("node1", "node3")
                await asyncio.sleep(0.01)

        async def simulate_updates():
            for i in range(20):
                sample_graph.update_edge_metrics(
                    "node1", "node2", 50.0 + i, 0.3 + i / 100
                )
                await asyncio.sleep(0.01)

        # Run concurrent operations
        await asyncio.gather(
            simulate_routing(),
            simulate_updates(),
            asyncio.sleep(0.5),  # Let monitoring run
        )

        # Stop monitoring
        await monitor.stop_monitoring()

        # Check that monitoring handled concurrent operations
        status = monitor.get_comprehensive_status()
        assert status.metrics_collector.metrics_statistics.total_collected > 0
        assert status.metrics_collector.metrics_statistics.success_rate > 0.5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
