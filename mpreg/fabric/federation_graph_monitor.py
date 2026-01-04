"""
Real-Time Graph Updates for the MPREG Fabric.

This module implements real-time monitoring and optimization of the fabric graph.
It provides intelligent metrics collection, cache management, and dynamic graph optimization
to maintain optimal routing performance as network conditions change.

Key Features:
- Real-time edge weight updates from network measurements
- Intelligent path cache invalidation to maintain routing accuracy
- Dynamic graph restructuring for optimal performance
- Proactive bottleneck detection and mitigation

This is Phase 1.2 of the Planet-Scale Fabric Roadmap.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from threading import RLock
from typing import Protocol

from mpreg.fabric.federation_graph import (
    FederationGraph,
    FederationGraphEdge,
    FederationGraphNode,
    NodeId,
)

from ..core.statistics import (
    GraphCollectionStatus,
    GraphMetricsCollectorStatistics,
    GraphMonitoringStatistics,
    GraphOptimizerStatistics,
    InvalidationThresholds,
    MetricsStatistics,
    MonitoringStatus,
    OptimizationHistory,
    OptimizationStatus,
    PathCacheManagerStatistics,
)


class MetricType(Enum):
    """Types of metrics collected for graph optimization."""

    LATENCY = "latency"
    UTILIZATION = "utilization"
    PACKET_LOSS = "packet_loss"
    JITTER = "jitter"
    BANDWIDTH = "bandwidth"
    HEALTH = "health"


@dataclass(slots=True)
class GraphMetric:
    """A single metric measurement for graph optimization."""

    metric_type: MetricType
    source_node: NodeId
    target_node: NodeId | None  # None for node metrics
    value: float
    timestamp: float
    confidence: float = 1.0  # 0.0 to 1.0

    def is_recent(self, max_age_seconds: float = 30.0) -> bool:
        """Check if metric is recent enough to be useful."""
        return time.time() - self.timestamp <= max_age_seconds

    def is_reliable(self, min_confidence: float = 0.7) -> bool:
        """Check if metric is reliable enough for routing decisions."""
        return self.confidence >= min_confidence


@dataclass(slots=True)
class OptimizationSuggestion:
    """A suggestion for graph optimization."""

    optimization_type: str
    priority: int  # 1=high, 2=medium, 3=low
    description: str
    affected_nodes: list[NodeId]
    estimated_improvement: float
    implementation_cost: float

    def __lt__(self, other):
        """For priority queue ordering."""
        return self.priority < other.priority


class MetricsCollectorProtocol(Protocol):
    """Protocol for metrics collection sources."""

    def collect_node_metrics(self, node_id: NodeId) -> list[GraphMetric]:
        """Collect metrics for a specific node."""
        ...

    def collect_edge_metrics(self, source: NodeId, target: NodeId) -> list[GraphMetric]:
        """Collect metrics for a specific edge."""
        ...

    def get_collection_interval(self) -> float:
        """Get the collection interval in seconds."""
        ...


@dataclass(slots=True)
class GraphMetricsCollector:
    """
    Collects real-time metrics from the federation graph.

    Aggregates metrics from multiple sources and provides intelligent
    filtering and smoothing to maintain routing accuracy while avoiding
    excessive sensitivity to transient network conditions.
    """

    graph: FederationGraph
    collection_interval: float = 5.0
    collectors: list[MetricsCollectorProtocol] = field(default_factory=list)
    recent_metrics: dict[tuple[NodeId, NodeId | None], deque[GraphMetric]] = field(
        default_factory=lambda: defaultdict(lambda: deque(maxlen=100))
    )
    is_collecting: bool = False
    collection_task: asyncio.Task | None = None
    last_collection_time: float = 0.0
    metrics_collected: int = 0
    collection_errors: int = 0
    update_successes: int = 0
    _lock: RLock = field(default_factory=RLock)

    def add_collector(self, collector: MetricsCollectorProtocol) -> None:
        """Add a metrics collector source."""
        with self._lock:
            self.collectors.append(collector)

    def remove_collector(self, collector: MetricsCollectorProtocol) -> bool:
        """Remove a metrics collector source."""
        with self._lock:
            try:
                self.collectors.remove(collector)
                return True
            except ValueError:
                return False

    async def start_collection(self) -> None:
        """Start automated metrics collection."""
        if self.is_collecting:
            return

        self.is_collecting = True
        self.collection_task = asyncio.create_task(self._collection_loop())

    async def stop_collection(self) -> None:
        """Stop automated metrics collection."""
        self.is_collecting = False
        if self.collection_task:
            self.collection_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.collection_task
            self.collection_task = None

    async def _collection_loop(self) -> None:
        """Main collection loop."""
        while self.is_collecting:
            try:
                await self._collect_all_metrics()
                await asyncio.sleep(self.collection_interval)
            except Exception as e:
                self.collection_errors += 1
                # Log error but continue collecting
                await asyncio.sleep(self.collection_interval)

    async def _collect_all_metrics(self) -> None:
        """Collect metrics from all sources."""
        collection_tasks = []

        # Collect node metrics
        for node_id in self.graph.nodes:
            for collector in self.collectors:
                task = asyncio.create_task(
                    self._collect_node_metrics(collector, node_id)
                )
                collection_tasks.append(task)

        # Collect edge metrics
        for source_id, adjacency in self.graph.adjacency.items():
            for target_id in adjacency:
                for collector in self.collectors:
                    task = asyncio.create_task(
                        self._collect_edge_metrics(collector, source_id, target_id)
                    )
                    collection_tasks.append(task)

        # Wait for all collections to complete
        if collection_tasks:
            await asyncio.gather(*collection_tasks, return_exceptions=True)

        self.last_collection_time = time.time()

    async def _collect_node_metrics(
        self, collector: MetricsCollectorProtocol, node_id: NodeId
    ) -> None:
        """Collect metrics for a specific node."""
        try:
            metrics = collector.collect_node_metrics(node_id)
            for metric in metrics:
                if metric.is_recent() and metric.is_reliable():
                    self._store_metric(metric)
                    self._apply_node_metric(metric)
        except Exception:
            self.collection_errors += 1

    async def _collect_edge_metrics(
        self, collector: MetricsCollectorProtocol, source: NodeId, target: NodeId
    ) -> None:
        """Collect metrics for a specific edge."""
        try:
            metrics = collector.collect_edge_metrics(source, target)
            for metric in metrics:
                if metric.is_recent() and metric.is_reliable():
                    self._store_metric(metric)
                    self._apply_edge_metric(metric)
        except Exception:
            self.collection_errors += 1

    def _store_metric(self, metric: GraphMetric) -> None:
        """Store metric in recent metrics buffer."""
        with self._lock:
            key = (metric.source_node, metric.target_node)
            self.recent_metrics[key].append(metric)
            self.metrics_collected += 1

    def _apply_node_metric(self, metric: GraphMetric) -> None:
        """Apply node metric to graph."""
        if metric.metric_type == MetricType.HEALTH:
            success = self.graph.update_node_metrics(
                metric.source_node,
                current_load=0.0,  # Would need separate load metric
                health_score=metric.value,
                processing_latency_ms=0.0,  # Would need separate latency metric
            )
            if success:
                self.update_successes += 1

    def _apply_edge_metric(self, metric: GraphMetric) -> None:
        """Apply edge metric to graph."""
        if not metric.target_node:
            return

        # Get current edge to preserve other metrics
        edge = self.graph.get_edge(metric.source_node, metric.target_node)
        if not edge:
            return

        # Apply the specific metric type
        if metric.metric_type == MetricType.LATENCY:
            success = self.graph.update_edge_metrics(
                metric.source_node,
                metric.target_node,
                latency_ms=metric.value,
                utilization=edge.current_utilization,
                packet_loss=edge.packet_loss_rate,
                jitter_ms=edge.jitter_ms,
            )
        elif metric.metric_type == MetricType.UTILIZATION:
            success = self.graph.update_edge_metrics(
                metric.source_node,
                metric.target_node,
                latency_ms=edge.latency_ms,
                utilization=metric.value,
                packet_loss=edge.packet_loss_rate,
                jitter_ms=edge.jitter_ms,
            )
        elif metric.metric_type == MetricType.PACKET_LOSS:
            success = self.graph.update_edge_metrics(
                metric.source_node,
                metric.target_node,
                latency_ms=edge.latency_ms,
                utilization=edge.current_utilization,
                packet_loss=metric.value,
                jitter_ms=edge.jitter_ms,
            )
        elif metric.metric_type == MetricType.JITTER:
            success = self.graph.update_edge_metrics(
                metric.source_node,
                metric.target_node,
                latency_ms=edge.latency_ms,
                utilization=edge.current_utilization,
                packet_loss=edge.packet_loss_rate,
                jitter_ms=metric.value,
            )
        else:
            success = False

        if success:
            self.update_successes += 1

    def get_recent_metrics(
        self,
        node_id: NodeId,
        target_node: NodeId | None = None,
        metric_type: MetricType | None = None,
        max_age_seconds: float = 30.0,
    ) -> list[GraphMetric]:
        """Get recent metrics for a node or edge."""
        with self._lock:
            key = (node_id, target_node)
            if key not in self.recent_metrics:
                return []

            metrics = []
            for metric in self.recent_metrics[key]:
                if not metric.is_recent(max_age_seconds):
                    continue
                if metric_type and metric.metric_type != metric_type:
                    continue
                metrics.append(metric)

            return metrics

    def get_statistics(self) -> GraphMetricsCollectorStatistics:
        """Get comprehensive collector statistics."""
        with self._lock:
            total_metrics = sum(len(queue) for queue in self.recent_metrics.values())
            success_rate = self.update_successes / max(1, self.metrics_collected)
            error_rate = self.collection_errors / max(
                1, self.metrics_collected + self.collection_errors
            )

            collection_status = GraphCollectionStatus(
                is_collecting=self.is_collecting,
                last_collection=self.last_collection_time,
                collection_interval=self.collection_interval,
                active_collectors=len(self.collectors),
            )

            metrics_statistics = MetricsStatistics(
                total_collected=self.metrics_collected,
                update_successes=self.update_successes,
                collection_errors=self.collection_errors,
                success_rate=success_rate,
                error_rate=error_rate,
                stored_metrics=total_metrics,
            )

            recent_metrics_by_type = {
                metric_type.value: len(
                    [
                        m
                        for queue in self.recent_metrics.values()
                        for m in queue
                        if m.metric_type == metric_type
                    ]
                )
                for metric_type in MetricType
            }

            return GraphMetricsCollectorStatistics(
                collection_status=collection_status,
                metrics_statistics=metrics_statistics,
                recent_metrics_by_type=recent_metrics_by_type,
            )


@dataclass(slots=True)
class PathCacheManager:
    """
    Manages intelligent path cache invalidation.

    Monitors graph changes and selectively invalidates cached paths
    to maintain routing accuracy while preserving cache performance.
    """

    graph: FederationGraph
    invalidation_strategy: str = "conservative"
    invalidations_triggered: int = 0
    invalidations_by_reason: dict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    cache_performance_before: dict[str, float] = field(default_factory=dict)
    cache_performance_after: dict[str, float] = field(default_factory=dict)
    latency_change_threshold: float = 0.2  # 20% change
    utilization_change_threshold: float = 0.1  # 10% change
    health_change_threshold: float = 0.1  # 10% change
    _lock: RLock = field(default_factory=RLock)

    def monitor_edge_update(
        self,
        source: NodeId,
        target: NodeId,
        old_edge: FederationGraphEdge,
        new_edge: FederationGraphEdge,
    ) -> None:
        """Monitor edge updates and invalidate cache if needed."""
        with self._lock:
            should_invalidate, reason = self._should_invalidate_for_edge(
                old_edge, new_edge
            )

            if should_invalidate:
                self._invalidate_paths_using_edge(source, target, reason)

    def monitor_node_update(
        self,
        node_id: NodeId,
        old_node: FederationGraphNode,
        new_node: FederationGraphNode,
    ) -> None:
        """Monitor node updates and invalidate cache if needed."""
        with self._lock:
            should_invalidate, reason = self._should_invalidate_for_node(
                old_node, new_node
            )

            if should_invalidate:
                self._invalidate_paths_using_node(node_id, reason)

    def _should_invalidate_for_edge(
        self, old_edge: FederationGraphEdge, new_edge: FederationGraphEdge
    ) -> tuple[bool, str]:
        """Determine if edge change should trigger cache invalidation."""
        # Check latency change
        latency_change = abs(new_edge.latency_ms - old_edge.latency_ms) / max(
            1, old_edge.latency_ms
        )
        if latency_change > self.latency_change_threshold:
            return True, f"latency_change_{latency_change:.2f}"

        # Check utilization change
        util_change = abs(new_edge.current_utilization - old_edge.current_utilization)
        if util_change > self.utilization_change_threshold:
            return True, f"utilization_change_{util_change:.2f}"

        # Check reliability change
        reliability_change = abs(
            new_edge.reliability_score - old_edge.reliability_score
        )
        if reliability_change > 0.1:  # 10% reliability change
            return True, f"reliability_change_{reliability_change:.2f}"

        # Check if edge became unusable
        if old_edge.is_usable() and not new_edge.is_usable():
            return True, "edge_became_unusable"

        # Check if edge became usable
        if not old_edge.is_usable() and new_edge.is_usable():
            return True, "edge_became_usable"

        return False, ""

    def _should_invalidate_for_node(
        self, old_node: FederationGraphNode, new_node: FederationGraphNode
    ) -> tuple[bool, str]:
        """Determine if node change should trigger cache invalidation."""
        # Check health change
        health_change = abs(new_node.health_score - old_node.health_score)
        if health_change > self.health_change_threshold:
            return True, f"health_change_{health_change:.2f}"

        # Check if node became unhealthy
        if old_node.is_healthy() and not new_node.is_healthy():
            return True, "node_became_unhealthy"

        # Check if node became healthy
        if not old_node.is_healthy() and new_node.is_healthy():
            return True, "node_became_healthy"

        # Check significant load change
        load_change = abs(new_node.current_load - old_node.current_load) / max(
            1, old_node.max_capacity
        )
        if load_change > 0.2:  # 20% capacity change
            return True, f"load_change_{load_change:.2f}"

        return False, ""

    def _invalidate_paths_using_edge(
        self, source: NodeId, target: NodeId, reason: str
    ) -> None:
        """Invalidate paths that use a specific edge."""
        if self.invalidation_strategy == "aggressive":
            # Invalidate all paths
            self.graph.path_cache.clear()
            self.invalidations_triggered += 1
            self.invalidations_by_reason[f"edge_{reason}"] += 1
        elif self.invalidation_strategy == "conservative":
            # Only invalidate paths that directly use this edge
            self._invalidate_paths_containing_edge(source, target, reason)
        else:  # adaptive
            # Use heuristics to determine invalidation scope
            self._adaptive_invalidate_for_edge(source, target, reason)

    def _invalidate_paths_using_node(self, node_id: NodeId, reason: str) -> None:
        """Invalidate paths that use a specific node."""
        if self.invalidation_strategy == "aggressive":
            # Invalidate all paths
            self.graph.path_cache.clear()
            self.invalidations_triggered += 1
            self.invalidations_by_reason[f"node_{reason}"] += 1
        elif self.invalidation_strategy == "conservative":
            # Only invalidate paths that use this node
            self._invalidate_paths_containing_node(node_id, reason)
        else:  # adaptive
            # Use heuristics to determine invalidation scope
            self._adaptive_invalidate_for_node(node_id, reason)

    def _invalidate_paths_containing_edge(
        self, source: NodeId, target: NodeId, reason: str
    ) -> None:
        """Invalidate only paths that contain a specific edge."""
        # For now, use conservative approach and invalidate all
        # In production, would track which paths use which edges
        self.graph.path_cache.clear()
        self.invalidations_triggered += 1
        self.invalidations_by_reason[f"edge_{reason}"] += 1

    def _invalidate_paths_containing_node(self, node_id: NodeId, reason: str) -> None:
        """Invalidate only paths that contain a specific node."""
        # Remove paths that involve this node
        to_remove = [
            key
            for key in self.graph.path_cache
            if node_id in key  # key is (source, target)
        ]

        for key in to_remove:
            del self.graph.path_cache[key]

        self.invalidations_triggered += 1
        self.invalidations_by_reason[f"node_{reason}"] += len(to_remove)

    def _adaptive_invalidate_for_edge(
        self, source: NodeId, target: NodeId, reason: str
    ) -> None:
        """Use adaptive strategy for edge invalidation."""
        # For now, use conservative approach
        # In production, would use machine learning to optimize invalidation
        self._invalidate_paths_containing_edge(source, target, reason)

    def _adaptive_invalidate_for_node(self, node_id: NodeId, reason: str) -> None:
        """Use adaptive strategy for node invalidation."""
        # For now, use conservative approach
        # In production, would use machine learning to optimize invalidation
        self._invalidate_paths_containing_node(node_id, reason)

    def get_statistics(self) -> PathCacheManagerStatistics:
        """Get cache manager statistics."""
        with self._lock:
            thresholds = InvalidationThresholds(
                latency_change=self.latency_change_threshold,
                utilization_change=self.utilization_change_threshold,
                health_change=self.health_change_threshold,
            )

            return PathCacheManagerStatistics(
                invalidation_strategy=self.invalidation_strategy,
                invalidations_triggered=self.invalidations_triggered,
                invalidations_by_reason=dict(self.invalidations_by_reason),
                thresholds=thresholds,
            )


@dataclass(slots=True)
class GraphOptimizer:
    """
    Performs dynamic graph optimization for improved routing performance.

    Analyzes graph topology and performance metrics to suggest and implement
    optimizations such as adding redundant connections, removing poor-performing
    edges, and restructuring the graph for better load distribution.
    """

    graph: FederationGraph
    optimization_interval: float = 60.0
    is_optimizing: bool = False
    optimization_task: asyncio.Task | None = None
    last_optimization_time: float = 0.0
    optimizations_performed: int = 0
    optimizations_by_type: dict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    optimization_suggestions: list[OptimizationSuggestion] = field(default_factory=list)
    performance_before_optimization: dict[str, float] = field(default_factory=dict)
    performance_after_optimization: dict[str, float] = field(default_factory=dict)
    _lock: RLock = field(default_factory=RLock)

    async def start_optimization(self) -> None:
        """Start automated graph optimization."""
        if self.is_optimizing:
            return

        self.is_optimizing = True
        self.optimization_task = asyncio.create_task(self._optimization_loop())

    async def stop_optimization(self) -> None:
        """Stop automated graph optimization."""
        self.is_optimizing = False
        if self.optimization_task:
            self.optimization_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.optimization_task
            self.optimization_task = None

    async def _optimization_loop(self) -> None:
        """Main optimization loop."""
        while self.is_optimizing:
            try:
                await self._run_optimization_cycle()
                await asyncio.sleep(self.optimization_interval)
            except Exception as e:
                # Log error but continue optimizing
                await asyncio.sleep(self.optimization_interval)

    async def _run_optimization_cycle(self) -> None:
        """Run a single optimization cycle."""
        # Analyze current graph performance
        current_stats = self.graph.get_statistics()

        # Generate optimization suggestions
        suggestions = self._generate_optimization_suggestions()

        # Apply high-priority optimizations
        for suggestion in suggestions:
            if suggestion.priority == 1:  # High priority
                await self._apply_optimization_suggestion(suggestion)

        self.last_optimization_time = time.time()

    def _generate_optimization_suggestions(self) -> list[OptimizationSuggestion]:
        """Generate optimization suggestions based on graph analysis."""
        suggestions = []

        # Analyze connectivity
        suggestions.extend(self._analyze_connectivity())

        # Analyze performance bottlenecks
        suggestions.extend(self._analyze_bottlenecks())

        # Analyze redundancy
        suggestions.extend(self._analyze_redundancy())

        # Sort by priority
        suggestions.sort()

        with self._lock:
            self.optimization_suggestions = suggestions

        return suggestions

    def _analyze_connectivity(self) -> list[OptimizationSuggestion]:
        """Analyze graph connectivity and suggest improvements."""
        suggestions = []

        # Find nodes with low connectivity
        for node_id, node in self.graph.nodes.items():
            neighbor_count = len(self.graph.get_neighbors(node_id))

            if neighbor_count < 2:  # Isolated or single-connected nodes
                suggestions.append(
                    OptimizationSuggestion(
                        optimization_type="add_redundant_connection",
                        priority=1,  # High priority
                        description=f"Add redundant connection for isolated node {node_id}",
                        affected_nodes=[node_id],
                        estimated_improvement=0.3,
                        implementation_cost=0.1,
                    )
                )

        return suggestions

    def _analyze_bottlenecks(self) -> list[OptimizationSuggestion]:
        """Analyze performance bottlenecks and suggest improvements."""
        suggestions = []

        # Find edges with high utilization
        for source_id, adjacency in self.graph.adjacency.items():
            for target_id, edge in adjacency.items():
                if edge.current_utilization > 0.8:  # High utilization
                    suggestions.append(
                        OptimizationSuggestion(
                            optimization_type="add_parallel_connection",
                            priority=2,  # Medium priority
                            description=f"Add parallel connection for high-utilization edge {source_id}-{target_id}",
                            affected_nodes=[source_id, target_id],
                            estimated_improvement=0.2,
                            implementation_cost=0.2,
                        )
                    )

        # Find nodes with high load
        for node_id, node in self.graph.nodes.items():
            if node.get_capacity_utilization() > 0.9:  # High load
                suggestions.append(
                    OptimizationSuggestion(
                        optimization_type="load_balancing",
                        priority=1,  # High priority
                        description=f"Implement load balancing for high-load node {node_id}",
                        affected_nodes=[node_id],
                        estimated_improvement=0.4,
                        implementation_cost=0.3,
                    )
                )

        return suggestions

    def _analyze_redundancy(self) -> list[OptimizationSuggestion]:
        """Analyze redundancy and suggest improvements."""
        suggestions = []

        # Find single points of failure
        for node_id, node in self.graph.nodes.items():
            if self._is_single_point_of_failure(node_id):
                suggestions.append(
                    OptimizationSuggestion(
                        optimization_type="add_redundancy",
                        priority=1,  # High priority
                        description=f"Add redundancy for single point of failure {node_id}",
                        affected_nodes=[node_id],
                        estimated_improvement=0.5,
                        implementation_cost=0.4,
                    )
                )

        return suggestions

    def _is_single_point_of_failure(self, node_id: NodeId) -> bool:
        """Check if a node is a single point of failure."""
        # Remove node temporarily and check connectivity
        neighbors = self.graph.get_neighbors(node_id)

        if len(neighbors) < 2:
            return False  # Not a bridge

        # Check if removing this node would disconnect the graph
        # This is a simplified check - in production would use more sophisticated analysis
        return len(neighbors) > 2 and any(
            len(self.graph.get_neighbors(neighbor)) == 1 for neighbor in neighbors
        )

    async def _apply_optimization_suggestion(
        self, suggestion: OptimizationSuggestion
    ) -> bool:
        """Apply an optimization suggestion."""
        try:
            if suggestion.optimization_type == "add_redundant_connection":
                return await self._add_redundant_connection(suggestion)
            elif suggestion.optimization_type == "add_parallel_connection":
                return await self._add_parallel_connection(suggestion)
            elif suggestion.optimization_type == "load_balancing":
                return await self._implement_load_balancing(suggestion)
            elif suggestion.optimization_type == "add_redundancy":
                return await self._add_redundancy(suggestion)
            else:
                return False
        except Exception:
            return False

    async def _add_redundant_connection(
        self, suggestion: OptimizationSuggestion
    ) -> bool:
        """Add redundant connection for improved reliability."""
        # This would require coordination with the federation system
        # For now, just track the suggestion
        with self._lock:
            self.optimizations_by_type[suggestion.optimization_type] += 1
            self.optimizations_performed += 1
        return True

    async def _add_parallel_connection(
        self, suggestion: OptimizationSuggestion
    ) -> bool:
        """Add parallel connection for improved performance."""
        # This would require coordination with the federation system
        # For now, just track the suggestion
        with self._lock:
            self.optimizations_by_type[suggestion.optimization_type] += 1
            self.optimizations_performed += 1
        return True

    async def _implement_load_balancing(
        self, suggestion: OptimizationSuggestion
    ) -> bool:
        """Implement load balancing for high-load nodes."""
        # This would require coordination with the federation system
        # For now, just track the suggestion
        with self._lock:
            self.optimizations_by_type[suggestion.optimization_type] += 1
            self.optimizations_performed += 1
        return True

    async def _add_redundancy(self, suggestion: OptimizationSuggestion) -> bool:
        """Add redundancy for single points of failure."""
        # This would require coordination with the federation system
        # For now, just track the suggestion
        with self._lock:
            self.optimizations_by_type[suggestion.optimization_type] += 1
            self.optimizations_performed += 1
        return True

    def get_optimization_suggestions(self) -> list[OptimizationSuggestion]:
        """Get current optimization suggestions."""
        with self._lock:
            return self.optimization_suggestions.copy()

    def get_statistics(self) -> GraphOptimizerStatistics:
        """Get comprehensive optimizer statistics."""
        with self._lock:
            optimization_status = OptimizationStatus(
                is_optimizing=self.is_optimizing,
                last_optimization=self.last_optimization_time,
                optimization_interval=self.optimization_interval,
            )

            optimization_history = OptimizationHistory(
                total_optimizations=self.optimizations_performed,
                optimizations_by_type=dict(self.optimizations_by_type),
            )

            suggestions_by_priority = {
                priority: len(
                    [s for s in self.optimization_suggestions if s.priority == priority]
                )
                for priority in [1, 2, 3]
            }

            return GraphOptimizerStatistics(
                optimization_status=optimization_status,
                optimization_history=optimization_history,
                current_suggestions=len(self.optimization_suggestions),
                suggestions_by_priority=suggestions_by_priority,
            )


@dataclass(slots=True)
class FederationGraphMonitor:
    """
    Unified real-time monitoring system for federation graphs.

    Combines metrics collection, cache management, and optimization
    into a single coordinated system for maintaining optimal federation
    performance.
    """

    graph: FederationGraph
    collection_interval: float = 5.0
    optimization_interval: float = 60.0

    # Fields assigned in __post_init__
    metrics_collector: GraphMetricsCollector = field(init=False)
    cache_manager: PathCacheManager = field(init=False)
    optimizer: GraphOptimizer = field(init=False)
    is_monitoring: bool = False
    start_time: float = 0.0
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """Initialize monitoring components."""
        self.metrics_collector = GraphMetricsCollector(
            self.graph, self.collection_interval
        )
        self.cache_manager = PathCacheManager(self.graph, "adaptive")
        self.optimizer = GraphOptimizer(self.graph, self.optimization_interval)

    async def start_monitoring(self) -> None:
        """Start comprehensive graph monitoring."""
        if self.is_monitoring:
            return

        self.is_monitoring = True
        self.start_time = time.time()

        # Start all components
        await self.metrics_collector.start_collection()
        await self.optimizer.start_optimization()

    async def stop_monitoring(self) -> None:
        """Stop comprehensive graph monitoring."""
        if not self.is_monitoring:
            return

        self.is_monitoring = False

        # Stop all components
        await self.metrics_collector.stop_collection()
        await self.optimizer.stop_optimization()

    def add_metrics_collector(self, collector: MetricsCollectorProtocol) -> None:
        """Add a metrics collector to the monitoring system."""
        self.metrics_collector.add_collector(collector)

    def get_comprehensive_status(self) -> GraphMonitoringStatistics:
        """Get comprehensive monitoring status."""
        with self._lock:
            uptime = time.time() - self.start_time if self.is_monitoring else 0.0

            monitoring_status = MonitoringStatus(
                is_monitoring=self.is_monitoring,
                uptime_seconds=uptime,
                start_time=self.start_time,
            )

            return GraphMonitoringStatistics(
                monitoring_status=monitoring_status,
                metrics_collector=self.metrics_collector.get_statistics(),
                cache_manager=self.cache_manager.get_statistics(),
                optimizer=self.optimizer.get_statistics(),
                graph_statistics=self.graph.get_statistics(),
            )
