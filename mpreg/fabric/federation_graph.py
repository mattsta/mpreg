"""
Planet-Scale Graph-Based Routing for the MPREG Fabric.

This module implements the core graph routing engine that provides ultra-graph-traversal-based
scalability for the MPREG fabric. It enables optimal multi-hop routing through
sophisticated graph algorithms while maintaining the high performance characteristics of the
existing fabric routing infrastructure.

Key Features:
- Dijkstra's algorithm for optimal path finding
- A* algorithm with geographic heuristics for global-scale routing
- Multiple disjoint paths for load balancing and redundancy
- Real-time graph updates with intelligent caching
- O(log N) routing complexity through hierarchical optimization

This is Phase 1 of the Planet-Scale Fabric Roadmap.
"""

from __future__ import annotations

import math
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from threading import RLock
from typing import Protocol

from mpreg.core.statistics import (
    AlgorithmPerformance,
    GraphCacheStatistics,
    GraphRouterStatistics,
    GraphStatistics,
    RoutingPerformance,
)
from mpreg.datastructures import (
    AStarAlgorithm,
    AStarConfig,
    CoordinateMap,
    DijkstraAlgorithm,
    DijkstraConfig,
)

# Type aliases for clarity
NodeId = str
EdgeWeight = float
PathList = list[str]


class NodeType(Enum):
    """Types of nodes in the federation graph."""

    CLUSTER = "cluster"
    LOCAL_HUB = "local_hub"
    REGIONAL_HUB = "regional_hub"
    GLOBAL_HUB = "global_hub"


@dataclass(slots=True, frozen=True)
class GeographicCoordinate:
    """Geographic coordinates for distance-based routing."""

    latitude: float
    longitude: float

    def distance_to(self, other: GeographicCoordinate) -> float:
        """
        Calculate geographic distance using Haversine formula.

        Returns distance in kilometers.
        """
        # Convert to radians
        lat1, lon1 = math.radians(self.latitude), math.radians(self.longitude)
        lat2, lon2 = math.radians(other.latitude), math.radians(other.longitude)

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.asin(math.sqrt(a))

        # Earth's radius in kilometers
        return 6371.0 * c


@dataclass(slots=True)
class FederationGraphNode:
    """
    A node in the federation graph representing a cluster or hub.

    Encapsulates all node-specific information needed for graph-based routing
    including geographic location, capacity, and current performance metrics.
    """

    node_id: NodeId
    node_type: NodeType
    region: str
    coordinates: GeographicCoordinate
    max_capacity: int
    current_load: float = 0.0
    health_score: float = 1.0  # 0.0 to 1.0

    # Performance characteristics
    processing_latency_ms: float = 1.0
    bandwidth_mbps: int = 1000
    reliability_score: float = 1.0  # 0.0 to 1.0

    # Connection tracking
    connections: dict[NodeId, FederationGraphEdge] = field(default_factory=dict)
    last_updated: float = field(default_factory=time.time)

    def get_capacity_utilization(self) -> float:
        """Get current capacity utilization as a ratio (0.0 to 1.0)."""
        return min(1.0, self.current_load / max(1, self.max_capacity))

    def get_node_weight(self) -> float:
        """
        Calculate node weight for routing decisions.

        Lower weight = better node. Considers health, utilization, and latency.
        """
        utilization_penalty = self.get_capacity_utilization() * 10.0
        health_penalty = (1.0 - self.health_score) * 20.0
        latency_penalty = self.processing_latency_ms / 10.0

        return utilization_penalty + health_penalty + latency_penalty

    def is_healthy(self) -> bool:
        """Check if node is healthy enough for routing."""
        return (
            self.health_score > 0.5
            and self.get_capacity_utilization() < 0.9
            and time.time() - self.last_updated < 300.0
        )  # 5 minute timeout

    def get_node_id(self) -> str:
        """Get unique identifier for this node (GraphNode protocol compatibility)."""
        return self.node_id


@dataclass(slots=True)
class FederationGraphEdge:
    """
    An edge representing a connection between federation nodes.

    Tracks real-time performance metrics used for optimal routing decisions.
    """

    source_id: NodeId
    target_id: NodeId

    # Base characteristics
    latency_ms: float
    bandwidth_mbps: int
    reliability_score: float  # 0.0 to 1.0

    # Dynamic metrics
    current_utilization: float = 0.0  # 0.0 to 1.0
    packet_loss_rate: float = 0.0  # 0.0 to 1.0
    jitter_ms: float = 0.0

    # Quality of service
    priority_class: int = 1  # 1=high, 2=normal, 3=low
    cost_factor: float = 1.0  # For cost-based routing

    # Monitoring
    last_updated: float = field(default_factory=time.time)
    sample_count: int = 0

    def get_edge_weight(self) -> EdgeWeight:
        """
        Calculate edge weight for routing algorithms.

        Lower weight = better edge. Combines latency, utilization, reliability, and QoS.
        """
        # Base latency with utilization penalty
        base_weight = self.latency_ms * (1.0 + self.current_utilization * 2.0)

        # Reliability penalty
        reliability_penalty = (1.0 - self.reliability_score) * 50.0

        # Packet loss penalty
        loss_penalty = self.packet_loss_rate * 100.0

        # Jitter penalty
        jitter_penalty = self.jitter_ms * 0.5

        # Priority class penalty
        priority_penalty = (self.priority_class - 1) * 10.0

        # Cost factor
        cost_penalty = (self.cost_factor - 1.0) * 20.0

        return (
            base_weight
            + reliability_penalty
            + loss_penalty
            + jitter_penalty
            + priority_penalty
            + cost_penalty
        )

    def is_usable(self) -> bool:
        """Check if edge is usable for routing."""
        return (
            self.reliability_score > 0.3
            and self.current_utilization < 0.95
            and self.packet_loss_rate < 0.1
            and time.time() - self.last_updated < 300.0
        )  # 5 minute timeout

    def update_metrics(
        self,
        latency_ms: float,
        utilization: float,
        packet_loss: float = 0.0,
        jitter_ms: float = 0.0,
    ) -> None:
        """Update edge metrics with exponential moving average."""
        alpha = 0.3  # Smoothing factor

        if self.sample_count > 0:
            self.latency_ms = alpha * latency_ms + (1 - alpha) * self.latency_ms
            self.current_utilization = (
                alpha * utilization + (1 - alpha) * self.current_utilization
            )
            self.packet_loss_rate = (
                alpha * packet_loss + (1 - alpha) * self.packet_loss_rate
            )
            self.jitter_ms = alpha * jitter_ms + (1 - alpha) * self.jitter_ms
        else:
            self.latency_ms = latency_ms
            self.current_utilization = utilization
            self.packet_loss_rate = packet_loss
            self.jitter_ms = jitter_ms

        self.sample_count += 1
        self.last_updated = time.time()


class FederationGraphAdapter:
    """Adapter to make FederationGraph compatible with Graph protocol."""

    def __init__(self, federation_graph: FederationGraph):
        self.federation_graph = federation_graph

    def get_node(self, node_id: str) -> FederationGraphNode | None:
        """Get node by ID."""
        return self.federation_graph.get_node(node_id)

    def get_edge(self, source: str, target: str) -> FederationGraphEdge | None:
        """Get edge between two nodes."""
        return self.federation_graph.get_edge(source, target)

    def get_neighbors(self, node_id: str) -> list[str]:
        """Get list of neighboring node IDs."""
        return self.federation_graph.get_neighbors(node_id)

    def get_all_nodes(self) -> list[str]:
        """Get all node IDs in the graph."""
        return list(self.federation_graph.nodes.keys())


class GraphRouterProtocol(Protocol):
    """Protocol defining the graph router interface."""

    def find_optimal_path(
        self, source: NodeId, target: NodeId, max_hops: int = 5
    ) -> PathList | None:
        """Find the optimal path between two nodes."""
        ...

    def find_multiple_paths(
        self, source: NodeId, target: NodeId, num_paths: int = 3
    ) -> list[PathList]:
        """Find multiple disjoint paths for load balancing."""
        ...

    def update_edge_metrics(
        self, source: NodeId, target: NodeId, latency_ms: float, utilization: float
    ) -> None:
        """Update real-time edge metrics."""
        ...


@dataclass(slots=True)
class PathCacheEntry:
    """Cache entry for computed paths with TTL."""

    path: PathList
    computed_at: float
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)


@dataclass(slots=True)
class FederationGraph:
    """
    Core federation graph data structure with high-performance operations.

    Manages the federation topology and provides efficient graph operations
    for routing algorithms. Thread-safe and optimized for frequent updates.
    """

    cache_ttl_seconds: float = 30.0
    max_cache_size: int = 10000
    nodes: dict[NodeId, FederationGraphNode] = field(default_factory=dict)
    adjacency: dict[NodeId, dict[NodeId, FederationGraphEdge]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    path_cache: dict[tuple[NodeId, NodeId], PathCacheEntry] = field(
        default_factory=dict
    )
    _lock: RLock = field(default_factory=RLock)
    cache_hits: int = 0
    cache_misses: int = 0
    path_computations: int = 0

    def add_node(self, node: FederationGraphNode) -> None:
        """Add a node to the federation graph."""
        with self._lock:
            self.nodes[node.node_id] = node
            if node.node_id not in self.adjacency:
                self.adjacency[node.node_id] = {}

    def remove_node(self, node_id: NodeId) -> bool:
        """Remove a node and all its edges from the graph."""
        with self._lock:
            if node_id not in self.nodes:
                return False

            # Remove all edges to/from this node
            for neighbor_id in list(self.adjacency[node_id].keys()):
                self.remove_edge(node_id, neighbor_id)

            # Remove the node
            del self.nodes[node_id]
            del self.adjacency[node_id]

            # Remove from other nodes' adjacency lists
            for adj_dict in self.adjacency.values():
                adj_dict.pop(node_id, None)

            # Invalidate cache entries involving this node
            self._invalidate_cache_for_node(node_id)

            return True

    def add_edge(self, edge: FederationGraphEdge) -> None:
        """Add a bidirectional edge to the graph."""
        with self._lock:
            # Add forward edge
            self.adjacency[edge.source_id][edge.target_id] = edge

            # Add to source node's connections
            if edge.source_id in self.nodes:
                self.nodes[edge.source_id].connections[edge.target_id] = edge

            # Create reverse edge for bidirectional graph
            reverse_edge = FederationGraphEdge(
                source_id=edge.target_id,
                target_id=edge.source_id,
                latency_ms=edge.latency_ms,
                bandwidth_mbps=edge.bandwidth_mbps,
                reliability_score=edge.reliability_score,
                current_utilization=edge.current_utilization,
                packet_loss_rate=edge.packet_loss_rate,
                jitter_ms=edge.jitter_ms,
                priority_class=edge.priority_class,
                cost_factor=edge.cost_factor,
            )

            self.adjacency[edge.target_id][edge.source_id] = reverse_edge

            if edge.target_id in self.nodes:
                self.nodes[edge.target_id].connections[edge.source_id] = reverse_edge

            # Invalidate relevant cache entries
            self._invalidate_cache_for_edge(edge.source_id, edge.target_id)

    def remove_edge(self, source: NodeId, target: NodeId) -> bool:
        """Remove a bidirectional edge from the graph."""
        with self._lock:
            removed = False

            # Remove forward edge
            if source in self.adjacency and target in self.adjacency[source]:
                del self.adjacency[source][target]
                removed = True

                if source in self.nodes:
                    self.nodes[source].connections.pop(target, None)

            # Remove reverse edge
            if target in self.adjacency and source in self.adjacency[target]:
                del self.adjacency[target][source]
                removed = True

                if target in self.nodes:
                    self.nodes[target].connections.pop(source, None)

            if removed:
                self._invalidate_cache_for_edge(source, target)

            return removed

    def get_node(self, node_id: NodeId) -> FederationGraphNode | None:
        """Get a node by ID."""
        with self._lock:
            return self.nodes.get(node_id)

    def get_edge(self, source: NodeId, target: NodeId) -> FederationGraphEdge | None:
        """Get an edge between two nodes."""
        with self._lock:
            return self.adjacency.get(source, {}).get(target)

    def get_neighbors(self, node_id: NodeId) -> list[NodeId]:
        """Get all neighbors of a node."""
        with self._lock:
            return list(self.adjacency.get(node_id, {}).keys())

    def update_node_metrics(
        self,
        node_id: NodeId,
        current_load: float,
        health_score: float,
        processing_latency_ms: float,
    ) -> bool:
        """Update real-time node metrics."""
        with self._lock:
            if node_id not in self.nodes:
                return False

            node = self.nodes[node_id]
            node.current_load = current_load
            node.health_score = health_score
            node.processing_latency_ms = processing_latency_ms
            node.last_updated = time.time()

            # Invalidate cache entries involving this node
            self._invalidate_cache_for_node(node_id)

            return True

    def update_edge_metrics(
        self,
        source: NodeId,
        target: NodeId,
        latency_ms: float,
        utilization: float,
        packet_loss: float = 0.0,
        jitter_ms: float = 0.0,
    ) -> bool:
        """Update real-time edge metrics."""
        with self._lock:
            # Update forward edge
            forward_edge = self.adjacency.get(source, {}).get(target)
            if forward_edge:
                forward_edge.update_metrics(
                    latency_ms, utilization, packet_loss, jitter_ms
                )

            # Update reverse edge
            reverse_edge = self.adjacency.get(target, {}).get(source)
            if reverse_edge:
                reverse_edge.update_metrics(
                    latency_ms, utilization, packet_loss, jitter_ms
                )

            # Invalidate relevant cache entries
            if forward_edge or reverse_edge:
                self._invalidate_cache_for_edge(source, target)
                return True

            return False

    def get_path_from_cache(self, source: NodeId, target: NodeId) -> PathList | None:
        """Get cached path if valid."""
        with self._lock:
            cache_key = (source, target)
            if cache_key in self.path_cache:
                entry = self.path_cache[cache_key]

                # Check TTL
                if time.time() - entry.computed_at <= self.cache_ttl_seconds:
                    entry.access_count += 1
                    entry.last_accessed = time.time()
                    self.cache_hits += 1
                    return entry.path
                else:
                    # Expired entry
                    del self.path_cache[cache_key]

            self.cache_misses += 1
            return None

    def cache_path(self, source: NodeId, target: NodeId, path: PathList) -> None:
        """Cache a computed path."""
        with self._lock:
            # Evict old entries if cache is full
            if len(self.path_cache) >= self.max_cache_size:
                self._evict_cache_entries()

            cache_key = (source, target)
            self.path_cache[cache_key] = PathCacheEntry(
                path=path, computed_at=time.time()
            )

    def _invalidate_cache_for_node(self, node_id: NodeId) -> None:
        """Invalidate all cache entries involving a specific node."""
        to_remove = [key for key in self.path_cache if node_id in key]
        for key in to_remove:
            del self.path_cache[key]

    def _invalidate_cache_for_edge(self, source: NodeId, target: NodeId) -> None:
        """Invalidate cache entries that might use a specific edge."""
        # For now, invalidate all cache entries (conservative approach)
        # In production, could use more sophisticated invalidation
        self.path_cache.clear()

    def _evict_cache_entries(self) -> None:
        """Evict least recently used cache entries."""
        if not self.path_cache:
            return

        # Remove 25% of entries, starting with least recently used
        num_to_remove = len(self.path_cache) // 4
        if num_to_remove == 0:
            num_to_remove = 1

        # Sort by last accessed time
        sorted_entries = sorted(
            self.path_cache.items(), key=lambda x: x[1].last_accessed
        )

        for i in range(num_to_remove):
            key, _ = sorted_entries[i]
            del self.path_cache[key]

    def get_statistics(self) -> GraphStatistics:
        """Get comprehensive graph statistics."""
        with self._lock:
            total_edges = (
                sum(len(neighbors) for neighbors in self.adjacency.values()) // 2
            )
            healthy_nodes = sum(1 for node in self.nodes.values() if node.is_healthy())
            usable_edges = (
                sum(
                    1
                    for neighbors in self.adjacency.values()
                    for edge in neighbors.values()
                    if edge.is_usable()
                )
                // 2
            )

            cache_hit_rate = self.cache_hits / max(
                1, self.cache_hits + self.cache_misses
            )

            cache_stats = GraphCacheStatistics(
                hit_rate=cache_hit_rate,
                total_entries=len(self.path_cache),
                cache_hits=self.cache_hits,
                cache_misses=self.cache_misses,
            )

            node_types = {
                node_type.value: len(
                    [n for n in self.nodes.values() if n.node_type == node_type]
                )
                for node_type in NodeType
            }

            return GraphStatistics(
                total_nodes=len(self.nodes),
                healthy_nodes=healthy_nodes,
                total_edges=total_edges,
                usable_edges=usable_edges,
                node_types=node_types,
                average_connectivity=(2 * total_edges) / max(1, len(self.nodes)),
                cache_statistics=cache_stats,
                path_computations=self.path_computations,
                health_ratio=healthy_nodes / max(1, len(self.nodes)),
                usability_ratio=usable_edges / max(1, total_edges),
            )


@dataclass(slots=True)
class DijkstraRouter:
    """
    Federation-specific router using centralized Dijkstra implementation.

    Provides federation-specific caching and integration with FederationGraph.
    """

    graph: FederationGraph
    computation_time_ms: float = 0.0

    def find_optimal_path(
        self, source: NodeId, target: NodeId, max_hops: int = 5
    ) -> PathList | None:
        """
        Find optimal path using centralized Dijkstra implementation.

        Args:
            source: Source node ID
            target: Target node ID
            max_hops: Maximum number of hops allowed

        Returns:
            List of node IDs representing the optimal path, or None if no path exists
        """
        # Check cache first
        cached_path = self.graph.get_path_from_cache(source, target)
        if cached_path is not None:
            return cached_path

        # Use centralized algorithm
        adapter = FederationGraphAdapter(self.graph)
        dijkstra = DijkstraAlgorithm(DijkstraConfig(max_hops=max_hops))
        result = dijkstra.find_shortest_path(adapter, source, target)

        if result is None:
            return None

        path = result.path
        self.computation_time_ms = result.computation_time_ms

        # Cache the result
        if path:
            self.graph.cache_path(source, target, path)

        # Update statistics
        self.graph.path_computations += 1

        return path


@dataclass(slots=True)
class MultiPathRouter:
    """
    Multiple disjoint paths router for load balancing and redundancy.

    Finds multiple paths between nodes to enable load distribution
    and provide backup routes for improved reliability.
    """

    graph: FederationGraph
    dijkstra_router: DijkstraRouter = field(init=False)

    def __post_init__(self) -> None:
        """Initialize multi-path router with a federation graph."""
        self.dijkstra_router = DijkstraRouter(self.graph)

    def find_multiple_paths(
        self, source: NodeId, target: NodeId, num_paths: int = 3, max_hops: int = 5
    ) -> list[PathList]:
        """
        Find multiple disjoint paths for load balancing.

        Args:
            source: Source node ID
            target: Target node ID
            num_paths: Number of paths to find
            max_hops: Maximum hops per path

        Returns:
            List of paths, each path is a list of node IDs
        """
        if source == target:
            return [[source]]

        paths = []

        # Store original edges for restoration
        removed_edges: list[tuple[NodeId, NodeId, FederationGraphEdge]] = []

        try:
            for path_index in range(num_paths):
                # Find optimal path with current graph state
                path = self.dijkstra_router.find_optimal_path(source, target, max_hops)

                if not path or len(path) <= 1:
                    break

                paths.append(path)

                # Remove edges from this path to find disjoint paths
                for i in range(len(path) - 1):
                    current_node = path[i]
                    next_node = path[i + 1]

                    # Store edges before removing
                    forward_edge = self.graph.get_edge(current_node, next_node)
                    reverse_edge = self.graph.get_edge(next_node, current_node)

                    if forward_edge:
                        removed_edges.append((current_node, next_node, forward_edge))
                    if reverse_edge:
                        removed_edges.append((next_node, current_node, reverse_edge))

                    # Temporarily remove edges
                    self.graph.remove_edge(current_node, next_node)

        finally:
            # Restore all removed edges
            for source_id, target_id, edge in removed_edges:
                # Re-add the edge
                self.graph.adjacency[source_id][target_id] = edge

                # Update node connections
                if source_id in self.graph.nodes:
                    self.graph.nodes[source_id].connections[target_id] = edge

        return paths


@dataclass(slots=True)
class GeographicAStarRouter:
    """
    A* algorithm with geographic heuristics for global-scale routing.

    Uses centralized A* implementation with geographic distance heuristic,
    making it particularly effective for global federation routing.
    """

    graph: FederationGraph
    computation_time_ms: float = 0.0

    def find_geographic_path(
        self, source: NodeId, target: NodeId, max_hops: int = 5
    ) -> PathList | None:
        """
        Find path using centralized A* with geographic distance heuristic.

        Args:
            source: Source node ID
            target: Target node ID
            max_hops: Maximum number of hops allowed

        Returns:
            List of node IDs representing the path, or None if no path exists
        """
        # Build coordinate map from federation nodes
        coord_map = CoordinateMap()
        for node_id, node in self.graph.nodes.items():
            coord_map.add_node(
                node_id, node.coordinates.latitude, node.coordinates.longitude
            )

        # Create geographic heuristic function
        def geographic_heuristic(node_id: str, target_id: str) -> float:
            """Convert geographic distance to estimated latency."""
            node_coord = coord_map.get_coordinate(node_id)
            target_coord = coord_map.get_coordinate(target_id)

            if not node_coord or not target_coord:
                return 0.0  # Conservative admissible estimate

            # Distance in kilometers -> estimated latency in ms
            # Assume ~0.2ms per 100km (rough speed of light in fiber)
            distance_km = node_coord.distance_to(target_coord)
            return distance_km * 0.002

        # Use centralized A* algorithm
        adapter = FederationGraphAdapter(self.graph)
        astar = AStarAlgorithm(AStarConfig(max_hops=max_hops))
        result = astar.find_path_with_heuristic(
            adapter, source, target, geographic_heuristic
        )

        if result is None:
            return None

        self.computation_time_ms = result.computation_time_ms
        return result.path


@dataclass(slots=True)
class GraphBasedFederationRouter:
    """
    Unified graph-based federation router combining all routing algorithms.

    This is the main interface for graph-based routing in the MPREG federation system.
    It provides optimal routing through various algorithms and maintains high performance
    through intelligent caching and algorithm selection.
    """

    cache_ttl_seconds: float = 30.0
    max_cache_size: int = 10000

    # Fields assigned in __post_init__
    graph: FederationGraph = field(init=False)
    dijkstra_router: DijkstraRouter = field(init=False)
    multipath_router: MultiPathRouter = field(init=False)
    geographic_router: GeographicAStarRouter = field(init=False)
    total_routing_requests: int = 0
    successful_routes: int = 0
    average_computation_time_ms: float = 0.0

    def __post_init__(self) -> None:
        """
        Initialize the graph-based federation router.
        """
        self.graph = FederationGraph(self.cache_ttl_seconds, self.max_cache_size)
        self.dijkstra_router = DijkstraRouter(self.graph)
        self.multipath_router = MultiPathRouter(self.graph)
        self.geographic_router = GeographicAStarRouter(self.graph)

    def add_node(self, node: FederationGraphNode) -> None:
        """Add a federation node to the graph."""
        self.graph.add_node(node)

    def remove_node(self, node_id: NodeId) -> bool:
        """Remove a federation node from the graph."""
        return self.graph.remove_node(node_id)

    def add_edge(self, edge: FederationGraphEdge) -> None:
        """Add a connection between federation nodes."""
        self.graph.add_edge(edge)

    def remove_edge(self, source: NodeId, target: NodeId) -> bool:
        """Remove a connection between federation nodes."""
        return self.graph.remove_edge(source, target)

    def find_optimal_path(
        self, source: NodeId, target: NodeId, max_hops: int = 5
    ) -> PathList | None:
        """
        Find optimal path between nodes using Dijkstra's algorithm.

        This is the primary routing method for most federation routing needs.
        """
        self.total_routing_requests += 1

        path = self.dijkstra_router.find_optimal_path(source, target, max_hops)

        if path:
            self.successful_routes += 1

        # Update average computation time
        computation_time = self.dijkstra_router.computation_time_ms
        if self.total_routing_requests == 1:
            self.average_computation_time_ms = computation_time
        else:
            alpha = 0.1  # Smoothing factor
            self.average_computation_time_ms = (
                alpha * computation_time
                + (1 - alpha) * self.average_computation_time_ms
            )

        return path

    def find_multiple_paths(
        self,
        source: NodeId,
        target: NodeId,
        num_paths: int = 3,
        max_hops: int = 5,
    ) -> list[PathList]:
        """Find multiple disjoint paths for load balancing and redundancy."""
        self.total_routing_requests += 1

        paths = self.multipath_router.find_multiple_paths(
            source, target, num_paths, max_hops
        )

        if paths:
            self.successful_routes += 1

        return paths

    def find_geographic_path(
        self, source: NodeId, target: NodeId, max_hops: int = 5
    ) -> PathList | None:
        """Find path using geographic-aware A* algorithm."""
        self.total_routing_requests += 1

        path = self.geographic_router.find_geographic_path(source, target, max_hops)

        if path:
            self.successful_routes += 1

        return path

    def update_edge_metrics(
        self,
        source: NodeId,
        target: NodeId,
        latency_ms: float,
        utilization: float,
        packet_loss: float = 0.0,
        jitter_ms: float = 0.0,
    ) -> None:
        """Update real-time edge metrics for dynamic routing."""
        self.graph.update_edge_metrics(
            source, target, latency_ms, utilization, packet_loss, jitter_ms
        )

    def update_node_metrics(
        self,
        node_id: NodeId,
        current_load: float,
        health_score: float,
        processing_latency_ms: float,
    ) -> bool:
        """Update real-time node metrics."""
        return self.graph.update_node_metrics(
            node_id, current_load, health_score, processing_latency_ms
        )

    def get_performance_statistics(self) -> GraphRouterStatistics:
        """Get performance statistics for the router."""
        return self.get_comprehensive_statistics()

    def get_comprehensive_statistics(self) -> GraphRouterStatistics:
        """Get comprehensive routing and graph statistics."""
        graph_stats = self.graph.get_statistics()

        success_rate = self.successful_routes / max(1, self.total_routing_requests)

        routing_performance = RoutingPerformance(
            total_requests=self.total_routing_requests,
            successful_routes=self.successful_routes,
            success_rate=success_rate,
            average_computation_time_ms=self.average_computation_time_ms,
        )

        algorithm_performance = AlgorithmPerformance(
            dijkstra_last_computation_ms=self.dijkstra_router.computation_time_ms,
            astar_last_computation_ms=self.geographic_router.computation_time_ms,
        )

        return GraphRouterStatistics(
            graph_statistics=graph_stats,
            routing_performance=routing_performance,
            algorithm_performance=algorithm_performance,
        )
