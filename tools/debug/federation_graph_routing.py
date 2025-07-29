#!/usr/bin/env python3
"""
Graph-based routing for MPREG federation - missing component for ultra-scalability.

This implements the graph traversal algorithms needed to achieve the original vision
of ultra-graph-traversal-based scalability with optimal multi-hop path discovery.
"""

import heapq
import time
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass(slots=True)
class FederationGraphNode:
    """A node in the federation graph representing a cluster or hub."""

    cluster_id: str
    node_type: str  # "cluster", "regional_hub", "global_hub"
    region: str
    coordinates: tuple[float, float]
    capacity: int
    current_load: float = 0.0
    connections: dict[str, "FederationGraphEdge"] = field(default_factory=dict)


@dataclass(slots=True)
class FederationGraphEdge:
    """An edge representing connection between federation nodes."""

    source_id: str
    target_id: str
    latency_ms: float
    bandwidth_mbps: int
    reliability: float  # 0.0 to 1.0
    current_utilization: float = 0.0

    def get_weight(self) -> float:
        """Calculate routing weight considering latency, utilization, and reliability."""
        # Higher utilization and latency = higher weight (worse)
        # Lower reliability = higher weight (worse)
        base_weight = self.latency_ms * (1.0 + self.current_utilization)
        reliability_penalty = (1.0 - self.reliability) * 100
        return base_weight + reliability_penalty


class GraphBasedFederationRouter:
    """
    Graph-based router for optimal multi-hop federation routing.

    Implements the missing ultra-graph-traversal-based scalability:
    - Dijkstra's algorithm for shortest path
    - A* with geographic heuristics
    - Multi-path routing for load balancing
    - Dynamic graph updates based on real-time metrics
    """

    def __init__(self):
        self.nodes: dict[str, FederationGraphNode] = {}
        self.adjacency: dict[str, dict[str, FederationGraphEdge]] = defaultdict(dict)
        self.path_cache: dict[tuple[str, str], list[str]] = {}
        self.cache_ttl: dict[tuple[str, str], float] = {}

    def add_node(self, node: FederationGraphNode) -> None:
        """Add a federation node to the graph."""
        self.nodes[node.cluster_id] = node
        if node.cluster_id not in self.adjacency:
            self.adjacency[node.cluster_id] = {}

    def add_edge(self, edge: FederationGraphEdge) -> None:
        """Add a bidirectional edge between federation nodes."""
        self.adjacency[edge.source_id][edge.target_id] = edge
        # Create reverse edge for bidirectional graph
        reverse_edge = FederationGraphEdge(
            source_id=edge.target_id,
            target_id=edge.source_id,
            latency_ms=edge.latency_ms,
            bandwidth_mbps=edge.bandwidth_mbps,
            reliability=edge.reliability,
            current_utilization=edge.current_utilization,
        )
        self.adjacency[edge.target_id][edge.source_id] = reverse_edge

    def find_optimal_path(
        self, source: str, target: str, max_hops: int = 5
    ) -> list[str] | None:
        """
        Find optimal path using Dijkstra's algorithm with caching.

        This provides the missing graph traversal capability for the federation.
        """
        if source == target:
            return [source]

        # Check cache first
        cache_key = (source, target)
        if cache_key in self.path_cache:
            if time.time() - self.cache_ttl[cache_key] < 30.0:  # 30s TTL
                return self.path_cache[cache_key]

        # Dijkstra's algorithm implementation
        distances = {node: float("inf") for node in self.nodes}
        distances[source] = 0
        previous: dict[str, str | None] = {}
        visited = set()

        # Priority queue: (distance, node_id)
        pq: list[tuple[float, str]] = [(0, source)]

        while pq:
            current_dist, current = heapq.heappop(pq)

            if current in visited:
                continue

            visited.add(current)

            if current == target:
                break

            # Don't exceed max hops
            if len(self._reconstruct_path(previous, source, current)) >= max_hops:
                continue

            # Check all neighbors
            for neighbor, edge in self.adjacency[current].items():
                if neighbor in visited:
                    continue

                new_dist = current_dist + edge.get_weight()

                if new_dist < distances[neighbor]:
                    distances[neighbor] = new_dist
                    previous[neighbor] = current
                    heapq.heappush(pq, (new_dist, neighbor))

        # Reconstruct path
        if target not in previous and target != source:
            return None

        path = self._reconstruct_path(previous, source, target)

        # Cache result
        self.path_cache[cache_key] = path
        self.cache_ttl[cache_key] = time.time()

        return path

    def find_multiple_paths(
        self, source: str, target: str, num_paths: int = 3
    ) -> list[list[str]]:
        """
        Find multiple disjoint paths for load balancing and redundancy.

        This enables multi-path routing for better federation resilience.
        """
        paths = []

        # Create a copy of the graph for modification
        original_edges = {}

        for _ in range(num_paths):
            path = self.find_optimal_path(source, target)
            if not path or len(path) <= 1:
                break

            paths.append(path)

            # Remove edges from this path to find disjoint paths
            for i in range(len(path) - 1):
                edge_key = (path[i], path[i + 1])
                if edge_key not in original_edges:
                    original_edges[edge_key] = self.adjacency[path[i]].get(path[i + 1])

                # Temporarily remove edge
                if path[i + 1] in self.adjacency[path[i]]:
                    del self.adjacency[path[i]][path[i + 1]]
                if path[i] in self.adjacency[path[i + 1]]:
                    del self.adjacency[path[i + 1]][path[i]]

        # Restore original edges
        for (src, dst), edge in original_edges.items():
            if edge:
                self.adjacency[src][dst] = edge
                # Restore reverse edge too
                reverse_edge = FederationGraphEdge(
                    source_id=dst,
                    target_id=src,
                    latency_ms=edge.latency_ms,
                    bandwidth_mbps=edge.bandwidth_mbps,
                    reliability=edge.reliability,
                    current_utilization=edge.current_utilization,
                )
                self.adjacency[dst][src] = reverse_edge

        return paths

    def find_path_with_geographic_heuristic(
        self, source: str, target: str
    ) -> list[str] | None:
        """
        A* algorithm with geographic distance heuristic for federation routing.

        This provides geographically-aware routing for global-scale federation.
        """
        if source not in self.nodes or target not in self.nodes:
            return None

        def geographic_distance(node1_id: str, node2_id: str) -> float:
            """Calculate approximate geographic distance as heuristic."""
            node1 = self.nodes[node1_id]
            node2 = self.nodes[node2_id]

            lat1, lon1 = node1.coordinates
            lat2, lon2 = node2.coordinates

            # Simplified distance calculation (Euclidean approximation)
            # In production, use proper haversine formula
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            return (dlat**2 + dlon**2) ** 0.5 * 111.0  # Rough km conversion

        # A* algorithm
        g_score = {node: float("inf") for node in self.nodes}
        g_score[source] = 0

        f_score = {node: float("inf") for node in self.nodes}
        f_score[source] = geographic_distance(source, target)

        open_set = [(f_score[source], source)]
        came_from: dict[str, str] = {}
        closed_set = set()

        while open_set:
            current_f, current = heapq.heappop(open_set)

            if current == target:
                return self._reconstruct_path(came_from, source, target)

            closed_set.add(current)

            for neighbor, edge in self.adjacency[current].items():
                if neighbor in closed_set:
                    continue

                tentative_g = g_score[current] + edge.get_weight()

                if tentative_g < g_score[neighbor]:
                    came_from[neighbor] = current
                    g_score[neighbor] = tentative_g
                    f_score[neighbor] = tentative_g + geographic_distance(
                        neighbor, target
                    )

                    # Add to open set if not already there
                    if (f_score[neighbor], neighbor) not in open_set:
                        heapq.heappush(open_set, (f_score[neighbor], neighbor))

        return None

    def update_edge_metrics(
        self, source: str, target: str, latency_ms: float, utilization: float
    ) -> None:
        """Update real-time edge metrics for dynamic routing."""
        if source in self.adjacency and target in self.adjacency[source]:
            edge = self.adjacency[source][target]
            edge.latency_ms = latency_ms
            edge.current_utilization = utilization

            # Update reverse edge too
            if target in self.adjacency and source in self.adjacency[target]:
                reverse_edge = self.adjacency[target][source]
                reverse_edge.latency_ms = latency_ms
                reverse_edge.current_utilization = utilization

        # Invalidate path cache for affected routes
        to_remove = []
        for cache_key in self.path_cache:
            if source in cache_key or target in cache_key:
                to_remove.append(cache_key)

        for key in to_remove:
            del self.path_cache[key]
            del self.cache_ttl[key]

    def _reconstruct_path(self, previous: dict, source: str, target: str) -> list[str]:
        """Reconstruct path from Dijkstra's previous pointers."""
        path = []
        current: str | None = target

        while current is not None:
            path.append(current)
            current = previous.get(current)  # type: ignore

        path.reverse()
        return path if path[0] == source else []

    def get_graph_statistics(self) -> dict:
        """Get comprehensive graph statistics for monitoring."""
        return {
            "total_nodes": len(self.nodes),
            "total_edges": sum(len(neighbors) for neighbors in self.adjacency.values())
            // 2,
            "node_types": {
                node_type: len(
                    [n for n in self.nodes.values() if n.node_type == node_type]
                )
                for node_type in ["cluster", "regional_hub", "global_hub"]
            },
            "average_connectivity": sum(
                len(neighbors) for neighbors in self.adjacency.values()
            )
            / len(self.nodes)
            if self.nodes
            else 0,
            "cache_hit_ratio": len(self.path_cache)
            / max(
                1,
                len(self.path_cache)
                + len(
                    [k for k in self.cache_ttl if time.time() - self.cache_ttl[k] > 30]
                ),
            ),
        }


# Example usage demonstrating the missing graph capabilities
def demo_graph_routing():
    """Demonstrate the graph-based routing capabilities."""
    router = GraphBasedFederationRouter()

    # Add federation nodes (mix of clusters and hubs)
    nodes = [
        FederationGraphNode(
            "us-west-hub", "regional_hub", "us-west", (37.7749, -122.4194), 10000
        ),
        FederationGraphNode(
            "us-east-hub", "regional_hub", "us-east", (40.7128, -74.0060), 10000
        ),
        FederationGraphNode(
            "eu-central-hub", "regional_hub", "eu-central", (50.1109, 8.6821), 10000
        ),
        FederationGraphNode(
            "cluster-sf", "cluster", "us-west", (37.7749, -122.4194), 1000
        ),
        FederationGraphNode(
            "cluster-nyc", "cluster", "us-east", (40.7128, -74.0060), 1000
        ),
        FederationGraphNode(
            "cluster-london", "cluster", "eu-central", (51.5074, -0.1278), 1000
        ),
    ]

    for node in nodes:
        router.add_node(node)

    # Add edges representing federation links
    edges = [
        FederationGraphEdge("cluster-sf", "us-west-hub", 5.0, 1000, 0.99),
        FederationGraphEdge("us-west-hub", "us-east-hub", 50.0, 1000, 0.95),
        FederationGraphEdge("us-east-hub", "cluster-nyc", 5.0, 1000, 0.99),
        FederationGraphEdge("us-east-hub", "eu-central-hub", 100.0, 1000, 0.90),
        FederationGraphEdge("eu-central-hub", "cluster-london", 10.0, 1000, 0.99),
        # Direct cluster-to-cluster backup links
        FederationGraphEdge("cluster-sf", "cluster-nyc", 70.0, 500, 0.85),
    ]

    for edge in edges:
        router.add_edge(edge)

    print("🌐 Graph-Based Federation Routing Demo")
    print("=" * 45)

    # Test optimal path finding
    path = router.find_optimal_path("cluster-sf", "cluster-london")
    print(
        f"📍 Optimal path SF → London: {' → '.join(path) if path else 'No path found'}"
    )

    # Test multiple paths for redundancy
    paths = router.find_multiple_paths("cluster-sf", "cluster-london", 2)
    print("🔀 Multiple paths:")
    for i, path in enumerate(paths):
        print(f"   Path {i + 1}: {' → '.join(path)}")

    # Test geographic-aware routing
    geo_path = router.find_path_with_geographic_heuristic(
        "cluster-sf", "cluster-london"
    )
    print(
        f"🗺️  Geographic path: {' → '.join(geo_path) if geo_path else 'No path found'}"
    )

    # Show graph statistics
    stats = router.get_graph_statistics()
    print("\n📊 Graph Statistics:")
    print(f"   Nodes: {stats['total_nodes']}, Edges: {stats['total_edges']}")
    print(f"   Average connectivity: {stats['average_connectivity']:.1f}")
    print(f"   Node types: {stats['node_types']}")


if __name__ == "__main__":
    demo_graph_routing()
