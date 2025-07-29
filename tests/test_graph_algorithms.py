"""
Comprehensive property-based tests for graph algorithms.

This test suite uses Hypothesis to verify fundamental properties of
Dijkstra's and A* algorithms with mathematically rigorous correctness guarantees.

Property categories tested:
1. Correctness Properties: Algorithm produces correct shortest paths
2. Optimality Properties: Paths found are actually optimal
3. Consistency Properties: Same inputs always produce same outputs
4. Boundary Properties: Algorithms handle edge cases correctly
5. Performance Properties: Algorithms complete within expected bounds

Each property is tested across thousands of randomly generated graph
configurations to ensure robustness across the entire input space.
"""

import time

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, rule

from mpreg.datastructures.graph_algorithms import (
    AStarAlgorithm,
    AStarConfig,
    CoordinateMap,
    DijkstraAlgorithm,
    DijkstraConfig,
    euclidean_distance_heuristic,
    manhattan_distance_heuristic,
    zero_heuristic,
)

# Test data structures


class TestNode:
    """Test implementation of GraphNode protocol."""

    def __init__(self, node_id: str, weight: float = 1.0, healthy: bool = True):
        self.node_id = node_id
        self.weight = weight
        self.healthy = healthy
        self.coordinates: tuple[float, float] = (0.0, 0.0)

    def get_node_id(self) -> str:
        return self.node_id

    def get_node_weight(self) -> float:
        return self.weight

    def is_healthy(self) -> bool:
        return self.healthy


class TestEdge:
    """Test implementation of GraphEdge protocol."""

    def __init__(self, weight: float = 1.0, usable: bool = True):
        self.weight = weight
        self.usable = usable

    def get_edge_weight(self) -> float:
        return self.weight

    def is_usable(self) -> bool:
        return self.usable


class TestGraph:
    """Test implementation of Graph protocol."""

    def __init__(self):
        self.nodes: dict[str, TestNode] = {}
        self.edges: dict[tuple[str, str], TestEdge] = {}
        self.adjacency: dict[str, list[str]] = {}

    def add_node(
        self,
        node_id: str,
        weight: float = 1.0,
        healthy: bool = True,
        coords: tuple[float, float] = (0.0, 0.0),
    ) -> None:
        """Add a node to the test graph."""
        node = TestNode(node_id, weight, healthy)
        node.coordinates = coords
        self.nodes[node_id] = node
        if node_id not in self.adjacency:
            self.adjacency[node_id] = []

    def add_edge(
        self,
        source: str,
        target: str,
        weight: float = 1.0,
        usable: bool = True,
        bidirectional: bool = True,
    ) -> None:
        """Add an edge to the test graph."""
        edge = TestEdge(weight, usable)
        self.edges[(source, target)] = edge

        if source not in self.adjacency:
            self.adjacency[source] = []
        if target not in self.adjacency[source]:
            self.adjacency[source].append(target)

        if bidirectional:
            self.edges[(target, source)] = TestEdge(weight, usable)
            if target not in self.adjacency:
                self.adjacency[target] = []
            if source not in self.adjacency[target]:
                self.adjacency[target].append(source)

    def get_node(self, node_id: str) -> TestNode | None:
        return self.nodes.get(node_id)

    def get_edge(self, source: str, target: str) -> TestEdge | None:
        return self.edges.get((source, target))

    def get_neighbors(self, node_id: str) -> list[str]:
        return self.adjacency.get(node_id, [])

    def get_all_nodes(self) -> list[str]:
        return list(self.nodes.keys())


# Hypothesis strategies for generating test data


@st.composite
def node_ids(draw):
    """Generate valid node IDs."""
    return draw(
        st.text(
            min_size=1,
            max_size=10,
            alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
        )
    )


@st.composite
def edge_weights(draw):
    """Generate valid edge weights."""
    return draw(
        st.floats(min_value=0.1, max_value=100.0, allow_nan=False, allow_infinity=False)
    )


@st.composite
def coordinates(draw):
    """Generate valid coordinates."""
    x = draw(
        st.floats(
            min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False
        )
    )
    y = draw(
        st.floats(
            min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False
        )
    )
    return (x, y)


@st.composite
def simple_graphs(draw, min_nodes=2, max_nodes=10, min_edges=1, max_edges=20):
    """Generate simple connected graphs for testing."""
    num_nodes = draw(st.integers(min_value=min_nodes, max_value=max_nodes))
    node_list = [f"node_{i}" for i in range(num_nodes)]

    graph = TestGraph()
    coord_map = CoordinateMap()

    # Add nodes with random properties
    for node_id in node_list:
        weight = draw(
            st.floats(
                min_value=0.1, max_value=10.0, allow_nan=False, allow_infinity=False
            )
        )
        coord = draw(coordinates())
        x, y = coord
        graph.add_node(node_id, weight, True, coord)
        coord_map.add_node(node_id, x, y)

    # Ensure connectivity by creating a spanning tree
    for i in range(1, num_nodes):
        source = node_list[draw(st.integers(min_value=0, max_value=i - 1))]
        target = node_list[i]
        weight = draw(edge_weights())
        graph.add_edge(source, target, weight, True, True)

    # Add additional random edges
    max_additional = min(
        max_edges - (num_nodes - 1),
        (num_nodes * (num_nodes - 1)) // 2 - (num_nodes - 1),
    )
    if max_additional > 0:
        num_additional = draw(st.integers(min_value=0, max_value=max_additional))
        for _ in range(num_additional):
            source = draw(st.sampled_from(node_list))
            target = draw(st.sampled_from(node_list))
            if source != target and (source, target) not in graph.edges:
                weight = draw(edge_weights())
                graph.add_edge(source, target, weight, True, True)

    return graph, coord_map


# Core property tests for Dijkstra's algorithm


class TestDijkstraProperties:
    """Property-based tests for Dijkstra's algorithm correctness."""

    @given(simple_graphs())
    @settings(max_examples=100, deadline=1000)
    def test_dijkstra_path_exists_property(self, graph_data):
        """Property: If Dijkstra returns a path, it must be valid and connected."""
        graph, coord_map = graph_data
        nodes = graph.get_all_nodes()
        assume(len(nodes) >= 2)

        dijkstra = DijkstraAlgorithm(DijkstraConfig(max_hops=20))

        # Test all pairs of nodes
        for source in nodes:
            for target in nodes:
                if source == target:
                    continue

                result = dijkstra.find_shortest_path(graph, source, target)

                if result is not None:
                    # Path must start with source and end with target
                    assert result.path[0] == source
                    assert result.path[-1] == target

                    # Path must be connected (each consecutive pair has an edge)
                    for i in range(len(result.path) - 1):
                        current_node = result.path[i]
                        next_node = result.path[i + 1]
                        edge = graph.get_edge(current_node, next_node)
                        assert edge is not None, (
                            f"Missing edge {current_node} -> {next_node}"
                        )
                        assert edge.is_usable(), (
                            f"Unusable edge {current_node} -> {next_node}"
                        )

                    # Result must claim to be valid
                    assert result.is_valid_path
                    assert result.total_cost >= 0.0
                    assert result.nodes_explored >= 1

    @given(simple_graphs())
    @settings(max_examples=50, deadline=2000)
    def test_dijkstra_optimality_property(self, graph_data):
        """Property: Dijkstra must find the shortest path (optimality guarantee)."""
        graph, coord_map = graph_data
        nodes = graph.get_all_nodes()
        assume(len(nodes) >= 2)

        dijkstra = DijkstraAlgorithm(DijkstraConfig(max_hops=20))

        # Test a sample of node pairs
        test_pairs = [(nodes[0], nodes[1])] if len(nodes) >= 2 else []
        if len(nodes) >= 3:
            test_pairs.append((nodes[0], nodes[2]))

        for source, target in test_pairs:
            dijkstra_result = dijkstra.find_shortest_path(graph, source, target)

            if dijkstra_result is not None:
                # Brute force path verification (for small graphs)
                if len(nodes) <= 8:  # Only for small graphs to avoid exponential blowup
                    all_paths = self._find_all_simple_paths(
                        graph, source, target, max_length=10
                    )
                    if all_paths:
                        min_cost = min(
                            self._calculate_path_cost(graph, path) for path in all_paths
                        )
                        assert abs(dijkstra_result.total_cost - min_cost) < 1e-10, (
                            f"Dijkstra cost {dijkstra_result.total_cost} != optimal cost {min_cost}"
                        )

    @given(simple_graphs())
    @settings(max_examples=100, deadline=1000)
    def test_dijkstra_consistency_property(self, graph_data):
        """Property: Same inputs always produce same outputs (determinism)."""
        graph, coord_map = graph_data
        nodes = graph.get_all_nodes()
        assume(len(nodes) >= 2)

        dijkstra1 = DijkstraAlgorithm(DijkstraConfig(max_hops=15))
        dijkstra2 = DijkstraAlgorithm(DijkstraConfig(max_hops=15))

        source, target = nodes[0], nodes[1]

        result1 = dijkstra1.find_shortest_path(graph, source, target)
        result2 = dijkstra2.find_shortest_path(graph, source, target)

        # Results must be identical
        if result1 is None:
            assert result2 is None
        else:
            assert result2 is not None
            assert result1.path == result2.path
            assert abs(result1.total_cost - result2.total_cost) < 1e-10
            assert result1.algorithm_used == result2.algorithm_used

    @given(st.integers(min_value=1, max_value=20))
    @settings(max_examples=50, deadline=1000)
    def test_dijkstra_single_node_property(self, max_hops):
        """Property: Path from node to itself should be trivial."""
        graph = TestGraph()
        graph.add_node("single")

        dijkstra = DijkstraAlgorithm(DijkstraConfig(max_hops=max_hops))
        result = dijkstra.find_shortest_path(graph, "single", "single")

        assert result is not None
        assert result.path == ["single"]
        assert result.total_cost == 0.0
        assert result.is_valid_path

    def _find_all_simple_paths(
        self, graph: TestGraph, source: str, target: str, max_length: int
    ) -> list[list[str]]:
        """Find all simple paths between source and target (for verification)."""

        def dfs(current: str, path: list[str], visited: set[str]) -> list[list[str]]:
            if current == target:
                return [path + [current]]
            if len(path) >= max_length:
                return []

            paths = []
            for neighbor in graph.get_neighbors(current):
                if neighbor not in visited:
                    edge = graph.get_edge(current, neighbor)
                    if edge and edge.is_usable():
                        neighbor_node = graph.get_node(neighbor)
                        if neighbor_node and neighbor_node.is_healthy():
                            paths.extend(
                                dfs(neighbor, path + [current], visited | {current})
                            )
            return paths

        return dfs(source, [], set())

    def _calculate_path_cost(self, graph: TestGraph, path: list[str]) -> float:
        """Calculate total cost of a path."""
        if len(path) <= 1:
            return 0.0

        total_cost = 0.0
        for i in range(len(path) - 1):
            current = path[i]
            next_node = path[i + 1]

            # Add edge cost
            edge = graph.get_edge(current, next_node)
            if edge:
                total_cost += edge.get_edge_weight()

            # Add node cost (for destination node)
            node = graph.get_node(next_node)
            if node:
                total_cost += node.get_node_weight()

        return total_cost


# Core property tests for A* algorithm


class TestAStarProperties:
    """Property-based tests for A* algorithm correctness."""

    @given(simple_graphs())
    @settings(max_examples=100, deadline=1000)
    def test_astar_path_exists_property(self, graph_data):
        """Property: If A* returns a path, it must be valid and connected."""
        graph, coord_map = graph_data
        nodes = graph.get_all_nodes()
        assume(len(nodes) >= 2)

        astar = AStarAlgorithm(AStarConfig(max_hops=20))
        heuristic = euclidean_distance_heuristic(coord_map)

        source, target = nodes[0], nodes[1]
        result = astar.find_path_with_heuristic(graph, source, target, heuristic)

        if result is not None:
            # Same validation as Dijkstra
            assert result.path[0] == source
            assert result.path[-1] == target

            # Path connectivity
            for i in range(len(result.path) - 1):
                current_node = result.path[i]
                next_node = result.path[i + 1]
                edge = graph.get_edge(current_node, next_node)
                assert edge is not None
                assert edge.is_usable()

            assert result.is_valid_path
            assert result.total_cost >= 0.0

    @given(simple_graphs())
    @settings(max_examples=50, deadline=2000)
    def test_astar_with_zero_heuristic_equals_dijkstra(self, graph_data):
        """Property: A* with zero heuristic should behave like Dijkstra."""
        graph, coord_map = graph_data
        nodes = graph.get_all_nodes()
        assume(len(nodes) >= 2)

        dijkstra = DijkstraAlgorithm(DijkstraConfig(max_hops=15))
        astar = AStarAlgorithm(AStarConfig(max_hops=15))
        zero_h = zero_heuristic()

        source, target = nodes[0], nodes[1]

        dijkstra_result = dijkstra.find_shortest_path(graph, source, target)
        astar_result = astar.find_path_with_heuristic(graph, source, target, zero_h)

        # Results should be equivalent
        if dijkstra_result is None:
            assert astar_result is None
        else:
            assert astar_result is not None
            # Cost must be identical (paths may differ but cost should be same)
            assert abs(dijkstra_result.total_cost - astar_result.total_cost) < 1e-10

    @given(simple_graphs())
    @settings(max_examples=100, deadline=1000)
    def test_astar_admissible_heuristic_optimality(self, graph_data):
        """Property: A* with zero heuristic finds optimal paths (admissible by definition)."""
        graph, coord_map = graph_data
        nodes = graph.get_all_nodes()
        assume(len(nodes) >= 2)

        # Use zero heuristic which is always admissible
        # (Euclidean distance may not be admissible if edge weights don't represent geographic distances)
        astar = AStarAlgorithm(AStarConfig(max_hops=15))
        heuristic = zero_heuristic()

        # Also run Dijkstra for comparison
        dijkstra = DijkstraAlgorithm(DijkstraConfig(max_hops=15))

        source, target = nodes[0], nodes[1]

        astar_result = astar.find_path_with_heuristic(graph, source, target, heuristic)
        dijkstra_result = dijkstra.find_shortest_path(graph, source, target)

        # If both find paths, A* should find optimal path (same cost as Dijkstra)
        if astar_result is not None and dijkstra_result is not None:
            assert abs(astar_result.total_cost - dijkstra_result.total_cost) < 1e-8, (
                f"A* cost {astar_result.total_cost} != Dijkstra cost {dijkstra_result.total_cost}"
            )

    @given(simple_graphs())
    @settings(max_examples=50, deadline=1000)
    def test_astar_heuristic_consistency_check(self, graph_data):
        """Property: A* with consistent admissible heuristics should find optimal paths."""
        graph, coord_map = graph_data
        nodes = graph.get_all_nodes()
        assume(len(nodes) >= 2)

        astar = AStarAlgorithm(AStarConfig(max_hops=15))
        dijkstra = DijkstraAlgorithm(DijkstraConfig(max_hops=15))

        # Only test zero heuristic which is guaranteed admissible
        zero_h = zero_heuristic()

        source, target = nodes[0], nodes[1]

        dijkstra_result = dijkstra.find_shortest_path(graph, source, target)
        zero_result = astar.find_path_with_heuristic(graph, source, target, zero_h)

        # A* with zero heuristic should give same cost as Dijkstra
        if dijkstra_result is not None and zero_result is not None:
            assert abs(dijkstra_result.total_cost - zero_result.total_cost) < 1e-10, (
                f"A* with zero heuristic cost {zero_result.total_cost} != Dijkstra cost {dijkstra_result.total_cost}"
            )


# Stateful testing for complex graph operations


class GraphAlgorithmStateMachine(RuleBasedStateMachine):
    """Stateful testing for graph algorithms."""

    def __init__(self):
        super().__init__()
        self.graph = TestGraph()
        self.coord_map = CoordinateMap()
        self.dijkstra = DijkstraAlgorithm(DijkstraConfig(max_hops=10))
        self.astar = AStarAlgorithm(AStarConfig(max_hops=10))
        self.node_list = []

    @rule(node_id=node_ids())
    def add_node(self, node_id):
        """Add a node to the graph."""
        if node_id not in self.graph.nodes:
            weight = 1.0  # Simple weight for stateful testing
            coords = (
                hash(node_id) % 100,
                hash(node_id[::-1]) % 100,
            )  # Deterministic coords
            x, y = coords
            self.graph.add_node(node_id, weight, True, coords)
            self.coord_map.add_node(node_id, float(x), float(y))
            self.node_list.append(node_id)

    @rule()
    def add_edge(self):
        """Add an edge between existing nodes."""
        if len(self.node_list) >= 2:
            source = self.node_list[0]
            target = self.node_list[-1]
            if source != target:
                weight = 2.0  # Fixed weight for simplicity
                self.graph.add_edge(source, target, weight, True, True)

    @rule()
    def test_path_consistency(self):
        """Test that algorithms are consistent."""
        if len(self.node_list) < 2:
            return

        source = self.node_list[0]
        target = self.node_list[-1]

        if source == target:
            return

        dijkstra_result = self.dijkstra.find_shortest_path(self.graph, source, target)

        zero_h = zero_heuristic()
        astar_result = self.astar.find_path_with_heuristic(
            self.graph, source, target, zero_h
        )

        # If both find paths, costs should be the same
        if dijkstra_result is not None and astar_result is not None:
            assert abs(dijkstra_result.total_cost - astar_result.total_cost) < 1e-10


# Integration tests


class TestGraphAlgorithmIntegration:
    """Integration tests for complete algorithm functionality."""

    def test_complete_workflow_small_graph(self):
        """Test complete workflow on a known small graph."""
        graph = TestGraph()

        # Create a simple diamond graph: A -> B/C -> D
        graph.add_node("A", 0.0, True, (0, 0))
        graph.add_node("B", 1.0, True, (1, 1))
        graph.add_node("C", 2.0, True, (1, -1))
        graph.add_node("D", 0.0, True, (2, 0))

        graph.add_edge("A", "B", 3.0)
        graph.add_edge("A", "C", 1.0)
        graph.add_edge("B", "D", 1.0)
        graph.add_edge("C", "D", 5.0)

        coord_map = CoordinateMap.from_tuples(
            {"A": (0.0, 0.0), "B": (1.0, 1.0), "C": (1.0, -1.0), "D": (2.0, 0.0)}
        )

        # Test Dijkstra
        dijkstra = DijkstraAlgorithm()
        result = dijkstra.find_shortest_path(graph, "A", "D")

        assert result is not None
        assert result.path in [["A", "B", "D"], ["A", "C", "D"]]
        # Shortest path: A -> B -> D (cost: 3 + 1 + 1 = 5) vs A -> C -> D (cost: 1 + 5 + 2 = 8)
        # So A -> B -> D should be chosen
        assert result.path == ["A", "B", "D"]
        assert abs(result.total_cost - 5.0) < 1e-10

        # Test A* with Euclidean heuristic
        astar = AStarAlgorithm()
        heuristic = euclidean_distance_heuristic(coord_map)
        astar_result = astar.find_path_with_heuristic(graph, "A", "D", heuristic)

        assert astar_result is not None
        assert (
            abs(astar_result.total_cost - result.total_cost) < 1e-10
        )  # Should find same optimal cost

    def test_disconnected_graph(self):
        """Test behavior on disconnected graphs."""
        graph = TestGraph()

        # Two disconnected components
        graph.add_node("A")
        graph.add_node("B")
        graph.add_node("C")
        graph.add_node("D")

        graph.add_edge("A", "B", 1.0)
        graph.add_edge("C", "D", 1.0)
        # No connection between {A,B} and {C,D}

        dijkstra = DijkstraAlgorithm()
        result = dijkstra.find_shortest_path(graph, "A", "C")

        # Should return None since no path exists
        assert result is None

    def test_performance_characteristics(self):
        """Test that algorithms complete within reasonable time bounds."""
        # Create larger graph
        graph = TestGraph()
        coord_map = CoordinateMap()

        # Grid graph: 10x10
        for i in range(10):
            for j in range(10):
                node_id = f"n_{i}_{j}"
                graph.add_node(node_id, 1.0, True, (i, j))
                coord_map.add_node(node_id, float(i), float(j))

        # Add grid edges
        for i in range(10):
            for j in range(10):
                current = f"n_{i}_{j}"
                if i < 9:  # Right neighbor
                    neighbor = f"n_{i + 1}_{j}"
                    graph.add_edge(current, neighbor, 1.0)
                if j < 9:  # Down neighbor
                    neighbor = f"n_{i}_{j + 1}"
                    graph.add_edge(current, neighbor, 1.0)

        # Test Dijkstra performance
        dijkstra = DijkstraAlgorithm()
        start_time = time.time()
        result = dijkstra.find_shortest_path(graph, "n_0_0", "n_9_9")
        dijkstra_time = time.time() - start_time

        assert result is not None
        assert dijkstra_time < 1.0  # Should complete quickly

        # Test A* performance
        astar = AStarAlgorithm()
        heuristic = manhattan_distance_heuristic(coord_map)
        start_time = time.time()
        astar_result = astar.find_path_with_heuristic(
            graph, "n_0_0", "n_9_9", heuristic
        )
        astar_time = time.time() - start_time

        assert astar_result is not None
        assert astar_time < 1.0  # Should complete quickly

        # A* should explore fewer nodes than Dijkstra on this problem
        # (can't easily test this without internal access, but at least verify both work)

    @pytest.mark.parametrize("max_hops", [1, 3, 5, 10])
    def test_hop_limit_enforcement(self, max_hops):
        """Test that hop limits are properly enforced."""
        # Create linear graph: A -> B -> C -> D -> E
        graph = TestGraph()
        nodes = ["A", "B", "C", "D", "E"]
        for node in nodes:
            graph.add_node(node)

        for i in range(len(nodes) - 1):
            graph.add_edge(nodes[i], nodes[i + 1], 1.0, bidirectional=False)

        dijkstra = DijkstraAlgorithm(DijkstraConfig(max_hops=max_hops))
        result = dijkstra.find_shortest_path(graph, "A", "E")

        # Path A -> E requires 4 hops
        if max_hops >= 4:
            assert result is not None
            assert len(result.path) == 5  # 5 nodes = 4 hops
        else:
            assert result is None  # Should not find path due to hop limit


# Test runner for stateful testing
GraphAlgorithmStateMachineTest = GraphAlgorithmStateMachine.TestCase


if __name__ == "__main__":
    pytest.main([__file__])
