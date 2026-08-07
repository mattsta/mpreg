"""Unit tests for federated RPC routing decisions."""

from dataclasses import dataclass

from mpreg.fabric.federation_graph import (
    FederationGraphEdge,
    FederationGraphNode,
    GeographicCoordinate,
    GraphBasedFederationRouter,
    NodeType,
)
from mpreg.fabric.federation_planner import (
    FabricFederationPlanner,
    FabricForwardingFailureReason,
)
from mpreg.fabric.peer_directory import PeerNeighbor


@dataclass(frozen=True, slots=True)
class ClusterLink:
    source_id: str
    target_id: str


def _build_graph(
    edges: list[ClusterLink], nodes: tuple[str, ...] | None = None
) -> GraphBasedFederationRouter:
    router = GraphBasedFederationRouter(cache_ttl_seconds=30.0, max_cache_size=100)
    node_set = {edge.source_id for edge in edges} | {edge.target_id for edge in edges}
    if nodes:
        node_set.update(nodes)
    for node_id in node_set:
        router.add_node(
            FederationGraphNode(
                node_id=node_id,
                node_type=NodeType.CLUSTER,
                region="test",
                coordinates=GeographicCoordinate(0.0, 0.0),
                max_capacity=100,
                current_load=0.0,
                health_score=1.0,
                processing_latency_ms=1.0,
                bandwidth_mbps=1000,
                reliability_score=1.0,
            )
        )
    for edge in edges:
        router.add_edge(
            FederationGraphEdge(
                source_id=edge.source_id,
                target_id=edge.target_id,
                latency_ms=5.0,
                bandwidth_mbps=1000,
                reliability_score=0.99,
            )
        )
    return router


def _peer_locator(mapping: dict[str, list[str]]):
    return lambda cluster_id: mapping.get(cluster_id, [])


def test_plan_next_hop_direct() -> None:
    router = _build_graph([ClusterLink("cluster-a", "cluster-b")])
    planner = FabricFederationPlanner(
        local_cluster="cluster-a",
        graph_router=router,
        peer_locator=_peer_locator({"cluster-b": ["ws://peer-b"]}),
        default_max_hops=3,
    )

    plan = planner.plan_next_hop(
        target_cluster="cluster-b",
        visited_clusters=(),
        remaining_hops=None,
    )

    assert plan.can_forward
    assert plan.next_cluster == "cluster-b"
    assert plan.next_peer_url == "ws://peer-b"
    assert plan.planned_path == ("cluster-a", "cluster-b")
    assert plan.federation_path == ("cluster-a",)
    assert plan.remaining_hops == 2
    assert plan.reason == FabricForwardingFailureReason.OK


def test_plan_next_hop_loop_detected() -> None:
    router = _build_graph([ClusterLink("cluster-a", "cluster-b")])
    planner = FabricFederationPlanner(
        local_cluster="cluster-a",
        graph_router=router,
        peer_locator=_peer_locator({"cluster-b": ["ws://peer-b"]}),
        default_max_hops=3,
    )

    plan = planner.plan_next_hop(
        target_cluster="cluster-b",
        visited_clusters=("cluster-a", "cluster-b"),
        remaining_hops=2,
    )

    assert not plan.can_forward
    assert plan.reason == FabricForwardingFailureReason.LOOP_DETECTED


def test_plan_next_hop_no_path() -> None:
    router = _build_graph([], nodes=("cluster-a", "cluster-b"))
    planner = FabricFederationPlanner(
        local_cluster="cluster-a",
        graph_router=router,
        peer_locator=_peer_locator({}),
        default_max_hops=3,
    )

    plan = planner.plan_next_hop(
        target_cluster="cluster-b",
        visited_clusters=(),
        remaining_hops=3,
    )

    assert not plan.can_forward
    assert plan.reason == FabricForwardingFailureReason.NO_PATH


def test_plan_next_hop_no_peer_for_next_cluster() -> None:
    router = _build_graph([ClusterLink("cluster-a", "cluster-b")])
    planner = FabricFederationPlanner(
        local_cluster="cluster-a",
        graph_router=router,
        peer_locator=_peer_locator({}),
        default_max_hops=3,
    )

    plan = planner.plan_next_hop(
        target_cluster="cluster-b",
        visited_clusters=(),
        remaining_hops=3,
    )

    assert not plan.can_forward
    assert plan.reason == FabricForwardingFailureReason.NO_PEER
    assert plan.planned_path == ("cluster-a", "cluster-b")


def test_plan_next_hop_hop_budget_exhausted() -> None:
    router = _build_graph([ClusterLink("cluster-a", "cluster-b")])
    planner = FabricFederationPlanner(
        local_cluster="cluster-a",
        graph_router=router,
        peer_locator=_peer_locator({"cluster-b": ["ws://peer-b"]}),
        default_max_hops=3,
    )

    plan = planner.plan_next_hop(
        target_cluster="cluster-b",
        visited_clusters=(),
        remaining_hops=0,
    )

    assert not plan.can_forward
    assert plan.reason == FabricForwardingFailureReason.HOP_BUDGET_EXHAUSTED


def test_plan_next_hop_fallback_neighbor() -> None:
    router = _build_graph([], nodes=("cluster-a", "cluster-b", "cluster-c"))
    planner = FabricFederationPlanner(
        local_cluster="cluster-a",
        graph_router=router,
        peer_locator=_peer_locator({}),
        neighbor_locator=lambda: [
            PeerNeighbor(cluster_id="cluster-b", node_id="ws://peer-b")
        ],
        default_max_hops=2,
    )

    plan = planner.plan_next_hop(
        target_cluster="cluster-c",
        visited_clusters=(),
        remaining_hops=2,
    )

    assert plan.can_forward
    assert plan.next_cluster == "cluster-b"
    assert plan.next_peer_url == "ws://peer-b"
    assert plan.reason == FabricForwardingFailureReason.FALLBACK_NEIGHBOR
