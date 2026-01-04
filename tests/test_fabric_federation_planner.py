"""Fabric federation planner tests."""

from __future__ import annotations

import time

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
from mpreg.fabric.link_state import LinkStateMode
from mpreg.fabric.route_control import (
    RouteAnnouncement,
    RouteDestination,
    RouteMetrics,
    RoutePath,
    RouteTable,
)


def _make_router() -> GraphBasedFederationRouter:
    router = GraphBasedFederationRouter()
    router.add_node(
        FederationGraphNode(
            node_id="cluster-a",
            node_type=NodeType.CLUSTER,
            region="local",
            coordinates=GeographicCoordinate(0.0, 0.0),
            max_capacity=1000,
        )
    )
    router.add_node(
        FederationGraphNode(
            node_id="cluster-b",
            node_type=NodeType.CLUSTER,
            region="hub",
            coordinates=GeographicCoordinate(1.0, 1.0),
            max_capacity=1000,
        )
    )
    router.add_node(
        FederationGraphNode(
            node_id="cluster-c",
            node_type=NodeType.CLUSTER,
            region="remote",
            coordinates=GeographicCoordinate(2.0, 2.0),
            max_capacity=1000,
        )
    )
    router.add_edge(
        FederationGraphEdge(
            "cluster-a",
            "cluster-b",
            latency_ms=5.0,
            bandwidth_mbps=1000,
            reliability_score=0.99,
        )
    )
    router.add_edge(
        FederationGraphEdge(
            "cluster-b",
            "cluster-c",
            latency_ms=8.0,
            bandwidth_mbps=1000,
            reliability_score=0.97,
        )
    )
    return router


def _make_link_state_router() -> GraphBasedFederationRouter:
    router = GraphBasedFederationRouter()
    router.add_node(
        FederationGraphNode(
            node_id="cluster-a",
            node_type=NodeType.CLUSTER,
            region="local",
            coordinates=GeographicCoordinate(0.0, 0.0),
            max_capacity=1000,
        )
    )
    router.add_node(
        FederationGraphNode(
            node_id="cluster-c",
            node_type=NodeType.CLUSTER,
            region="remote",
            coordinates=GeographicCoordinate(2.0, 2.0),
            max_capacity=1000,
        )
    )
    router.add_edge(
        FederationGraphEdge(
            "cluster-a",
            "cluster-c",
            latency_ms=12.0,
            bandwidth_mbps=1000,
            reliability_score=0.98,
        )
    )
    return router


def _make_link_state_ecmp_router() -> GraphBasedFederationRouter:
    router = GraphBasedFederationRouter()
    for node_id in ("cluster-a", "cluster-b", "cluster-c", "cluster-d"):
        router.add_node(
            FederationGraphNode(
                node_id=node_id,
                node_type=NodeType.CLUSTER,
                region="ecmp",
                coordinates=GeographicCoordinate(0.0, 0.0),
                max_capacity=1000,
            )
        )
    router.add_edge(
        FederationGraphEdge(
            "cluster-a",
            "cluster-b",
            latency_ms=5.0,
            bandwidth_mbps=1000,
            reliability_score=0.99,
        )
    )
    router.add_edge(
        FederationGraphEdge(
            "cluster-b",
            "cluster-d",
            latency_ms=5.0,
            bandwidth_mbps=1000,
            reliability_score=0.99,
        )
    )
    router.add_edge(
        FederationGraphEdge(
            "cluster-a",
            "cluster-c",
            latency_ms=5.0,
            bandwidth_mbps=1000,
            reliability_score=0.99,
        )
    )
    router.add_edge(
        FederationGraphEdge(
            "cluster-c",
            "cluster-d",
            latency_ms=5.0,
            bandwidth_mbps=1000,
            reliability_score=0.99,
        )
    )
    return router


def test_route_table_plan_preferred() -> None:
    table = RouteTable(local_cluster="cluster-a")
    announcement = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-c"),
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(hop_count=1, latency_ms=8.0),
        advertiser="cluster-b",
        advertised_at=time.time(),
        ttl_seconds=30.0,
        epoch=1,
    )
    assert table.apply_announcement(announcement, received_from="cluster-b")

    router = _make_router()
    planner = FabricFederationPlanner(
        local_cluster="cluster-a",
        graph_router=router,
        peer_locator=lambda cluster_id: [f"ws://{cluster_id}"],
        route_table=table,
    )

    plan = planner.plan_next_hop(target_cluster="cluster-c")
    assert plan.reason is FabricForwardingFailureReason.OK
    assert plan.next_cluster == "cluster-b"
    assert plan.next_peer_url == "ws://cluster-b"


def test_graph_router_fallback_path() -> None:
    router = _make_router()
    planner = FabricFederationPlanner(
        local_cluster="cluster-a",
        graph_router=router,
        peer_locator=lambda cluster_id: [f"ws://{cluster_id}"],
    )

    plan = planner.plan_next_hop(target_cluster="cluster-c")
    assert plan.reason is FabricForwardingFailureReason.OK
    assert plan.next_cluster == "cluster-b"
    assert plan.planned_path == ("cluster-a", "cluster-b", "cluster-c")


def test_link_state_preferred_over_route_table() -> None:
    table = RouteTable(local_cluster="cluster-a")
    announcement = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-c"),
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(hop_count=1, latency_ms=8.0),
        advertiser="cluster-b",
        advertised_at=time.time(),
        ttl_seconds=30.0,
        epoch=1,
    )
    assert table.apply_announcement(announcement, received_from="cluster-b")

    graph_router = _make_router()
    link_state_router = _make_link_state_router()
    planner = FabricFederationPlanner(
        local_cluster="cluster-a",
        graph_router=graph_router,
        link_state_router=link_state_router,
        link_state_mode=LinkStateMode.PREFER,
        peer_locator=lambda cluster_id: [f"ws://{cluster_id}"],
        route_table=table,
    )

    plan = planner.plan_next_hop(target_cluster="cluster-c")
    assert plan.reason is FabricForwardingFailureReason.OK
    assert plan.next_cluster == "cluster-c"
    assert plan.planned_path == ("cluster-a", "cluster-c")


def test_link_state_ecmp_round_robin() -> None:
    link_state_router = _make_link_state_ecmp_router()
    planner = FabricFederationPlanner(
        local_cluster="cluster-a",
        graph_router=_make_router(),
        link_state_router=link_state_router,
        link_state_mode=LinkStateMode.ONLY,
        link_state_ecmp_paths=2,
        peer_locator=lambda cluster_id: [f"ws://{cluster_id}"],
    )

    plan_a = planner.plan_next_hop(target_cluster="cluster-d")
    plan_b = planner.plan_next_hop(target_cluster="cluster-d")

    assert plan_a.reason is FabricForwardingFailureReason.OK
    assert plan_b.reason is FabricForwardingFailureReason.OK
    assert plan_a.planned_path != plan_b.planned_path
    assert {plan_a.planned_path, plan_b.planned_path} == {
        ("cluster-a", "cluster-b", "cluster-d"),
        ("cluster-a", "cluster-c", "cluster-d"),
    }


def test_visited_local_cluster_is_ignored() -> None:
    router = _make_router()
    planner = FabricFederationPlanner(
        local_cluster="cluster-a",
        graph_router=router,
        peer_locator=lambda cluster_id: [f"ws://{cluster_id}"],
    )

    plan = planner.plan_next_hop(
        target_cluster="cluster-c", visited_clusters=("cluster-x", "cluster-a")
    )

    assert plan.reason is FabricForwardingFailureReason.OK
    assert plan.next_cluster == "cluster-b"
    assert plan.federation_path == ("cluster-x", "cluster-a")


def test_hop_budget_exhausted() -> None:
    router = _make_router()
    planner = FabricFederationPlanner(
        local_cluster="cluster-a",
        graph_router=router,
        peer_locator=lambda cluster_id: [f"ws://{cluster_id}"],
        default_max_hops=0,
    )

    plan = planner.plan_next_hop(target_cluster="cluster-c")
    assert plan.reason is FabricForwardingFailureReason.HOP_BUDGET_EXHAUSTED
    assert plan.next_peer_url is None
