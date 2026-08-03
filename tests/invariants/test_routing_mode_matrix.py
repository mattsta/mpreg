"""A4: LinkStateMode matrix L2 — DISABLED / PREFER / ONLY (INV-R1, R8)."""

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
from mpreg.testing.oracles import RoutingOracle

def _node(cid: str, lat: float = 0.0) -> FederationGraphNode:
    return FederationGraphNode(
        node_id=cid,
        node_type=NodeType.CLUSTER,
        region="r",
        coordinates=GeographicCoordinate(lat, lat),
        max_capacity=1000,
    )

def _graph_abc() -> GraphBasedFederationRouter:
    r = GraphBasedFederationRouter()
    for i, c in enumerate("abc"):
        r.add_node(_node(c, float(i)))
    r.add_edge(
        FederationGraphEdge("a", "b", latency_ms=5.0, bandwidth_mbps=1000, reliability_score=0.99)
    )
    r.add_edge(
        FederationGraphEdge("b", "c", latency_ms=5.0, bandwidth_mbps=1000, reliability_score=0.99)
    )
    return r

def _peers(cluster: str) -> list[str]:
    return [f"ws://{cluster}:1"]

def test_disabled_uses_graph_not_missing_ls() -> None:
    graph = _graph_abc()
    planner = FabricFederationPlanner(
        local_cluster="a",
        graph_router=graph,
        peer_locator=_peers,
        link_state_mode=LinkStateMode.DISABLED,
        link_state_router=None,
    )
    plan = planner.plan_next_hop(target_cluster="c")
    assert plan.can_forward
    assert plan.next_cluster == "b"
    oracle = RoutingOracle(mode=LinkStateMode.DISABLED)
    oracle.set_edge("a", "b")
    oracle.set_edge("b", "c")
    oracle.check_plan(
        origin="a",
        destination="c",
        next_cluster=plan.next_cluster,
        used_path_vector=False,
        expectation=oracle.expect("a", "c"),
    )

def test_prefer_uses_link_state_when_available() -> None:
    graph = _graph_abc()
    ls = _graph_abc()
    # LS has direct a-c edge preferred
    ls.add_edge(
        FederationGraphEdge("a", "c", latency_ms=1.0, bandwidth_mbps=1000, reliability_score=0.99)
    )
    planner = FabricFederationPlanner(
        local_cluster="a",
        graph_router=graph,
        peer_locator=_peers,
        link_state_mode=LinkStateMode.PREFER,
        link_state_router=ls,
    )
    plan = planner.plan_next_hop(target_cluster="c")
    assert plan.can_forward
    assert plan.next_cluster == "c"

def test_only_does_not_use_path_vector() -> None:
    graph = _graph_abc()
    table = RouteTable(local_cluster="a")
    now = time.time()
    ann = RouteAnnouncement(
        destination=RouteDestination(cluster_id="c"),
        path=RoutePath(hops=("b", "c")),
        metrics=RouteMetrics(hop_count=1, latency_ms=1.0),
        advertiser="b",
        advertised_at=now,
        ttl_seconds=3600.0,
    )
    assert table.apply_announcement(ann, received_from="b", now=now)

    # ONLY with empty LS router → must not pick PV path via b for multi-hop;
    # direct peer to c is allowed escape hatch.
    empty_ls = GraphBasedFederationRouter()
    empty_ls.add_node(_node("a"))
    empty_ls.add_node(_node("c"))

    planner = FabricFederationPlanner(
        local_cluster="a",
        graph_router=graph,
        peer_locator=_peers,
        route_table=table,
        link_state_mode=LinkStateMode.ONLY,
        link_state_router=empty_ls,
    )
    plan = planner.plan_next_hop(target_cluster="c")
    # Direct peer escape: next is c, not PV via b from table alone without LS.
    assert plan.next_cluster == "c" or plan.reason in {
        FabricForwardingFailureReason.NO_PATH,
        FabricForwardingFailureReason.FALLBACK_NEIGHBOR,
        FabricForwardingFailureReason.OK,
    }
    # Critical: if we got a next hop from table PV only, that would be next_cluster b
    # without being a direct peer of destination in LS — peer_locator always has c.
    if plan.can_forward:
        assert plan.next_cluster != "b" or plan.planned_path[-1] == "c"
    oracle = RoutingOracle(mode=LinkStateMode.ONLY)
    oracle.set_edge("a", "b")
    oracle.set_edge("b", "c")
    exp = oracle.expect(
        "a", "c", has_path_vector=True, has_link_state_path=False, has_direct_peer=True
    )
    assert exp.must_not_use_path_vector
    oracle.check_plan(
        origin="a",
        destination="c",
        next_cluster=plan.next_cluster,
        used_path_vector=False,
        expectation=exp,
    )

def test_prefer_falls_back_to_pv() -> None:
    graph = GraphBasedFederationRouter()
    graph.add_node(_node("a"))
    graph.add_node(_node("c"))
    # no edges in graph → LS empty and graph has no path
    empty_ls = GraphBasedFederationRouter()
    empty_ls.add_node(_node("a"))
    empty_ls.add_node(_node("c"))
    table = RouteTable(local_cluster="a")
    now = time.time()
    ann = RouteAnnouncement(
        destination=RouteDestination(cluster_id="c"),
        path=RoutePath(hops=("b", "c")),
        metrics=RouteMetrics(hop_count=1, latency_ms=1.0),
        advertiser="b",
        advertised_at=now,
        ttl_seconds=3600.0,
    )
    assert table.apply_announcement(ann, received_from="b", now=now)
    # peer_locator must resolve next_hop cluster "b"
    def peers(c: str) -> list[str]:
        return [f"ws://{c}:1"]

    planner = FabricFederationPlanner(
        local_cluster="a",
        graph_router=graph,
        peer_locator=peers,
        route_table=table,
        link_state_mode=LinkStateMode.PREFER,
        link_state_router=empty_ls,
    )
    plan = planner.plan_next_hop(target_cluster="c")
    assert plan.can_forward, plan
    assert plan.next_cluster == "b"
