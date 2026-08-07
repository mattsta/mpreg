import time
from dataclasses import dataclass

from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    FunctionSelector,
    SemanticVersion,
)
from mpreg.fabric.catalog import FunctionEndpoint
from mpreg.fabric.engine import FunctionRouteReason, RoutingEngine
from mpreg.fabric.federation_planner import (
    FabricForwardingFailureReason,
    FabricForwardingPlan,
)
from mpreg.fabric.index import FunctionQuery, RoutingIndex
from tests.test_helpers import TestPortManager


def _endpoint(
    *,
    node_id: str,
    cluster_id: str,
    resources: frozenset[str],
) -> FunctionEndpoint:
    identity = FunctionIdentity(
        name="mesh_function",
        function_id="func-mesh",
        version=SemanticVersion.parse("1.0.0"),
    )
    return FunctionEndpoint(
        identity=identity,
        resources=resources,
        node_id=node_id,
        cluster_id=cluster_id,
        advertised_at=time.time(),
        ttl_seconds=30.0,
    )


@dataclass(slots=True)
class _StubFederationPlanner:
    next_peer_url: str | None

    def plan_next_hop(
        self,
        *,
        target_cluster: str | None,
        visited_clusters: tuple[str, ...] = (),
        remaining_hops: int | None = None,
    ) -> FabricForwardingPlan:
        remaining = 0 if remaining_hops is None else remaining_hops
        next_peer = self.next_peer_url if remaining > 0 else None
        reason = (
            FabricForwardingFailureReason.OK
            if next_peer
            else FabricForwardingFailureReason.HOP_BUDGET_EXHAUSTED
        )
        return FabricForwardingPlan(
            target_cluster=target_cluster or "",
            next_cluster=target_cluster,
            next_peer_url=next_peer,
            planned_path=(target_cluster or "",),
            federation_path=visited_clusters,
            remaining_hops=max(0, remaining - 1),
            reason=reason,
        )


def test_routing_engine_local_match() -> None:
    with TestPortManager() as port_manager:
        node_id = port_manager.get_server_url()
        index = RoutingIndex()
        entry = _endpoint(
            node_id=node_id,
            cluster_id="cluster-a",
            resources=frozenset({"mesh-resource"}),
        )
        assert index.catalog.functions.register(entry, now=100.0)
        engine = RoutingEngine(local_cluster="cluster-a", routing_index=index)

        selector = FunctionSelector(name="mesh_function", function_id="func-mesh")
        plan = engine.plan_function_route(
            FunctionQuery(selector=selector, resources=frozenset({"mesh-resource"}))
        )

        assert plan.is_local
        assert plan.selected_target == entry
        assert plan.reason is FunctionRouteReason.LOCAL_MATCH


def test_routing_engine_resource_mismatch() -> None:
    with TestPortManager() as port_manager:
        node_id = port_manager.get_server_url()
        index = RoutingIndex()
        entry = _endpoint(
            node_id=node_id,
            cluster_id="cluster-a",
            resources=frozenset({"cpu"}),
        )
        assert index.catalog.functions.register(entry, now=100.0)
        engine = RoutingEngine(local_cluster="cluster-a", routing_index=index)

        selector = FunctionSelector(name="mesh_function", function_id="func-mesh")
        plan = engine.plan_function_route(
            FunctionQuery(selector=selector, resources=frozenset({"gpu"}))
        )

        assert plan.selected_target is None
        assert plan.reason is FunctionRouteReason.NO_MATCH


def test_routing_engine_remote_requires_planner() -> None:
    with TestPortManager() as port_manager:
        node_id = port_manager.get_server_url()
        index = RoutingIndex()
        entry = _endpoint(
            node_id=node_id,
            cluster_id="cluster-b",
            resources=frozenset({"mesh-resource"}),
        )
        assert index.catalog.functions.register(entry, now=100.0)
        engine = RoutingEngine(local_cluster="cluster-a", routing_index=index)

        selector = FunctionSelector(function_id="func-mesh")
        plan = engine.plan_function_route(
            FunctionQuery(
                selector=selector,
                resources=frozenset({"mesh-resource"}),
                cluster_id="cluster-b",
            )
        )

        assert plan.selected_target == entry
        assert plan.reason is FunctionRouteReason.NO_FEDERATION


def test_routing_engine_hop_budget_blocks_remote() -> None:
    with TestPortManager() as port_manager:
        node_id = port_manager.get_server_url()
        index = RoutingIndex()
        entry = _endpoint(
            node_id=node_id,
            cluster_id="cluster-b",
            resources=frozenset({"mesh-resource"}),
        )
        assert index.catalog.functions.register(entry, now=100.0)
        engine = RoutingEngine(
            local_cluster="cluster-a",
            routing_index=index,
            federation_planner=_StubFederationPlanner(next_peer_url="peer-b"),
        )

        selector = FunctionSelector(function_id="func-mesh")
        plan = engine.plan_function_route(
            FunctionQuery(
                selector=selector,
                resources=frozenset({"mesh-resource"}),
                cluster_id="cluster-b",
            ),
            hop_budget=0,
        )

        assert plan.selected_target == entry
        assert plan.forwarding is not None
        assert (
            plan.forwarding.reason is FabricForwardingFailureReason.HOP_BUDGET_EXHAUSTED
        )
        assert plan.reason is FunctionRouteReason.REMOTE_UNAVAILABLE
