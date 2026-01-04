from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    FunctionSelector,
    SemanticVersion,
)
from mpreg.fabric import (
    ClusterRouteReason,
    FunctionEndpoint,
    FunctionQuery,
    FunctionRouteReason,
    RoutingEngine,
    RoutingIndex,
)
from mpreg.fabric.federation_planner import (
    FabricForwardingFailureReason,
    FabricForwardingPlan,
)


class StubFederationPlanner:
    def __init__(self, plan: FabricForwardingPlan) -> None:
        self.plan = plan
        self.calls: list[tuple[str | None, tuple[str, ...], int | None]] = []

    def plan_next_hop(
        self,
        *,
        target_cluster: str | None,
        visited_clusters: tuple[str, ...] = (),
        remaining_hops: int | None = None,
    ) -> FabricForwardingPlan:
        self.calls.append((target_cluster, visited_clusters, remaining_hops))
        return self.plan


def test_routing_engine_local_match() -> None:
    index = RoutingIndex()
    endpoint = FunctionEndpoint(
        identity=FunctionIdentity(
            name="echo",
            function_id="func-echo",
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset({"cpu"}),
        node_id="node-1",
        cluster_id="cluster-a",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    index.catalog.functions.register(endpoint, now=100.0)

    engine = RoutingEngine(local_cluster="cluster-a", routing_index=index)
    plan = engine.plan_function_route(
        FunctionQuery(
            selector=FunctionSelector(name="echo"),
            resources=frozenset({"cpu"}),
        ),
        now=105.0,
    )

    assert plan.is_local is True
    assert plan.targets == (endpoint,)
    assert plan.selected_target == endpoint
    assert plan.forwarding is None
    assert plan.reason is FunctionRouteReason.LOCAL_MATCH


def test_routing_engine_remote_planned() -> None:
    index = RoutingIndex()
    endpoint = FunctionEndpoint(
        identity=FunctionIdentity(
            name="echo",
            function_id="func-echo",
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset({"cpu"}),
        node_id="node-remote",
        cluster_id="cluster-b",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    index.catalog.functions.register(endpoint, now=100.0)
    forwarding = FabricForwardingPlan(
        target_cluster="cluster-b",
        next_cluster="cluster-b",
        next_peer_url="ws://peer-1",
        planned_path=("cluster-a", "cluster-b"),
        federation_path=("cluster-a",),
        remaining_hops=3,
        reason=FabricForwardingFailureReason.OK,
    )
    planner = StubFederationPlanner(plan=forwarding)
    engine = RoutingEngine(
        local_cluster="cluster-a",
        routing_index=index,
        federation_planner=planner,
    )

    plan = engine.plan_function_route(
        FunctionQuery(
            selector=FunctionSelector(name="echo"),
            cluster_id="cluster-b",
        ),
        routing_path=("cluster-a",),
        hop_budget=4,
        now=105.0,
    )

    assert plan.is_local is False
    assert plan.forwarding is forwarding
    assert plan.selected_target == endpoint
    assert plan.reason is FunctionRouteReason.REMOTE_PLANNED
    assert planner.calls == [("cluster-b", ("cluster-a",), 4)]


def test_routing_engine_remote_without_planner() -> None:
    index = RoutingIndex()
    endpoint = FunctionEndpoint(
        identity=FunctionIdentity(
            name="echo",
            function_id="func-echo",
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset({"cpu"}),
        node_id="node-remote",
        cluster_id="cluster-b",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    index.catalog.functions.register(endpoint, now=100.0)
    engine = RoutingEngine(local_cluster="cluster-a", routing_index=index)

    plan = engine.plan_function_route(
        FunctionQuery(
            selector=FunctionSelector(name="echo"),
            cluster_id="cluster-b",
        ),
        now=105.0,
    )

    assert plan.is_local is False
    assert plan.forwarding is None
    assert plan.selected_target == endpoint
    assert plan.reason is FunctionRouteReason.NO_FEDERATION


def test_routing_engine_cluster_route_local() -> None:
    engine = RoutingEngine(local_cluster="cluster-a", routing_index=RoutingIndex())

    plan = engine.plan_cluster_route("cluster-a")

    assert plan.is_local is True
    assert plan.forwarding is None
    assert plan.reason is ClusterRouteReason.LOCAL


def test_routing_engine_cluster_route_remote_planned() -> None:
    forwarding = FabricForwardingPlan(
        target_cluster="cluster-b",
        next_cluster="cluster-b",
        next_peer_url="ws://peer-1",
        planned_path=("cluster-a", "cluster-b"),
        federation_path=("cluster-a",),
        remaining_hops=2,
        reason=FabricForwardingFailureReason.OK,
    )
    planner = StubFederationPlanner(plan=forwarding)
    engine = RoutingEngine(
        local_cluster="cluster-a",
        routing_index=RoutingIndex(),
        federation_planner=planner,
    )

    plan = engine.plan_cluster_route(
        "cluster-b", routing_path=("cluster-a",), hop_budget=3
    )

    assert plan.is_local is False
    assert plan.forwarding is forwarding
    assert plan.reason is ClusterRouteReason.REMOTE_PLANNED
    assert planner.calls == [("cluster-b", ("cluster-a",), 3)]


def test_routing_engine_cluster_route_remote_unavailable() -> None:
    forwarding = FabricForwardingPlan(
        target_cluster="cluster-b",
        next_cluster=None,
        next_peer_url=None,
        planned_path=("cluster-a", "cluster-b"),
        federation_path=("cluster-a",),
        remaining_hops=0,
        reason=FabricForwardingFailureReason.NO_PATH,
    )
    planner = StubFederationPlanner(plan=forwarding)
    engine = RoutingEngine(
        local_cluster="cluster-a",
        routing_index=RoutingIndex(),
        federation_planner=planner,
    )

    plan = engine.plan_cluster_route("cluster-b")

    assert plan.is_local is False
    assert plan.forwarding is forwarding
    assert plan.reason is ClusterRouteReason.REMOTE_UNAVAILABLE
