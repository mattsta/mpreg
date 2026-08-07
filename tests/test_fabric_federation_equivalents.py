import time

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.datastructures.function_identity import FunctionIdentity, SemanticVersion
from mpreg.fabric.catalog import (
    CacheRole,
    CacheRoleEntry,
    FunctionEndpoint,
    QueueEndpoint,
)
from mpreg.fabric.engine import RoutingEngine
from mpreg.fabric.federation_planner import (
    FabricForwardingFailureReason,
    FabricForwardingPlan,
)
from mpreg.fabric.index import RoutingIndex
from mpreg.fabric.message import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    RoutingPriority,
    UnifiedMessage,
)
from mpreg.fabric.router import (
    FabricRouter,
    FabricRouteReason,
    FabricRoutingConfig,
    FabricRoutingPolicy,
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


def _make_message(
    *,
    topic: str,
    message_type: MessageType,
    target_cluster: str | None = None,
    federation_path: tuple[str, ...] = (),
    hop_budget: int | None = None,
) -> UnifiedMessage:
    headers = MessageHeaders(
        correlation_id="corr-1",
        source_cluster="cluster-a",
        target_cluster=target_cluster,
        federation_path=federation_path,
        hop_budget=hop_budget,
        priority=RoutingPriority.NORMAL,
    )
    return UnifiedMessage(
        message_id="msg-1",
        topic=topic,
        message_type=message_type,
        delivery=DeliveryGuarantee.AT_LEAST_ONCE,
        payload={},
        headers=headers,
        timestamp=123.0,
    )


def test_policy_pattern_matching() -> None:
    policy = FabricRoutingPolicy(
        policy_id="policy-test",
        message_type_filter={MessageType.RPC},
        topic_pattern_filter="user.*.login",
    )

    message_match = _make_message(topic="user.123.login", message_type=MessageType.RPC)
    message_no_match = _make_message(
        topic="user.123.profile", message_type=MessageType.RPC
    )
    message_wrong_type = _make_message(
        topic="user.123.login", message_type=MessageType.PUBSUB
    )

    assert policy.matches_message(message_match)
    assert not policy.matches_message(message_no_match)
    assert not policy.matches_message(message_wrong_type)


@pytest.mark.asyncio
async def test_router_cache_and_statistics() -> None:
    routing_index = RoutingIndex()
    now = time.time()
    endpoint = FunctionEndpoint(
        identity=FunctionIdentity(
            name="echo",
            function_id="func-echo",
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset({"cpu"}),
        node_id="node-a",
        cluster_id="cluster-a",
        advertised_at=now,
        ttl_seconds=300.0,
    )
    routing_index.catalog.functions.register(endpoint, now=now)

    router = FabricRouter(
        config=FabricRoutingConfig(local_cluster_id="cluster-a"),
        routing_index=routing_index,
        routing_engine=RoutingEngine(
            local_cluster="cluster-a", routing_index=routing_index
        ),
    )

    message = _make_message(topic="mpreg.rpc.echo", message_type=MessageType.RPC)

    route_first = await router.route_message(message)
    route_second = await router.route_message(message)
    stats = await router.get_routing_statistics()

    assert route_first.reason is FabricRouteReason.LOCAL
    assert route_second.reason is FabricRouteReason.LOCAL
    assert stats.total_routes_computed == 2
    assert stats.cache_hit_ratio > 0.0


@pytest.mark.asyncio
async def test_federation_route_hop_budget() -> None:
    routing_index = RoutingIndex()
    now = time.time()
    endpoint = FunctionEndpoint(
        identity=FunctionIdentity(
            name="_federation",
            function_id="federation",
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset(),
        node_id="node-b",
        cluster_id="cluster-b",
        advertised_at=now,
        ttl_seconds=300.0,
    )
    routing_index.catalog.functions.register(endpoint, now=now)

    plan = FabricForwardingPlan(
        target_cluster="cluster-b",
        next_cluster="cluster-b",
        next_peer_url="ws://peer-b",
        planned_path=("cluster-a", "cluster-b"),
        federation_path=("cluster-a",),
        remaining_hops=2,
        reason=FabricForwardingFailureReason.OK,
    )
    planner = StubFederationPlanner(plan=plan)

    router = FabricRouter(
        config=FabricRoutingConfig(local_cluster_id="cluster-a"),
        routing_index=routing_index,
        routing_engine=RoutingEngine(
            local_cluster="cluster-a",
            routing_index=routing_index,
            federation_planner=planner,
        ),
    )

    message = _make_message(
        topic="mpreg.rpc.echo",
        message_type=MessageType.RPC,
        target_cluster="cluster-b",
        federation_path=("cluster-a",),
        hop_budget=1,
    )

    route = await router.route_message(message)

    assert route.reason is FabricRouteReason.FEDERATED
    assert route.federation_required is True
    assert route.federation_path == ["cluster-a", "cluster-b"]
    assert planner.calls == [("cluster-b", ("cluster-a",), 1)]


@pytest.mark.asyncio
async def test_queue_and_cache_routing_targets() -> None:
    routing_index = RoutingIndex()
    routing_index.catalog.queues.register(
        QueueEndpoint(
            queue_name="jobs",
            cluster_id="cluster-a",
            node_id="node-a",
        ),
        now=100.0,
    )
    routing_index.catalog.caches.register(
        CacheRoleEntry(
            role=CacheRole.INVALIDATOR,
            node_id="node-a",
            cluster_id="cluster-a",
        ),
        now=100.0,
    )

    router = FabricRouter(
        config=FabricRoutingConfig(
            local_cluster_id="cluster-a", local_node_id="node-a"
        ),
        routing_index=routing_index,
        routing_engine=RoutingEngine(
            local_cluster="cluster-a", routing_index=routing_index
        ),
    )

    queue_message = _make_message(
        topic="mpreg.queue.jobs", message_type=MessageType.QUEUE
    )
    cache_message = _make_message(
        topic="mpreg.cache.invalidation.jobs", message_type=MessageType.CACHE
    )

    queue_route = await router.route_message(queue_message)
    cache_route = await router.route_message(cache_message)

    assert queue_route.targets[0].target_id == "jobs"
    assert queue_route.targets[0].node_id == "node-a"
    assert cache_route.targets[0].node_id == "node-a"
    assert cache_route.reason is FabricRouteReason.LOCAL


@given(
    token=st.text(
        min_size=1,
        max_size=6,
        alphabet=st.characters(whitelist_categories=["Ll", "Nd"]),
    ),
    suffix=st.text(
        min_size=1,
        max_size=6,
        alphabet=st.characters(whitelist_categories=["Ll", "Nd"]),
    ),
    other=st.text(
        min_size=1,
        max_size=6,
        alphabet=st.characters(whitelist_categories=["Ll", "Nd"]),
    ),
)
@settings(max_examples=20, deadline=2000)
def test_policy_pattern_properties(token: str, suffix: str, other: str) -> None:
    if other == token:
        other = f"{other}x"

    policy = FabricRoutingPolicy(
        policy_id="policy-prop",
        message_type_filter={MessageType.RPC},
        topic_pattern_filter=f"{token}.*",
    )

    message_match = _make_message(
        topic=f"{token}.{suffix}", message_type=MessageType.RPC
    )
    message_no_match = _make_message(
        topic=f"{other}.{suffix}", message_type=MessageType.RPC
    )

    assert policy.matches_message(message_match)
    assert not policy.matches_message(message_no_match)
