"""
Fabric router tests.

These tests validate the fabric-native routing components that replace the
previous unified router layer.
"""

import time
from unittest.mock import AsyncMock, Mock

import pytest
from hypothesis import given, settings

from mpreg.datastructures.function_identity import FunctionIdentity, SemanticVersion
from mpreg.fabric.catalog import (
    CacheRole,
    CacheRoleEntry,
    FunctionEndpoint,
    QueueEndpoint,
    TopicSubscription,
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
    FabricRouteResult,
    FabricRouteTarget,
    FabricRoutingConfig,
    RouteHandlerRegistry,
    RoutingMetrics,
    create_correlation_id,
    create_fabric_router,
)
from mpreg.fabric.rpc_messages import FabricRPCRequest

from .test_unified_routing import routing_configs, unified_messages


class StubFederationPlanner:
    def __init__(self, plan: FabricForwardingPlan) -> None:
        self._plan = plan

    def plan_next_hop(
        self,
        *,
        target_cluster: str | None,
        visited_clusters: tuple[str, ...] = (),
        remaining_hops: int | None = None,
    ) -> FabricForwardingPlan:
        return self._plan


def _make_router(
    *,
    routing_index: RoutingIndex | None = None,
    routing_engine: RoutingEngine | None = None,
    message_queue: object | None = None,
    local_cluster: str = "cluster-a",
    local_node: str = "node-a",
) -> FabricRouter:
    config = FabricRoutingConfig(
        local_cluster_id=local_cluster,
        local_node_id=local_node,
    )
    return FabricRouter(
        config=config,
        routing_index=routing_index,
        routing_engine=routing_engine,
        message_queue=message_queue,
    )


class TestRouteHandlerRegistry:
    @pytest.mark.asyncio
    async def test_handler_registration(self) -> None:
        registry = RouteHandlerRegistry()
        handler = Mock()
        patterns = ["user.*.events", "system.#"]

        await registry.register_handler(
            "test_handler", handler, patterns, MessageType.PUBSUB
        )

        assert "test_handler" in registry.handlers
        assert registry.handlers["test_handler"] == handler
        assert "user.*.events" in registry.pattern_mappings
        assert "test_handler" in registry.pattern_mappings["user.*.events"]
        assert MessageType.PUBSUB in registry.system_mappings
        assert "test_handler" in registry.system_mappings[MessageType.PUBSUB]

    def test_pattern_matching(self) -> None:
        registry = RouteHandlerRegistry()

        assert registry._pattern_matches("user.*.events", "user.123.events")
        assert registry._pattern_matches("user.#", "user.123.login.success")
        assert not registry._pattern_matches("user.*.events", "admin.123.events")

    @pytest.mark.asyncio
    async def test_get_handlers_for_message(self) -> None:
        registry = RouteHandlerRegistry()

        handler1 = Mock()
        handler2 = Mock()

        await registry.register_handler(
            "handler1", handler1, ["user.*.events"], MessageType.PUBSUB
        )
        await registry.register_handler(
            "handler2", handler2, ["user.#"], MessageType.PUBSUB
        )

        message = UnifiedMessage(
            message_id="msg_test",
            topic="user.123.events",
            message_type=MessageType.PUBSUB,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={},
            headers=MessageHeaders(correlation_id=create_correlation_id()),
        )

        handlers = await registry.get_handlers_for_message(message)
        handler_ids = [handler_id for handler_id, _ in handlers]
        assert "handler1" in handler_ids
        assert "handler2" in handler_ids


class TestRoutingMetrics:
    def test_metrics_recording(self) -> None:
        metrics = RoutingMetrics()

        route = FabricRouteResult(
            route_id="test_route",
            targets=[FabricRouteTarget(MessageType.RPC, "test_function")],
            routing_path=["node1"],
            federation_path=[],
            estimated_latency_ms=50.0,
            route_cost=10.0,
            federation_required=False,
            hops_required=1,
            reason=FabricRouteReason.LOCAL,
        )

        metrics.record_route_computation(route, 15.0, MessageType.RPC, used_cache=False)

        assert metrics.total_routes_computed == 1
        assert metrics.local_routes == 1
        assert metrics.federation_routes == 0
        assert metrics.rpc_routes == 1
        assert metrics.cache_misses == 1
        assert metrics.total_route_computation_time_ms == 15.0

    def test_statistics_snapshot(self) -> None:
        metrics = RoutingMetrics()

        route = FabricRouteResult(
            route_id="test_route",
            targets=[FabricRouteTarget(MessageType.PUBSUB, "subscription")],
            routing_path=["node1", "node2"],
            federation_path=["cluster1", "cluster2"],
            estimated_latency_ms=100.0,
            route_cost=20.0,
            federation_required=True,
            hops_required=2,
            reason=FabricRouteReason.FEDERATED,
        )

        metrics.record_route_computation(
            route, 25.0, MessageType.PUBSUB, used_cache=True
        )

        snapshot = metrics.get_statistics_snapshot()

        assert snapshot.total_routes_computed == 1
        assert snapshot.federation_routes == 1
        assert snapshot.pubsub_routes == 1
        assert snapshot.cache_hit_ratio == 1.0
        assert snapshot.average_route_computation_ms == 25.0
        assert snapshot.federation_ratio == 1.0


class TestFabricRouter:
    def test_router_initialization(self) -> None:
        config = FabricRoutingConfig(
            local_cluster_id="cluster-a", local_node_id="node-a"
        )
        router = FabricRouter(config)

        assert router.config == config
        assert isinstance(router.handler_registry, RouteHandlerRegistry)
        assert isinstance(router.metrics, RoutingMetrics)
        assert router.route_cache == {}

    def test_factory_creation(self) -> None:
        config = FabricRoutingConfig(
            local_cluster_id="cluster-a", local_node_id="node-a"
        )
        router = create_fabric_router(config)
        assert isinstance(router, FabricRouter)

    def test_cache_key_generation(self) -> None:
        router = _make_router()

        payload = FabricRPCRequest(
            request_id="req-1",
            command="echo",
            args=(),
            kwargs={},
            resources=("gpu", "cpu"),
            function_id="fn-1",
            version_constraint=">=1.0.0",
            target_node="node-b",
        ).to_dict()
        message = UnifiedMessage(
            message_id="msg_test",
            topic="mpreg.rpc.execute.echo",
            message_type=MessageType.RPC,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload=payload,
            headers=MessageHeaders(
                correlation_id="test_corr",
                target_cluster="cluster-a",
                hop_budget=3,
                federation_path=("cluster-a",),
            ),
        )

        cache_key = router._get_cache_key(message)
        assert (
            cache_key
            == "mpreg.rpc.execute.echo:rpc:at_least_once:cluster-a:normal:3:cluster-a:"
            "echo:fn-1:>=1.0.0:node-b:cpu,gpu"
        )

    def test_route_caching(self) -> None:
        router = _make_router()

        route = FabricRouteResult(
            route_id="test_route",
            targets=[FabricRouteTarget(MessageType.RPC, "test_function")],
            routing_path=["local"],
            federation_path=[],
            estimated_latency_ms=5.0,
            route_cost=1.0,
            federation_required=False,
            hops_required=0,
            reason=FabricRouteReason.LOCAL,
        )

        cache_key = "test:rpc:at_least_once"
        router._cache_route(cache_key, route)

        cached_route = router._get_cached_route(cache_key)
        assert cached_route == route

        router.config.routing_cache_ttl_ms = 1.0
        time.sleep(0.002)

        expired_route = router._get_cached_route(cache_key)
        assert expired_route is None

    @pytest.mark.asyncio
    async def test_cache_invalidation(self) -> None:
        router = _make_router()

        route1 = FabricRouteResult(
            route_id="route1",
            targets=[],
            routing_path=[],
            federation_path=[],
            estimated_latency_ms=5.0,
            route_cost=1.0,
            federation_required=False,
            hops_required=0,
            reason=FabricRouteReason.LOCAL,
        )
        route2 = FabricRouteResult(
            route_id="route2",
            targets=[],
            routing_path=[],
            federation_path=[],
            estimated_latency_ms=5.0,
            route_cost=1.0,
            federation_required=False,
            hops_required=0,
            reason=FabricRouteReason.LOCAL,
        )

        router._cache_route("user.123.events:pubsub:at_least_once", route1)
        router._cache_route("admin.456.events:pubsub:at_least_once", route2)

        assert len(router.route_cache) == 2

        await router.invalidate_routing_cache("user")

        assert len(router.route_cache) == 1
        assert "admin.456.events:pubsub:at_least_once" in router.route_cache

        await router.invalidate_routing_cache()
        assert len(router.route_cache) == 0

    @pytest.mark.asyncio
    async def test_local_route_computation(self) -> None:
        router = _make_router()

        message = UnifiedMessage(
            message_id="msg_local",
            topic="local.test",
            message_type=MessageType.DATA,
            delivery=DeliveryGuarantee.FIRE_AND_FORGET,
            payload={"test": "data"},
            headers=MessageHeaders(correlation_id="test_corr"),
        )

        policy = router.config.get_policy_for_message(message)
        route = await router._compute_local_route(message, policy, "test_route")

        assert route.route_id == "test_route"
        assert not route.federation_required
        assert route.is_local_route
        assert route.targets[0].system_type == MessageType.DATA
        assert route.reason == FabricRouteReason.FALLBACK_LOCAL

    @pytest.mark.asyncio
    async def test_control_plane_priority_routing(self) -> None:
        router = _make_router()

        message = UnifiedMessage(
            message_id="msg_control",
            topic="mpreg.rpc.command.123.started",
            message_type=MessageType.CONTROL,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={},
            headers=MessageHeaders(
                correlation_id="test_corr",
                priority=RoutingPriority.CRITICAL,
            ),
        )

        route = await router.route_message(message)

        assert route.estimated_latency_ms == 1.0
        assert route.targets[0].priority_weight == 2.0

    @pytest.mark.asyncio
    async def test_route_pattern_registration(self) -> None:
        router = _make_router()

        await router.register_route_pattern("user.*.events", "test_handler")

        assert "user.*.events" in router.handler_registry.pattern_mappings
        assert (
            "test_handler" in router.handler_registry.pattern_mappings["user.*.events"]
        )

        with pytest.raises(ValueError, match="Invalid topic pattern"):
            await router.register_route_pattern("invalid.**.pattern", "bad_handler")

    @pytest.mark.asyncio
    async def test_rpc_route_local(self) -> None:
        routing_index = RoutingIndex()
        identity = FunctionIdentity(
            name="echo",
            function_id="fn-1",
            version=SemanticVersion(1, 0, 0),
        )
        endpoint = FunctionEndpoint(
            identity=identity,
            resources=frozenset(),
            node_id="node-a",
            cluster_id="cluster-a",
        )
        routing_index.catalog.functions.register(endpoint)

        router = _make_router(routing_index=routing_index)

        payload = FabricRPCRequest(
            request_id="req-1",
            command="echo",
            args=(),
            kwargs={},
        ).to_dict()
        message = UnifiedMessage(
            message_id="msg_rpc",
            topic="mpreg.rpc.execute.echo",
            message_type=MessageType.RPC,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload=payload,
            headers=MessageHeaders(correlation_id="corr_rpc"),
        )

        route = await router.route_message(message)

        assert route.reason == FabricRouteReason.LOCAL
        assert not route.federation_required
        assert route.targets[0].node_id == "node-a"
        assert route.targets[0].cluster_id == "cluster-a"

    @pytest.mark.asyncio
    async def test_rpc_route_federated(self) -> None:
        routing_index = RoutingIndex()
        identity = FunctionIdentity(
            name="echo",
            function_id="fn-1",
            version=SemanticVersion(1, 0, 0),
        )
        endpoint = FunctionEndpoint(
            identity=identity,
            resources=frozenset(),
            node_id="node-b",
            cluster_id="cluster-b",
        )
        routing_index.catalog.functions.register(endpoint)

        plan = FabricForwardingPlan(
            target_cluster="cluster-b",
            next_cluster="cluster-b",
            next_peer_url="node-b",
            planned_path=("cluster-a", "cluster-b"),
            federation_path=("cluster-a", "cluster-b"),
            remaining_hops=3,
            reason=FabricForwardingFailureReason.OK,
        )
        planner = StubFederationPlanner(plan)
        routing_engine = RoutingEngine(
            local_cluster="cluster-a",
            routing_index=routing_index,
            federation_planner=planner,
        )
        router = _make_router(
            routing_index=routing_index, routing_engine=routing_engine
        )

        payload = FabricRPCRequest(
            request_id="req-2",
            command="echo",
            args=(),
            kwargs={},
        ).to_dict()
        message = UnifiedMessage(
            message_id="msg_rpc_fed",
            topic="mpreg.rpc.execute.echo",
            message_type=MessageType.RPC,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload=payload,
            headers=MessageHeaders(
                correlation_id="corr_rpc_fed",
                target_cluster="cluster-b",
            ),
        )

        route = await router.route_message(message)

        assert route.reason == FabricRouteReason.FEDERATED
        assert route.federation_required
        assert route.routing_path == ["cluster-a", "cluster-b"]
        assert route.targets[0].cluster_id == "cluster-b"

    @pytest.mark.asyncio
    async def test_rpc_route_respects_selector_fields(self) -> None:
        routing_index = RoutingIndex()
        endpoint_cpu = FunctionEndpoint(
            identity=FunctionIdentity(
                name="echo",
                function_id="fn-1",
                version=SemanticVersion(1, 0, 0),
            ),
            resources=frozenset({"cpu"}),
            node_id="node-a",
            cluster_id="cluster-a",
        )
        endpoint_gpu = FunctionEndpoint(
            identity=FunctionIdentity(
                name="echo",
                function_id="fn-1",
                version=SemanticVersion(2, 0, 0),
            ),
            resources=frozenset({"gpu"}),
            node_id="node-b",
            cluster_id="cluster-a",
        )
        routing_index.catalog.functions.register(endpoint_cpu)
        routing_index.catalog.functions.register(endpoint_gpu)

        router = _make_router(routing_index=routing_index)

        payload = FabricRPCRequest(
            request_id="req-3",
            command="echo",
            args=(),
            kwargs={},
            resources=("gpu",),
            function_id="fn-1",
            version_constraint=">=2.0.0",
        ).to_dict()
        message = UnifiedMessage(
            message_id="msg_rpc_selector",
            topic="mpreg.rpc.execute.echo",
            message_type=MessageType.RPC,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload=payload,
            headers=MessageHeaders(correlation_id="corr_rpc_selector"),
        )

        route = await router.route_message(message)

        assert route.targets[0].node_id == "node-b"
        assert route.targets[0].cluster_id == "cluster-a"

    @pytest.mark.asyncio
    async def test_rpc_route_respects_target_node(self) -> None:
        routing_index = RoutingIndex()
        identity = FunctionIdentity(
            name="echo",
            function_id="fn-1",
            version=SemanticVersion(1, 0, 0),
        )
        routing_index.catalog.functions.register(
            FunctionEndpoint(
                identity=identity,
                resources=frozenset(),
                node_id="node-a",
                cluster_id="cluster-a",
            )
        )
        routing_index.catalog.functions.register(
            FunctionEndpoint(
                identity=identity,
                resources=frozenset(),
                node_id="node-b",
                cluster_id="cluster-a",
            )
        )

        router = _make_router(routing_index=routing_index)

        payload = FabricRPCRequest(
            request_id="req-4",
            command="echo",
            target_node="node-b",
        ).to_dict()
        message = UnifiedMessage(
            message_id="msg_rpc_target",
            topic="mpreg.rpc.execute.echo",
            message_type=MessageType.RPC,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload=payload,
            headers=MessageHeaders(correlation_id="corr_rpc_target"),
        )

        route = await router.route_message(message)

        assert route.targets[0].node_id == "node-b"

    @pytest.mark.asyncio
    async def test_pubsub_route_planning(self) -> None:
        routing_index = RoutingIndex()
        routing_index.catalog.topics.register(
            TopicSubscription(
                subscription_id="sub_local",
                node_id="node-a",
                cluster_id="cluster-a",
                patterns=("user.*.events",),
            )
        )
        routing_index.catalog.topics.register(
            TopicSubscription(
                subscription_id="sub_remote",
                node_id="node-b",
                cluster_id="cluster-b",
                patterns=("user.*.events",),
            )
        )

        plan = FabricForwardingPlan(
            target_cluster="cluster-b",
            next_cluster="cluster-b",
            next_peer_url="node-b",
            planned_path=("cluster-a", "cluster-b"),
            federation_path=("cluster-a",),
            remaining_hops=3,
            reason=FabricForwardingFailureReason.OK,
        )
        planner = StubFederationPlanner(plan)
        routing_engine = RoutingEngine(
            local_cluster="cluster-a",
            routing_index=routing_index,
            federation_planner=planner,
        )
        router = _make_router(
            routing_index=routing_index, routing_engine=routing_engine
        )

        message = UnifiedMessage(
            message_id="msg_pubsub",
            topic="user.123.events",
            message_type=MessageType.PUBSUB,
            delivery=DeliveryGuarantee.BROADCAST,
            payload={"event": "login"},
            headers=MessageHeaders(correlation_id="corr_pubsub"),
        )

        route = await router.route_message(message)

        assert route.is_multi_target
        assert route.federation_required
        assert route.federation_path == ["cluster-a", "cluster-b"]
        assert {target.node_id for target in route.targets} == {"node-a", "node-b"}

    @pytest.mark.asyncio
    async def test_queue_route_planning(self) -> None:
        routing_index = RoutingIndex()
        routing_index.catalog.queues.register(
            QueueEndpoint(
                queue_name="jobs",
                cluster_id="cluster-a",
                node_id="node-a",
            )
        )

        router = _make_router(routing_index=routing_index)

        message = UnifiedMessage(
            message_id="msg_queue",
            topic="mpreg.queue.jobs",
            message_type=MessageType.QUEUE,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={"job": "run"},
            headers=MessageHeaders(correlation_id="corr_queue"),
        )

        route = await router.route_message(message)

        assert route.targets[0].target_id == "jobs"
        assert route.targets[0].node_id == "node-a"

    @pytest.mark.asyncio
    async def test_queue_route_planning_remote(self) -> None:
        routing_index = RoutingIndex()
        routing_index.catalog.queues.register(
            QueueEndpoint(
                queue_name="jobs",
                cluster_id="cluster-b",
                node_id="node-b",
            )
        )
        plan = FabricForwardingPlan(
            target_cluster="cluster-b",
            next_cluster="cluster-b",
            next_peer_url="node-b",
            planned_path=("cluster-a", "cluster-b"),
            federation_path=("cluster-a",),
            remaining_hops=3,
            reason=FabricForwardingFailureReason.OK,
        )
        planner = StubFederationPlanner(plan)
        routing_engine = RoutingEngine(
            local_cluster="cluster-a",
            routing_index=routing_index,
            federation_planner=planner,
        )
        router = _make_router(
            routing_index=routing_index, routing_engine=routing_engine
        )

        message = UnifiedMessage(
            message_id="msg_queue_remote",
            topic="mpreg.queue.jobs",
            message_type=MessageType.QUEUE,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={"job": "run"},
            headers=MessageHeaders(correlation_id="corr_queue_remote"),
        )

        route = await router.route_message(message)

        assert route.federation_required
        assert route.federation_path == ["cluster-a", "cluster-b"]
        assert route.targets[0].cluster_id == "cluster-b"

    @pytest.mark.asyncio
    async def test_queue_route_create_queue(self) -> None:
        message_queue = Mock()
        message_queue.list_queues.return_value = []
        message_queue.create_queue = AsyncMock()

        router = _make_router(message_queue=message_queue)

        message = UnifiedMessage(
            message_id="msg_queue_create",
            topic="mpreg.queue.backlog",
            message_type=MessageType.QUEUE,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={"job": "run"},
            headers=MessageHeaders(correlation_id="corr_queue"),
        )

        route = await router.route_message(message)

        message_queue.create_queue.assert_awaited_once_with("backlog")
        assert route.targets[0].node_id == "node-a"

    @pytest.mark.asyncio
    async def test_cache_route_with_roles(self) -> None:
        routing_index = RoutingIndex()
        routing_index.catalog.caches.register(
            CacheRoleEntry(
                role=CacheRole.INVALIDATOR,
                node_id="node-a",
                cluster_id="cluster-a",
            )
        )

        router = _make_router(routing_index=routing_index)

        message = UnifiedMessage(
            message_id="msg_cache",
            topic="mpreg.cache.invalidation.user_data.user_123",
            message_type=MessageType.CACHE,
            delivery=DeliveryGuarantee.BROADCAST,
            payload={"namespace": "user_data", "key": "user_123"},
            headers=MessageHeaders(correlation_id="corr_cache"),
        )

        route = await router.route_message(message)

        assert route.targets[0].target_id == CacheRole.INVALIDATOR.value
        assert route.targets[0].priority_weight == 2.0

    @pytest.mark.asyncio
    async def test_cache_route_remote(self) -> None:
        routing_index = RoutingIndex()
        routing_index.catalog.caches.register(
            CacheRoleEntry(
                role=CacheRole.COORDINATOR,
                node_id="node-b",
                cluster_id="cluster-b",
            )
        )
        plan = FabricForwardingPlan(
            target_cluster="cluster-b",
            next_cluster="cluster-b",
            next_peer_url="node-b",
            planned_path=("cluster-a", "cluster-b"),
            federation_path=("cluster-a",),
            remaining_hops=3,
            reason=FabricForwardingFailureReason.OK,
        )
        planner = StubFederationPlanner(plan)
        routing_engine = RoutingEngine(
            local_cluster="cluster-a",
            routing_index=routing_index,
            federation_planner=planner,
        )
        router = _make_router(
            routing_index=routing_index, routing_engine=routing_engine
        )

        message = UnifiedMessage(
            message_id="msg_cache_remote",
            topic="mpreg.cache.coordination.replication.user_data",
            message_type=MessageType.CACHE,
            delivery=DeliveryGuarantee.BROADCAST,
            payload={},
            headers=MessageHeaders(correlation_id="corr_cache_remote"),
        )

        route = await router.route_message(message)

        assert route.federation_required
        assert route.federation_path == ["cluster-a", "cluster-b"]
        assert route.targets[0].cluster_id == "cluster-b"

    @pytest.mark.asyncio
    async def test_cache_route_fallback(self) -> None:
        router = _make_router()

        message = UnifiedMessage(
            message_id="msg_cache_fallback",
            topic="mpreg.cache.analytics.hit_rate.global",
            message_type=MessageType.CACHE,
            delivery=DeliveryGuarantee.FIRE_AND_FORGET,
            payload={},
            headers=MessageHeaders(correlation_id="corr_cache"),
        )

        route = await router.route_message(message)

        assert route.is_local_route
        assert route.estimated_latency_ms == 1.0

    @pytest.mark.asyncio
    @given(unified_messages(), routing_configs())
    @settings(max_examples=50, deadline=5000)
    async def test_route_message_property(
        self, message: UnifiedMessage, config: FabricRoutingConfig
    ) -> None:
        router = FabricRouter(config=config)

        route_result = await router.route_message(message)

        assert route_result is not None
        assert isinstance(route_result, FabricRouteResult)
        assert route_result.route_id is not None
        assert len(route_result.targets) >= 1
        assert router.metrics.total_routes_computed >= 1


if __name__ == "__main__":
    pytest.main([__file__])
