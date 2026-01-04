#!/usr/bin/env python3
"""
Comprehensive fabric routing tests.

These tests exercise the core routing behaviors across RPC, pub/sub,
queues, and cache coordination with explicit policy and cost checks.
"""

import pytest

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
    UnifiedMessage,
)
from mpreg.fabric.pubsub_router import PubSubRoutingPlanner
from mpreg.fabric.router import (
    FabricRouter,
    FabricRouteReason,
    FabricRoutingConfig,
    FabricRoutingPolicy,
)
from mpreg.fabric.rpc_messages import FabricRPCRequest


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
    *, routing_index: RoutingIndex, local_cluster: str, local_node: str, planner=None
) -> FabricRouter:
    config = FabricRoutingConfig(
        local_cluster_id=local_cluster,
        local_node_id=local_node,
    )
    engine = RoutingEngine(
        local_cluster=local_cluster,
        routing_index=routing_index,
        federation_planner=planner,
    )
    pubsub_planner = PubSubRoutingPlanner(
        routing_index=routing_index,
        local_node_id=local_node,
    )
    return FabricRouter(
        config=config,
        routing_index=routing_index,
        routing_engine=engine,
        pubsub_planner=pubsub_planner,
    )


class TestFabricRoutingComprehensive:
    @pytest.mark.asyncio
    async def test_rpc_routing_with_target_cluster(self) -> None:
        routing_index = RoutingIndex()
        identity = FunctionIdentity(
            name="echo",
            function_id="fn-1",
            version=SemanticVersion(1, 2, 0),
        )
        endpoint = FunctionEndpoint(
            identity=identity,
            resources=frozenset(),
            node_id="node-remote",
            cluster_id="cluster-remote",
        )
        routing_index.catalog.functions.register(endpoint)

        plan = FabricForwardingPlan(
            target_cluster="cluster-remote",
            next_cluster="cluster-remote",
            next_peer_url="node-remote",
            planned_path=("cluster-local", "cluster-remote"),
            federation_path=("cluster-local", "cluster-remote"),
            remaining_hops=3,
            reason=FabricForwardingFailureReason.OK,
        )
        planner = StubFederationPlanner(plan)
        router = _make_router(
            routing_index=routing_index,
            local_cluster="cluster-local",
            local_node="node-local",
            planner=planner,
        )

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
            headers=MessageHeaders(
                correlation_id="corr_rpc",
                target_cluster="cluster-remote",
            ),
        )

        route = await router.route_message(message)

        assert route.reason == FabricRouteReason.FEDERATED
        assert route.targets[0].cluster_id == "cluster-remote"
        assert route.routing_path == ["cluster-local", "cluster-remote"]

    @pytest.mark.asyncio
    async def test_queue_route_cost_by_delivery(self) -> None:
        routing_index = RoutingIndex()
        routing_index.catalog.queues.register(
            QueueEndpoint(
                queue_name="jobs",
                cluster_id="cluster-local",
                node_id="node-local",
            )
        )
        router = _make_router(
            routing_index=routing_index,
            local_cluster="cluster-local",
            local_node="node-local",
        )

        message_exact = UnifiedMessage(
            message_id="msg_queue_exact",
            topic="mpreg.queue.jobs",
            message_type=MessageType.QUEUE,
            delivery=DeliveryGuarantee.EXACTLY_ONCE,
            payload={"job": "run"},
            headers=MessageHeaders(correlation_id="corr_queue"),
        )
        message_at_least = UnifiedMessage(
            message_id="msg_queue_once",
            topic="mpreg.queue.jobs",
            message_type=MessageType.QUEUE,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={"job": "run"},
            headers=MessageHeaders(correlation_id="corr_queue_2"),
        )

        route_exact = await router.route_message(message_exact)
        route_once = await router.route_message(message_at_least)

        assert route_exact.route_cost > route_once.route_cost
        assert route_exact.estimated_latency_ms > route_once.estimated_latency_ms

    @pytest.mark.asyncio
    async def test_pubsub_routing_multi_cluster(self) -> None:
        routing_index = RoutingIndex()
        routing_index.catalog.topics.register(
            TopicSubscription(
                subscription_id="sub-local",
                node_id="node-local",
                cluster_id="cluster-local",
                patterns=("user.*.events",),
            )
        )
        routing_index.catalog.topics.register(
            TopicSubscription(
                subscription_id="sub-remote",
                node_id="node-remote",
                cluster_id="cluster-remote",
                patterns=("user.*.events",),
            )
        )

        router = _make_router(
            routing_index=routing_index,
            local_cluster="cluster-local",
            local_node="node-local",
        )

        message = UnifiedMessage(
            message_id="msg_pubsub",
            topic="user.42.events",
            message_type=MessageType.PUBSUB,
            delivery=DeliveryGuarantee.BROADCAST,
            payload={"event": "login"},
            headers=MessageHeaders(correlation_id="corr_pubsub"),
        )

        route = await router.route_message(message)

        assert route.is_multi_target
        assert {target.node_id for target in route.targets} == {
            "node-local",
            "node-remote",
        }

    @pytest.mark.asyncio
    async def test_cache_role_selection(self) -> None:
        routing_index = RoutingIndex()
        routing_index.catalog.caches.register(
            CacheRoleEntry(
                role=CacheRole.INVALIDATOR,
                node_id="node-local",
                cluster_id="cluster-local",
            )
        )
        routing_index.catalog.caches.register(
            CacheRoleEntry(
                role=CacheRole.MONITOR,
                node_id="node-monitor",
                cluster_id="cluster-local",
            )
        )

        router = _make_router(
            routing_index=routing_index,
            local_cluster="cluster-local",
            local_node="node-local",
        )

        invalidation_message = UnifiedMessage(
            message_id="msg_cache_inv",
            topic="mpreg.cache.invalidation.user_data.user_123",
            message_type=MessageType.CACHE,
            delivery=DeliveryGuarantee.BROADCAST,
            payload={"namespace": "user_data", "key": "user_123"},
            headers=MessageHeaders(correlation_id="corr_cache"),
        )

        analytics_message = UnifiedMessage(
            message_id="msg_cache_analytics",
            topic="mpreg.cache.analytics.hit_rate.global",
            message_type=MessageType.CACHE,
            delivery=DeliveryGuarantee.FIRE_AND_FORGET,
            payload={},
            headers=MessageHeaders(correlation_id="corr_cache_2"),
        )

        invalidation_route = await router.route_message(invalidation_message)
        analytics_route = await router.route_message(analytics_message)

        assert invalidation_route.targets[0].target_id == CacheRole.INVALIDATOR.value
        assert invalidation_route.targets[0].priority_weight == 2.0
        assert analytics_route.targets[0].target_id == CacheRole.MONITOR.value
        assert analytics_route.targets[0].priority_weight == 1.0

    @pytest.mark.asyncio
    async def test_policy_selection_by_topic_and_priority(self) -> None:
        routing_index = RoutingIndex()
        config = FabricRoutingConfig(
            local_cluster_id="cluster-local",
            local_node_id="node-local",
            policies=[
                FabricRoutingPolicy(
                    policy_id="critical-control",
                    message_type_filter={MessageType.CONTROL},
                    topic_pattern_filter="mpreg.control.#",
                )
            ],
        )
        router = FabricRouter(
            config=config,
            routing_index=routing_index,
            routing_engine=RoutingEngine(
                local_cluster="cluster-local",
                routing_index=routing_index,
            ),
        )

        message = UnifiedMessage(
            message_id="msg_policy",
            topic="mpreg.control.health",
            message_type=MessageType.CONTROL,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={},
            headers=MessageHeaders(correlation_id="corr_policy"),
        )

        policy = router.config.get_policy_for_message(message)

        assert policy.policy_id == "critical-control"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
