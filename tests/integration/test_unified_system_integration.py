#!/usr/bin/env python3
"""
Fabric routing integration tests.

These tests validate end-to-end integration across the fabric catalog,
announcers/adapters, and routing decisions.
"""

import time

import pytest

from mpreg.core.model import PubSubSubscription, TopicPattern
from mpreg.core.topic_exchange import TopicExchange
from mpreg.datastructures.function_identity import FunctionIdentity, SemanticVersion
from mpreg.fabric.adapters.topic_exchange import TopicExchangeCatalogAdapter
from mpreg.fabric.catalog import (
    CacheRole,
    CacheRoleEntry,
    FunctionEndpoint,
    QueueEndpoint,
)
from mpreg.fabric.catalog_delta import RoutingCatalogApplier, RoutingCatalogDelta
from mpreg.fabric.engine import RoutingEngine
from mpreg.fabric.index import RoutingIndex
from mpreg.fabric.message import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    UnifiedMessage,
)
from mpreg.fabric.router import FabricRouter, FabricRouteReason, FabricRoutingConfig
from mpreg.fabric.rpc_messages import FabricRPCRequest


def _make_router(
    *, routing_index: RoutingIndex, local_cluster: str, local_node: str
) -> FabricRouter:
    config = FabricRoutingConfig(
        local_cluster_id=local_cluster,
        local_node_id=local_node,
    )
    engine = RoutingEngine(local_cluster=local_cluster, routing_index=routing_index)
    return FabricRouter(
        config=config,
        routing_index=routing_index,
        routing_engine=engine,
    )


class TestFabricSystemIntegration:
    @pytest.mark.asyncio
    async def test_topic_exchange_catalog_to_router(self) -> None:
        exchange = TopicExchange(server_url="node-a", cluster_id="cluster-a")
        subscription = PubSubSubscription(
            subscription_id="sub-1",
            patterns=(TopicPattern(pattern="user.*.events"),),
            subscriber="client-1",
            created_at=time.time(),
        )
        exchange.add_subscription(subscription)

        adapter = TopicExchangeCatalogAdapter.for_exchange(exchange)
        delta = adapter.build_delta(subscription)

        routing_index = RoutingIndex()
        applier = RoutingCatalogApplier(routing_index.catalog)
        applier.apply(delta)

        router = _make_router(
            routing_index=routing_index,
            local_cluster="cluster-a",
            local_node="node-a",
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

        assert route.targets
        assert route.targets[0].target_id == "sub-1"
        assert route.targets[0].node_id == "node-a"
        assert route.reason in {FabricRouteReason.LOCAL, FabricRouteReason.FEDERATED}

    @pytest.mark.asyncio
    async def test_function_catalog_to_router(self) -> None:
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
        delta = RoutingCatalogDelta(
            update_id="update-1",
            cluster_id="cluster-a",
            functions=(endpoint,),
        )
        applier = RoutingCatalogApplier(routing_index.catalog)
        applier.apply(delta)

        router = _make_router(
            routing_index=routing_index,
            local_cluster="cluster-a",
            local_node="node-a",
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
            headers=MessageHeaders(correlation_id="corr_rpc"),
        )

        route = await router.route_message(message)

        assert route.targets
        assert route.targets[0].node_id == "node-a"
        assert route.reason == FabricRouteReason.LOCAL

    @pytest.mark.asyncio
    async def test_queue_catalog_to_router(self) -> None:
        routing_index = RoutingIndex()
        endpoint = QueueEndpoint(
            queue_name="jobs",
            cluster_id="cluster-a",
            node_id="node-a",
        )
        delta = RoutingCatalogDelta(
            update_id="update-queue",
            cluster_id="cluster-a",
            queues=(endpoint,),
        )
        applier = RoutingCatalogApplier(routing_index.catalog)
        applier.apply(delta)

        router = _make_router(
            routing_index=routing_index,
            local_cluster="cluster-a",
            local_node="node-a",
        )

        message = UnifiedMessage(
            message_id="msg_queue",
            topic="mpreg.queue.jobs",
            message_type=MessageType.QUEUE,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={"job": "run"},
            headers=MessageHeaders(correlation_id="corr_queue"),
        )

        route = await router.route_message(message)

        assert route.targets
        assert route.targets[0].target_id == "jobs"
        assert route.targets[0].node_id == "node-a"

    @pytest.mark.asyncio
    async def test_cache_catalog_to_router(self) -> None:
        routing_index = RoutingIndex()
        entry = CacheRoleEntry(
            role=CacheRole.INVALIDATOR,
            node_id="node-a",
            cluster_id="cluster-a",
        )
        delta = RoutingCatalogDelta(
            update_id="update-cache",
            cluster_id="cluster-a",
            caches=(entry,),
        )
        applier = RoutingCatalogApplier(routing_index.catalog)
        applier.apply(delta)

        router = _make_router(
            routing_index=routing_index,
            local_cluster="cluster-a",
            local_node="node-a",
        )

        message = UnifiedMessage(
            message_id="msg_cache",
            topic="mpreg.cache.invalidation.user_data.user_123",
            message_type=MessageType.CACHE,
            delivery=DeliveryGuarantee.BROADCAST,
            payload={"namespace": "user_data", "key": "user_123"},
            headers=MessageHeaders(correlation_id="corr_cache"),
        )

        route = await router.route_message(message)

        assert route.targets
        assert route.targets[0].target_id == CacheRole.INVALIDATOR.value
        assert route.targets[0].priority_weight == 2.0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
