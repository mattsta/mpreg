"""Platform-wide audit P0 proofs: client policy, wire retryable, honesty APIs, ops."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from mpreg.client.call_policy import (
    ClientCallPolicy,
    RpcExecutionMode,
    call_with_policy,
)
from mpreg.client.client import Client
from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.cache_models import (
    CacheLevel,
    CacheMetadata,
    CacheOptions,
    ConsistencyLevel,
    GlobalCacheKey,
)
from mpreg.core.errors import MpregError, MpregErrorCode, map_exception, timeout_error
from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager
from mpreg.core.model import MPREGException
from mpreg.fabric.message import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    UnifiedMessage,
)
from mpreg.fabric.route_decision_log import RouteDecisionLog, make_record_from_route
from mpreg.fabric.router import FabricRouteReason
from mpreg.server_pkg.openapi_surface import build_monitoring_openapi


def test_rpc_error_carries_retryable_on_wire() -> None:
    err = timeout_error("slow")
    wire = err.to_rpc_error()
    assert wire.code == 1006
    assert wire.retryable is True

    # Round-trip through map_exception preserves wire retryable
    mapped = map_exception(MPREGException(rpc_error=wire))
    assert mapped.retryable is True
    assert mapped.code == 1006


def test_rpc_error_non_retryable_on_wire() -> None:
    err = MpregError.of(MpregErrorCode.COMMAND_NOT_FOUND, details="nope")
    assert err.to_rpc_error().retryable is False


def test_client_defaults_are_prod_safe() -> None:
    c = Client(url="ws://127.0.0.1:1")
    assert c.full_log is False
    assert c.default_timeout_seconds == 30.0
    api = MPREGClientAPI(url="ws://127.0.0.1:1")
    assert api.full_log is False


@pytest.mark.asyncio
async def test_call_policy_applies_with_max_attempts_one() -> None:
    """Deadline-only policies (M3) must still enforce fail-closed deadline."""
    policy = ClientCallPolicy.for_mode(
        RpcExecutionMode.M3_STREAMING, deadline_seconds=0.05
    )
    assert policy.max_attempts == 1

    async def slow() -> str:
        await asyncio.sleep(0.2)
        return "late"

    with pytest.raises(MpregError) as ei:
        await call_with_policy(slow, policy)
    assert ei.value.code == int(MpregErrorCode.TIMEOUT)


@pytest.mark.asyncio
async def test_send_raw_message_timeout_fails_closed() -> None:
    client = Client(url="ws://127.0.0.1:1", full_log=False)
    transport = MagicMock()
    transport.send = AsyncMock()
    client._transport = transport

    # Never resolve the pending future → timeout
    with pytest.raises(MpregError) as ei:
        await client.send_raw_message({"hello": "world"})
    assert ei.value.code == int(MpregErrorCode.TIMEOUT)


@pytest.mark.asyncio
async def test_strong_cache_put_fails_closed_when_l3_requested() -> None:
    mgr = GlobalCacheManager(GlobalCacheConfiguration(enable_l2_persistent=False))
    try:
        key = GlobalCacheKey(namespace="t", identifier="k1")
        options = CacheOptions(
            cache_levels=frozenset({CacheLevel.L1, CacheLevel.L3}),
            consistency_level=ConsistencyLevel.STRONG,
        )
        result = await mgr.put(key, {"v": 1}, CacheMetadata(), options)
        assert result.success is False
        assert result.error_message is not None
        assert "STRONG" in result.error_message
        # COR-01: no dirty L1 residual after refused STRONG put
        get_result = await mgr.get(
            key, CacheOptions(cache_levels=frozenset({CacheLevel.L1}))
        )
        assert get_result.success is False
    finally:
        mgr.shutdown_sync()


@pytest.mark.asyncio
async def test_strong_cache_put_l1_only_fails_closed_no_residual() -> None:
    """L1-only + STRONG must not paper-succeed (COR-01)."""
    mgr = GlobalCacheManager(GlobalCacheConfiguration(enable_l2_persistent=False))
    try:
        key = GlobalCacheKey(namespace="t", identifier="k-l1-strong")
        options = CacheOptions(
            cache_levels=frozenset({CacheLevel.L1}),
            consistency_level=ConsistencyLevel.STRONG,
        )
        result = await mgr.put(key, {"v": 2}, CacheMetadata(), options)
        assert result.success is False
        assert result.error_message is not None
        assert "STRONG" in result.error_message
        get_result = await mgr.get(
            key, CacheOptions(cache_levels=frozenset({CacheLevel.L1}))
        )
        assert get_result.success is False
    finally:
        mgr.shutdown_sync()


@pytest.mark.asyncio
async def test_exactly_once_queue_route_unsupported() -> None:
    from mpreg.fabric.catalog import QueueEndpoint
    from mpreg.fabric.engine import RoutingEngine
    from mpreg.fabric.index import RoutingIndex
    from mpreg.fabric.router import FabricRouter, FabricRoutingConfig

    index = RoutingIndex()
    index.catalog.queues.register(
        QueueEndpoint(
            queue_name="jobs",
            cluster_id="c-local",
            node_id="n-local",
        )
    )
    config = FabricRoutingConfig(
        local_cluster_id="c-local",
        local_node_id="n-local",
    )
    engine = RoutingEngine(local_cluster="c-local", routing_index=index)
    router = FabricRouter(
        config=config,
        routing_index=index,
        routing_engine=engine,
    )
    msg = UnifiedMessage(
        message_id="m1",
        topic="mpreg.queue.jobs",
        message_type=MessageType.QUEUE,
        delivery=DeliveryGuarantee.EXACTLY_ONCE,
        payload={"job": 1},
        headers=MessageHeaders(correlation_id="c1"),
    )
    result = await router.route_message(msg)
    assert result.reason == FabricRouteReason.UNSUPPORTED_DELIVERY
    assert result.targets == []


@pytest.mark.asyncio
async def test_exactly_once_all_message_types_unsupported() -> None:
    """COR-03: EO is fabric-wide, not queue-route-only."""
    from mpreg.fabric.engine import RoutingEngine
    from mpreg.fabric.index import RoutingIndex
    from mpreg.fabric.router import FabricRouter, FabricRoutingConfig

    index = RoutingIndex()
    config = FabricRoutingConfig(
        local_cluster_id="c-local",
        local_node_id="n-local",
    )
    engine = RoutingEngine(local_cluster="c-local", routing_index=index)
    router = FabricRouter(
        config=config,
        routing_index=index,
        routing_engine=engine,
    )
    for mtype in (
        MessageType.RPC,
        MessageType.PUBSUB,
        MessageType.QUEUE,
        MessageType.CACHE,
        MessageType.CONTROL,
        MessageType.DATA,
    ):
        msg = UnifiedMessage(
            message_id=f"eo-{mtype.value}",
            topic=f"mpreg.test.{mtype.value}",
            message_type=mtype,
            delivery=DeliveryGuarantee.EXACTLY_ONCE,
            payload={},
            headers=MessageHeaders(correlation_id="c-eo"),
        )
        result = await router.route_message(msg)
        assert result.reason == FabricRouteReason.UNSUPPORTED_DELIVERY, mtype
        assert result.targets == []


@pytest.mark.asyncio
async def test_exactly_once_does_not_auto_create_queue() -> None:
    """COR-07: EO reject must not create queue side effects."""
    from unittest.mock import AsyncMock, MagicMock

    from mpreg.fabric.engine import RoutingEngine
    from mpreg.fabric.index import RoutingIndex
    from mpreg.fabric.router import FabricRouter, FabricRoutingConfig

    index = RoutingIndex()
    config = FabricRoutingConfig(
        local_cluster_id="c-local",
        local_node_id="n-local",
    )
    engine = RoutingEngine(local_cluster="c-local", routing_index=index)
    mq = MagicMock()
    mq.list_queues.return_value = []
    mq.create_queue = AsyncMock()
    router = FabricRouter(
        config=config,
        routing_index=index,
        routing_engine=engine,
        message_queue=mq,
    )
    msg = UnifiedMessage(
        message_id="eo-side",
        topic="mpreg.queue.never-create-me",
        message_type=MessageType.QUEUE,
        delivery=DeliveryGuarantee.EXACTLY_ONCE,
        payload={},
        headers=MessageHeaders(correlation_id="c-eo"),
    )
    result = await router.route_message(msg)
    assert result.reason == FabricRouteReason.UNSUPPORTED_DELIVERY
    mq.create_queue.assert_not_called()


def test_openapi_includes_live_ready_traceparent() -> None:
    doc = build_monitoring_openapi()
    paths = doc["paths"]
    assert "/live" in paths
    assert "/ready" in paths
    params = paths["/routing/decisions"]["get"]["parameters"]
    names = {p["name"] for p in params}
    assert "traceparent" in names


def test_decision_log_unsupported_delivery_counts_blackhole() -> None:
    log = RouteDecisionLog()
    log.record(
        make_record_from_route(
            message_id="m",
            correlation_id="c",
            topic="t",
            message_type="queue",
            reason="unsupported_delivery",
            cached=False,
            targets=[],
            routing_path=[],
            hops_required=0,
        )
    )
    assert log.blackhole_count == 1
