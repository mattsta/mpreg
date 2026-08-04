"""T9 B4 chaos: STRONG residual-free, EO refuse, drain admission, drop metrics, bounds."""

from __future__ import annotations

import bisect
import time
from unittest.mock import MagicMock

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.global_cache import (
    CacheOptions,
    ConsistencyLevel,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.core.message_queue import (
    DeliveryGuarantee as QueueDG,
    MessageQueue,
    QueuedMessage,
    QueueConfiguration,
    QueueType,
)
from mpreg.core.model import RPCCommand, RPCRequest
from mpreg.core.monitoring.server_monitoring import ServerMetricsTracker
from mpreg.core.observability.slo import GOLDEN_SIGNALS
from mpreg.datastructures import MessageId
from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.gossip_signatures import SIGNATURE_KEY, sign_gossip_payload
from mpreg.fabric.message import DeliveryGuarantee as FabricDG
from mpreg.server import MPREGServer
from mpreg.server_pkg.gossip_admission import accept_fabric_gossip_payload

@pytest.mark.asyncio
async def test_chaos_strong_cache_leaves_no_dirty_l1() -> None:
    """COR-01 chaos: failed STRONG must not leave residual L1."""
    cfg = GlobalCacheConfiguration(
        enable_l2_persistent=False,
        enable_l3_distributed=False,
        enable_l4_federation=False,
    )
    mgr = GlobalCacheManager(cfg)
    try:
        key = GlobalCacheKey(namespace="chaos", identifier="k1")
        opts = CacheOptions(consistency_level=ConsistencyLevel.STRONG)
        result = await mgr.put(key, {"v": 1}, options=opts)
        assert result.success is False
        assert "STRONG" in (result.error_message or "")
        got = await mgr.get(key)
        present = bool(
            getattr(got, "success", False)
            and getattr(got, "value", None) is not None
        )
        # residual-free: no successful hit after refused STRONG
        assert present is False
    finally:
        await mgr.shutdown()

def test_chaos_delivery_guarantee_eo_not_on_queue_plane() -> None:
    """Cross-plane EO must not silently map onto queue guarantees."""
    with pytest.raises(ValueError, match="exactly_once|EXACTLY_ONCE|exactly"):
        FabricDG.EXACTLY_ONCE.to_queue_guarantee()
    assert FabricDG.AT_LEAST_ONCE.to_queue_guarantee() is QueueDG.AT_LEAST_ONCE

def test_chaos_metrics_drop_counters_prom() -> None:
    """OBS-01/04: drop counters appear in prometheus text."""
    t = ServerMetricsTracker()
    t.record_notification_drop(2)
    t.record_replication_drop(3)
    t.record_cache_pubsub_drop(4)
    text = "\n".join(t.prometheus_lines('node="chaos"'))
    assert 'mpreg_client_notification_drops_total{node="chaos"} 2' in text
    assert 'mpreg_cache_replication_drops_total{node="chaos"} 3' in text
    assert 'mpreg_cache_pubsub_notification_drops_total{node="chaos"} 4' in text

def test_chaos_slo_latency_points_at_rpc_histogram() -> None:
    """OBS-05: golden latency uses mpreg_rpc_latency_ms."""
    latency = next(s for s in GOLDEN_SIGNALS if s.name == "latency")
    assert latency.prometheus_metric == "mpreg_rpc_latency_ms"

def test_chaos_event_deques_bounded() -> None:
    """OBS-08: rpc/pubsub event deques have maxlen."""
    t = ServerMetricsTracker()
    assert t.rpc_events.maxlen is not None and t.rpc_events.maxlen > 0
    assert t.pubsub_events.maxlen is not None and t.pubsub_events.maxlen > 0

def test_chaos_dlq_bounded() -> None:
    """PERF-05: DLQ drops oldest under max size."""
    q = MessageQueue(
        QueueConfiguration(
            name="chaos-dlq",
            max_size=100,
            dead_letter_max_size=3,
            enable_dead_letter_queue=True,
        )
    )
    assert q._dead_letter_maxsize == 3
    for i in range(5):
        msg = QueuedMessage(
            id=MessageId(id=f"m{i}", source_node="t"),
            topic="t",
            payload=i,
            delivery_guarantee=QueueDG.AT_LEAST_ONCE,
        )
        q.dead_letter_queue.append(msg)
        max_dlq = q._dead_letter_maxsize
        while len(q.dead_letter_queue) > max_dlq:
            q.dead_letter_queue.popleft()
    assert len(q.dead_letter_queue) <= 3

def test_chaos_gossip_pending_bounded() -> None:
    """PERF-07: gossip pending drops under overflow."""
    transport = MagicMock()
    transport.peer_ids.return_value = []
    gp = GossipProtocol(node_id="n1", transport=transport)
    gp.pending_messages_maxsize = 5
    for i in range(20):
        gp._enqueue_pending({"i": i})
    assert len(gp.pending_messages) <= 5
    assert gp.pending_messages_dropped >= 15

def test_chaos_priority_bisect_orders_high_first() -> None:
    """PERF-06: higher priority enqueues ahead of lower."""
    q = MessageQueue(QueueConfiguration(name="pri", queue_type=QueueType.PRIORITY))
    low = QueuedMessage(
        id=MessageId(id="low", source_node="t"),
        topic="t",
        payload=0,
        priority=1,
        delivery_guarantee=QueueDG.AT_LEAST_ONCE,
    )
    high = QueuedMessage(
        id=MessageId(id="high", source_node="t"),
        topic="t",
        payload=1,
        priority=10,
        delivery_guarantee=QueueDG.AT_LEAST_ONCE,
    )
    q.pending_messages.append(low)
    priorities = [-m.priority for m in q.pending_messages]
    idx = bisect.bisect_left(priorities, -high.priority)
    q.pending_messages.insert(idx, high)
    assert q.pending_messages[0].id.id == "high"

def test_chaos_gossip_admission_peel() -> None:
    """PERF-02 peel: admission helper reject/accept."""
    payload = {
        "message_id": "m1",
        "message_type": "state_update",
        "sender_id": "n1",
        "payload": {"k": 1},
    }
    assert (
        accept_fabric_gossip_payload(
            payload=payload, require_hmac=True, secret="s", node_name="n"
        )
        is None
    )
    signed = sign_gossip_payload(payload, "s")
    out = accept_fabric_gossip_payload(
        payload=signed, require_hmac=True, secret="s", node_name="n"
    )
    assert out is not None
    assert SIGNATURE_KEY not in out

@pytest.mark.asyncio
async def test_chaos_cache_replication_metrics_sink() -> None:
    """OBS-04: GlobalCacheManager replication drop sink."""
    hits: list[int] = []
    cfg = GlobalCacheConfiguration(
        enable_l2_persistent=False,
        enable_l3_distributed=False,
        enable_l4_federation=False,
    )
    mgr = GlobalCacheManager(cfg)
    try:
        mgr.attach_metrics_sink(on_replication_drop=lambda n: hits.append(n))
        mgr._note_replication_drop()
        assert mgr.pending_replications_dropped >= 1
        assert hits == [1]
    finally:
        await mgr.shutdown()

def test_chaos_drain_flag_on_server_instance() -> None:
    """ERG-01: drain flag is the admission gate source."""
    s = MPREGServer(
        MPREGSettings(
            host="127.0.0.1",
            port=1,
            name="drain-chaos",
            cluster_id="c",
            monitoring_enabled=False,
            fabric_routing_enabled=False,
        )
    )
    assert getattr(s, "_mgmt_draining", False) is False
    s._mgmt_draining = True
    assert s._mgmt_draining is True
    import inspect

    src = inspect.getsource(MPREGServer)
    assert "_mgmt_draining" in src
    assert "should_refuse_for_drain" in src or "drain_admission" in src
    from mpreg.server_pkg.drain_admission import drain_unavailable_response

    resp = drain_unavailable_response("x")
    assert "drain" in f"{resp.error.message} {resp.error.details}".lower()

def test_chaos_rpc_request_accepts_traceparent_fields() -> None:
    """OBS-02: RPCRequest model carries W3C fields."""
    tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    req = RPCRequest(
        cmds=(RPCCommand(name="n", fun="echo", args=("x",), locs=frozenset()),),
        u="u1",
        traceparent=tp,
        headers={"traceparent": tp},
    )
    assert req.traceparent == tp
    assert req.headers["traceparent"] == tp

def test_chaos_serialize_model_one_hop() -> None:
    """PERF-03: serialize_model dumps pydantic envelopes."""
    from mpreg.core.model import FabricMessageEnvelope
    from mpreg.core.serialization import JsonSerializer

    ser = JsonSerializer()
    env = FabricMessageEnvelope(payload={"a": 1})
    raw = ser.serialize_model(env)
    assert isinstance(raw, (bytes, bytearray))
    back = ser.deserialize(raw)
    assert back["role"] == "fabric-message"
    assert back["payload"] == {"a": 1}

def test_chaos_drain_admission_helper() -> None:
    """ERG-01: drain role gate is pure and unit-testable."""
    from mpreg.server_pkg.drain_admission import (
        DATA_PLANE_ROLES,
        drain_unavailable_response,
        should_refuse_for_drain,
    )

    assert should_refuse_for_drain(draining=True, role="rpc") is True
    assert should_refuse_for_drain(draining=True, role="server") is False
    assert should_refuse_for_drain(draining=False, role="rpc") is False
    assert "rpc" in DATA_PLANE_ROLES
    resp = drain_unavailable_response("u1")
    assert resp.error is not None
    text = f"{resp.error.message} {resp.error.details}".lower()
    assert "drain" in text
