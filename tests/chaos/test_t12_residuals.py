"""T12 residual closeout proofs: InstallSnapshot leader fail-closed, fed ACK, obs wiring."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from mpreg.core.message_queue import (
    DeliveryGuarantee as QueueDeliveryGuarantee,
)
from mpreg.core.message_queue import QueuedMessage
from mpreg.core.message_queue_manager import (
    MessageQueueManager,
    QueueManagerConfiguration,
)
from mpreg.core.monitoring.server_monitoring import ServerMetricsTracker
from mpreg.datastructures.message_structures import MessageId
from mpreg.datastructures.production_raft import (
    LeaderVolatileState,
    PersistentState,
    RaftState,
)
from mpreg.datastructures.production_raft_implementation import (
    ProductionRaft,
    RaftConfiguration,
)
from mpreg.datastructures.raft_storage_adapters import RaftStorageFactory
from mpreg.fabric.catalog import RoutingCatalog
from mpreg.fabric.catalog_delta import RoutingCatalogApplier, RoutingCatalogDelta
from mpreg.fabric.index import RoutingIndex
from mpreg.fabric.message import (
    DeliveryGuarantee as FabricDeliveryGuarantee,
)
from mpreg.fabric.message import MessageHeaders, MessageType, UnifiedMessage
from mpreg.fabric.queue_federation import (
    FabricQueueFederationManager,
    FabricQueueInFlight,
)
from mpreg.fabric.queue_messages import (
    QueueFederationAck,
    QueueFederationRequest,
    QueueMessageOptions,
)
from tests.test_production_raft_integration import TestableStateMachine

class _NullTransport:
    async def send_request_vote(self, target, request):  # type: ignore[no-untyped-def]
        return None

    async def send_append_entries(self, target, request):  # type: ignore[no-untyped-def]
        return None

    async def send_install_snapshot(self, target, request):  # type: ignore[no-untyped-def]
        return None

def _make_node(node_id: str = "n1", members: set[str] | None = None) -> ProductionRaft:
    members = members or {node_id, "n2"}
    return ProductionRaft(
        node_id=node_id,
        cluster_members=members,
        storage=RaftStorageFactory.create_memory_storage(node_id),
        transport=_NullTransport(),
        state_machine=TestableStateMachine(),
        config=RaftConfiguration(
            election_timeout_min=0.15,
            election_timeout_max=0.30,
            heartbeat_interval=0.025,
            snapshot_threshold=5,
        ),
    )

def _leader_with_snapshot_base(
    node_id: str = "L", follower: str = "f1"
) -> ProductionRaft:
    node = _make_node(node_id, {node_id, follower})
    node.current_state = RaftState.LEADER
    node.persistent_state = PersistentState(
        current_term=3, voted_for=node_id, log_entries=[]
    )
    node._snapshot_last_index = 50
    node._snapshot_last_term = 2
    node.volatile_state.last_applied = 50
    node.volatile_state.commit_index = 50
    node.leader_volatile_state = LeaderVolatileState(
        cluster_members={node_id, follower},
        last_log_index=50,
        leader_id=node_id,
    )
    # Force follower behind so match stays 0 until install succeeds
    node.leader_volatile_state.next_index[follower] = 1
    node.leader_volatile_state.match_index[follower] = 0
    return node

@pytest.mark.asyncio
async def test_cor_t12_01_install_snapshot_leader_fail_closed_missing_success() -> None:
    """COR-T12-01: leader must not advance match/next when success attr missing."""
    node = _leader_with_snapshot_base()
    legacy_resp = SimpleNamespace(term=3)  # no .success

    async def _send(_fid, _req):  # type: ignore[no-untyped-def]
        return legacy_resp

    node.transport.send_install_snapshot = _send  # type: ignore[method-assign]

    await node._send_snapshot_to_follower("f1")

    assert node.leader_volatile_state is not None
    assert node.leader_volatile_state.match_index["f1"] == 0
    assert node.leader_volatile_state.next_index["f1"] == 1

@pytest.mark.asyncio
async def test_cor_t12_01b_install_snapshot_leader_advances_on_explicit_success() -> (
    None
):
    """COR-T12-01: explicit success=True advances match/next after full install."""
    node = _leader_with_snapshot_base()
    ok_resp = SimpleNamespace(term=3, success=True)

    async def _send(_fid, _req):  # type: ignore[no-untyped-def]
        return ok_resp

    node.transport.send_install_snapshot = _send  # type: ignore[method-assign]

    await node._send_snapshot_to_follower("f1")

    assert node.leader_volatile_state is not None
    assert node.leader_volatile_state.match_index["f1"] == 50
    assert node.leader_volatile_state.next_index["f1"] == 51

def test_cor_t12_02_queue_federation_ack_missing_success_false() -> None:
    """COR-T12-02: missing success on wire → False (fail-closed)."""
    ack = QueueFederationAck.from_dict(
        {
            "ack_token": "t1",
            "message_id": "m1",
            "acknowledging_cluster": "c2",
            "acknowledging_subscriber": "s1",
        }
    )
    assert ack.success is False

    ack_ok = QueueFederationAck.from_dict(
        {
            "ack_token": "t1",
            "message_id": "m1",
            "acknowledging_cluster": "c2",
            "acknowledging_subscriber": "s1",
            "success": True,
        }
    )
    assert ack_ok.success is True

    bare = QueueFederationAck(
        ack_token="t",
        message_id="m",
        acknowledging_cluster="c",
        acknowledging_subscriber="s",
    )
    assert bare.success is False

def _fed_mgr(**kwargs: object) -> FabricQueueFederationManager:
    return FabricQueueFederationManager(
        cluster_id="c1",
        node_id="n1",
        routing_index=RoutingIndex(catalog=RoutingCatalog()),
        queue_manager=MessageQueueManager(QueueManagerConfiguration()),
        queue_announcer=SimpleNamespace(announce=AsyncMock()),  # type: ignore[arg-type]
        messenger=SimpleNamespace(),  # type: ignore[arg-type]
        **kwargs,  # type: ignore[arg-type]
    )

@pytest.mark.asyncio
async def test_cor_t12_02b_failed_ack_does_not_count_success() -> None:
    """COR-T12-02: success=False ACK must not satisfy in_flight quorum."""
    mgr = _fed_mgr()
    req = QueueFederationRequest(
        request_id="r1",
        queue_name="q",
        topic="t",
        payload={},
        delivery_guarantee=QueueDeliveryGuarantee.AT_LEAST_ONCE,
        source_cluster="c1",
        target_cluster="c2",
        ack_token="ack-1",
        required_cluster_acks=1,
        options=QueueMessageOptions(),
    )
    mgr.in_flight["ack-1"] = FabricQueueInFlight(request=req, required_cluster_acks=1)
    um = UnifiedMessage(
        message_id="m1",
        topic="mpreg.queue.ack",
        message_type=MessageType.QUEUE,
        delivery=FabricDeliveryGuarantee.AT_LEAST_ONCE,
        payload={},
        headers=MessageHeaders(
            correlation_id="corr-1",
            source_cluster="c2",
            target_cluster="c1",
            hop_budget=8,
        ),
    )
    fail_ack = QueueFederationAck(
        ack_token="ack-1",
        message_id="r1",
        acknowledging_cluster="c2",
        acknowledging_subscriber="s",
        success=False,
        error_message="boom",
    )
    await mgr._handle_queue_ack(fail_ack, um, None)
    assert "ack-1" not in mgr.in_flight
    assert mgr.stats.failed_deliveries == 1
    assert mgr.stats.successful_deliveries == 0

@pytest.mark.asyncio
async def test_cor_t12_02c_success_ack_counts() -> None:
    mgr = _fed_mgr()
    req = QueueFederationRequest(
        request_id="r1",
        queue_name="q",
        topic="t",
        payload={},
        delivery_guarantee=QueueDeliveryGuarantee.AT_LEAST_ONCE,
        source_cluster="c1",
        target_cluster="c2",
        ack_token="ack-1",
        required_cluster_acks=1,
        options=QueueMessageOptions(),
    )
    mgr.in_flight["ack-1"] = FabricQueueInFlight(request=req, required_cluster_acks=1)
    um = UnifiedMessage(
        message_id="m1",
        topic="mpreg.queue.ack",
        message_type=MessageType.QUEUE,
        delivery=FabricDeliveryGuarantee.AT_LEAST_ONCE,
        payload={},
        headers=MessageHeaders(
            correlation_id="corr-1",
            source_cluster="c2",
            target_cluster="c1",
            hop_budget=8,
        ),
    )
    ok_ack = QueueFederationAck(
        ack_token="ack-1",
        message_id="r1",
        acknowledging_cluster="c2",
        acknowledging_subscriber="s",
        success=True,
    )
    await mgr._handle_queue_ack(ok_ack, um, None)
    assert "ack-1" not in mgr.in_flight
    assert mgr.stats.successful_deliveries == 1
    assert mgr.stats.failed_deliveries == 0

def test_obs_t12_01_catalog_dedup_hook() -> None:
    hits: list[int] = []
    applier = RoutingCatalogApplier(
        catalog=RoutingCatalog(),
        on_dedup_skip=lambda n: hits.append(n),
    )
    empty = RoutingCatalogDelta(update_id="", cluster_id="c1")
    applier.apply(empty)
    assert hits == [1]

    d = RoutingCatalogDelta(update_id="u1", cluster_id="c1")
    applier.apply(d)
    applier.apply(d)  # dedup
    assert hits == [1, 1]

@pytest.mark.asyncio
async def test_obs_t12_01_fed_in_flight_drop_hook() -> None:
    hits: list[int] = []
    mgr = _fed_mgr(on_in_flight_drop=lambda n: hits.append(n))
    mgr.in_flight_maxsize = 1
    mgr.in_flight["existing"] = FabricQueueInFlight(
        request=QueueFederationRequest(
            request_id="r0",
            queue_name="q",
            topic="t",
            payload={},
            delivery_guarantee=QueueDeliveryGuarantee.AT_LEAST_ONCE,
            source_cluster="c1",
            target_cluster="c2",
            ack_token="existing",
            required_cluster_acks=1,
            options=QueueMessageOptions(),
        )
    )
    result = await mgr._send_remote_message(
        queue_name="q",
        topic="t",
        payload={},
        delivery_guarantee=QueueDeliveryGuarantee.AT_LEAST_ONCE,
        target_cluster="c2",
        required_cluster_acks=1,
        options=QueueMessageOptions(),
    )
    assert result.success is False
    assert result.error_message == "federation_in_flight_full"
    assert hits == [1]
    assert mgr.in_flight_drops == 1

@pytest.mark.asyncio
async def test_obs_t12_02_dlq_hook_on_manager() -> None:
    hits: list[int] = []
    mgr = MessageQueueManager(QueueManagerConfiguration())
    mgr.set_on_dlq(lambda n: hits.append(n))
    ok = await mgr.create_queue("q-dlq")
    assert ok
    q = mgr.queues["q-dlq"]
    assert q.on_dlq is not None
    msg = QueuedMessage(
        id=MessageId.generate(),
        topic="t",
        payload={},
        delivery_guarantee=QueueDeliveryGuarantee.AT_LEAST_ONCE,
    )
    await q._move_to_dead_letter_queue(msg, "test")
    assert hits == [1]

def test_obs_t12_metrics_prom_series_still_present() -> None:
    t = ServerMetricsTracker()
    t.record_federation_in_flight_drop()
    t.record_catalog_dedup_skip()
    t.record_queue_dlq()
    t.record_accept_reject()
    lines = "\n".join(t.prometheus_lines('cluster_id="c"'))
    assert "mpreg_queue_federation_in_flight_drops_total" in lines
    assert "mpreg_catalog_dedup_skips_total" in lines
    assert "mpreg_queue_dlq_total" in lines
    assert "mpreg_accept_rejects_total" in lines
