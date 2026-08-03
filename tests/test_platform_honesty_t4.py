"""Platform T4 honesty proofs: owner fail-closed, STRONG/EO reject, ops surface."""

from __future__ import annotations

import inspect
import time
from unittest.mock import MagicMock

import pytest

from mpreg.core.blockchain_message_queue import BlockchainMessageQueue
from mpreg.core.blockchain_message_queue_types import (
    BlockchainMessage,
    DeliveryGuarantee as BcDeliveryGuarantee,
    MessagePriority,
)
from mpreg.core.cache_models import GlobalCacheKey
from mpreg.core.config import MPREGSettings
from mpreg.core.location_consistency import (
    ConsistencyLevel,
    LocationConsistencyManager,
    ReplicationOperation,
    ReplicatedCacheEntry,
)
from mpreg.datastructures.vector_clock import VectorClock
from mpreg.core.logging import bind_trace_context
from mpreg.core.message_queue import DeliveryGuarantee as QueueDeliveryGuarantee
from mpreg.core.monitoring.server_monitoring import ServerMetricsTracker
from mpreg.core.namespace_policy import (
    NamespacePolicyEngine,
    NamespacePolicyRule,
)
from mpreg.fabric.federation_resilience import (
    FederationAutoRecovery,
    FederationHealthMonitor,
    HealthCheckConfiguration,
    HealthStatus,
    RetryConfiguration,
)
from mpreg.fabric.membership import MembershipProtocol
from mpreg.fabric.message import MessageHeaders
from mpreg.server import MPREGServer

def test_owner_write_fails_closed_without_actor_cluster() -> None:
    engine = NamespacePolicyEngine(
        enabled=True,
        default_allow=False,
        rules=(
            NamespacePolicyRule(
                namespace="owned",
                owners=("cluster-a",),
            ),
            NamespacePolicyRule(
                namespace="owned-vis",
                owners=("cluster-a",),
                visibility=("cluster-a", "cluster-b"),
            ),
        ),
    )
    denied = engine.allows_data_access(
        "owned", actor_cluster=None, actor_tenant_id=None, write=True
    )
    assert denied.allowed is False
    assert denied.reason == "owner_required"

    empty = engine.allows_data_access(
        "owned", actor_cluster="  ", actor_tenant_id=None, write=True
    )
    assert empty.allowed is False
    assert empty.reason == "owner_required"

    # Write + owners + missing actor: owner_required even when visibility exists.
    vis_missing = engine.allows_data_access(
        "owned-vis", actor_cluster=None, actor_tenant_id=None, write=True
    )
    assert vis_missing.allowed is False
    assert vis_missing.reason == "owner_required"

    ok = engine.allows_data_access(
        "owned", actor_cluster="cluster-a", actor_tenant_id=None, write=True
    )
    assert ok.allowed is True

    other = engine.allows_data_access(
        "owned", actor_cluster="cluster-b", actor_tenant_id=None, write=True
    )
    assert other.allowed is False
    assert other.reason == "owner_denied"

@pytest.mark.asyncio
async def test_location_strong_wait_and_get_fail_closed() -> None:
    key = GlobalCacheKey(namespace="ns", identifier="k", version="v1")
    entry = ReplicatedCacheEntry(
        key=key,
        value={"v": 1},
        version=1,
        vector_clock=VectorClock(),
        origin_cluster="c1",
        consistency_level=ConsistencyLevel.STRONG,
    )
    op = ReplicationOperation(
        operation_type="replicate",
        entry=entry,
        target_clusters=frozenset(["c2"]),
        source_cluster="c1",
    )
    mgr = object.__new__(LocationConsistencyManager)
    with pytest.raises(ValueError, match="STRONG is not implemented"):
        await LocationConsistencyManager._wait_for_strong_consistency(mgr, op)
    with pytest.raises(ValueError, match="STRONG is not implemented"):
        await LocationConsistencyManager._get_with_strong_consistency(mgr, key)

def test_blockchain_exactly_once_submit_rejected() -> None:
    q = BlockchainMessageQueue(queue_id="honesty-eo")
    msg = BlockchainMessage(
        sender_id="alice",
        recipient_id="bob",
        message_type="test",
        priority=MessagePriority.NORMAL,
        delivery_guarantee=BcDeliveryGuarantee.EXACTLY_ONCE,
        payload=b"nope",
        processing_fee=1,
    )
    assert q.submit_message(msg) is False

def test_queue_plane_has_no_exactly_once_member() -> None:
    names = {m.name for m in QueueDeliveryGuarantee}
    assert "EXACTLY_ONCE" not in names
    assert QueueDeliveryGuarantee.AT_LEAST_ONCE.value == "at_least_once"

@pytest.mark.asyncio
async def test_rpc_queue_send_rejects_exactly_once_and_unknown() -> None:
    server = object.__new__(MPREGServer)
    object.__setattr__(
        server, "settings", MPREGSettings(host="127.0.0.1", port=1, name="t4")
    )
    object.__setattr__(server, "_queue_manager", MagicMock())

    async def _never(*_a, **_k):  # pragma: no cover
        raise AssertionError("send_message must not be called for unsupported DG")

    server._queue_manager.send_message = _never

    eo = await MPREGServer._rpc_queue_send(
        server, {"queue_name": "q", "payload": 1, "delivery_guarantee": "exactly_once"}
    )
    assert eo["success"] is False
    assert "exactly_once" in eo["error_message"]

    bad = await MPREGServer._rpc_queue_send(
        server, {"queue_name": "q", "payload": 1, "delivery_guarantee": "magic"}
    )
    assert bad["success"] is False
    assert "unsupported_delivery_guarantee:magic" in bad["error_message"]

def test_metrics_tracker_emits_draining_and_mgmt() -> None:
    t = ServerMetricsTracker()
    t.set_draining(True)
    t.record_mgmt_mutation("node_drain", success=True)
    t.set_notification_drops(3)
    lines = "\n".join(t.prometheus_lines('node="n1"'))
    assert "mpreg_node_draining" in lines
    assert 'mpreg_node_draining{node="n1"} 1' in lines
    assert "mpreg_mgmt_mutations_total" in lines
    assert "node_drain" in lines
    assert (
        'mpreg_client_notification_drops_total{node="n1"} 3' in lines
    )

def test_bind_trace_context_adds_fields() -> None:
    log = bind_trace_context(
        traceparent="00-abc-def-01",
        correlation_id="corr-1",
        request_u="u-9",
    )
    assert log is not None
    assert hasattr(log, "info")

def test_mpreg_root_exports_unified_client() -> None:
    import mpreg

    assert hasattr(mpreg, "MPREGClient")
    assert hasattr(mpreg, "UnifiedMPREGClient")
    assert mpreg.MPREGClient is mpreg.UnifiedMPREGClient

def test_cli_admin_commands_registered() -> None:
    from click.testing import CliRunner

    from mpreg.cli.main import cli

    runner = CliRunner()
    result = runner.invoke(cli, ["admin", "--help"])
    assert result.exit_code == 0
    assert "drain" in result.output
    assert "detach" in result.output
    assert "audit" in result.output

@pytest.mark.asyncio
async def test_federation_health_probe_reports_unknown_not_fake_healthy() -> None:
    monitor = FederationHealthMonitor(
        cluster_id="local",
        health_config=HealthCheckConfiguration(),
        retry_config=RetryConfiguration(),
    )
    result = await monitor._perform_health_check("remote")
    assert result.status == HealthStatus.UNKNOWN.value
    assert "unimplemented" in (result.error_message or "").lower()

@pytest.mark.asyncio
async def test_federation_recovery_strategies_refuse_silent_success() -> None:
    monitor = FederationHealthMonitor(
        cluster_id="local",
        health_config=HealthCheckConfiguration(),
        retry_config=RetryConfiguration(),
    )
    recovery = FederationAutoRecovery(
        cluster_id="local",
        health_monitor=monitor,
        retry_config=RetryConfiguration(),
    )
    assert await recovery._graceful_degradation_recovery("c1") is False
    assert await recovery._failover_recovery("c1") is False
    assert await recovery._exponential_backoff_recovery("c1") is False
    assert await recovery._immediate_retry_recovery("c1") is False
    assert await recovery._circuit_breaker_recovery("c1") is False

def test_membership_module_documents_library_only() -> None:
    import mpreg.fabric.membership as mem_mod

    blob = (inspect.getdoc(mem_mod) or "") + "\n" + (inspect.getdoc(MembershipProtocol) or "")
    assert (
        "library-only" in blob.lower()
        or "not wired" in blob.lower()
        or "Integration status" in blob
    )

def test_hop_deadline_uses_measured_mono_when_stamped() -> None:
    server = object.__new__(MPREGServer)
    server.settings = MPREGSettings(host="127.0.0.1", port=1, name="hop")

    class _Cluster:
        local_url = "ws://127.0.0.1:1"

    server.cluster = _Cluster()  # type: ignore[attr-defined]
    entered = time.monotonic() - 0.05  # ~50ms ago
    headers = MessageHeaders(
        correlation_id="c",
        source_cluster="c1",
        routing_path=("ws://other:1",),
        federation_path=("c1",),
        hop_budget=10,
        metadata={"mpreg.hop_entered_mono": f"{entered:.6f}"},
        deadline_remaining_ms=1000.0,
    )
    next_h = server._next_fabric_headers("c", headers, max_hops=10)
    assert next_h is not None
    assert next_h.deadline_remaining_ms is not None
    # Charged ~50ms; remaining must drop by more than the old 1ms stub.
    assert next_h.deadline_remaining_ms < 1000.0 - 10.0
    assert "mpreg.hop_entered_mono" in next_h.metadata
