"""Platform T4 honesty proofs: owner fail-closed, STRONG/EO reject, ops surface."""

from __future__ import annotations

import inspect
import time
from unittest.mock import MagicMock

import pytest

from mpreg.core.blockchain_message_queue import BlockchainMessageQueue
from mpreg.core.blockchain_message_queue_types import (
    BlockchainMessage,
    MessagePriority,
)
from mpreg.core.blockchain_message_queue_types import (
    DeliveryGuarantee as BcDeliveryGuarantee,
)
from mpreg.core.cache_models import GlobalCacheKey
from mpreg.core.config import MPREGSettings
from mpreg.core.location_consistency import (
    ConsistencyLevel,
    LocationConsistencyManager,
    ReplicatedCacheEntry,
    ReplicationOperation,
)
from mpreg.core.logging import bind_trace_context
from mpreg.core.message_queue import DeliveryGuarantee as QueueDeliveryGuarantee
from mpreg.core.monitoring.server_monitoring import ServerMetricsTracker
from mpreg.core.namespace_policy import (
    NamespacePolicyEngine,
    NamespacePolicyRule,
)
from mpreg.datastructures.vector_clock import VectorClock
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


@pytest.mark.asyncio
async def test_location_replicate_strong_no_residual() -> None:
    """COR-02: full replicate_cache_entry(STRONG) leaves no local entry or queue item."""
    import asyncio

    from mpreg.core.location_consistency import (
        LocationConsistencyConfig,
        LocationInfo,
    )
    from mpreg.datastructures.vector_clock import VectorClock as VC

    loc = LocationInfo(cluster_id="c1", region="r1", zone="az1")
    cfg = LocationConsistencyConfig(default_consistency_level=ConsistencyLevel.EVENTUAL)
    mgr = object.__new__(LocationConsistencyManager)
    mgr.location_info = loc
    mgr.config = cfg
    mgr.vector_clock = VC.from_dict({"c1": 0})
    mgr.replicated_entries = {}
    mgr.replication_queue = asyncio.Queue(maxsize=100)
    mgr.cluster_locations = {"c1": loc}

    key = GlobalCacheKey(namespace="ns", identifier="strong-k", version="v1")
    with pytest.raises(ValueError, match="STRONG is not implemented"):
        await mgr.replicate_cache_entry(
            key, {"v": 1}, consistency_level=ConsistencyLevel.STRONG
        )
    entry_key = mgr._entry_key(key)
    assert entry_key not in mgr.replicated_entries
    assert mgr.replication_queue.empty()


def test_blockchain_exactly_once_submit_rejected() -> None:
    from mpreg.core.blockchain_message_queue import UnsupportedDeliveryGuaranteeError

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
    with pytest.raises(
        UnsupportedDeliveryGuaranteeError, match="EXACTLY_ONCE|exactly_once"
    ):
        q.submit_message(msg)


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
    assert 'mpreg_client_notification_drops_total{node="n1"} 3' in lines


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

    blob = (
        (inspect.getdoc(mem_mod) or "")
        + "\n"
        + (inspect.getdoc(MembershipProtocol) or "")
    )
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


@pytest.mark.asyncio
async def test_global_quorum_name_vote_refused_without_lab_flag() -> None:
    from unittest.mock import MagicMock

    from mpreg.fabric.queue_delivery import FabricQueueDeliveryCoordinator

    coord = FabricQueueDeliveryCoordinator(
        cluster_id="c",
        queue_federation=MagicMock(),
    )
    result = await coord.deliver_with_global_quorum(
        queue_name="q",
        topic="t",
        payload=1,
        target_clusters={"a"},
    )
    assert result.success is False
    assert "unsupported_global_consensus" in (result.error_message or "")


def test_mgmt_audit_jsonl_persistence(tmp_path) -> None:
    from mpreg.server_pkg.mgmt_mutations import MgmtAuditEntry, MgmtAuditLog

    path = tmp_path / "audit.jsonl"
    log = MgmtAuditLog(max_entries=10, persist_path=str(path))
    log.record(
        MgmtAuditEntry(
            event="node_drain",
            timestamp=1.0,
            actor="op",
            success=True,
            detail={"draining": True},
        )
    )
    assert path.is_file()
    reloaded = MgmtAuditLog(max_entries=10, persist_path=str(path))
    snap = reloaded.snapshot()
    assert len(snap) == 1
    assert snap[0]["event"] == "node_drain"


def test_pending_replications_bounded_drop_oldest() -> None:
    from mpreg.core.cache_models import GlobalCacheKey
    from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager

    cfg = GlobalCacheConfiguration(
        enable_l2_persistent=False,
        enable_l3_distributed=False,
        enable_l4_federation=False,
        pending_replications_maxsize=2,
    )
    mgr = GlobalCacheManager(cfg)
    try:
        k1 = GlobalCacheKey(namespace="n", identifier="a")
        k2 = GlobalCacheKey(namespace="n", identifier="b")
        k3 = GlobalCacheKey(namespace="n", identifier="c")
        mgr._enqueue_replication("put", k1, None)
        mgr._enqueue_replication("put", k2, None)
        mgr._enqueue_replication("put", k3, None)
        assert mgr.pending_replications.qsize() == 2
        assert mgr.pending_replications_dropped >= 1
        stats = mgr.get_statistics()
        assert stats["replication_statistics"]["dropped_operations"] >= 1
    finally:
        mgr.shutdown_sync()


@pytest.mark.asyncio
async def test_plane_rpc_module_queue_send_rejects_eo() -> None:
    from unittest.mock import MagicMock

    from mpreg.core.config import MPREGSettings
    from mpreg.server_pkg import plane_rpc

    server = MagicMock()
    server.settings = MPREGSettings(host="127.0.0.1", port=1, name="t")
    server._queue_manager = MagicMock()

    async def _never(*a, **k):
        raise AssertionError("should not send")

    server._queue_manager.send_message = _never
    result = await plane_rpc.queue_send(
        server, {"queue_name": "q", "payload": 1, "delivery_guarantee": "exactly_once"}
    )
    assert result["success"] is False
    assert "exactly_once" in result["error_message"]


def test_dev_profile_four_planes_in_package() -> None:
    from pathlib import Path

    from mpreg.core.config import MPREGSettings

    path = Path(__file__).resolve().parents[1] / "mpreg" / "profiles" / "dev.toml"
    s = MPREGSettings.from_path(path)
    assert s.enable_default_cache and s.enable_default_queue


# --- T7 residual closeout ---


def test_gossip_hmac_sign_verify_roundtrip() -> None:
    from mpreg.fabric.gossip_signatures import (
        accept_gossip_payload,
        sign_gossip_payload,
        verify_gossip_payload,
    )

    payload = {"message_id": "m1", "sender_id": "n1", "payload": {"k": 1}}
    secret = "lab-secret"
    signed = sign_gossip_payload(payload, secret)
    assert verify_gossip_payload(signed, secret)
    assert accept_gossip_payload(signed, require_hmac=True, secret=secret)
    tampered = dict(signed)
    tampered["sender_id"] = "evil"
    assert not verify_gossip_payload(tampered, secret)
    assert not accept_gossip_payload(payload, require_hmac=True, secret=secret)
    # require without secret refuses
    assert not accept_gossip_payload(signed, require_hmac=True, secret=None)
    # require off accepts unsigned
    assert accept_gossip_payload(payload, require_hmac=False, secret=None)


@pytest.mark.asyncio
async def test_server_gossip_transport_signs_when_required() -> None:

    from mpreg.fabric.gossip import GossipMessage, GossipMessageType
    from mpreg.fabric.gossip_signatures import SIGNATURE_KEY, verify_gossip_payload
    from mpreg.fabric.server_gossip_transport import ServerGossipTransport

    class _T(ServerGossipTransport):
        def __init__(self) -> None:
            # Bypass full parent init; only exercise send_message signing path.
            self.require_hmac = True
            self.hmac_secret = "s3cret"
            self._captured: list = []

        async def send_envelope(self, peer_id, envelope):  # type: ignore[override]
            self._captured.append(envelope)
            return True

    transport = _T()
    msg = GossipMessage(
        message_id="g1",
        message_type=GossipMessageType.STATE_UPDATE,
        sender_id="n1",
        payload={"k": "v"},
    )
    assert await transport.send_message("peer", msg)
    assert transport._captured
    payload = transport._captured[0].payload
    assert SIGNATURE_KEY in payload
    assert verify_gossip_payload(payload, "s3cret")


@pytest.mark.asyncio
async def test_queue_receive_rpc_and_manager() -> None:
    from unittest.mock import MagicMock

    from mpreg.core.message_queue import DeliveryGuarantee
    from mpreg.core.message_queue_manager import (
        MessageQueueManager,
        QueueManagerConfiguration,
    )
    from mpreg.server_pkg import plane_rpc

    mgr = MessageQueueManager(QueueManagerConfiguration(local_cluster_id="c1"))
    await mgr.create_queue("jobs")
    sent = await mgr.send_message(
        "jobs", "mpreg.queue.jobs", {"x": 42}, DeliveryGuarantee.AT_LEAST_ONCE
    )
    assert sent.success

    # Direct manager receive
    msg = await mgr.receive_message("jobs", timeout_seconds=2.0, auto_acknowledge=True)
    assert msg is not None
    assert getattr(msg, "payload", None) == {"x": 42}

    # Second send + RPC path
    await mgr.send_message(
        "jobs", "mpreg.queue.jobs", {"y": 1}, DeliveryGuarantee.AT_LEAST_ONCE
    )
    server = MagicMock()
    server.settings = MagicMock(cluster_id="c1")
    server._queue_manager = mgr
    result = await plane_rpc.queue_receive(
        server,
        {
            "queue_name": "jobs",
            "timeout_seconds": 2.0,
            "auto_acknowledge": True,
        },
    )
    assert result["success"] is True
    assert result["empty"] is False
    assert result["message"]["payload"] == {"y": 1}


@pytest.mark.asyncio
async def test_unified_client_queue_receive_method() -> None:
    from mpreg.client import MPREGClient
    from mpreg.client.client_api import MPREGClientAPI

    client = MPREGClient(url="ws://127.0.0.1:9")
    calls: list = []

    from mpreg.core.rpc_naming import PlatformRpc

    async def fake_call(self, fun, *args, **kwargs):
        calls.append(fun)
        if fun == PlatformRpc.QUEUE_RECEIVE:
            return {"success": True, "empty": False, "message": {"payload": 1}}
        return {}

    orig = MPREGClientAPI.call
    try:
        MPREGClientAPI.call = fake_call  # type: ignore[method-assign]
        out = await client.queue_receive("jobs", timeout_seconds=1.0)
        assert out["message"]["payload"] == 1
        assert PlatformRpc.QUEUE_RECEIVE in calls
    finally:
        MPREGClientAPI.call = orig  # type: ignore[method-assign]


def test_accept_queue_default_is_bounded() -> None:
    from mpreg.core.transport.defaults import DEFAULT_ACCEPT_QUEUE_MAXSIZE

    assert DEFAULT_ACCEPT_QUEUE_MAXSIZE >= 64


def test_accept_fabric_gossip_strips_hmac_field() -> None:
    from mpreg.core.config import MPREGSettings
    from mpreg.fabric.gossip_signatures import SIGNATURE_KEY, sign_gossip_payload
    from mpreg.server import MPREGServer

    settings = MPREGSettings(
        host="127.0.0.1",
        port=1,
        name="hmac-node",
        cluster_id="c",
        fabric_gossip_require_hmac=True,
        fabric_gossip_hmac_secret="lab-secret",
    )
    server = MPREGServer(settings)
    payload = {
        "message_id": "m1",
        "message_type": "state_update",
        "sender_id": "n1",
        "payload": {"key": "a", "value": 1},
    }
    signed = sign_gossip_payload(payload, "lab-secret")
    assert SIGNATURE_KEY in signed
    out = server._accept_fabric_gossip_payload(signed)
    assert out is not None
    assert SIGNATURE_KEY not in out
    assert out["message_id"] == "m1"
    # reject unsigned when required
    assert server._accept_fabric_gossip_payload(payload) is None


# --- T9 COR residual closeout ---


def test_delivery_guarantee_cross_plane_conversion() -> None:
    """COR-14: fabric EO cannot convert onto queue plane; shared wires round-trip."""
    from mpreg.core.message_queue import DeliveryGuarantee as QueueDG
    from mpreg.fabric.message import DeliveryGuarantee as FabricDG

    assert FabricDG.from_wire("at_least_once") is FabricDG.AT_LEAST_ONCE
    assert FabricDG.AT_LEAST_ONCE.to_queue_guarantee() is QueueDG.AT_LEAST_ONCE
    assert FabricDG.BROADCAST.to_queue_guarantee() is QueueDG.BROADCAST
    with pytest.raises(ValueError, match="exactly_once"):
        FabricDG.EXACTLY_ONCE.to_queue_guarantee()


def test_name_vote_experimental_off_in_shipped_profiles() -> None:
    """COR-15: no profile enables name-vote theater."""
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "mpreg" / "profiles"
    for path in sorted(root.glob("*.toml")):
        text = path.read_text(encoding="utf-8")
        assert "experimental_name_vote_consensus" not in text, path.name


@pytest.mark.asyncio
async def test_create_queue_honors_namespace_policy() -> None:
    """COR-06: queue provision is a write gated by namespace policy."""
    from mpreg.core.message_queue_manager import (
        MessageQueueManager,
        QueueManagerConfiguration,
    )
    from mpreg.core.namespace_policy import (
        NamespacePolicyEngine,
        NamespacePolicyRule,
        actor_context,
    )

    engine = NamespacePolicyEngine(
        enabled=True,
        default_allow=False,
        rules=(
            NamespacePolicyRule(
                namespace="owned",
                owners=("owner-cluster",),
            ),
        ),
    )
    mgr = MessageQueueManager(
        QueueManagerConfiguration(enable_auto_queue_creation=False),
        namespace_policy=engine,
    )
    try:
        with actor_context(cluster_id="intruder", tenant_id=None):
            ok = await mgr.create_queue("owned.q1")
            assert ok is False
        with actor_context(cluster_id="owner-cluster", tenant_id=None):
            ok = await mgr.create_queue("owned.q1")
            assert ok is True
    finally:
        await mgr.shutdown()


def test_cli_admin_policy_registered() -> None:
    """ERG-02: admin policy CLI exists."""
    from click.testing import CliRunner

    from mpreg.cli.main import cli

    runner = CliRunner()
    result = runner.invoke(cli, ["admin", "--help"])
    assert result.exit_code == 0
    assert "policy" in result.output


def test_cli_client_plane_smokes_registered() -> None:
    """ERG-05: queue/cache/publish client commands registered."""
    from click.testing import CliRunner

    from mpreg.cli.main import cli

    runner = CliRunner()
    result = runner.invoke(cli, ["client", "--help"])
    assert result.exit_code == 0
    for name in (
        "queue-send",
        "cache-get",
        "cache-put",
        "cache-strong-retry-abort",
        "publish",
    ):
        assert name in result.output, name


def test_openapi_covers_golden_extra_routes() -> None:
    """ERG-09: OpenAPI lists topology/metrics splits/alerts/config."""
    from mpreg.server_pkg.openapi_surface import build_monitoring_openapi

    paths = build_monitoring_openapi()["paths"]
    for p in (
        "/health/clusters",
        "/metrics/rpc",
        "/metrics/cache",
        "/topology",
        "/alerts",
        "/config",
        "/discovery/summary",
    ):
        assert p in paths, p


def test_readme_honesty_no_bft_planet_private_api() -> None:
    """ERG-03: root README front door is honest."""
    import re
    from pathlib import Path

    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text(
        encoding="utf-8"
    )
    assert "Honesty banner" in readme or "honesty banner" in readme.lower()
    assert "_client.request" not in readme
    assert "not BFT" in readme or "not Byzantine" in readme
    # Product claims must not sell planet-scale; lab tool filenames may remain.
    body = re.sub(r"debug_planet_scale\S*", "", readme, flags=re.IGNORECASE)
    body = re.sub(r"planet_scale\S*", "", body, flags=re.IGNORECASE)
    assert "planet-scale" not in body.lower()


def test_support_only_section_in_claims() -> None:
    """B3: unclaimed invariant tests listed as support_only."""
    from pathlib import Path

    import yaml

    path = Path(__file__).resolve().parents[1] / "tests" / "invariants" / "claims.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert "support_only" in data
    assert any("test_fault_injector" in s for s in data["support_only"])


def test_docs_no_guaranteed_single_delivery_claim() -> None:
    """ERG-07: blockchain arch must not market EO as guaranteed single delivery."""
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    blob = (root / "docs" / "BLOCKCHAIN_MESSAGE_QUEUE_ARCHITECTURE.md").read_text(
        encoding="utf-8"
    )
    assert "Guaranteed single delivery" not in blob
    assert "Honesty banner" in blob or "honesty banner" in blob.lower()


def test_openapi_ready_documents_drain_semantics() -> None:
    """OBS-06/07: OpenAPI /ready describes drain and 503."""
    from mpreg.server_pkg.openapi_surface import build_monitoring_openapi

    ready = build_monitoring_openapi()["paths"]["/ready"]["get"]
    assert "503" in ready.get("responses", {})
    desc = (ready.get("description") or "") + ready.get("summary", "")
    assert "drain" in desc.lower() or "503" in desc


def test_rpc_actor_ids_ignores_body_when_policy_on() -> None:
    """COR-05: under discovery policy, body cluster_id cannot spoof actor."""
    from types import SimpleNamespace

    from mpreg.server_pkg.plane_rpc import rpc_actor_ids

    server = SimpleNamespace(
        settings=SimpleNamespace(
            discovery_policy_enabled=True,
            cluster_id="local-cluster",
        ),
        _rpc_session_cluster_id=None,
        _rpc_session_tenant_id=None,
        _rpc_actor_context={"cluster_id": "local-cluster", "tenant_id": None},
    )
    # Attacker body claims owner-cluster
    cluster, tenant = rpc_actor_ids(
        server,
        {"cluster_id": "owner-cluster", "tenant_id": "evil-tenant", "queue_name": "q"},
    )
    assert cluster == "local-cluster"
    assert tenant is None


def test_rpc_actor_ids_body_ok_when_policy_off() -> None:
    """COR-05 lab path: body identity allowed only when policy disabled."""
    from types import SimpleNamespace

    from mpreg.server_pkg.plane_rpc import rpc_actor_ids

    server = SimpleNamespace(
        settings=SimpleNamespace(
            discovery_policy_enabled=False,
            cluster_id="local-cluster",
        ),
        _rpc_session_cluster_id=None,
        _rpc_session_tenant_id=None,
        _rpc_actor_context=None,
    )
    cluster, tenant = rpc_actor_ids(
        server,
        {"cluster_id": "lab-actor", "tenant_id": "t1"},
    )
    assert cluster == "lab-actor"
    assert tenant == "t1"
