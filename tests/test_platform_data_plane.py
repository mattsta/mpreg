"""Platform residual data-plane proofs: tenant isolation, queue crash redelivery,
cache anti-entropy, federated secure profile defaults.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from mpreg.core.cache_models import (
    CacheLevel,
    CacheMetadata,
    CacheOptions,
    ConsistencyLevel,
    GlobalCacheKey,
)
from mpreg.core.config import MPREGSettings
from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager
from mpreg.core.message_queue import (
    DeliveryGuarantee,
    InFlightMessage,
    MessageQueue,
    QueueConfiguration,
    QueuedMessage,
)
from mpreg.core.message_queue_manager import (
    MessageQueueManager,
    QueueManagerConfiguration,
)
from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.core.namespace_policy import (
    NamespacePolicyEngine,
    NamespacePolicyRule,
    actor_context,
)
from mpreg.core.persistence.queue_store import MemoryQueueStore
from mpreg.core.topic_exchange import TopicExchange
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport
from mpreg.fabric.route_security import RouteSecurityConfig

# ---------------------------------------------------------------------------
# P0: Queue AT_LEAST_ONCE crash × in_flight restore → redelivery
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_queue_in_flight_restored_as_pending_for_redelivery() -> None:
    """Kill-9 mid-delivery: unacked in-flight must re-enter pending on restore."""
    store = MemoryQueueStore(namespace="mpreg:queues", queue_name="jobs")
    config = QueueConfiguration(
        name="jobs",
        default_acknowledgment_timeout_seconds=30.0,
        max_retries=5,
    )
    await store.save_config(config)

    q1 = MessageQueue(config, queue_store=store, autostart=False)
    result = await q1.send_message(
        "jobs.created",
        {"job": "pay"},
        DeliveryGuarantee.AT_LEAST_ONCE,
    )
    assert result.success
    message = q1.pending_messages[0]
    # Simulate delivery without ack (crash before complete).
    in_flight = InFlightMessage(
        message=message,
        delivery_attempt=1,
        delivered_at=time.time(),
        delivered_to={"worker-1"},
        acknowledged_by=set(),
    )
    q1.pending_messages.clear()
    q1.in_flight_messages[str(message.id)] = in_flight
    await store.mark_in_flight(in_flight)
    # Store should no longer hold a pending row for this id.
    state_mid = await store.load_state()
    assert str(message.id) in state_mid.in_flight
    assert all(str(m.id) != str(message.id) for m in state_mid.pending)
    await q1.shutdown()

    # Restart from store — AT_LEAST_ONCE redelivery.
    q2 = MessageQueue(config, queue_store=store, autostart=False)
    await q2.restore_from_store()
    assert len(q2.in_flight_messages) == 0
    assert len(q2.pending_messages) == 1
    restored = q2.pending_messages[0]
    assert restored.payload == {"job": "pay"}
    assert restored._delivery_attempt >= 1

    delivered: list[QueuedMessage] = []

    def _cb(msg: QueuedMessage) -> None:
        delivered.append(msg)

    q2.subscribe("worker-1", "jobs.*", callback=_cb, auto_acknowledge=False)
    q2.start_workers()
    await asyncio.sleep(0.3)
    assert len(delivered) >= 1
    assert delivered[0].payload == {"job": "pay"}
    await q2.shutdown()

@pytest.mark.asyncio
async def test_queue_acked_message_not_redelivered_after_restore() -> None:
    store = MemoryQueueStore(namespace="mpreg:queues", queue_name="jobs")
    config = QueueConfiguration(name="jobs")
    await store.save_config(config)

    q1 = MessageQueue(config, queue_store=store, autostart=False)
    result = await q1.send_message(
        "jobs.done", {"ok": True}, DeliveryGuarantee.AT_LEAST_ONCE
    )
    assert result.success
    mid = str(result.message_id)
    # Deliver + ack fully
    msg = q1.pending_messages.popleft()
    in_flight = InFlightMessage(
        message=msg,
        delivery_attempt=1,
        delivered_at=time.time(),
        delivered_to={"w"},
        acknowledged_by={"w"},
    )
    await store.mark_in_flight(in_flight)
    await store.ack(mid)
    await q1.shutdown()

    q2 = MessageQueue(config, queue_store=store, autostart=False)
    await q2.restore_from_store()
    assert len(q2.pending_messages) == 0
    assert len(q2.in_flight_messages) == 0
    await q2.shutdown()

# ---------------------------------------------------------------------------
# P0: Namespace/tenant data-plane isolation (queue / cache / pubsub)
# ---------------------------------------------------------------------------

def _tenant_engine() -> NamespacePolicyEngine:
    return NamespacePolicyEngine(
        enabled=True,
        default_allow=False,
        rules=(
            NamespacePolicyRule(
                namespace="tenant-a",
                owners=("cluster-a",),
                visibility=("cluster-a",),
                visibility_tenants=("tenant-a",),
            ),
            NamespacePolicyRule(
                namespace="tenant-b",
                owners=("cluster-b",),
                visibility=("cluster-b",),
                visibility_tenants=("tenant-b",),
            ),
        ),
    )

@pytest.mark.asyncio
async def test_queue_refuses_cross_tenant_send() -> None:
    engine = _tenant_engine()
    mgr = MessageQueueManager(
        QueueManagerConfiguration(enable_auto_queue_creation=True),
        namespace_policy=engine,
    )
    try:
        with actor_context(tenant_id="tenant-a", cluster_id="cluster-a"):
            ok = await mgr.send_message(
                "tenant-a", "tenant-a.jobs", {"v": 1}, DeliveryGuarantee.AT_LEAST_ONCE
            )
            assert ok.success

        with actor_context(tenant_id="tenant-b", cluster_id="cluster-b"):
            denied = await mgr.send_message(
                "tenant-a", "tenant-a.jobs", {"v": 2}, DeliveryGuarantee.AT_LEAST_ONCE
            )
            assert denied.success is False
            assert denied.error_message is not None
            assert "namespace_policy_denied" in denied.error_message
            assert "tenant_denied" in denied.error_message
    finally:
        await mgr.shutdown()

@pytest.mark.asyncio
async def test_cache_refuses_cross_tenant_get_put() -> None:
    engine = _tenant_engine()
    mgr = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="cluster-a",
        ),
        namespace_policy=engine,
    )
    try:
        key = GlobalCacheKey(namespace="tenant-a", identifier="k1")
        with actor_context(tenant_id="tenant-a", cluster_id="cluster-a"):
            put = await mgr.put(key, {"x": 1}, CacheMetadata())
            assert put.success
            got = await mgr.get(key)
            assert got.success
            assert got.entry is not None
            assert got.entry.value == {"x": 1}

        with actor_context(tenant_id="tenant-b", cluster_id="cluster-b"):
            denied_get = await mgr.get(key)
            assert denied_get.success is False
            assert "namespace_policy_denied" in (denied_get.error_message or "")
            denied_put = await mgr.put(key, {"x": 2}, CacheMetadata())
            assert denied_put.success is False
            assert "namespace_policy_denied" in (denied_put.error_message or "")
    finally:
        mgr.shutdown_sync()

def test_pubsub_refuses_cross_tenant_publish_and_subscribe() -> None:
    engine = _tenant_engine()
    exchange = TopicExchange(server_url="ws://local", cluster_id="cluster-a")
    exchange.attach_namespace_policy(engine)

    with actor_context(tenant_id="tenant-a", cluster_id="cluster-a"):
        sub = PubSubSubscription(
            subscription_id="s1",
            patterns=(TopicPattern(pattern="tenant-a.events.#"),),
            subscriber="client-a",
            created_at=time.time(),
            get_backlog=False,
        )
        assert exchange.add_subscription(sub) is True
        msg = PubSubMessage(
            topic="tenant-a.events.login",
            payload={"u": 1},
            timestamp=time.time(),
            message_id="m1",
            publisher="client-a",
        )
        notes = exchange.publish_message(msg)
        assert len(notes) == 1

    with actor_context(tenant_id="tenant-b", cluster_id="cluster-b"):
        assert (
            exchange.add_subscription(
                PubSubSubscription(
                    subscription_id="s2",
                    patterns=(TopicPattern(pattern="tenant-a.events.#"),),
                    subscriber="client-b",
                    created_at=time.time(),
                    get_backlog=False,
                )
            )
            is False
        )
        notes = exchange.publish_message(
            PubSubMessage(
                topic="tenant-a.events.login",
                payload={"u": 2},
                timestamp=time.time(),
                message_id="m2",
                publisher="client-b",
            )
        )
        assert notes == []

def test_allows_data_access_requires_tenant_when_rule_scoped() -> None:
    engine = _tenant_engine()
    # No tenant identity → deny when rule has visibility_tenants.
    d = engine.allows_data_access(
        "tenant-a", actor_cluster="cluster-a", actor_tenant_id=None, write=False
    )
    assert d.allowed is False
    assert d.reason == "tenant_required"

# ---------------------------------------------------------------------------
# P1: Cache EVENTUAL anti-entropy multi-node convergence
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cache_anti_entropy_converges_two_nodes() -> None:
    from mpreg.fabric.cache_federation import CacheOperationType

    transport = InProcessCacheTransport()
    a = FabricCacheProtocol(
        "node-a",
        transport=transport,
        gossip_interval=3600.0,
        anti_entropy_interval=3600.0,
    )
    b = FabricCacheProtocol(
        "node-b",
        transport=transport,
        gossip_interval=3600.0,
        anti_entropy_interval=3600.0,
    )
    try:
        key = GlobalCacheKey(namespace="shared", identifier="k-converge")
        # Divergent writes (EVENTUAL): A has v1, B has nothing then we sync.
        await a.propagate_cache_operation(
            CacheOperationType.PUT,
            key,
            {"from": "a", "n": 1},
            CacheMetadata(),
            ConsistencyLevel.EVENTUAL,
        )
        assert str(key) in a.cache_entries
        assert str(key) not in b.cache_entries

        synced = await a.sync_cache_state("node-b")
        assert synced is True
        # B should now hold A's entry via anti-entropy push/pull.
        assert str(key) in b.cache_entries
        assert b.cache_entries[str(key)].value == {"from": "a", "n": 1}

        # B writes a newer value; A pulls via reverse sync.
        await asyncio.sleep(0.01)
        await b.propagate_cache_operation(
            CacheOperationType.PUT,
            key,
            {"from": "b", "n": 2},
            CacheMetadata(),
            ConsistencyLevel.EVENTUAL,
        )
        synced_ba = await b.sync_cache_state("node-a")
        assert synced_ba is True
        assert a.cache_entries[str(key)].value == {"from": "b", "n": 2}
    finally:
        await a.shutdown()
        await b.shutdown()

@pytest.mark.asyncio
async def test_cache_l4_eventual_put_visible_via_protocol_peers() -> None:
    transport = InProcessCacheTransport()
    proto_a = FabricCacheProtocol(
        "n-a", transport=transport, gossip_interval=3600.0, anti_entropy_interval=3600.0
    )
    proto_b = FabricCacheProtocol(
        "n-b", transport=transport, gossip_interval=3600.0, anti_entropy_interval=3600.0
    )
    mgr_a = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=True,
            local_cluster_id="c-a",
        ),
        cache_protocol=proto_a,
    )
    try:
        key = GlobalCacheKey(namespace="fab", identifier="shared")
        put = await mgr_a.put(
            key,
            {"v": 99},
            CacheMetadata(),
            CacheOptions(
                cache_levels=frozenset({CacheLevel.L4}),
                consistency_level=ConsistencyLevel.EVENTUAL,
            ),
        )
        assert put.success
        # Anti-entropy from A → B
        await proto_a.sync_cache_state("n-b")
        entry = proto_b.get_cache_entry(key)
        assert entry is not None
        assert entry.value == {"v": 99}
    finally:
        mgr_a.shutdown_sync()
        await proto_a.shutdown()
        await proto_b.shutdown()

# ---------------------------------------------------------------------------
# P1: Federated profile secure-by-default
# ---------------------------------------------------------------------------

def test_federated_profile_requires_route_signatures_and_summary_hmac() -> None:
    from pathlib import Path

    profile = (
        Path(__file__).resolve().parents[1] / "mpreg" / "profiles" / "federated.toml"
    )
    settings = MPREGSettings.from_toml(profile)
    assert settings.fabric_route_security_config is not None
    assert settings.fabric_route_security_config.require_signatures is True
    assert settings.fabric_route_security_config.allow_unsigned is False
    assert settings.discovery_summary_export_enabled is True
    assert settings.discovery_summary_signing_secret
    assert settings.discovery_summary_signing_secret.startswith("change-me")

def test_route_security_config_parses_from_flat_toml_knobs(tmp_path) -> None:
    path = tmp_path / "sec.toml"
    path.write_text(
        """
[mpreg]
name = "n"
cluster_id = "c"
fabric_route_require_signatures = true
fabric_route_allow_unsigned = false
"""
    )
    settings = MPREGSettings.from_toml(path)
    assert isinstance(settings.fabric_route_security_config, RouteSecurityConfig)
    assert settings.fabric_route_security_config.require_signatures is True
    assert settings.fabric_route_security_config.allow_unsigned is False

def test_config_check_warns_on_unsigned_federated(tmp_path) -> None:
    """Unsigned multi-peer fabric must surface as config-check warning."""
    from click.testing import CliRunner

    from mpreg.cli.main import cli

    path = tmp_path / "weak.toml"
    path.write_text(
        """
[mpreg]
name = "n"
cluster_id = "c"
peers = ["ws://127.0.0.1:9001"]
enable_cache_federation = true
discovery_summary_export_enabled = true
"""
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["config-check", str(path), "--format", "json"])
    assert result.exit_code == 2
    assert "fabric_route_require_signatures" in result.output

def test_config_check_warns_discovery_policy_off_on_federated_profile() -> None:
    """COR-09: federated profile keeps policy lab-off but config-check must warn."""
    from pathlib import Path

    from click.testing import CliRunner

    from mpreg.cli.main import cli

    profile = (
        Path(__file__).resolve().parents[1] / "mpreg" / "profiles" / "federated.toml"
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["config-check", str(profile), "--format", "json"])
    assert result.exit_code == 2
    assert "discovery_policy_enabled" in result.output

# ---------------------------------------------------------------------------
# P1: Pub/sub delivery under simple drop oracle (local exchange)
# ---------------------------------------------------------------------------

def test_pubsub_delivery_only_to_matching_subscribers() -> None:
    exchange = TopicExchange(server_url="ws://local", cluster_id="c")
    exchange.add_subscription(
        PubSubSubscription(
            subscription_id="s-match",
            patterns=(TopicPattern(pattern="orders.*.created"),),
            subscriber="c1",
            created_at=time.time(),
            get_backlog=False,
        )
    )
    exchange.add_subscription(
        PubSubSubscription(
            subscription_id="s-other",
            patterns=(TopicPattern(pattern="inventory.#"),),
            subscriber="c2",
            created_at=time.time(),
            get_backlog=False,
        )
    )
    notes = exchange.publish_message(
        PubSubMessage(
            topic="orders.42.created",
            payload={"id": 42},
            timestamp=time.time(),
            message_id="m-ord",
            publisher="p",
        )
    )
    assert len(notes) == 1
    assert notes[0].subscription_id == "s-match"

# ---------------------------------------------------------------------------
# Gossip membership under FaultInjector loss (control plane)
# ---------------------------------------------------------------------------

def test_fault_injector_control_drop_blocks_gossip_path() -> None:
    from mpreg.testing.faults import FaultInjector

    inj = FaultInjector(seed=7, control_drop_rate=1.0, data_drop_rate=0.0)
    # Control plane always dropped
    assert inj.can_deliver("a", "b", plane="control") is False
    # Data plane still allowed at 0% drop
    assert inj.can_deliver("a", "b", plane="data") is True
    inj.partition({"a"}, {"b"})
    assert inj.can_deliver("a", "b", plane="data") is False

def test_client_notification_queue_is_bounded_drop_oldest() -> None:
    from mpreg.client.client import Client
    from mpreg.core.model import PubSubMessage, PubSubNotification

    client = Client(url="ws://127.0.0.1:1", notification_queue_maxsize=2)
    assert client.get_notification_queue().maxsize == 2

    def _note(i: int) -> PubSubNotification:
        return PubSubNotification(
            message=PubSubMessage(
                topic="t",
                payload={"i": i},
                timestamp=time.time(),
                message_id=f"m{i}",
                publisher="p",
            ),
            subscription_id="s",
            u=f"n{i}",
        )

    client._enqueue_notification(_note(1))
    client._enqueue_notification(_note(2))
    client._enqueue_notification(_note(3))  # drops oldest (1)
    assert client.notification_dropped_count >= 1
    q = client.get_notification_queue()
    assert q.qsize() == 2
    first = q.get_nowait()
    assert first.message.payload["i"] == 2

@pytest.mark.asyncio
async def test_client_api_exposes_request_dag() -> None:
    from mpreg.client.client_api import MPREGClientAPI

    api = MPREGClientAPI(url="ws://127.0.0.1:1")
    assert callable(api.request)
    assert callable(api.call_dag)
    with pytest.raises(ValueError):
        await api.request([])

def test_delivery_guarantee_plane_values_align() -> None:
    from mpreg.core.message_queue import DeliveryGuarantee as QDG
    from mpreg.fabric.message import DeliveryGuarantee as FDG

    # Shared wire values across planes (queue has no EXACTLY_ONCE member —
    # fabric rejects EXACTLY_ONCE as unsupported_delivery).
    for name in ("FIRE_AND_FORGET", "AT_LEAST_ONCE", "BROADCAST", "QUORUM"):
        assert getattr(QDG, name).value == getattr(FDG, name).value
    assert FDG.from_queue_value(QDG.AT_LEAST_ONCE.value) is FDG.AT_LEAST_ONCE
    assert FDG.EXACTLY_ONCE.value == "exactly_once"
