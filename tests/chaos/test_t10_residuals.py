"""T10 residual closeout proofs: snapshot ACK, drain fabric, actor ContextVar, INV-P7, catalog dedup, LRU, errors, gossip drops."""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from typing import ClassVar

import pytest

from mpreg.core.errors import MpregErrorCode, map_exception
from mpreg.core.monitoring.server_monitoring import ServerMetricsTracker
from mpreg.datastructures.federated_types import FederatedAnnouncementTracker
from mpreg.fabric.catalog_delta import RoutingCatalogApplier, RoutingCatalogDelta
from mpreg.server_pkg.drain_admission import (
    DATA_PLANE_ROLES,
    should_refuse_for_drain,
)


def test_drain_refuses_fabric_message() -> None:
    """COR-T10-03: fabric-message is a data-plane role under drain."""
    assert "fabric-message" in DATA_PLANE_ROLES
    assert should_refuse_for_drain(draining=True, role="fabric-message") is True
    assert should_refuse_for_drain(draining=True, role="server") is False
    assert should_refuse_for_drain(draining=True, role="fabric-gossip") is False
    assert should_refuse_for_drain(draining=False, role="fabric-message") is False


def test_error_codes_eo_strong() -> None:
    """ERG-T10-06: first-class MpregErrorCode for refused modalities."""
    assert int(MpregErrorCode.UNSUPPORTED_DELIVERY) == 1011
    assert int(MpregErrorCode.UNSUPPORTED_CONSISTENCY) == 1012
    from mpreg.core.blockchain_message_queue import UnsupportedDeliveryGuaranteeError

    mapped = map_exception(UnsupportedDeliveryGuaranteeError("exactly_once"))
    assert mapped.code == 1011
    mapped2 = map_exception(
        ValueError("ConsistencyLevel.STRONG is not implemented on the location path")
    )
    assert mapped2.code == 1012


def test_gossip_pending_drop_metric() -> None:
    """OBS-T10-01 / PERF-T10-05: tracker exports gossip pending drops."""
    t = ServerMetricsTracker()
    t.record_gossip_pending_drop(3)
    lines = "\n".join(t.prometheus_lines('cluster_id="c"'))
    assert "mpreg_gossip_pending_drops_total" in lines
    assert "mpreg_server_notification_drops_total" in lines
    assert "3" in lines


def test_catalog_update_id_dedup() -> None:
    """COR-T10-08: identical update_id applies once."""
    from mpreg.fabric.catalog import RoutingCatalog

    catalog = RoutingCatalog()
    applier = RoutingCatalogApplier(catalog=catalog)
    delta = RoutingCatalogDelta(
        update_id="uid-t10-1",
        cluster_id="c1",
        sent_at=time.time(),
    )
    c1 = applier.apply(delta, now=time.time())
    c2 = applier.apply(delta, now=time.time())
    assert c2.get("skipped_duplicate_update_id") == 1
    assert c1.get("skipped_duplicate_update_id") is None


def test_lru_access_order_is_ordered_dict() -> None:
    """PERF-T10-01: SmartCacheManager uses OrderedDict for O(1) LRU."""
    from mpreg.core.caching import CacheConfiguration, SmartCacheManager

    cfg = CacheConfiguration()
    mgr = SmartCacheManager(cfg)
    assert isinstance(mgr.access_order, OrderedDict)


@pytest.mark.asyncio
async def test_actor_contextvar_concurrent_isolation() -> None:
    """COR-T10-04: concurrent ContextVar actor binds do not cross-contaminate."""
    from mpreg.server import _current_rpc_actor_context
    from mpreg.server_pkg.plane_rpc import rpc_actor_ids

    class _S:
        settings = type("S", (), {"cluster_id": "local"})()
        _rpc_session_cluster_id = None
        _rpc_session_tenant_id = None
        _rpc_actor_context = None

    results: list[tuple[str | None, str | None]] = []

    async def _run(cluster: str, tenant: str, delay: float) -> None:
        token = _current_rpc_actor_context.set(
            {"cluster_id": cluster, "tenant_id": tenant}
        )
        try:
            await asyncio.sleep(delay)
            c, t = rpc_actor_ids(_S(), {})
            results.append((c, t))
        finally:
            _current_rpc_actor_context.reset(token)

    await asyncio.gather(
        _run("c-a", "t-a", 0.02),
        _run("c-b", "t-b", 0.01),
    )
    assert ("c-a", "t-a") in results
    assert ("c-b", "t-b") in results


def test_status_fingerprint_tracker_dedup() -> None:
    """COR-T10-02 / INV-P7 shape: tracker marks STATUS fingerprints once."""
    tracker = FederatedAnnouncementTracker(ttl_seconds=60.0)
    fp = "status:ws://n:1:inst:hash"
    assert tracker.has_seen(fp) is False
    tracker.mark_seen(fp, time.time())
    assert tracker.has_seen(fp) is True


def test_slo_traffic_metric_is_real_series() -> None:
    """OBS-T10-03: golden traffic names a real counter, not a fantasy regex."""
    from mpreg.core.observability.slo import GOLDEN_SIGNALS

    traffic = next(s for s in GOLDEN_SIGNALS if s.name == "traffic")
    assert "mpreg_rpc_requests_total" in traffic.prometheus_metric
    assert ".*" not in traffic.prometheus_metric


def test_openapi_covers_drain_and_extra_routes() -> None:
    """ERG-T10-05: OpenAPI includes drain + newly listed ops paths."""
    from mpreg.server_pkg.openapi_surface import build_monitoring_openapi

    doc = build_monitoring_openapi()
    paths = doc["paths"]
    assert "/mgmt/v1/nodes/drain" in paths
    assert "/performance/trends" in paths
    assert "/transport/endpoints" in paths


def test_rpc_to_fabric_traceparent_continuity() -> None:
    """OBS-T10-05: inject_trace_metadata continues bound RPC ingress parent."""
    from mpreg.core.observability.trace_context import (
        bind_current_trace,
        extract_traceparent,
        inject_trace_metadata,
    )

    ingress = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
    with bind_current_trace(ingress):
        meta = inject_trace_metadata({"mpreg.hop_entered_mono": "1.0"})
    assert extract_traceparent(meta) == ingress
    # Outside bind, a fresh parent is minted (not the ingress).
    meta2 = inject_trace_metadata({})
    assert extract_traceparent(meta2) != ingress
    assert extract_traceparent(meta2) is not None


def test_no_subscriber_requeue_bound_config() -> None:
    """COR-T10-10: QueueConfiguration exposes no-sub requeue bound."""
    from mpreg.core.message_queue import QueueConfiguration

    cfg = QueueConfiguration(name="t10-q", no_subscriber_max_requeues=5)
    assert cfg.no_subscriber_max_requeues == 5
    assert cfg.max_in_flight is None


@pytest.mark.asyncio
async def test_no_subscriber_eventually_dlq() -> None:
    """COR-T10-10: messages without subscribers hit DLQ after bound requeues."""
    from mpreg.core.message_queue import (
        DeliveryGuarantee,
        MessageQueue,
        QueueConfiguration,
        QueuedMessage,
    )

    cfg = QueueConfiguration(
        name="t10-nosub",
        no_subscriber_max_requeues=3,
        enable_dead_letter_queue=True,
    )
    q = MessageQueue(cfg, autostart=False)
    msg = QueuedMessage(
        id="msg-t10-nosub",  # type: ignore[arg-type]
        topic="topic.t10",
        payload={"x": 1},
        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
    )
    for _ in range(8):
        await q._deliver_message(msg)
        if len(q.dead_letter_queue) >= 1:
            break
    assert len(q.dead_letter_queue) >= 1


def test_status_fingerprint_dedup_on_server_method() -> None:
    """COR-T10-02: _handle_remote_status marks fingerprints via tracker."""
    from types import SimpleNamespace

    from mpreg.server import MPREGServer

    # Avoid slots/__del__ of real MPREGServer — bind the method onto a plain object.
    class _S:
        peer_status: ClassVar[dict] = {}
        _peer_instance_ids: ClassVar[dict] = {}
        _departed_peers: ClassVar[dict] = {}
        _status_announcement_tracker = None

        def _is_peer_departed(self, *a, **k):
            return False

    s = _S()
    s.peer_status = {}
    s._peer_instance_ids = {}
    s._departed_peers = {}
    status = SimpleNamespace(
        server_url="ws://peer:1",
        instance_id="i1",
        funs=("echo",),
        locs=("default",),
    )
    bound = MPREGServer._handle_remote_status.__get__(s, _S)
    assert bound(status) is True
    tracker = s._status_announcement_tracker
    assert tracker is not None
    assert tracker.announcement_count >= 1
    before = tracker.announcement_count
    assert bound(status) is True
    assert tracker.announcement_count == before
