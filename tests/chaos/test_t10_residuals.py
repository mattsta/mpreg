"""T10 residual closeout proofs: snapshot ACK, drain fabric, actor ContextVar, INV-P7, catalog dedup, LRU, errors, gossip drops."""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict

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
