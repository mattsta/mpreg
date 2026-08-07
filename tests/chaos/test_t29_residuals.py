"""T29 residual closeout: pending TTL is not residual L1 GC."""

from __future__ import annotations

import pytest

from mpreg.cli.main import evaluate_strong_doctor_payload
from mpreg.server_pkg.openapi_surface import build_monitoring_openapi
from mpreg.testing.distlab.builtins import ensure_builtins
from mpreg.testing.distlab.registry import get_registry, resolve_preset


@pytest.mark.asyncio
async def test_t29_distlab_cft_residual_survives_pending_purge() -> None:
    ensure_builtins()
    r = await get_registry().run("strong.cft_residual_survives_pending_purge")
    assert r.ok, r
    assert (r.meta or {}).get("pending_ttl_not_residual_gc") is True


def test_t29_strong_core_includes_ttl_honesty_scenario() -> None:
    ensure_builtins()
    names = resolve_preset("strong-core")
    assert "strong.cft_residual_survives_pending_purge" in names
    assert "strong.cft_residual_survives_pending_purge" in resolve_preset("ci-core")


def test_t29_openapi_pending_ttl_clears_residual_false() -> None:
    doc = build_monitoring_openapi()
    schemas = (doc.get("components") or {}).get("schemas") or {}
    strong = schemas["StrongMetricsResponse"]
    body = (strong.get("properties") or {}).get("strong") or {}
    caps = (body.get("properties") or {}).get("capabilities") or {}
    props = caps.get("properties") or {}
    assert props.get("pending_ttl_clears_residual_l1", {}).get("enum") == [False]
    assert "visible_count" in (body.get("properties") or {})
    assert "backups_count" in (body.get("properties") or {})


def test_t29_doctor_fails_closed_on_pending_ttl_residual_gc_claim() -> None:
    ok, detail = evaluate_strong_doctor_payload(
        {
            "strong": {
                "health": "ok",
                "capabilities": {
                    "get_quorum": False,
                    "cft_only": True,
                    "abort_best_effort": True,
                    "pending_ttl_clears_residual_l1": True,
                },
                "counters": {},
            }
        }
    )
    assert ok is False
    assert "pending_ttl" in detail


def test_t29_gcm_status_pending_ttl_cap_and_counts() -> None:
    from mpreg.core.cache_strong import (
        InProcessStrongTransport,
        StrongLocalBackend,
        StrongPutCoordinator,
    )
    from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager

    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="t29",
        )
    )
    be = StrongLocalBackend(node_id="origin")
    tr = InProcessStrongTransport()
    tr.register(be)
    gcm.attach_strong_coordinator(
        StrongPutCoordinator(
            origin_id="origin",
            local=be,
            transport=tr,
            lab_single_node=True,
            min_replicas=1,
            replica_factor=1,
        )
    )
    st = gcm.strong_status()
    caps = st["capabilities"]
    assert caps["pending_ttl_clears_residual_l1"] is False
    assert caps["cft_only"] is True
    assert "visible_count" in st
    assert "backups_count" in st
    snap = gcm.strong_metrics_snapshot()
    assert "visible_count" in snap
    assert "backups_count" in snap
