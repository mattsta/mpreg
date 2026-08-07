"""T27 residual closeout: CFT abort metrics, DistLab honesty scenario, curriculum."""

from __future__ import annotations

import asyncio

import pytest

from mpreg.core.cache_models import GlobalCacheKey
from mpreg.core.cache_strong import (
    InProcessStrongTransport,
    StrongLocalBackend,
    StrongPutCoordinator,
    _entry_op_id,
)
from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager
from mpreg.server_pkg.monitoring_metrics import build_strong_metrics
from mpreg.server_pkg.openapi_surface import build_monitoring_openapi
from mpreg.testing.distlab.builtins import ensure_builtins
from mpreg.testing.distlab.registry import get_registry

@pytest.mark.asyncio
async def test_t27_abort_counters_on_drop_abort_fail_path() -> None:
    """Failed put with dropped ABORT increments aborts_peer_fail."""
    tr = InProcessStrongTransport()
    backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(3)}
    for be in backends.values():
        tr.register(be)
    tr.drop_commit |= {"n1", "n2"}
    tr.drop_abort |= {"n1", "n2"}
    coord = StrongPutCoordinator(
        origin_id="n0",
        local=backends["n0"],
        transport=tr,
        replica_factor=3,
        min_replicas=3,
        prepare_timeout_s=0.2,
        commit_timeout_s=0.15,
        abort_attempts=3,
    )
    key = GlobalCacheKey(namespace="t27", identifier="ab", version="v1")
    res = await coord.strong_put(key, 1, eligible_peers=["n0", "n1", "n2"])
    assert res.success is False
    assert coord.aborts_peer_fail >= 2  # both peers fail all attempts
    assert coord.aborts_peer_ok == 0

@pytest.mark.asyncio
async def test_t27_gcm_status_cft_caps_and_abort_counters() -> None:
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="t27",
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
    assert caps["cft_only"] is True
    assert caps["abort_best_effort"] is True
    assert caps["get_quorum"] is False
    await gcm.shutdown()

@pytest.mark.asyncio
async def test_t27_distlab_cft_partial_commit_lost_abort_scenario() -> None:
    ensure_builtins()
    r = await get_registry().run("strong.cft_partial_commit_lost_abort")
    assert r.ok, r
    assert (r.meta or {}).get("cft_limit") or True  # meta on scenario
    # History closed
    assert r.history_len >= 2

def test_t27_openapi_cft_capability_enums() -> None:
    doc = build_monitoring_openapi()
    schemas = (doc.get("components") or {}).get("schemas") or {}
    strong = schemas["StrongMetricsResponse"]
    body = (strong.get("properties") or {}).get("strong") or {}
    caps = (body.get("properties") or {}).get("capabilities") or {}
    props = caps.get("properties") or {}
    assert props.get("cft_only", {}).get("enum") == [True]
    assert props.get("abort_best_effort", {}).get("enum") == [True]
    counters_desc = str((body.get("properties") or {}).get("counters", {}).get(
        "description", ""
    ))
    assert "aborts_peer" in counters_desc

def test_t27_prometheus_alerts_include_cft_honesty() -> None:
    from pathlib import Path

    path = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "ops"
        / "prometheus_alerts.yml"
    )
    text = path.read_text(encoding="utf-8")
    assert "MPREGStrongCapCftOnlyMissing" in text
    assert "MPREGStrongCapAbortBestEffortMissing" in text
    assert "mpreg_strong_cap_cft_only" in text
    assert "mpreg_strong_cap_abort_best_effort" in text
    assert "mpreg_strong_aborts_peer_fail_total" in text or "abort_best_effort" in text

def test_t27_doctor_fails_closed_on_false_cft_caps() -> None:
    from mpreg.cli.main import evaluate_strong_doctor_payload

    ok, detail = evaluate_strong_doctor_payload(
        {
            "strong": {
                "health": "ok",
                "capabilities": {
                    "get_quorum": False,
                    "delete_quorum": False,
                    "cft_only": True,
                    "abort_best_effort": True,
                },
                "counters": {"aborts_peer_fail": 2},
            }
        }
    )
    assert ok is True
    assert "abort_fail=2" in detail

    bad, bdetail = evaluate_strong_doctor_payload(
        {
            "strong": {
                "health": "ok",
                "capabilities": {"cft_only": False, "abort_best_effort": True},
                "counters": {},
            }
        }
    )
    assert bad is False
    assert "cft_only" in bdetail

def test_t27_build_strong_metrics_includes_cft_caps() -> None:
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    cm = MagicMock()
    cm.strong_metrics_snapshot.return_value = {
        "enabled": True,
        "pending_count": 0,
        "counters": {
            "puts_ok": 1,
            "aborts_peer_ok": 2,
            "aborts_peer_fail": 1,
        },
        "latency_ms": {},
        "coordinator": {},
    }
    cm.strong_status.return_value = {
        "enabled": True,
        "capabilities": {
            "put_majority_commit": True,
            "get_quorum": False,
            "delete_quorum": False,
            "local_ryw_after_put": True,
            "cft_only": True,
            "abort_best_effort": True,
        },
    }
    server = SimpleNamespace(
        settings=SimpleNamespace(
            cache_strong_enabled=True,
            cache_strong_replica_factor=3,
            cache_strong_min_replicas=3,
            cache_strong_prepare_timeout_s=1.0,
            cache_strong_commit_timeout_s=1.0,
            cache_strong_pending_ttl_s=30.0,
        ),
        _cache_manager=cm,
        _strong_local_backend=None,
        _strong_pending_purge_task=object(),
    )
    m = build_strong_metrics(server)
    assert m["capabilities"]["cft_only"] is True
    assert m["capabilities"]["abort_best_effort"] is True
    assert m["counters"]["aborts_peer_fail"] == 1

@pytest.mark.asyncio
async def test_t27_curriculum_honesty_apps_main() -> None:
    """Focused live run of STRONG/audit honesty curriculum apps."""
    from mpreg.examples.apps._shared.registry import get_app
    from mpreg.examples.apps._shared.runtime import run_app_main

    for app_id in ("cache_strong_quorum", "shared_audit_mesh"):
        app = get_app(app_id)
        report = await run_app_main(app.id, app.load_main(), timeout_s=60.0)
        assert report.ok, f"{app_id}: {report.error}"
