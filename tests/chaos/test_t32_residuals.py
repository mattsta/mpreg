"""T32 residual closeout: visible/backups prom + prune counter + config-check."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from mpreg.cli.main import cli
from mpreg.core.cache_models import GlobalCacheKey
from mpreg.core.cache_strong import (
    InProcessStrongTransport,
    StrongLocalBackend,
    StrongPutCoordinator,
)
from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager
from mpreg.server_pkg.monitoring_metrics import build_strong_metrics
from mpreg.server_pkg.openapi_surface import build_monitoring_openapi
from mpreg.testing.distlab.builtins import ensure_builtins
from mpreg.testing.distlab.registry import get_registry

@pytest.mark.asyncio
async def test_t32_backups_pruned_total_increments_on_cft_residual() -> None:
    tr = InProcessStrongTransport()
    backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(5)}
    for be in backends.values():
        tr.register(be)
    tr.drop_commit |= {"n2", "n3", "n4"}
    tr.drop_abort |= {"n1"}
    coord = StrongPutCoordinator(
        origin_id="n0",
        local=backends["n0"],
        transport=tr,
        replica_factor=5,
        min_replicas=5,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.2,
        abort_attempts=2,
    )
    key = GlobalCacheKey(namespace="t32", identifier="k", version="v1")
    peers = list(backends)
    # Seed a prior value so later residual keeps a backup that becomes orphan
    tr.drop_commit.clear()
    tr.drop_abort.clear()
    ok0 = await coord.strong_put(key, {"seed": True}, eligible_peers=peers)
    assert ok0.success is True
    tr.drop_commit |= {"n2", "n3", "n4"}
    tr.drop_abort |= {"n1"}
    n1 = backends["n1"]
    before = n1.backups_pruned_total
    for i in range(4):
        res = await coord.strong_put(key, {"i": i}, eligible_peers=peers)
        assert res.success is False
    # Orphan backups from superseded residuals should have been pruned
    assert n1.backups_pruned_total > before
    assert n1.backups_count() <= 1

@pytest.mark.asyncio
async def test_t32_gcm_snapshot_includes_prune_and_counts() -> None:
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="t32",
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
    snap = gcm.strong_metrics_snapshot()
    assert "visible_count" in snap
    assert "backups_count" in snap
    assert "backups_pruned_total" in snap
    assert "backups_pruned" in snap["counters"]
    st = gcm.strong_status()
    assert "backups_pruned_total" in st
    await gcm.shutdown()

def test_t32_build_strong_metrics_includes_prune() -> None:
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    cm = MagicMock()
    cm.strong_metrics_snapshot.return_value = {
        "enabled": True,
        "pending_count": 0,
        "visible_count": 2,
        "backups_count": 1,
        "backups_pruned_total": 7,
        "counters": {"puts_ok": 1, "backups_pruned": 7},
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
            "pending_ttl_clears_residual_l1": False,
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
    assert m["visible_count"] == 2
    assert m["backups_count"] == 1
    assert m["backups_pruned_total"] == 7
    assert m["counters"]["backups_pruned"] == 7

def test_t32_openapi_backups_pruned_field() -> None:
    doc = build_monitoring_openapi()
    schemas = (doc.get("components") or {}).get("schemas") or {}
    strong = schemas["StrongMetricsResponse"]
    body = (strong.get("properties") or {}).get("strong") or {}
    props = body.get("properties") or {}
    assert "backups_pruned_total" in props
    assert "visible_count" in props
    assert "backups_count" in props

def test_t32_config_check_cft_caps_on_dev_profile() -> None:
    runner = CliRunner()
    r = runner.invoke(
        cli, ["config-check", "mpreg/profiles/dev.toml", "--format", "json"]
    )
    assert r.exit_code in (0, 2)
    data = json.loads(r.output)
    caps = data["groups"]["strong_cache"]["capabilities"]
    assert caps.get("cft_only") is True
    assert caps.get("abort_best_effort") is True
    assert caps.get("pending_ttl_clears_residual_l1") is False

@pytest.mark.asyncio
async def test_t32_distlab_orphan_gc_reports_pruned() -> None:
    ensure_builtins()
    r = await get_registry().run("strong.cft_orphan_backup_gc")
    assert r.ok, r
