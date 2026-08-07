"""T36 residual closeout: abort_fail peer tracking + client honesty."""

from __future__ import annotations

from pathlib import Path

import pytest

from mpreg.core.cache_models import GlobalCacheKey
from mpreg.core.cache_strong import (
    InProcessStrongTransport,
    StrongLocalBackend,
    StrongPutCoordinator,
)
from mpreg.core.global_cache import GlobalCacheManager
from mpreg.server_pkg.monitoring_metrics import build_strong_metrics
from mpreg.server_pkg.openapi_surface import _strong_metrics_schema


@pytest.mark.asyncio
async def test_t36_abort_fail_peers_on_cft_residual() -> None:
    """Partial COMMIT + lost ABORT → residual peer in abort_fail_peers."""
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
    key = GlobalCacheKey(namespace="t36", identifier="k", version="v1")
    res = await coord.strong_put(key, {"v": 1}, eligible_peers=list(backends))
    assert res.success is False
    qi = res.quorum_info or {}
    assert "n1" in list(qi.get("abort_fail_peers") or [])
    assert qi.get("abort_best_effort_residual_candidates") is True
    assert "n1" in coord.last_abort_fail_peers
    assert coord.last_abort_fail_op_id == res.operation_id
    assert len(coord.recent_abort_fails) >= 1
    evt = coord.recent_abort_fails[-1]
    assert evt.get("op_id") == res.operation_id
    assert "n1" in list(evt.get("peers") or [])
    # Residual still on n1
    from mpreg.core.cache_strong import _entry_op_id

    ent = backends["n1"].get_visible(key)
    assert ent is not None and _entry_op_id(ent) == res.operation_id


@pytest.mark.asyncio
async def test_t36_abort_fail_peers_empty_when_abort_delivered() -> None:
    """Prepare-fail path with no drop_abort → empty abort_fail_peers."""
    tr = InProcessStrongTransport()
    backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(3)}
    for be in backends.values():
        tr.register(be)
    # Drop prepare on both peers → insufficient prepare, ABORT should succeed
    tr.drop_prepare |= {"n1", "n2"}
    coord = StrongPutCoordinator(
        origin_id="n0",
        local=backends["n0"],
        transport=tr,
        replica_factor=3,
        min_replicas=3,
        prepare_timeout_s=0.2,
        commit_timeout_s=0.2,
    )
    key = GlobalCacheKey(namespace="t36", identifier="ok", version="v1")
    res = await coord.strong_put(key, {"v": 1}, eligible_peers=list(backends))
    assert res.success is False
    qi = res.quorum_info or {}
    assert list(qi.get("abort_fail_peers") or []) == []
    assert qi.get("abort_best_effort_residual_candidates") is False
    assert coord.last_abort_fail_peers == []


@pytest.mark.asyncio
async def test_t36_gcm_status_surfaces_abort_fail_peers() -> None:
    """strong_status + build_strong_metrics expose last_abort_fail_peers."""
    tr = InProcessStrongTransport()
    backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(5)}
    for be in backends.values():
        tr.register(be)
    tr.drop_commit |= {"n2", "n3", "n4"}
    tr.drop_abort |= {"n1"}
    be0 = backends["n0"]
    coord = StrongPutCoordinator(
        origin_id="n0",
        local=be0,
        transport=tr,
        replica_factor=5,
        min_replicas=5,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.2,
        abort_attempts=2,
    )
    # Minimal GCM wiring: attach coordinator like production path
    gcm = GlobalCacheManager.__new__(GlobalCacheManager)
    gcm._strong_coordinator = coord
    gcm._strong_backend = be0
    gcm._strong_metrics = {
        "puts_ok": 0,
        "puts_fail": 0,
        "aborts_peer_ok": 0,
        "aborts_peer_fail": 0,
    }
    gcm._strong_latency_ms = []
    key = GlobalCacheKey(namespace="t36", identifier="st", version="v1")
    res = await coord.strong_put(key, {"v": 2}, eligible_peers=list(backends))
    assert res.success is False
    # Sync abort counters into GCM metrics the way put path does
    gcm._strong_metrics["aborts_peer_fail"] = coord.aborts_peer_fail
    gcm._strong_metrics["aborts_peer_ok"] = coord.aborts_peer_ok
    st = gcm.strong_status()
    assert "n1" in list(st.get("last_abort_fail_peers") or [])
    assert st.get("last_abort_fail_op_id") == res.operation_id

    class _Srv:
        settings = type("S", (), {"cache_strong_enabled": True})()
        _cache_manager = gcm
        _strong_local_backend = be0
        _strong_pending_purge_task = None

    payload = build_strong_metrics(_Srv())
    assert "n1" in list(payload.get("last_abort_fail_peers") or [])
    assert payload.get("last_abort_fail_op_id") == res.operation_id


@pytest.mark.asyncio
async def test_t36_distlab_cft_scenario_abort_fail_peers() -> None:
    from mpreg.testing.distlab.builtins import ensure_builtins
    from mpreg.testing.distlab.registry import get_registry

    ensure_builtins()
    r = await get_registry().run("strong.cft_partial_commit_lost_abort")
    assert r.ok, r


def test_t36_openapi_documents_abort_fail_peers() -> None:
    schema = _strong_metrics_schema()
    props = schema["properties"]["strong"]["properties"]
    assert "last_abort_fail_peers" in props
    assert "last_abort_fail_op_id" in props
    assert "recent_abort_fails" in props
    desc = props["last_abort_fail_peers"]["description"].lower()
    assert "cft" in desc or "residual" in desc
    assert "not" in desc  # not residual-free / not auto-heal


def test_t36_client_guide_cft_qualified() -> None:
    path = Path(__file__).resolve().parents[2] / "docs" / "MPREG_CLIENT_GUIDE.md"
    text = path.read_text(encoding="utf-8")
    # Must not keep unqualified "residual-free failures" alone
    assert "residual-free failures" not in text
    lower = text.lower()
    assert "abort_fail_peers" in lower or "cft best-effort" in lower
    assert "lost abort" in lower or "best-effort" in lower


def test_t36_catalogs_cft_honesty() -> None:
    root = Path(__file__).resolve().parents[2]
    app = (root / "docs" / "examples-curriculum" / "APP_CATALOG.md").read_text(
        encoding="utf-8"
    )
    assert "residual-free 1015" not in app
    assert "CFT" in app or "cft" in app.lower()
    reg = (root / "mpreg" / "examples" / "apps" / "_shared" / "registry.py").read_text(
        encoding="utf-8"
    )
    assert "residual-free 1015" not in reg
    assert "CFT residual" in reg or "CFT" in reg


def test_t36_design_alt_table_cft_qualified() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "residual-free with rollback" not in text
    assert "residual-free when ABORT delivered" in text or "CFT best-effort" in text


def test_t36_claims_mention_abort_fail_peers() -> None:
    path = Path(__file__).resolve().parents[2] / "tests" / "invariants" / "claims.yaml"
    text = path.read_text(encoding="utf-8")
    assert "abort_fail_peers" in text
    assert "not automatic residual heal" in text or "not auto-heal" in text
