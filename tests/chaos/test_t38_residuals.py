"""T38 residual closeout: GCM strong_retry_abort + product docs."""

from __future__ import annotations

from pathlib import Path

import pytest

from mpreg.core.cache_models import GlobalCacheKey
from mpreg.core.cache_strong import (
    InProcessStrongTransport,
    StrongLocalBackend,
    StrongPutCoordinator,
    _entry_op_id,
)
from mpreg.core.global_cache import GlobalCacheManager

def _wire_gcm() -> tuple[
    GlobalCacheManager,
    StrongPutCoordinator,
    InProcessStrongTransport,
    dict[str, StrongLocalBackend],
]:
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
    gcm = GlobalCacheManager.__new__(GlobalCacheManager)
    gcm._strong_coordinator = coord
    gcm._strong_backend = backends["n0"]
    gcm._strong_metrics = __import__("collections").defaultdict(int)
    gcm._strong_latency_ms = []
    gcm._strong_latency_max = 256
    return gcm, coord, tr, backends

@pytest.mark.asyncio
async def test_t38_gcm_strong_retry_abort_clears() -> None:
    gcm, coord, tr, backends = _wire_gcm()
    key = GlobalCacheKey(namespace="t38", identifier="k", version="v1")
    res = await coord.strong_put(key, {"v": 1}, eligible_peers=list(backends))
    assert res.success is False
    oid = res.operation_id or ""
    assert backends["n1"].get_visible(key) is not None
    tr.drop_abort.clear()
    tr.drop_commit.clear()
    out = await gcm.strong_retry_abort(key, oid)
    assert out.get("cleared") is True, out
    ent = backends["n1"].get_visible(key)
    assert ent is None or _entry_op_id(ent) != oid
    st = gcm.strong_status()
    assert int(st.get("retry_abort_calls") or 0) >= 1
    assert int(st.get("retry_abort_cleared") or 0) >= 1
    assert coord.last_abort_fail_peers == []

@pytest.mark.asyncio
async def test_t38_gcm_retry_abort_unbound() -> None:
    gcm = GlobalCacheManager.__new__(GlobalCacheManager)
    gcm._strong_coordinator = None
    gcm._strong_backend = None
    gcm._strong_metrics = __import__("collections").defaultdict(int)
    gcm._strong_latency_ms = []
    key = GlobalCacheKey(namespace="t38", identifier="u", version="v1")
    out = await gcm.strong_retry_abort(key, "x")
    assert out.get("error")
    assert out.get("cleared") is False
    assert int(gcm._strong_metrics.get("retry_abort_unbound") or 0) >= 1

def test_t38_caching_system_documents_retry_abort() -> None:
    path = Path(__file__).resolve().parents[2] / "docs" / "CACHING_SYSTEM.md"
    text = path.read_text(encoding="utf-8").lower()
    assert "retry_abort" in text
    assert "abort_fail_peers" in text
    assert "not" in text and ("automatic" in text or "background heal" in text)

def test_t38_client_guide_retry_abort() -> None:
    path = Path(__file__).resolve().parents[2] / "docs" / "MPREG_CLIENT_GUIDE.md"
    text = path.read_text(encoding="utf-8")
    assert "strong_retry_abort" in text
    assert "retry_abort" in text.lower()

def test_t38_curriculum_readme_retry() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "examples"
        / "apps"
        / "02_moderate"
        / "cache_strong_quorum"
        / "README.md"
    )
    text = path.read_text(encoding="utf-8").lower()
    assert "retry_abort" in text
    assert "abort_fail_peers" in text

def test_t38_residual_honesty_phase() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    # Phase 25 already covers retry; T38 extends product surface — ensure doc
    # still honest after CACHING_SYSTEM updates (Phase 26 added below).
    text = path.read_text(encoding="utf-8")
    assert "retry_abort" in text.lower()
