"""T34 residual closeout: CACHING_SYSTEM honesty + purge prune path."""

from __future__ import annotations

from pathlib import Path

import pytest

from mpreg.core.cache_models import GlobalCacheKey
from mpreg.core.cache_strong import (
    InProcessStrongTransport,
    StrongLocalBackend,
    StrongPutCoordinator,
)

def test_t34_caching_system_doc_cft_honesty() -> None:
    path = Path(__file__).resolve().parents[2] / "docs" / "CACHING_SYSTEM.md"
    text = path.read_text(encoding="utf-8").lower()
    assert "cft" in text or "best-effort" in text
    assert "lost abort" in text or "abort" in text
    assert "pending" in text and ("residual" in text or "not residual" in text)
    assert "lww" in text or "not residual-free" in text

@pytest.mark.asyncio
async def test_t34_purge_expired_pending_prunes_orphan_backups() -> None:
    """Manually inject orphan backup; purge must drop it without clearing L1."""
    be = StrongLocalBackend(node_id="n1")
    key = GlobalCacheKey(namespace="t34", identifier="k", version="v1")
    # Simulate residual L1 for op live
    from mpreg.core.cache_models import CacheMetadata, GlobalCacheEntry
    from mpreg.core.cache_strong import StrongVersion, _attach_strong_version

    live_oid = "live-op"
    stale_oid = "stale-orphan"
    sv = StrongVersion(logical_ts=1, origin_node="n0", op_id=live_oid)
    entry = GlobalCacheEntry(
        key=key,
        value={"live": True},
        metadata=_attach_strong_version(CacheMetadata(), sv),
    )
    be._visible[be._key_str(key)] = entry
    be._key_op[be._key_str(key)] = live_oid
    # Live backup for current op + orphan from lost-abort path
    prior = GlobalCacheEntry(
        key=key, value={"prior": True}, metadata=CacheMetadata()
    )
    be._backups[live_oid] = prior
    be._backups[stale_oid] = prior
    assert be.backups_count() == 2
    before = be.backups_pruned_total
    n = be.purge_expired_pending()
    assert n == 0  # no pending
    assert be.backups_count() == 1
    assert live_oid in be._backups
    assert stale_oid not in be._backups
    assert be.backups_pruned_total == before + 1
    # Residual L1 untouched
    assert be.get_visible(key) is not None
    assert be.get_visible(key).value == {"live": True}

@pytest.mark.asyncio
async def test_t34_hypothesis_orphan_gc_smoke() -> None:
    """Quick deterministic smoke aligning with Hypothesis property."""
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
    )
    key = GlobalCacheKey(namespace="t34", identifier="h", version="v1")
    peers = list(backends)
    for i in range(4):
        res = await coord.strong_put(key, {"i": i}, eligible_peers=peers)
        assert res.success is False
        assert backends["n1"].backups_count() <= 1
    backends["n1"].purge_expired_pending()
    assert backends["n1"].backups_count() <= 1
