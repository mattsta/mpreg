"""GlobalCacheManager STRONG enable/refuse paths (PR-S3a/S3b unit)."""

from __future__ import annotations

import pytest

from mpreg.core.cache_models import (
    CacheMetadata,
    CacheOptions,
    ConsistencyLevel,
    GlobalCacheKey,
)
from mpreg.core.cache_strong import (
    InProcessStrongTransport,
    StrongErrorCode,
    StrongLocalBackend,
    StrongPutCoordinator,
)
from mpreg.core.errors import MpregErrorCode
from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager

def _key() -> GlobalCacheKey:
    return GlobalCacheKey(namespace="t", identifier="k", version="v1")

@pytest.mark.asyncio
async def test_strong_refused_when_disabled() -> None:
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )
    )
    try:
        res = await gcm.put(
            _key(),
            1,
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert res.success is False
        assert res.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY)
        # No L1 residual
        got = await gcm.get(_key())
        assert not got.success or got.entry is None
    finally:
        await gcm.shutdown()

@pytest.mark.asyncio
async def test_strong_lab_single_node_success() -> None:
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="c1",
        )
    )
    be = StrongLocalBackend(node_id="origin")
    tr = InProcessStrongTransport()
    tr.register(be)
    coord = StrongPutCoordinator(
        origin_id="origin",
        local=be,
        transport=tr,
        lab_single_node=True,
        min_replicas=1,
        replica_factor=1,
    )
    gcm.attach_strong_coordinator(coord)
    try:
        res = await gcm.put(
            _key(),
            {"v": 9},
            metadata=CacheMetadata(),
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert res.success is True
        assert res.quorum_info is not None
        got = await gcm.get(_key())
        assert got.success and got.entry is not None
        assert got.entry.value == {"v": 9}
    finally:
        await gcm.shutdown()

@pytest.mark.asyncio
async def test_strong_insufficient_quorum_no_residual() -> None:
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )
    )
    be = StrongLocalBackend(node_id="origin")
    tr = InProcessStrongTransport()
    tr.register(be)
    coord = StrongPutCoordinator(
        origin_id="origin",
        local=be,
        transport=tr,
        lab_single_node=False,
        min_replicas=3,
        replica_factor=3,
    )
    gcm.attach_strong_coordinator(coord)
    try:
        res = await gcm.put(
            _key(),
            1,
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert res.success is False
        assert res.error_code == int(StrongErrorCode.INSUFFICIENT_QUORUM)
        got = await gcm.get(_key())
        assert not got.success or got.entry is None
    finally:
        await gcm.shutdown()

@pytest.mark.asyncio
async def test_strong_status_and_metrics_after_puts() -> None:
    """T17: GCM strong_status / metrics counters track ok/fail/refused."""
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="st",
        )
    )
    # refused while unbound
    r0 = await gcm.put(
        _key(),
        0,
        options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
    )
    assert r0.success is False
    st0 = gcm.strong_status()
    assert st0["enabled"] is False
    assert st0["refused_disabled"] >= 1

    be = StrongLocalBackend(node_id="origin")
    tr = InProcessStrongTransport()
    tr.register(be)
    coord = StrongPutCoordinator(
        origin_id="origin",
        local=be,
        transport=tr,
        lab_single_node=True,
        min_replicas=1,
        replica_factor=1,
    )
    gcm.attach_strong_coordinator(coord)
    try:
        r1 = await gcm.put(
            _key(),
            {"ok": True},
            metadata=CacheMetadata(),
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert r1.success
        snap = gcm.strong_metrics_snapshot()
        assert snap["enabled"] is True
        assert snap["counters"].get("puts_ok", 0) >= 1
        assert snap["latency_ms"].get("sample_count", 0) >= 1
        st = gcm.strong_status()
        assert st["puts_ok"] >= 1
        caps = st.get("capabilities") or {}
        assert caps.get("put_majority_commit") is True
        assert caps.get("get_quorum") is False
        assert caps.get("delete_quorum") is False
        assert caps.get("local_ryw_after_put") is True
        # local RYW via EVENTUAL/default get (not STRONG)
        got = await gcm.get(_key())
        assert got.success and got.entry is not None
        assert got.entry.value == {"ok": True}
    finally:
        await gcm.shutdown()

@pytest.mark.asyncio
async def test_strong_get_always_refuses_1012() -> None:
    """T18: ConsistencyLevel.STRONG get is design-refuse (quorum get is v1.1)."""
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="gref",
        )
    )
    be = StrongLocalBackend(node_id="origin")
    tr = InProcessStrongTransport()
    tr.register(be)
    coord = StrongPutCoordinator(
        origin_id="origin",
        local=be,
        transport=tr,
        lab_single_node=True,
        min_replicas=1,
        replica_factor=1,
    )
    gcm.attach_strong_coordinator(coord)
    try:
        put = await gcm.put(
            _key(),
            {"v": 1},
            metadata=CacheMetadata(),
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert put.success
        # STRONG get must refuse even when value is visible via EVENTUAL
        bad = await gcm.get(
            _key(),
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert bad.success is False
        assert bad.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY)
        assert "not implemented" in (bad.error_message or "").lower() or "v1.1" in (
            bad.error_message or ""
        )
        st = gcm.strong_status()
        assert st["gets_refused"] >= 1
        snap = gcm.strong_metrics_snapshot()
        assert int(snap["counters"].get("gets_refused", 0)) >= 1
        # EVENTUAL still RYW
        good = await gcm.get(_key())
        assert good.success and good.entry is not None
        assert good.entry.value == {"v": 1}
    finally:
        await gcm.shutdown()

@pytest.mark.asyncio
async def test_strong_delete_always_refuses_1012() -> None:
    """T18: ConsistencyLevel.STRONG delete is design-refuse (quorum delete is v1.1)."""
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="dref",
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
    try:
        put = await gcm.put(
            _key(),
            7,
            metadata=CacheMetadata(),
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert put.success
        bad = await gcm.delete(
            _key(),
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert bad.success is False
        assert bad.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY)
        st = gcm.strong_status()
        assert st["deletes_refused"] >= 1
        # Value still present (STRONG delete did not evict)
        got = await gcm.get(_key())
        assert got.success and got.entry is not None
        assert got.entry.value == 7
        # EVENTUAL delete still works
        ev = await gcm.delete(_key())
        assert ev.success
    finally:
        await gcm.shutdown()
