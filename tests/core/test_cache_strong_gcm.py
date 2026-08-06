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
