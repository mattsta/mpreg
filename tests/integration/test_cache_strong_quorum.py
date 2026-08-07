"""Multi-node STRONG quorum integration (INV-CACHE-STRONG-01 L2).

In-process multi-backend mesh exercises the same StrongPutCoordinator +
transport contract ServerCacheTransport implements on the wire.
"""

from __future__ import annotations

import asyncio

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
    _entry_op_id,
)
from mpreg.core.errors import MpregErrorCode
from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager


def _mesh(n: int = 3):
    transport = InProcessStrongTransport()
    backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(n)}
    for be in backends.values():
        transport.register(be)
    coord = StrongPutCoordinator(
        origin_id="n0",
        local=backends["n0"],
        transport=transport,
        cluster_id="strong-int",
        replica_factor=n,
        min_replicas=n,
        prepare_timeout_s=0.5,
        commit_timeout_s=0.5,
    )
    return coord, transport, backends


@pytest.mark.asyncio
async def test_three_node_quorum_put_visible_on_majority() -> None:
    coord, _t, backends = _mesh(3)
    key = GlobalCacheKey(namespace="int", identifier="k1", version="v1")
    res = await coord.strong_put(
        key,
        {"v": 7},
        metadata=CacheMetadata(created_by="n0"),
        eligible_peers=["n0", "n1", "n2"],
    )
    assert res.success is True
    assert res.quorum_info is not None
    assert res.quorum_info["quorum"] == 2
    assert len(res.quorum_info["commit_acks"]) >= 2
    assert "n0" in res.quorum_info["commit_acks"]
    for nid in res.quorum_info["commit_acks"]:
        ent = backends[nid].get_visible(key)
        assert ent is not None and ent.value == {"v": 7}
        assert _entry_op_id(ent) == res.operation_id


@pytest.mark.asyncio
async def test_five_node_quorum_math_and_partial_failure() -> None:
    coord, transport, backends = _mesh(5)
    # Q = 3 for N=5; drop 3 peers' prepare → fail residual-free
    transport.drop_prepare |= {"n2", "n3", "n4"}
    key = GlobalCacheKey(namespace="int", identifier="k5", version="v1")
    res = await coord.strong_put(key, 1, eligible_peers=[f"n{i}" for i in range(5)])
    assert res.success is False
    assert res.error_code in (
        int(StrongErrorCode.QUORUM_TIMEOUT),
        int(StrongErrorCode.INSUFFICIENT_QUORUM),
    )
    for be in backends.values():
        assert be.get_visible(key) is None
        assert be.pending_count() == 0


@pytest.mark.asyncio
async def test_gcm_enabled_path_lab_and_disabled() -> None:
    # lab single-node success through GCM
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="lab",
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
    key = GlobalCacheKey(namespace="int", identifier="lab", version="v1")
    try:
        ok = await gcm.put(
            key,
            99,
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert ok.success is True
        got = await gcm.get(key)
        assert got.success and got.entry is not None and got.entry.value == 99
    finally:
        await gcm.shutdown()

    # disabled
    gcm2 = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )
    )
    try:
        bad = await gcm2.put(
            key,
            1,
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert bad.success is False
        assert bad.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY)
    finally:
        await gcm2.shutdown()


@pytest.mark.asyncio
async def test_burst_puts_then_crash_style_abort_clean() -> None:
    """Burst of successful puts then injected commit drops leave no pending."""
    coord, transport, backends = _mesh(3)
    peers = ["n0", "n1", "n2"]
    for i in range(10):
        key = GlobalCacheKey(namespace="int", identifier=f"b{i}", version="v1")
        res = await coord.strong_put(key, i, eligible_peers=peers)
        assert res.success is True

    transport.drop_commit |= {"n1", "n2"}
    key = GlobalCacheKey(namespace="int", identifier="boom", version="v1")
    res = await coord.strong_put(key, -1, eligible_peers=peers)
    assert res.success is False
    for be in backends.values():
        assert be.pending_count() == 0
        ent = be.get_visible(key)
        assert ent is None or _entry_op_id(ent) != res.operation_id


@pytest.mark.asyncio
async def test_concurrent_origins_same_backends() -> None:
    """Two coordinators sharing backends (different origins) do not corrupt pending."""
    transport = InProcessStrongTransport()
    backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(3)}
    for be in backends.values():
        transport.register(be)

    c0 = StrongPutCoordinator(
        origin_id="n0",
        local=backends["n0"],
        transport=transport,
        replica_factor=3,
        min_replicas=3,
        prepare_timeout_s=0.5,
        commit_timeout_s=0.5,
    )
    c1 = StrongPutCoordinator(
        origin_id="n1",
        local=backends["n1"],
        transport=transport,
        replica_factor=3,
        min_replicas=3,
        prepare_timeout_s=0.5,
        commit_timeout_s=0.5,
    )
    peers = ["n0", "n1", "n2"]
    k0 = GlobalCacheKey(namespace="int", identifier="o0", version="v1")
    k1 = GlobalCacheKey(namespace="int", identifier="o1", version="v1")
    r0, r1 = await asyncio.gather(
        c0.strong_put(k0, "from-0", eligible_peers=peers),
        c1.strong_put(k1, "from-1", eligible_peers=peers),
    )
    assert r0.success and r1.success
    assert backends["n0"].get_visible(k0).value == "from-0"  # type: ignore[union-attr]
    assert backends["n1"].get_visible(k1).value == "from-1"  # type: ignore[union-attr]
    for be in backends.values():
        assert be.pending_count() == 0
