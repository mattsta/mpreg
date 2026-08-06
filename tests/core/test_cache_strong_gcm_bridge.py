"""STRONG peer commit bridges into GlobalCacheManager L1 (residual honesty)."""

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
    StrongLocalBackend,
    StrongPutCoordinator,
    StrongVersion,
    _entry_op_id,
)
from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager

@pytest.mark.asyncio
async def test_peer_commit_callback_promotes_gcm_l1() -> None:
    """Peer backend commit must make value readable via GCM.get."""
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="bridge",
        )
    )
    applied: list[str] = []

    def on_apply(entry) -> None:
        applied.append(_entry_op_id(entry) or "")
        gcm._put_to_l1(entry)

    be = StrongLocalBackend(node_id="peer", on_visible_apply=on_apply)
    key = GlobalCacheKey(namespace="b", identifier="k", version="v1")
    sv = StrongVersion(logical_ts=1, origin_node="origin", op_id="op-bridge-1")
    await be.prepare(
        key=key,
        value={"x": 1},
        metadata=CacheMetadata(created_by="origin"),
        strong_version=sv,
        replica_set=("origin", "peer"),
        quorum=2,
        ttl_s=30.0,
    )
    ack = await be.commit(op_id="op-bridge-1", key=key)
    assert ack.ok and ack.applied
    assert applied == ["op-bridge-1"]

    got = await gcm.get(key)
    assert got.success and got.entry is not None
    assert got.entry.value == {"x": 1}
    assert _entry_op_id(got.entry) == "op-bridge-1"
    await gcm.shutdown()

@pytest.mark.asyncio
async def test_abort_uncommit_evicts_gcm_l1() -> None:
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="bridge",
        )
    )

    def on_apply(entry) -> None:
        gcm._put_to_l1(entry)

    def on_uncommit(key, restored) -> None:
        if restored is not None:
            gcm._put_to_l1(restored)
        else:
            gcm.l1_cache.evict(key.to_local_key())

    be = StrongLocalBackend(
        node_id="peer",
        on_visible_apply=on_apply,
        on_visible_uncommit=on_uncommit,
    )
    key = GlobalCacheKey(namespace="b", identifier="k2", version="v1")
    sv = StrongVersion(logical_ts=2, origin_node="o", op_id="op-u1")
    await be.prepare(
        key=key,
        value=99,
        metadata=CacheMetadata(),
        strong_version=sv,
        replica_set=("o", "peer"),
        quorum=2,
        ttl_s=30.0,
    )
    await be.commit(op_id="op-u1", key=key)
    assert (await gcm.get(key)).success

    await be.abort(op_id="op-u1", key=key)
    miss = await gcm.get(key)
    assert miss.success is False
    await gcm.shutdown()

@pytest.mark.asyncio
async def test_gcm_get_promotes_from_strong_backend_on_l1_miss() -> None:
    """Defensive path: backend visible without callback still surfaces on get."""
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="bridge",
        )
    )
    be = StrongLocalBackend(node_id="peer")  # no callback
    tr = InProcessStrongTransport()
    tr.register(be)
    coord = StrongPutCoordinator(
        origin_id="peer",
        local=be,
        transport=tr,
        lab_single_node=True,
        min_replicas=1,
        replica_factor=1,
    )
    gcm.attach_strong_coordinator(coord)
    key = GlobalCacheKey(namespace="b", identifier="k3", version="v1")
    # Manually commit into backend only
    sv = StrongVersion(logical_ts=3, origin_node="peer", op_id="op-def")
    await be.prepare(
        key=key,
        value="only-backend",
        metadata=CacheMetadata(),
        strong_version=sv,
        replica_set=("peer",),
        quorum=1,
        ttl_s=30.0,
    )
    await be.commit(op_id="op-def", key=key)
    # L1 empty until get promotes
    assert gcm.l1_cache.get(key.to_local_key()) is None
    got = await gcm.get(key)
    assert got.success and got.entry is not None
    assert got.entry.value == "only-backend"
    # Second get hits real L1
    assert gcm.l1_cache.get(key.to_local_key()) is not None
    await gcm.shutdown()

@pytest.mark.asyncio
async def test_mesh_peer_gcm_readable_after_strong_put() -> None:
    """Full mesh: peer GCM with bridge sees committed STRONG value."""
    transport = InProcessStrongTransport()
    gcms: dict[str, GlobalCacheManager] = {}
    backends: dict[str, StrongLocalBackend] = {}

    for nid in ("n0", "n1", "n2"):
        gcm = GlobalCacheManager(
            GlobalCacheConfiguration(
                enable_l2_persistent=False,
                enable_l3_distributed=False,
                enable_l4_federation=False,
                local_cluster_id="mesh",
            )
        )
        gcms[nid] = gcm

        def make_apply(g):
            return lambda e: g._put_to_l1(e)

        def make_uncommit(g):
            def _u(k, restored):
                if restored is not None:
                    g._put_to_l1(restored)
                else:
                    g.l1_cache.evict(k.to_local_key())

            return _u

        be = StrongLocalBackend(
            node_id=nid,
            on_visible_apply=make_apply(gcm),
            on_visible_uncommit=make_uncommit(gcm),
        )
        backends[nid] = be
        transport.register(be)
        gcm.attach_strong_coordinator(
            StrongPutCoordinator(
                origin_id=nid,
                local=be,
                transport=transport,
                cluster_id="mesh",
                replica_factor=3,
                min_replicas=3,
            )
        )

    key = GlobalCacheKey(namespace="m", identifier="shared", version="v1")
    coord = gcms["n0"]._strong_coordinator
    res = await coord.strong_put(
        key,
        {"v": 42},
        metadata=CacheMetadata(created_by="n0"),
        eligible_peers=["n0", "n1", "n2"],
    )
    assert res.success
    if res.entry is not None:
        gcms["n0"]._put_to_l1(res.entry)

    for nid in ("n0", "n1", "n2"):
        got = await gcms[nid].get(key)
        assert got.success, f"{nid} miss"
        assert got.entry is not None and got.entry.value == {"v": 42}

    for g in gcms.values():
        await g.shutdown()

@pytest.mark.asyncio
async def test_mesh_ryw_all_gcms_after_strong_put() -> None:
    """T17: after majority put, every peer GCM local get sees committed value (bridge)."""
    transport = InProcessStrongTransport()
    gcms: dict[str, GlobalCacheManager] = {}
    backends: dict[str, StrongLocalBackend] = {}
    coords: dict[str, StrongPutCoordinator] = {}

    for nid in ("n0", "n1", "n2"):
        gcm = GlobalCacheManager(
            GlobalCacheConfiguration(
                enable_l2_persistent=False,
                enable_l3_distributed=False,
                enable_l4_federation=False,
                local_cluster_id="ryw",
            )
        )
        gcms[nid] = gcm

        def make_apply(g):
            return lambda e: g._put_to_l1(e)

        be = StrongLocalBackend(node_id=nid, on_visible_apply=make_apply(gcm))
        backends[nid] = be
        transport.register(be)

    for nid in ("n0", "n1", "n2"):
        coord = StrongPutCoordinator(
            origin_id=nid,
            local=backends[nid],
            transport=transport,
            cluster_id="ryw",
            replica_factor=3,
            min_replicas=3,
            prepare_timeout_s=0.5,
            commit_timeout_s=0.5,
        )
        coords[nid] = coord
        gcms[nid].attach_strong_coordinator(coord)

    key = GlobalCacheKey(namespace="ryw", identifier="k", version="v1")
    try:
        res = await coords["n0"].strong_put(
            key,
            {"ryw": 7},
            metadata=CacheMetadata(created_by="n0"),
            eligible_peers=["n0", "n1", "n2"],
        )
        assert res.success, res.error_message
        # Origin GCM put path also applies L1 on success when using gcm.put —
        # here coordinator-only; apply origin entry like production GCM does.
        if res.entry is not None:
            gcms["n0"]._put_to_l1(res.entry)
        for nid, gcm in gcms.items():
            got = await gcm.get(key)
            assert got.success and got.entry is not None, nid
            assert got.entry.value == {"ryw": 7}, nid
            assert gcm.strong_status()["enabled"] is True
    finally:
        for g in gcms.values():
            await g.shutdown()
