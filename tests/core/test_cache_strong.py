"""Unit tests for StrongPutCoordinator commit-barrier (PR-S1)."""

from __future__ import annotations

import asyncio
import time

import pytest

from mpreg.core.cache_models import CacheMetadata, GlobalCacheKey
from mpreg.core.cache_strong import (
    InProcessStrongTransport,
    StrongErrorCode,
    StrongLocalBackend,
    StrongPutCoordinator,
    StrongVersion,
    _entry_op_id,
    majority_quorum,
)

def _key(name: str = "k1") -> GlobalCacheKey:
    return GlobalCacheKey(namespace="ns", identifier=name, version="v1")

def _cluster(
    n: int = 3, *, lab: bool = False, min_replicas: int | None = None
) -> tuple[StrongPutCoordinator, InProcessStrongTransport, dict[str, StrongLocalBackend]]:
    transport = InProcessStrongTransport()
    backends: dict[str, StrongLocalBackend] = {}
    for i in range(n):
        be = StrongLocalBackend(node_id=f"n{i}")
        transport.register(be)
        backends[be.node_id] = be
    origin = backends["n0"]
    coord = StrongPutCoordinator(
        origin_id="n0",
        local=origin,
        transport=transport,
        cluster_id="c1",
        replica_factor=n,
        min_replicas=min_replicas if min_replicas is not None else n,
        lab_single_node=lab,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.3,
    )
    return coord, transport, backends

@pytest.mark.asyncio
async def test_majority_quorum_math() -> None:
    assert majority_quorum(0) == 0
    assert majority_quorum(1) == 1
    assert majority_quorum(2) == 2
    assert majority_quorum(3) == 2
    assert majority_quorum(5) == 3

@pytest.mark.asyncio
async def test_happy_path_three_node() -> None:
    coord, _t, backends = _cluster(3)
    res = await coord.strong_put(
        _key(),
        {"v": 1},
        eligible_peers=["n0", "n1", "n2"],
        metadata=CacheMetadata(created_by="n0"),
    )
    assert res.success is True
    assert res.error_code is None
    assert res.entry is not None
    assert res.entry.value == {"v": 1}
    assert res.quorum_info is not None
    assert res.quorum_info["quorum"] == 2
    assert len(res.quorum_info["commit_acks"]) >= 2
    # Visible on origin + peers that committed
    for nid in res.quorum_info["commit_acks"]:
        ent = backends[nid].get_visible(_key())
        assert ent is not None
        assert ent.value == {"v": 1}
        assert _entry_op_id(ent) == res.operation_id
    # No residual pending
    for be in backends.values():
        assert be.pending_count() == 0

@pytest.mark.asyncio
async def test_insufficient_replicas_1015() -> None:
    coord, _t, backends = _cluster(3, min_replicas=3)
    res = await coord.strong_put(_key(), 1, eligible_peers=["n0"])  # only origin
    assert res.success is False
    assert res.error_code == int(StrongErrorCode.INSUFFICIENT_QUORUM)
    for be in backends.values():
        assert be.get_visible(_key()) is None
        assert be.pending_count() == 0

@pytest.mark.asyncio
async def test_prepare_drop_residual_free() -> None:
    coord, transport, backends = _cluster(3)
    transport.drop_prepare.add("n1")
    transport.drop_prepare.add("n2")
    res = await coord.strong_put(_key(), 42, eligible_peers=["n0", "n1", "n2"])
    assert res.success is False
    assert res.error_code in (
        int(StrongErrorCode.QUORUM_TIMEOUT),
        int(StrongErrorCode.INSUFFICIENT_QUORUM),
    )
    for be in backends.values():
        assert be.get_visible(_key()) is None, f"residual on {be.node_id}"
        assert be.pending_count() == 0

@pytest.mark.asyncio
async def test_partial_commit_abort_uncommits_peers() -> None:
    """Peer commits then origin fails path — abort clears peer visible for op_id."""
    coord, transport, backends = _cluster(3)
    # First put succeeds to establish baseline absence
    # Drop commit on enough peers that after one peer commits we still fail Q-1
    # With R=3, Q=2, need_peers=1. If we drop both peers' commits → fail and abort.
    transport.drop_commit.add("n1")
    transport.drop_commit.add("n2")
    res = await coord.strong_put(_key(), "x", eligible_peers=["n0", "n1", "n2"])
    assert res.success is False
    assert res.error_code == int(StrongErrorCode.QUORUM_TIMEOUT)
    for be in backends.values():
        ent = be.get_visible(_key())
        assert ent is None or _entry_op_id(ent) != res.operation_id
        assert be.pending_count() == 0

@pytest.mark.asyncio
async def test_pending_invisible_before_commit() -> None:
    be = StrongLocalBackend(node_id="n0")
    sv = StrongVersion(1, "n0", "op-1")
    ack = await be.prepare(
        key=_key(),
        value=1,
        metadata=CacheMetadata(),
        strong_version=sv,
        replica_set=("n0",),
        quorum=1,
        ttl_s=30.0,
    )
    assert ack.ok
    assert be.has_pending("op-1")
    assert be.get_visible(_key()) is None  # pending not visible
    cack = await be.commit(op_id="op-1", key=_key())
    assert cack.applied
    assert be.get_visible(_key()) is not None
    assert not be.has_pending("op-1")

@pytest.mark.asyncio
async def test_abort_restores_backup() -> None:
    be = StrongLocalBackend(node_id="n0")
    # Commit v1
    sv1 = StrongVersion(1, "n0", "op-a")
    await be.prepare(
        key=_key(),
        value="old",
        metadata=CacheMetadata(),
        strong_version=sv1,
        replica_set=("n0",),
        quorum=1,
        ttl_s=30,
    )
    await be.commit(op_id="op-a", key=_key())
    assert be.get_visible(_key()).value == "old"  # type: ignore[union-attr]
    # Prepare v2 then abort
    sv2 = StrongVersion(2, "n0", "op-b")
    await be.prepare(
        key=_key(),
        value="new",
        metadata=CacheMetadata(),
        strong_version=sv2,
        replica_set=("n0",),
        quorum=1,
        ttl_s=30,
    )
    await be.commit(op_id="op-b", key=_key())
    assert be.get_visible(_key()).value == "new"  # type: ignore[union-attr]
    await be.abort(op_id="op-b", key=_key())
    ent = be.get_visible(_key())
    assert ent is not None
    assert ent.value == "old"
    assert _entry_op_id(ent) == "op-a"

@pytest.mark.asyncio
async def test_lww_lost_does_not_clobber_newer() -> None:
    be = StrongLocalBackend(node_id="n0")
    newer = StrongVersion(10, "n0", "op-new")
    await be.prepare(
        key=_key(),
        value="newer",
        metadata=CacheMetadata(),
        strong_version=newer,
        replica_set=("n0",),
        quorum=1,
        ttl_s=30,
    )
    await be.commit(op_id="op-new", key=_key())
    older = StrongVersion(1, "n0", "op-old")
    await be.prepare(
        key=_key(),
        value="older",
        metadata=CacheMetadata(),
        strong_version=older,
        replica_set=("n0",),
        quorum=1,
        ttl_s=30,
    )
    cack = await be.commit(op_id="op-old", key=_key())
    assert cack.ok and not cack.applied
    assert cack.reason == "lww_lost"
    assert be.get_visible(_key()).value == "newer"  # type: ignore[union-attr]

@pytest.mark.asyncio
async def test_idempotent_reprepare() -> None:
    be = StrongLocalBackend(node_id="n0")
    sv = StrongVersion(1, "n0", "op-1")
    a1 = await be.prepare(
        key=_key(),
        value=1,
        metadata=CacheMetadata(),
        strong_version=sv,
        replica_set=("n0",),
        quorum=1,
        ttl_s=30,
    )
    a2 = await be.prepare(
        key=_key(),
        value=1,
        metadata=CacheMetadata(),
        strong_version=sv,
        replica_set=("n0",),
        quorum=1,
        ttl_s=30,
    )
    assert a1.ok and a2.ok
    assert be.pending_count() == 1

@pytest.mark.asyncio
async def test_pending_ttl_purge_fake_clock() -> None:
    be = StrongLocalBackend(node_id="n0")
    sv = StrongVersion(1, "n0", "op-1")
    await be.prepare(
        key=_key(),
        value=1,
        metadata=CacheMetadata(),
        strong_version=sv,
        replica_set=("n0",),
        quorum=1,
        ttl_s=0.01,
    )
    # Force expire via fake now
    n = be.purge_expired_pending(now=time.time() + 10)
    assert n == 1
    assert be.pending_count() == 0

@pytest.mark.asyncio
async def test_lab_single_node() -> None:
    coord, _t, backends = _cluster(1, lab=True, min_replicas=1)
    res = await coord.strong_put(_key(), 7, eligible_peers=["n0"])
    assert res.success is True
    assert backends["n0"].get_visible(_key()).value == 7  # type: ignore[union-attr]

@pytest.mark.asyncio
async def test_pending_full_1018() -> None:
    be = StrongLocalBackend(node_id="n0", max_pending=1)
    transport = InProcessStrongTransport()
    transport.register(be)
    coord = StrongPutCoordinator(
        origin_id="n0",
        local=be,
        transport=transport,
        lab_single_node=True,
        min_replicas=1,
        replica_factor=1,
    )
    # Fill pending without commit
    await be.prepare(
        key=_key("a"),
        value=1,
        metadata=CacheMetadata(),
        strong_version=StrongVersion(1, "n0", "hold"),
        replica_set=("n0",),
        quorum=1,
        ttl_s=60,
    )
    res = await coord.strong_put(_key("b"), 2, eligible_peers=["n0"])
    assert res.success is False
    assert res.error_code == int(StrongErrorCode.STRONG_PENDING_FULL)
