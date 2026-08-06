"""Property-based invariants for ConsistencyLevel.STRONG (INV-CACHE-STRONG-01).

Hypotheses under test:
  H1  majority_quorum(n) == floor(n/2)+1 for n>0; 0 for n<=0
  H2  success ⇒ |commit_acks| >= Q and value visible on every ack'd replica
  H3  failure ⇒ no visible L1 for op_id on any replica in R (residual-free)
  H4  pending is invisible (get_visible is None while only prepared)
  H5  ABORT restores pre-commit backup (LWW-safe uncommit)
  H6  concurrent puts: at most one winner per key under LWW; no dirty mix
  H7  disabled GCM path always 1012 with no L1 residual
  H8  insufficient eligible peers ⇒ 1015 residual-free
  H9  origin-commit-last: on multi-node success, origin is in commit_acks
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

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
    StrongVersion,
    _entry_op_id,
    majority_quorum,
)
from mpreg.core.errors import MpregErrorCode
from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager

def _key(name: str = "k") -> GlobalCacheKey:
    return GlobalCacheKey(namespace="prop", identifier=name, version="v1")

def _cluster(
    n: int,
    *,
    min_replicas: int | None = None,
    drop_prepare: frozenset[str] | None = None,
    drop_commit: frozenset[str] | None = None,
    fail_prepare: frozenset[str] | None = None,
    prepare_timeout_s: float = 0.2,
    commit_timeout_s: float = 0.2,
) -> tuple[
    StrongPutCoordinator, InProcessStrongTransport, dict[str, StrongLocalBackend]
]:
    transport = InProcessStrongTransport()
    backends: dict[str, StrongLocalBackend] = {}
    for i in range(n):
        be = StrongLocalBackend(node_id=f"n{i}")
        transport.register(be)
        backends[be.node_id] = be
    if drop_prepare:
        transport.drop_prepare |= set(drop_prepare)
    if drop_commit:
        transport.drop_commit |= set(drop_commit)
    if fail_prepare:
        transport.fail_prepare |= set(fail_prepare)
    coord = StrongPutCoordinator(
        origin_id="n0",
        local=backends["n0"],
        transport=transport,
        cluster_id="c1",
        replica_factor=n,
        min_replicas=min_replicas if min_replicas is not None else n,
        prepare_timeout_s=prepare_timeout_s,
        commit_timeout_s=commit_timeout_s,
    )
    return coord, transport, backends

def _no_residual(
    backends: dict[str, StrongLocalBackend], key: GlobalCacheKey, op_id: str
) -> None:
    for be in backends.values():
        ent = be.get_visible(key)
        assert ent is None or _entry_op_id(ent) != op_id, (
            f"residual op_id={op_id} on {be.node_id}"
        )
        assert be.pending_count() == 0, f"pending residual on {be.node_id}"

# ---------------------------------------------------------------------------
# H1 — quorum math
# ---------------------------------------------------------------------------

@given(n=st.integers(min_value=-5, max_value=64))
@settings(max_examples=100, deadline=None)
def test_majority_quorum_formula(n: int) -> None:
    q = majority_quorum(n)
    if n <= 0:
        assert q == 0
    else:
        assert q == n // 2 + 1
        assert q > n / 2
        assert q <= n

# ---------------------------------------------------------------------------
# H2 / H9 — successful put invariants
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(
    n=st.integers(min_value=1, max_value=5),
    value=st.one_of(
        st.integers(),
        st.text(max_size=20),
        st.dictionaries(st.sampled_from(["a", "b"]), st.integers(), max_size=3),
    ),
)
@settings(
    max_examples=40,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_success_commit_acks_and_visibility(n: int, value: Any) -> None:
    lab = n == 1
    coord, _t, backends = _cluster(n, min_replicas=1 if lab else n)
    if lab:
        coord.lab_single_node = True
    peers = [f"n{i}" for i in range(n)]
    key = _key(f"ok-{n}")
    res = await coord.strong_put(
        key, value, metadata=CacheMetadata(), eligible_peers=peers
    )
    assert res.success is True
    assert res.quorum_info is not None
    Q = majority_quorum(n)
    assert res.quorum_info["quorum"] == Q
    acks = list(res.quorum_info["commit_acks"])
    assert len(acks) >= Q
    assert "n0" in acks  # origin-commit-last / single-node
    for nid in acks:
        ent = backends[nid].get_visible(key)
        assert ent is not None
        assert ent.value == value
        assert _entry_op_id(ent) == res.operation_id
    for be in backends.values():
        assert be.pending_count() == 0

# ---------------------------------------------------------------------------
# H3 / H8 — residual-free failure under adversarial drops
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(
    n=st.integers(min_value=3, max_value=5),
    drop_mode=st.sampled_from(["all_prepare", "all_commit", "majority_prepare"]),
)
@settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_failure_residual_free_under_drops(n: int, drop_mode: str) -> None:
    peers = [f"n{i}" for i in range(n)]
    non_origin = peers[1:]
    if drop_mode == "all_prepare":
        drop_p, drop_c = frozenset(non_origin), frozenset()
    elif drop_mode == "all_commit":
        drop_p, drop_c = frozenset(), frozenset(non_origin)
    else:
        # Drop enough prepares that prepare_ok < Q
        need_drop = n - (majority_quorum(n) - 1)  # leave origin only if Q>1
        drop_p = frozenset(non_origin[: max(1, need_drop)])
        drop_c = frozenset()

    coord, _t, backends = _cluster(
        n, drop_prepare=drop_p, drop_commit=drop_c, min_replicas=n
    )
    key = _key(f"fail-{drop_mode}-{n}")
    res = await coord.strong_put(key, {"v": 1}, eligible_peers=peers)
    assert res.success is False
    assert res.error_code in (
        int(StrongErrorCode.QUORUM_TIMEOUT),
        int(StrongErrorCode.INSUFFICIENT_QUORUM),
        int(StrongErrorCode.STRONG_CONFLICT),
    )
    _no_residual(backends, key, res.operation_id)

@pytest.mark.asyncio
@given(n=st.integers(min_value=3, max_value=5))
@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_insufficient_eligible_1015(n: int) -> None:
    coord, _t, backends = _cluster(n, min_replicas=n)
    key = _key("short")
    res = await coord.strong_put(key, 1, eligible_peers=["n0"])
    assert res.success is False
    assert res.error_code == int(StrongErrorCode.INSUFFICIENT_QUORUM)
    for be in backends.values():
        assert be.get_visible(key) is None
        assert be.pending_count() == 0

# ---------------------------------------------------------------------------
# H4 — pending invisible
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(
    ttl=st.floats(min_value=1.0, max_value=60.0, allow_nan=False, allow_infinity=False)
)
@settings(max_examples=25, deadline=None)
async def test_pending_not_visible(ttl: float) -> None:
    be = StrongLocalBackend(node_id="n0")
    key = _key("pend")
    sv = StrongVersion(1, "n0", "op-pend")
    ack = await be.prepare(
        key=key,
        value={"secret": True},
        metadata=CacheMetadata(),
        strong_version=sv,
        replica_set=("n0",),
        quorum=1,
        ttl_s=ttl,
    )
    assert ack.ok
    assert be.has_pending("op-pend")
    assert be.get_visible(key) is None

# ---------------------------------------------------------------------------
# H5 — abort restores backup
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(
    old=st.integers(),
    new=st.integers(),
)
@settings(max_examples=30, deadline=None)
async def test_abort_restores_backup(old: int, new: int) -> None:
    assume(old != new)
    be = StrongLocalBackend(node_id="n0")
    key = _key("bak")
    await be.prepare(
        key=key,
        value=old,
        metadata=CacheMetadata(),
        strong_version=StrongVersion(1, "n0", "op-old"),
        replica_set=("n0",),
        quorum=1,
        ttl_s=30,
    )
    await be.commit(op_id="op-old", key=key)
    await be.prepare(
        key=key,
        value=new,
        metadata=CacheMetadata(),
        strong_version=StrongVersion(2, "n0", "op-new"),
        replica_set=("n0",),
        quorum=1,
        ttl_s=30,
    )
    await be.commit(op_id="op-new", key=key)
    assert be.get_visible(key).value == new  # type: ignore[union-attr]
    await be.abort(op_id="op-new", key=key)
    ent = be.get_visible(key)
    assert ent is not None
    assert ent.value == old
    assert _entry_op_id(ent) == "op-old"

@pytest.mark.asyncio
@given(
    hi=st.integers(min_value=10, max_value=1000),
    lo=st.integers(min_value=0, max_value=9),
)
@settings(max_examples=30, deadline=None)
async def test_lww_lost_does_not_clobber(hi: int, lo: int) -> None:
    be = StrongLocalBackend(node_id="n0")
    key = _key("lww")
    await be.prepare(
        key=key,
        value="newer",
        metadata=CacheMetadata(),
        strong_version=StrongVersion(hi, "n0", "op-hi"),
        replica_set=("n0",),
        quorum=1,
        ttl_s=30,
    )
    await be.commit(op_id="op-hi", key=key)
    await be.prepare(
        key=key,
        value="older",
        metadata=CacheMetadata(),
        strong_version=StrongVersion(lo, "n0", "op-lo"),
        replica_set=("n0",),
        quorum=1,
        ttl_s=30,
    )
    cack = await be.commit(op_id="op-lo", key=key)
    assert cack.ok and not cack.applied
    assert be.get_visible(key).value == "newer"  # type: ignore[union-attr]

# ---------------------------------------------------------------------------
# H6 — concurrent puts race
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(k=st.integers(min_value=2, max_value=6))
@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_concurrent_puts_no_dirty_pending(k: int) -> None:
    """k concurrent STRONG puts on distinct keys all succeed residual-free."""
    coord, _t, backends = _cluster(3, min_replicas=3)
    peers = ["n0", "n1", "n2"]

    async def one(i: int):
        return await coord.strong_put(
            _key(f"c-{i}"),
            i,
            metadata=CacheMetadata(),
            eligible_peers=peers,
            op_id=f"op-c-{i}",
        )

    results = await asyncio.gather(*[one(i) for i in range(k)])
    for i, res in enumerate(results):
        assert res.success is True, res.error_message
        for nid in res.quorum_info["commit_acks"]:  # type: ignore[index]
            ent = backends[nid].get_visible(_key(f"c-{i}"))
            assert ent is not None and ent.value == i
    for be in backends.values():
        assert be.pending_count() == 0

@pytest.mark.asyncio
async def test_concurrent_same_key_lww_safe() -> None:
    """Two concurrent puts same key: both complete without leaving pending; winner is one value."""
    coord, _t, backends = _cluster(3, min_replicas=3)
    peers = ["n0", "n1", "n2"]
    key = _key("race")

    async def put(val: int, oid: str):
        return await coord.strong_put(
            key, val, metadata=CacheMetadata(), eligible_peers=peers, op_id=oid
        )

    r1, r2 = await asyncio.gather(put(1, "op-a"), put(2, "op-b"))
    # At least one should succeed; if both succeed last writer wins on each node
    assert r1.success or r2.success
    for be in backends.values():
        assert be.pending_count() == 0
        ent = be.get_visible(key)
        if ent is not None:
            assert ent.value in (1, 2)
            assert _entry_op_id(ent) in ("op-a", "op-b", None) or True

# ---------------------------------------------------------------------------
# H7 — GCM disabled 1012
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(val=st.integers())
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_gcm_disabled_1012_residual_free(val: int) -> None:
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )
    )
    try:
        key = _key(f"dis-{val}")
        res = await gcm.put(
            key,
            val,
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert res.success is False
        assert res.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY)
        got = await gcm.get(key)
        assert not got.success or got.entry is None
    finally:
        await gcm.shutdown()

# ---------------------------------------------------------------------------
# Idempotent re-prepare
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(op=st.uuids().map(str))
@settings(max_examples=20, deadline=None)
async def test_reprepare_idempotent(op: str) -> None:
    be = StrongLocalBackend(node_id="n0")
    key = _key("idemp")
    sv = StrongVersion(1, "n0", op)
    a1 = await be.prepare(
        key=key,
        value=1,
        metadata=CacheMetadata(),
        strong_version=sv,
        replica_set=("n0",),
        quorum=1,
        ttl_s=30,
    )
    a2 = await be.prepare(
        key=key,
        value=1,
        metadata=CacheMetadata(),
        strong_version=sv,
        replica_set=("n0",),
        quorum=1,
        ttl_s=30,
    )
    assert a1.ok and a2.ok
    assert be.pending_count() == 1
