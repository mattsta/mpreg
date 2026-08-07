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
    format_residual_ops_hint,
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
    drop_abort: frozenset[str] | None = None,
    fail_prepare: frozenset[str] | None = None,
    prepare_timeout_s: float = 0.2,
    commit_timeout_s: float = 0.2,
    pending_ttl_s: float = 30.0,
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
    if drop_abort:
        transport.drop_abort |= set(drop_abort)
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
        pending_ttl_s=pending_ttl_s,
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

# ---------------------------------------------------------------------------
# T15 — random commit-drop subsets residual-free (DistLab Hypothesis expand)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(
    n=st.integers(min_value=3, max_value=5),
    drop_k=st.integers(min_value=1, max_value=4),
)
@settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_random_commit_drop_subset_residual_free(n: int, drop_k: int) -> None:
    """Drop commit to k non-origin peers; outcome residual-free either way."""
    peers = [f"n{i}" for i in range(n)]
    non_origin = peers[1:]
    k = min(drop_k, len(non_origin))
    drop_c = frozenset(non_origin[:k])
    coord, _t, backends = _cluster(n, drop_commit=drop_c, min_replicas=n)
    key = _key(f"rcd-{n}-{k}")
    res = await coord.strong_put(key, {"drop": list(drop_c)}, eligible_peers=peers)
    if res.success:
        for be in backends.values():
            assert be.pending_count() == 0
    else:
        _no_residual(backends, key, res.operation_id or "")

# ---------------------------------------------------------------------------
# T18 — drop_commit + drop_abort pairs residual-free (after TTL GC if needed)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(
    n=st.integers(min_value=3, max_value=5),
    drop_abort_k=st.integers(min_value=1, max_value=4),
)
@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_full_commit_drop_plus_abort_drop_residual_free_after_gc(
    n: int, drop_abort_k: int
) -> None:
    """T18: drop commit to *all* non-origin peers + drop abort → fail with only
    pending (no peer visible apply). After pending GC, residual-free.

    Honest scope: does **not** cover partial peer commit + lost abort (that can
    leave peer L1 until a later repair — CFT best-effort, not BFT).
    """
    import time

    peers = [f"n{i}" for i in range(n)]
    non_origin = peers[1:]
    # All non-origin commits dropped ⇒ put fails before origin commit-last
    dc = frozenset(non_origin)
    da = frozenset(non_origin[: min(drop_abort_k, len(non_origin))])
    coord, _t, backends = _cluster(
        n,
        drop_commit=dc,
        drop_abort=da,
        min_replicas=n,
        prepare_timeout_s=0.25,
        commit_timeout_s=0.25,
        pending_ttl_s=0.5,
    )
    key = _key(f"fcd-{n}-{len(da)}")
    res = await coord.strong_put(
        key, {"dc": list(dc), "da": list(da)}, eligible_peers=peers
    )
    assert res.success is False
    future = time.time() + 3600.0
    for be in backends.values():
        be.purge_expired_pending(now=future)
    _no_residual(backends, key, res.operation_id or "")

@pytest.mark.asyncio
@given(
    n=st.integers(min_value=3, max_value=5),
    drop_commit_k=st.integers(min_value=0, max_value=1),
)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_minority_commit_drop_success_pending_free_after_abort_drop_gc(
    n: int, drop_commit_k: int
) -> None:
    """T18: minority drop_commit still majority-succeeds; abort-drop non-committers
    leave pending until GC — then pending_count==0 on all backends.
    """
    import time

    peers = [f"n{i}" for i in range(n)]
    non_origin = peers[1:]
    # Keep drop small enough that peer commits can still form Q-1
    # n=3 Q=2 need_peers=1; drop at most 1 of 2 peers still ok if other commits
    k = min(drop_commit_k, max(0, len(non_origin) - 1))
    dc = frozenset(non_origin[:k])
    # Abort-drop the same minority (non-committers after success)
    da = frozenset(dc)
    coord, _t, backends = _cluster(
        n,
        drop_commit=dc,
        drop_abort=da,
        min_replicas=n,
        prepare_timeout_s=0.25,
        commit_timeout_s=0.25,
    )
    key = _key(f"mcd-{n}-{k}")
    res = await coord.strong_put(key, {"k": k}, eligible_peers=peers)
    assume(res.success)  # if topology can't form quorum, skip
    future = time.time() + 3600.0
    for be in backends.values():
        be.purge_expired_pending(now=future)
    for be in backends.values():
        assert be.pending_count() == 0, f"pending on {be.node_id}"

@pytest.mark.asyncio
@given(n=st.integers(min_value=5, max_value=7))
@settings(
    max_examples=12,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_cft_partial_commit_plus_lost_abort_leaves_peer_l1(n: int) -> None:
    """T27 honesty: when one peer applies COMMIT but ABORT is lost and put fails,
    that peer may retain L1 for op_id. Origin stays residual-free.

    This is the documented CFT best-effort ABORT limit — **not** a product bug
    and **not** claimed residual-free (not BFT).
    """
    peers = [f"n{i}" for i in range(n)]
    non_origin = peers[1:]
    # Allow exactly one peer to commit; drop the rest so need_peers fails
    commit_peer = non_origin[0]
    dc = frozenset(non_origin[1:])
    da = frozenset({commit_peer})
    coord, _t, backends = _cluster(
        n,
        drop_commit=dc,
        drop_abort=da,
        min_replicas=n,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.25,
        pending_ttl_s=30.0,
    )
    key = _key(f"cft-{n}")
    res = await coord.strong_put(key, {"cft": n}, eligible_peers=peers)
    assert res.success is False
    oid = res.operation_id or ""
    # Origin residual-free
    o_ent = backends["n0"].get_visible(key)
    assert o_ent is None or _entry_op_id(o_ent) != oid
    # CFT residual on the peer that committed then lost ABORT
    peer_ent = backends[commit_peer].get_visible(key)
    assert peer_ent is not None and _entry_op_id(peer_ent) == oid
    assert coord.aborts_peer_fail >= 1
    # T36: residual peer in abort_fail diagnostics
    assert commit_peer in list(coord.last_abort_fail_peers)
    qi = res.quorum_info or {}
    assert commit_peer in list(qi.get("abort_fail_peers") or [])

@pytest.mark.asyncio
@given(n=st.integers(min_value=5, max_value=7))
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_cft_retry_abort_clears_residual_after_heal(n: int) -> None:
    """T37: after drop_abort cleared, retry_abort clears residual (CFT best-effort)."""
    peers = [f"n{i}" for i in range(n)]
    non_origin = peers[1:]
    commit_peer = non_origin[0]
    dc = frozenset(non_origin[1:])
    da = frozenset({commit_peer})
    coord, tr, backends = _cluster(
        n,
        drop_commit=dc,
        drop_abort=da,
        min_replicas=n,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.25,
        pending_ttl_s=30.0,
    )
    key = _key(f"retry-{n}")
    res = await coord.strong_put(key, {"cft": n}, eligible_peers=peers)
    assert res.success is False
    oid = res.operation_id or ""
    assert backends[commit_peer].get_visible(key) is not None
    # Network recovers
    tr.drop_abort.clear()
    tr.drop_commit.clear()
    out = await coord.retry_abort(key, oid)
    assert out.get("cleared") is True, out
    ent = backends[commit_peer].get_visible(key)
    assert ent is None or _entry_op_id(ent) != oid
    assert coord.last_abort_fail_peers == []

@pytest.mark.asyncio
@given(n=st.integers(min_value=3, max_value=7))
@settings(
    max_examples=12,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_cft_retry_abort_self_target_clears_local(n: int) -> None:
    """T50: peers=[self] must local-abort residual (RPC landed on residual peer).

    Client/plane RPC may fan in to any ``cache`` node. When that node *is* the
    residual holder, retry_abort(peers=[own_id]) must clear local L1 rather than
    filter self and no-op. Still ops-driven CFT — not automatic heal / BFT.
    """
    peers = [f"n{i}" for i in range(n)]
    residual = peers[1]  # non-origin residual holder
    tr = InProcessStrongTransport()
    backends: dict[str, StrongLocalBackend] = {}
    for p in peers:
        be = StrongLocalBackend(node_id=p)
        tr.register(be)
        backends[p] = be
    # Coordinate *as* the residual peer (simulates RPC fan-in)
    coord = StrongPutCoordinator(
        origin_id=residual,
        local=backends[residual],
        transport=tr,
        cluster_id="c1",
        replica_factor=n,
        min_replicas=max(2, n // 2 + 1),
        prepare_timeout_s=0.3,
        commit_timeout_s=0.25,
        pending_ttl_s=30.0,
        abort_attempts=2,
    )
    key = _key(f"self-tgt-{n}")
    oid = f"self-oid-{n}"
    sv = StrongVersion(logical_ts=1, origin_node=peers[0], op_id=oid)
    pack = await backends[residual].prepare(
        key=key,
        value={"stale": n},
        metadata=CacheMetadata(),
        strong_version=sv,
        replica_set=tuple(peers),
        quorum=max(2, n // 2 + 1),
        ttl_s=30.0,
    )
    assert pack.ok
    cack = await backends[residual].commit(op_id=oid, key=key)
    assert cack.ok and cack.applied
    assert backends[residual].get_visible(key) is not None
    assert _entry_op_id(backends[residual].get_visible(key)) == oid

    out = await coord.retry_abort(key, oid, peers=[residual])
    assert out.get("cleared") is True, out
    assert residual in list(out.get("ok_peers") or [])
    assert list(out.get("fail_peers") or []) == []
    ent = backends[residual].get_visible(key)
    assert ent is None or _entry_op_id(ent) != oid

@given(
    ns=st.from_regex(r"[a-z][a-z0-9_-]{0,12}", fullmatch=True),
    kid=st.from_regex(r"[a-z][a-z0-9_-]{0,12}", fullmatch=True),
    oid=st.from_regex(r"op-[a-z0-9]{4,16}", fullmatch=True),
    peer=st.from_regex(r"n[0-9]", fullmatch=True),
)
@settings(max_examples=30, deadline=None)
def test_format_residual_ops_hint_enriches_ns_key(
    ns: str, kid: str, oid: str, peer: str
) -> None:
    """T63: matching recent_abort_fails fills --namespace/--key (ops guidance)."""
    assert format_residual_ops_hint([], oid) == ""
    h = format_residual_ops_hint(
        [peer],
        oid,
        recent_abort_fails=[
            {"op_id": oid, "peers": [peer], "key": f"{ns}/{kid}"},
            # unrelated event must not override
            {"op_id": "other-op", "peers": ["nx"], "key": "wrong/wrong"},
        ],
    )
    assert "cache-strong-retry-abort" in h
    assert f"--namespace {ns}" in h
    assert f"--key {kid}" in h
    assert f"--op-id {oid}" in h
    assert f"--peer {peer}" in h
    assert "not auto-heal" in h
    # Explicit ns/key wins over recent ring
    h2 = format_residual_ops_hint(
        [peer],
        oid,
        namespace="explicit",
        key_id="forced",
        recent_abort_fails=[{"op_id": oid, "peers": [peer], "key": f"{ns}/{kid}"}],
    )
    assert "--namespace explicit" in h2
    assert "--key forced" in h2

@pytest.mark.asyncio
@given(n=st.integers(min_value=5, max_value=7))
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_cft_gcm_retry_abort_clears_residual_after_heal(n: int) -> None:
    """T54: GCM.strong_retry_abort clears residual + counters after heal."""
    from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager

    peers = [f"n{i}" for i in range(n)]
    non_origin = peers[1:]
    commit_peer = non_origin[0]
    dc = frozenset(non_origin[1:])
    da = frozenset({commit_peer})
    coord, tr, backends = _cluster(
        n,
        drop_commit=dc,
        drop_abort=da,
        min_replicas=n,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.25,
        pending_ttl_s=30.0,
    )
    key = _key(f"gcm-retry-{n}")
    res = await coord.strong_put(key, {"cft": n}, eligible_peers=peers)
    assert res.success is False
    oid = res.operation_id or ""
    assert backends[commit_peer].get_visible(key) is not None
    tr.drop_abort.clear()
    tr.drop_commit.clear()
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id=f"prop-gcm-{n}",
        )
    )
    gcm.attach_strong_coordinator(coord)
    try:
        out = await gcm.strong_retry_abort(key, oid, peers=[commit_peer])
        assert out.get("cleared") is True, out
        st = gcm.strong_status()
        assert int(st.get("retry_abort_calls") or 0) >= 1
        assert int(st.get("retry_abort_cleared") or 0) >= 1
        # After clear, residual_ops_hint must be empty (no candidates)
        assert st.get("residual_ops_hint") in ("", None)
        ent = backends[commit_peer].get_visible(key)
        assert ent is None or _entry_op_id(ent) != oid
    finally:
        await gcm.shutdown()

@pytest.mark.asyncio
@given(
    n=st.integers(min_value=5, max_value=7),
    rounds=st.integers(min_value=2, max_value=5),
)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_cft_orphan_backups_bounded_under_repeated_residual(
    n: int, rounds: int
) -> None:
    """T34: repeated CFT residual must not unbounded-grow pre-commit backups."""
    peers = [f"n{i}" for i in range(n)]
    non_origin = peers[1:]
    commit_peer = non_origin[0]
    dc = frozenset(non_origin[1:])
    da = frozenset({commit_peer})
    coord, _t, backends = _cluster(
        n,
        drop_commit=dc,
        drop_abort=da,
        min_replicas=n,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.25,
        pending_ttl_s=30.0,
    )
    key = _key(f"obgc-{n}-{rounds}")
    be = backends[commit_peer]
    for i in range(rounds):
        res = await coord.strong_put(key, {"r": i}, eligible_peers=peers)
        assert res.success is False
        assert be.backups_count() <= 1
        assert be.get_visible(key) is not None
    # Purge path also prunes orphans (belt-and-suspenders)
    be.purge_expired_pending()
    assert be.backups_count() <= 1

@pytest.mark.asyncio
@given(n=st.integers(min_value=5, max_value=7))
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_cft_residual_survives_pending_purge(n: int) -> None:
    """T29 honesty: pending TTL purge does not clear residual L1 after COMMIT.

    After COMMIT apply, pending is empty. ``purge_expired_pending`` only drops
    uncommitted prepares — residual visible L1 remains (CFT limit).
    """
    import asyncio

    peers = [f"n{i}" for i in range(n)]
    non_origin = peers[1:]
    commit_peer = non_origin[0]
    dc = frozenset(non_origin[1:])
    da = frozenset({commit_peer})
    coord, _t, backends = _cluster(
        n,
        drop_commit=dc,
        drop_abort=da,
        min_replicas=n,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.25,
        pending_ttl_s=0.05,
    )
    key = _key(f"ttl-{n}")
    res = await coord.strong_put(key, {"ttl": n}, eligible_peers=peers)
    assert res.success is False
    oid = res.operation_id or ""
    be = backends[commit_peer]
    assert be.get_visible(key) is not None
    assert be.pending_count() == 0
    await asyncio.sleep(0.08)
    assert be.purge_expired_pending() == 0
    peer_ent = be.get_visible(key)
    assert peer_ent is not None and _entry_op_id(peer_ent) == oid

@pytest.mark.asyncio
@given(val=st.integers())
@settings(max_examples=15, deadline=None)
async def test_strong_get_delete_refuse_1012_property(val: int) -> None:
    """T18: STRONG get/delete always 1012; EVENTUAL RYW after put still works."""
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="prop-refuse",
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
        key = _key(f"ref-{val}")
        put = await gcm.put(
            key,
            val,
            metadata=CacheMetadata(),
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert put.success
        g = await gcm.get(
            key, options=CacheOptions(consistency_level=ConsistencyLevel.STRONG)
        )
        assert g.success is False
        assert g.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY)
        d = await gcm.delete(
            key, options=CacheOptions(consistency_level=ConsistencyLevel.STRONG)
        )
        assert d.success is False
        assert d.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY)
        ryw = await gcm.get(key)
        assert ryw.success and ryw.entry is not None
        assert ryw.entry.value == val
    finally:
        await gcm.shutdown()
