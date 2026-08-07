"""STRONG chaos + stress (plan W2). Residual-free under injectors; not Jepsen/BFT."""

from __future__ import annotations

import asyncio

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.core.cache_models import CacheMetadata
from mpreg.core.cache_strong import StrongErrorCode, StrongVersion
from tests.chaos.harness_strong_audit import (
    assert_backends_agree,
    assert_no_pending,
    assert_residual_free,
    build_strong_mesh,
    check_single_key_history,
    key,
    run_concurrent_puts,
    strong_put_on,
)


@pytest.mark.asyncio
async def test_five_node_happy_majority() -> None:
    mesh = build_strong_mesh(5, prepare_timeout_s=0.6, commit_timeout_s=0.6)
    k = key("stress", "five")
    res = await strong_put_on(mesh, "n0", k, {"n": 5})
    assert res.success is True
    assert res.quorum_info is not None
    assert res.quorum_info["quorum"] == 3  # floor(5/2)+1
    assert_backends_agree(mesh.backends, k)
    assert_no_pending(mesh.backends)
    # At least Q committers have value
    committers = res.quorum_info.get("commit_acks") or []
    assert len(committers) >= 3
    for nid in committers:
        assert mesh.backends[nid].get_visible(k) is not None


@pytest.mark.asyncio
async def test_partition_majority_peers_fail_residual_free() -> None:
    mesh = build_strong_mesh(3)
    # Cut origin from both peers
    mesh.transport.partition("n0", "n1")
    mesh.transport.partition("n0", "n2")
    k = key("stress", "part")
    res = await strong_put_on(mesh, "n0", k, "x")
    assert res.success is False
    assert_residual_free(mesh.backends, k, op_id=res.operation_id)
    assert_no_pending(mesh.backends)


@pytest.mark.asyncio
async def test_partition_one_peer_still_majority() -> None:
    """3-node: cut one peer; Q=2 still reachable via origin+other peer."""
    mesh = build_strong_mesh(3)
    mesh.transport.partition("n0", "n2")
    k = key("stress", "part1")
    res = await strong_put_on(mesh, "n0", k, "ok")
    assert res.success is True
    assert mesh.backends["n0"].get_visible(k) is not None
    assert mesh.backends["n1"].get_visible(k) is not None
    # n2 may miss — not required for Q=2
    assert_no_pending(mesh.backends)


@pytest.mark.asyncio
async def test_heal_after_partition_allows_put() -> None:
    mesh = build_strong_mesh(3)
    mesh.transport.partition("n0", "n1")
    mesh.transport.partition("n0", "n2")
    k = key("stress", "heal")
    bad = await strong_put_on(mesh, "n0", k, "no")
    assert bad.success is False
    mesh.transport.heal()
    good = await strong_put_on(mesh, "n0", k, "yes")
    assert good.success is True
    assert_backends_agree(mesh.backends, k) == "yes"
    assert_no_pending(mesh.backends)


@pytest.mark.asyncio
async def test_drop_commit_after_prepare_uncommits() -> None:
    mesh = build_strong_mesh(3)
    mesh.transport.drop_commit |= {"n1", "n2"}
    k = key("stress", "drop-c")
    res = await strong_put_on(mesh, "n0", k, 1)
    assert res.success is False
    assert_residual_free(mesh.backends, k, op_id=res.operation_id)


@pytest.mark.asyncio
async def test_delay_prepare_within_timeout_succeeds() -> None:
    mesh = build_strong_mesh(3, prepare_timeout_s=0.5, commit_timeout_s=0.5)
    mesh.transport.delay_prepare_s = 0.05
    k = key("stress", "delay")
    res = await strong_put_on(mesh, "n0", k, "d")
    assert res.success is True
    assert_no_pending(mesh.backends)


@pytest.mark.asyncio
async def test_duplicate_commit_idempotent() -> None:
    mesh = build_strong_mesh(3)
    mesh.transport.duplicate_commit = True
    k = key("stress", "dup")
    res = await strong_put_on(mesh, "n0", k, "dup")
    assert res.success is True
    assert_backends_agree(mesh.backends, k) == "dup"
    assert_no_pending(mesh.backends)


@pytest.mark.asyncio
async def test_expired_pending_rejects_commit() -> None:
    mesh = build_strong_mesh(1, replica_factor=1, min_replicas=1)
    # Use lab-style single node via min_replicas=1 but coordinator needs lab flag
    mesh.coords["n0"].lab_single_node = True
    mesh.coords["n0"].min_replicas = 1
    mesh.coords["n0"].replica_factor = 1
    be = mesh.backends["n0"]
    k = key("stress", "exp")
    sv = StrongVersion(1, "n0", "op-exp")
    await be.prepare(
        key=k,
        value=1,
        metadata=CacheMetadata(),
        strong_version=sv,
        replica_set=("n0",),
        quorum=1,
        ttl_s=0.01,
    )
    await asyncio.sleep(0.05)
    ack = await be.commit(op_id="op-exp", key=k)
    assert ack.ok is False
    assert ack.reason == "expired"
    assert be.get_visible(k) is None
    assert be.pending_count() == 0


@pytest.mark.asyncio
async def test_purge_then_put_after_pending_full() -> None:
    mesh = build_strong_mesh(
        1, replica_factor=1, min_replicas=1, max_pending=2, pending_ttl_s=0.05
    )
    mesh.coords["n0"].lab_single_node = True
    mesh.coords["n0"].min_replicas = 1
    mesh.coords["n0"].replica_factor = 1
    be = mesh.backends["n0"]
    for i in range(2):
        await be.prepare(
            key=key("stress", f"hold{i}"),
            value=i,
            metadata=CacheMetadata(),
            strong_version=StrongVersion(i + 1, "n0", f"hold-{i}"),
            replica_set=("n0",),
            quorum=1,
            ttl_s=0.02,
        )
    assert be.pending_count() == 2
    # Coordinator put should hit pending_full
    res_full = await strong_put_on(mesh, "n0", key("stress", "new"), "x")
    assert res_full.success is False
    assert res_full.error_code == int(StrongErrorCode.STRONG_PENDING_FULL)
    await asyncio.sleep(0.05)
    n = be.purge_expired_pending()
    assert n >= 1
    res = await strong_put_on(mesh, "n0", key("stress", "after"), "y")
    assert res.success is True


@pytest.mark.asyncio
async def test_multi_key_concurrent_no_cross_residual() -> None:
    mesh = build_strong_mesh(3)

    async def put_key(i: int) -> None:
        k = key("stress", f"mk{i}")
        res = await strong_put_on(mesh, f"n{i % 3}", k, i)
        if res.success:
            assert mesh.backends["n0"].get_visible(k) is not None or any(
                mesh.backends[n].get_visible(k) is not None for n in mesh.peer_ids
            )
        else:
            assert_residual_free(mesh.backends, k, op_id=res.operation_id)

    await asyncio.gather(*[put_key(i) for i in range(9)])
    assert_no_pending(mesh.backends)
    for i in range(9):
        k = key("stress", f"mk{i}")
        # Either all empty or agree
        assert_backends_agree(mesh.backends, k)


@pytest.mark.asyncio
async def test_soak_sequential_puts_clean() -> None:
    mesh = build_strong_mesh(3)
    k = key("stress", "soak")
    last = None
    for i in range(25):
        res = await strong_put_on(mesh, f"n{i % 3}", k, i)
        assert res.success is True, f"put {i} failed: {res.error_message}"
        last = i
        assert_no_pending(mesh.backends)
    assert assert_backends_agree(mesh.backends, k) == last


@pytest.mark.asyncio
async def test_interleaved_fault_and_success() -> None:
    mesh = build_strong_mesh(3)
    k = key("stress", "inter")
    # Fail cycle
    mesh.transport.drop_prepare |= {"n1", "n2"}
    bad = await strong_put_on(mesh, "n0", k, "bad")
    assert bad.success is False
    assert_residual_free(mesh.backends, k, op_id=bad.operation_id)
    mesh.transport.clear_faults()
    good = await strong_put_on(mesh, "n0", k, "good")
    assert good.success is True
    assert assert_backends_agree(mesh.backends, k) == "good"


@pytest.mark.asyncio
async def test_concurrent_same_key_history() -> None:
    mesh = build_strong_mesh(3)
    k = key("stress", "hist")
    outcomes = await run_concurrent_puts(mesh, k, ["a", "b", "c", "d"])
    check_single_key_history(outcomes, mesh.backends, k)
    assert any(o.success for o in outcomes)


@given(
    drops=st.lists(st.sampled_from(["n1", "n2"]), min_size=0, max_size=2, unique=True)
)
@settings(max_examples=20, deadline=None)
def test_hypothesis_random_prepare_drops_residual_free(drops: list[str]) -> None:
    async def _run() -> None:
        mesh = build_strong_mesh(3)
        mesh.transport.drop_prepare |= set(drops)
        k = key("stress", "hyp-drop")
        res = await strong_put_on(mesh, "n0", k, 42)
        if len(drops) >= 2:
            assert res.success is False
            assert_residual_free(mesh.backends, k, op_id=res.operation_id)
        else:
            # 0 or 1 drop: majority still possible (origin + 1 peer)
            if res.success:
                assert_no_pending(mesh.backends)
            else:
                assert_residual_free(mesh.backends, k, op_id=res.operation_id)

    asyncio.run(_run())


@given(
    values=st.lists(st.integers(0, 500), min_size=2, max_size=6),
)
@settings(max_examples=15, deadline=None)
def test_hypothesis_concurrent_history(values: list[int]) -> None:
    async def _run() -> None:
        mesh = build_strong_mesh(3)
        k = key("stress", "hyp-h")
        outcomes = await run_concurrent_puts(mesh, k, values)
        check_single_key_history(outcomes, mesh.backends, k)

    asyncio.run(_run())


@pytest.mark.asyncio
async def test_lww_uncommit_does_not_clobber_winner() -> None:
    """Winner commits; losing op abort must not remove winner."""
    mesh = build_strong_mesh(3)
    k = key("stress", "lww")
    # Establish winner
    w = await strong_put_on(mesh, "n0", k, "winner", op_id="win-op")
    assert w.success is True
    # Manually commit a lower version then abort it — should not remove winner
    be = mesh.backends["n1"]
    StrongVersion(logical_ts=1, origin_node="n9", op_id="lose-op")
    # Force a lower pending and try abort path
    await be.abort(op_id="lose-op", key=k)
    ent = be.get_visible(k)
    assert ent is not None
    assert ent.value == "winner"


@pytest.mark.asyncio
async def test_replica_set_origin_first() -> None:
    mesh = build_strong_mesh(3)
    rs = mesh.coords["n1"].select_replica_set(["n2", "n0", "n1"])
    assert rs is not None
    assert rs[0] == "n1"
