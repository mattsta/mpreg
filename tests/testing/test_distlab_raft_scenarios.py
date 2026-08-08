"""DistLab first-class Raft scenarios — election, partition, heal, re-elect."""

from __future__ import annotations

import pytest

from mpreg.testing.distlab import (
    RaftSUT,
    Scenario,
    default_raft_checkers,
    ensure_builtins,
    get_registry,
)


@pytest.mark.asyncio
async def test_distlab_raft_elect_3() -> None:
    ensure_builtins()
    r = await get_registry().run("raft.elect_3")
    assert r.ok, r.check
    assert r.history_len >= 2


@pytest.mark.asyncio
async def test_distlab_raft_elect_5() -> None:
    ensure_builtins()
    r = await get_registry().run("raft.elect_5")
    assert r.ok, r.check


@pytest.mark.asyncio
async def test_distlab_raft_partition_majority() -> None:
    ensure_builtins()
    r = await get_registry().run("raft.partition_majority")
    assert r.ok, r.check


@pytest.mark.asyncio
async def test_distlab_raft_partition_heal() -> None:
    ensure_builtins()
    r = await get_registry().run("raft.partition_heal")
    assert r.ok, r.check


@pytest.mark.asyncio
async def test_distlab_raft_leader_stepdown_reelect() -> None:
    ensure_builtins()
    r = await get_registry().run("raft.leader_stepdown_reelect")
    assert r.ok, r.check


@pytest.mark.asyncio
async def test_raft_sut_direct_put_and_snapshot() -> None:
    """Adapter surface without registry: elect + put + unique leader."""
    from mpreg.testing.distlab.history import History

    sut = RaftSUT.create(3)
    await sut.start()
    try:
        leader = await sut.wait_for_leader()
        h = History()
        res = await sut.put(h, process="c0", key="k", value=7, leader=leader)
        assert res is not None
        await sut.wait_sm_key("k", 7)
        snap = sut.snapshot_state()
        assert snap.leader_count() == 1
        assert snap.sm_value("k") == {"n0": 7, "n1": 7, "n2": 7}
        check = default_raft_checkers(key="k", require_all_sm=True).check(h, state=snap)
        assert check.ok, check
    finally:
        await sut.stop()


@pytest.mark.asyncio
async def test_registry_lists_raft_scenarios() -> None:
    ensure_builtins()
    names = get_registry().list()
    for n in (
        "raft.elect_3",
        "raft.elect_5",
        "raft.partition_majority",
        "raft.partition_heal",
        "raft.leader_stepdown_reelect",
    ):
        assert n in names
    cat = get_registry().catalog()
    raft = [c for c in cat if c["name"].startswith("raft.")]
    assert all(c.get("track") == "raft" for c in raft)


@pytest.mark.asyncio
async def test_suite_preset_raft_core() -> None:
    ensure_builtins()
    selected = get_registry().select(preset="raft-core")
    assert "raft.elect_3" in selected
    assert "raft.partition_heal" in selected
    report = await get_registry().run_suite(preset="raft-core", fail_fast=True)
    assert report["ok"], report.get("failed")


@pytest.mark.asyncio
async def test_scenario_inline_elect() -> None:
    """Inline Scenario construction (docs/example path)."""
    from mpreg.testing.distlab.history import History
    from mpreg.testing.distlab.models import OpKind

    sut = RaftSUT.create(3)

    async def setup() -> object:
        await sut.start()
        return sut

    async def body(history: History, s: object) -> None:
        leader = await sut.wait_for_leader()
        history.ok("c0", OpKind.BARRIER, meta={"leader": leader.node_id})
        assert await sut.put(history, process="c0", key="x", value=1) is not None

    async def teardown(_s: object) -> None:
        await sut.stop()

    r = await Scenario(
        name="inline.raft",
        setup=setup,
        body=body,
        teardown=teardown,
        checker=default_raft_checkers(key="x", require_all_sm=True),
    ).run()
    assert r.ok, r.check
