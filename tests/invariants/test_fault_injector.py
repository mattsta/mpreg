"""A1: FaultInjector and NetworkView unit tests."""

from __future__ import annotations

from mpreg.testing.faults import FaultInjector, assert_at_most_one_leader, assert_no_routing_loop
from mpreg.testing.oracles import RaftOracle, RoutingOracle, RpcOracle, RpcStreamEvent

def test_partition_blocks_cross_group() -> None:
    inj = FaultInjector(seed=1)
    inj.partition({"a", "b"}, {"c", "d"})
    view = inj.view()
    assert view.can_communicate("a", "b")
    assert not view.can_communicate("a", "c")
    inj.heal()
    assert inj.view().can_communicate("a", "c")

def test_crash_blocks_delivery() -> None:
    inj = FaultInjector()
    inj.crash("x")
    assert not inj.can_deliver("x", "y")
    inj.recover("x")
    assert inj.can_deliver("x", "y")

def test_clock_skew_now_for() -> None:
    inj = FaultInjector()
    inj.set_clock_skew("n1", 5.0)
    wall = 1000.0
    assert inj.view().now_for("n1", wall) == 1005.0
    assert inj.view().now_for("n2", wall) == 1000.0

def test_drop_rate_deterministic() -> None:
    inj = FaultInjector(seed=42, control_drop_rate=1.0)
    assert not inj.can_deliver("a", "b", plane="control")

def test_routing_loop_helper() -> None:
    assert_no_routing_loop(("a", "b", "c"))
    try:
        assert_no_routing_loop(("a", "b", "a"))
        raise AssertionError("expected failure")
    except AssertionError as exc:
        assert "loop" in str(exc).lower()

def test_leader_helper() -> None:
    assert_at_most_one_leader({1: {"n1"}})
    try:
        assert_at_most_one_leader({1: {"n1", "n2"}})
        raise AssertionError("expected")
    except AssertionError:
        pass

def test_routing_oracle_bfs() -> None:
    o = RoutingOracle()
    o.set_edge("a", "b")
    o.set_edge("b", "c")
    assert o.bfs_next_hops("a", "c") == frozenset({"b"})
    assert o.bfs_next_hops("a", "b") == frozenset({"b"})

def test_raft_oracle_safety() -> None:
    o = RaftOracle()
    o.observe_role("n1", 1, "leader")
    o.observe_commit("n1", 1)
    o.observe_commit("n1", 2)
    try:
        o.observe_role("n2", 1, "leader")
        raise AssertionError("expected multi-leader")
    except AssertionError:
        pass

def test_rpc_oracle_monotonic() -> None:
    o = RpcOracle()
    o.observe(RpcStreamEvent(kind="intermediate", level=0))
    o.observe(RpcStreamEvent(kind="intermediate", level=1))
    o.observe(RpcStreamEvent(kind="final"))
    try:
        o.observe(RpcStreamEvent(kind="partial"))
        raise AssertionError("expected")
    except AssertionError:
        pass
