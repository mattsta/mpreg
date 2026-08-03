"""X6: Bounded soak — invariant runner under churn (short wall time for CI)."""

from __future__ import annotations

import time

from mpreg.fabric.link_state import LinkStateMode
from mpreg.fabric.route_control import (
    RouteAnnouncement,
    RouteDestination,
    RouteMetrics,
    RoutePath,
    RouteTable,
    RouteWithdrawal,
)
from mpreg.testing.faults import FaultInjector, assert_no_routing_loop
from mpreg.testing.oracles import RaftOracle, RoutingOracle

def test_short_soak_routing_and_faults() -> None:
    """Run a compressed soak loop (~1s) asserting zero invariant violations."""
    inj = FaultInjector(seed=99)
    table = RouteTable(local_cluster="home")
    oracle = RoutingOracle(mode=LinkStateMode.DISABLED)
    oracle.set_edge("home", "a")
    oracle.set_edge("a", "b")
    raft_o = RaftOracle()
    raft_o.observe_role("leader", 1, "leader")
    end = time.time() + 0.8
    now = time.time()
    cycles = 0
    while time.time() < end:
        cycles += 1
        if cycles % 5 == 0:
            inj.partition({"home", "a"}, {"b"})
        if cycles % 7 == 0:
            inj.heal()
        ann = RouteAnnouncement(
            destination=RouteDestination(cluster_id="b"),
            path=RoutePath(hops=("a", "b")),
            metrics=RouteMetrics(hop_count=1, latency_ms=1.0),
            advertiser="a",
            advertised_at=now,
            ttl_seconds=30.0,
        )
        table.apply_announcement(ann, received_from="a", now=now)
        RoutingOracle.assert_table_loop_free(table)
        for records in table.routes.values():
            for rec in records:
                assert_no_routing_loop(rec.path.hops)
        if cycles % 3 == 0:
            table.apply_withdrawal(
                RouteWithdrawal(
                    destination=RouteDestination(cluster_id="b"),
                    path=RoutePath(hops=("a", "b")),
                    advertiser="a",
                    withdrawn_at=now,
                ),
                received_from="a",
                now=now,
            )
        raft_o.observe_commit("leader", min(cycles, 100))
        now = time.time()
    raft_o.assert_safe()
    assert cycles >= 5
