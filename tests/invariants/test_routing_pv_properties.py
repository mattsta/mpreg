"""A2: Path-vector loop freedom, withdraw, tie-break (INV-R2, R3, R7)."""

from __future__ import annotations

import time

from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.fabric.route_control import (
    RouteAnnouncement,
    RouteDestination,
    RouteMetrics,
    RoutePath,
    RouteTable,
    RouteWithdrawal,
)
from mpreg.testing.oracles import RoutingOracle

def _announce(
    *,
    dest: str,
    hops: tuple[str, ...],
    advertiser: str,
    received_from: str,
    table: RouteTable,
    now: float,
    latency: float = 1.0,
) -> bool:
    ann = RouteAnnouncement(
        destination=RouteDestination(cluster_id=dest),
        path=RoutePath(hops=hops),
        metrics=RouteMetrics(hop_count=max(0, len(hops) - 1), latency_ms=latency),
        advertiser=advertiser,
        advertised_at=now,
        ttl_seconds=60.0,
    )
    return table.apply_announcement(
        ann, received_from=received_from, now=now, link_latency_ms=0.5
    )

def test_rejects_path_containing_local() -> None:
    now = time.time()
    table = RouteTable(local_cluster="local")
    ok = _announce(
        dest="z",
        hops=("peer", "local", "z"),
        advertiser="peer",
        received_from="peer",
        table=table,
        now=now,
    )
    assert ok is False
    RoutingOracle.assert_table_loop_free(table)

def test_accepts_simple_path_and_selects() -> None:
    now = time.time()
    table = RouteTable(local_cluster="a")
    assert _announce(
        dest="c",
        hops=("b", "c"),
        advertiser="b",
        received_from="b",
        table=table,
        now=now,
    )
    rec = table.select_route(RouteDestination(cluster_id="c"), now=now)
    assert rec is not None
    assert rec.next_hop == "b"
    RoutingOracle.assert_table_loop_free(table)

def test_withdraw_removes_advertiser_routes() -> None:
    now = time.time()
    table = RouteTable(local_cluster="a")
    assert _announce(
        dest="c",
        hops=("b", "c"),
        advertiser="b",
        received_from="b",
        table=table,
        now=now,
    )
    wd = RouteWithdrawal(
        destination=RouteDestination(cluster_id="c"),
        path=RoutePath(hops=("b", "c")),
        advertiser="b",
        withdrawn_at=now,
    )
    removed = table.apply_withdrawal(wd, received_from="b", now=now)
    assert removed
    RoutingOracle.assert_no_route_from_advertiser(table, "c", "b")

@given(
    mid=st.sampled_from(["x", "y", "z", "m1", "m2"]),
)
@settings(max_examples=50, deadline=None)
def test_property_no_loop_accept(mid: str) -> None:
    now = time.time()
    table = RouteTable(local_cluster="home")
    # Path that would loop through home must be rejected
    ok = _announce(
        dest="far",
        hops=(mid, "home", "far"),
        advertiser=mid,
        received_from=mid,
        table=table,
        now=now,
    )
    assert ok is False

def test_tiebreak_stable() -> None:
    now = time.time()
    table = RouteTable(local_cluster="a")
    _announce(
        dest="d",
        hops=("b", "d"),
        advertiser="b",
        received_from="b",
        table=table,
        now=now,
        latency=10.0,
    )
    _announce(
        dest="d",
        hops=("c", "d"),
        advertiser="c",
        received_from="c",
        table=table,
        now=now,
        latency=10.0,
    )
    r1 = table.select_route(RouteDestination(cluster_id="d"), now=now)
    r2 = table.select_route(RouteDestination(cluster_id="d"), now=now)
    assert r1 is not None and r2 is not None
    assert r1.advertiser == r2.advertiser

def test_hold_down_rejects_then_recovers() -> None:
    """INV-R11: hold-down blocks re-announce, then recovers after quiet period."""
    from mpreg.fabric.route_control import RouteStabilityPolicy

    table = RouteTable(
        local_cluster="a",
        stability_policy=RouteStabilityPolicy(hold_down_seconds=5.0),
    )
    now = 100.0
    assert _announce(
        dest="z",
        hops=("b", "z"),
        advertiser="b",
        received_from="b",
        table=table,
        now=now,
    )
    wd = RouteWithdrawal(
        destination=RouteDestination(cluster_id="z"),
        path=RoutePath(hops=("b", "z")),
        advertiser="b",
        withdrawn_at=now + 1,
    )
    assert table.apply_withdrawal(wd, received_from="b", now=now + 1)
    assert table.select_route(RouteDestination(cluster_id="z"), now=now + 1) is None

    # Immediate re-announce rejected (hold-down)
    assert (
        _announce(
            dest="z",
            hops=("b", "z"),
            advertiser="b",
            received_from="b",
            table=table,
            now=now + 2,
        )
        is False
    )
    assert table.stats.hold_down_rejects >= 1

    # After hold-down expires, path recovers
    assert _announce(
        dest="z",
        hops=("b", "z"),
        advertiser="b",
        received_from="b",
        table=table,
        now=now + 7,
    )
    rec = table.select_route(RouteDestination(cluster_id="z"), now=now + 7)
    assert rec is not None
    assert rec.next_hop == "b"

def test_flap_suppression_not_permanent_blackhole() -> None:
    """INV-R11: flap damping suppresses, then clears after suppression window."""
    from mpreg.fabric.route_control import RouteStabilityPolicy

    table = RouteTable(
        local_cluster="a",
        stability_policy=RouteStabilityPolicy(
            flap_threshold=2,
            suppression_window_seconds=10.0,
        ),
    )
    dest = RouteDestination(cluster_id="z")
    now = 200.0

    def ann(t: float) -> bool:
        return _announce(
            dest="z",
            hops=("b", "z"),
            advertiser="b",
            received_from="b",
            table=table,
            now=t,
        )

    def wd(t: float) -> None:
        table.apply_withdrawal(
            RouteWithdrawal(
                destination=dest,
                path=RoutePath(hops=("b", "z")),
                advertiser="b",
                withdrawn_at=t,
            ),
            received_from="b",
            now=t,
        )

    assert ann(now)
    wd(now + 1)
    assert ann(now + 2)
    wd(now + 3)
    # Threshold hit → suppressed
    assert ann(now + 4) is False
    assert table.stats.suppression_rejects >= 1
    # Still suppressed mid-window
    assert ann(now + 8) is False
    # After suppression window, recovery succeeds
    assert ann(now + 15) is True
    rec = table.select_route(dest, now=now + 15)
    assert rec is not None
