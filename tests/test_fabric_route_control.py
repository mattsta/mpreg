from mpreg.fabric.route_control import (
    RouteAnnouncement,
    RouteDestination,
    RouteMetrics,
    RoutePath,
    RoutePolicy,
    RouteStabilityPolicy,
    RouteTable,
    RouteWithdrawal,
)


def test_route_announcement_roundtrip() -> None:
    announcement = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-c"),
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(
            hop_count=1,
            latency_ms=12.5,
            bandwidth_mbps=500,
            reliability_score=0.9,
            cost_score=3.0,
        ),
        advertiser="cluster-b",
        advertised_at=100.0,
        ttl_seconds=15.0,
        epoch=2,
    )

    restored = RouteAnnouncement.from_dict(announcement.to_dict())
    assert restored == announcement


def test_route_table_rejects_loops() -> None:
    table = RouteTable(local_cluster="cluster-a")
    announcement = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-c"),
        path=RoutePath(("cluster-a", "cluster-c")),
        metrics=RouteMetrics(),
        advertiser="cluster-a",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )

    assert (
        table.apply_announcement(announcement, received_from="cluster-b", now=101.0)
        is False
    )
    assert table.routes_for(RouteDestination("cluster-c"), now=101.0) == ()


def test_route_table_selects_best_route() -> None:
    table = RouteTable(local_cluster="cluster-a", policy=RoutePolicy())
    destination = RouteDestination(cluster_id="cluster-c")
    fast = RouteAnnouncement(
        destination=destination,
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(hop_count=1, latency_ms=5.0, bandwidth_mbps=200),
        advertiser="cluster-b",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )
    slow = RouteAnnouncement(
        destination=destination,
        path=RoutePath(("cluster-d", "cluster-c")),
        metrics=RouteMetrics(hop_count=1, latency_ms=50.0, bandwidth_mbps=200),
        advertiser="cluster-d",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )

    assert table.apply_announcement(fast, received_from="cluster-b", now=100.0) is True
    assert table.apply_announcement(slow, received_from="cluster-d", now=100.0) is True

    selected = table.select_route(destination, now=100.0)
    assert selected is not None
    assert selected.next_hop == "cluster-b"
    assert selected.path.hops == ("cluster-a", "cluster-b", "cluster-c")

    selected = table.select_route(destination, avoid_clusters=("cluster-b",), now=100.0)
    assert selected is not None
    assert selected.next_hop == "cluster-d"


def test_route_selection_trace_explains_choice() -> None:
    table = RouteTable(local_cluster="cluster-a", policy=RoutePolicy())
    destination = RouteDestination(cluster_id="cluster-d")
    fast = RouteAnnouncement(
        destination=destination,
        path=RoutePath(("cluster-b", "cluster-d")),
        metrics=RouteMetrics(hop_count=1, latency_ms=5.0, bandwidth_mbps=200),
        advertiser="cluster-b",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )
    slow = RouteAnnouncement(
        destination=destination,
        path=RoutePath(("cluster-c", "cluster-d")),
        metrics=RouteMetrics(hop_count=1, latency_ms=50.0, bandwidth_mbps=200),
        advertiser="cluster-c",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )

    assert table.apply_announcement(fast, received_from="cluster-b", now=100.0) is True
    assert table.apply_announcement(slow, received_from="cluster-c", now=100.0) is True

    trace = table.explain_selection(destination, now=100.0)
    assert trace.selected is not None
    assert trace.selected.next_hop == "cluster-b"
    assert len(trace.candidates) == 2
    assert (
        tuple(item.name for item in trace.selected.tiebreakers)
        == table.policy.deterministic_tiebreakers
    )

    trace_avoid = table.explain_selection(
        destination, avoid_clusters=("cluster-b",), now=100.0
    )
    assert trace_avoid.selected is not None
    assert trace_avoid.selected.next_hop == "cluster-c"
    avoided = [c for c in trace_avoid.candidates if c.next_hop == "cluster-b"][0]
    assert avoided.filtered_reason == "avoid_clusters"


def test_route_metrics_hop_extension() -> None:
    metrics = RouteMetrics(
        hop_count=0,
        latency_ms=1.0,
        bandwidth_mbps=100,
        reliability_score=1.0,
        cost_score=0.0,
    )

    extended = metrics.with_added_hop(
        latency_ms=5.0,
        bandwidth_mbps=50,
        reliability_score=0.9,
        cost_score=2.0,
    )

    assert extended.hop_count == 1
    assert extended.latency_ms == 6.0
    assert extended.bandwidth_mbps == 50
    assert extended.reliability_score == 0.9
    assert extended.cost_score == 2.0


def test_route_table_expiration() -> None:
    table = RouteTable(local_cluster="cluster-a")
    destination = RouteDestination(cluster_id="cluster-c")
    announcement = RouteAnnouncement(
        destination=destination,
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(),
        advertiser="cluster-b",
        advertised_at=100.0,
        ttl_seconds=1.0,
        epoch=1,
    )

    table.apply_announcement(announcement, received_from="cluster-b", now=100.0)
    assert table.select_route(destination, now=100.5) is not None
    assert table.select_route(destination, now=102.0) is None


def test_route_withdrawal_sets_hold_down() -> None:
    table = RouteTable(
        local_cluster="cluster-a",
        stability_policy=RouteStabilityPolicy(hold_down_seconds=5.0),
    )
    destination = RouteDestination(cluster_id="cluster-c")
    announcement = RouteAnnouncement(
        destination=destination,
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(),
        advertiser="cluster-b",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )

    assert (
        table.apply_announcement(announcement, received_from="cluster-b", now=100.0)
        is True
    )

    withdrawal = RouteWithdrawal(
        destination=destination,
        path=RoutePath(("cluster-b", "cluster-c")),
        advertiser="cluster-b",
        withdrawn_at=101.0,
        epoch=2,
    )
    removed = table.apply_withdrawal(withdrawal, received_from="cluster-b", now=101.0)
    assert removed
    assert table.select_route(destination, now=101.0) is None

    # Hold-down prevents immediate re-acceptance.
    assert (
        table.apply_announcement(announcement, received_from="cluster-b", now=103.0)
        is False
    )
    # After hold-down window, acceptance is allowed again.
    assert (
        table.apply_announcement(announcement, received_from="cluster-b", now=107.0)
        is True
    )


def test_route_table_stats_counts() -> None:
    table = RouteTable(local_cluster="cluster-a")
    destination = RouteDestination(cluster_id="cluster-c")
    announcement = RouteAnnouncement(
        destination=destination,
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(),
        advertiser="cluster-b",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )

    assert (
        table.apply_announcement(announcement, received_from="cluster-b", now=100.0)
        is True
    )
    assert table.stats.announcements_accepted == 1
    assert table.stats.announcements_rejected == 0

    withdrawal = RouteWithdrawal(
        destination=destination,
        path=RoutePath(("cluster-b", "cluster-c")),
        advertiser="cluster-b",
        withdrawn_at=101.0,
        epoch=2,
    )
    removed = table.apply_withdrawal(withdrawal, received_from="cluster-b", now=101.0)
    assert removed
    assert table.stats.withdrawals_received == 1
    assert table.stats.withdrawals_applied == 1


def test_route_flap_dampening_suppresses() -> None:
    table = RouteTable(
        local_cluster="cluster-a",
        stability_policy=RouteStabilityPolicy(
            flap_threshold=2,
            suppression_window_seconds=10.0,
        ),
    )
    destination = RouteDestination(cluster_id="cluster-c")
    announcement = RouteAnnouncement(
        destination=destination,
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(),
        advertiser="cluster-b",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )

    assert (
        table.apply_announcement(announcement, received_from="cluster-b", now=100.0)
        is True
    )

    withdrawal = RouteWithdrawal(
        destination=destination,
        path=RoutePath(("cluster-b", "cluster-c")),
        advertiser="cluster-b",
        withdrawn_at=101.0,
        epoch=2,
    )
    table.apply_withdrawal(withdrawal, received_from="cluster-b", now=101.0)

    assert (
        table.apply_announcement(announcement, received_from="cluster-b", now=102.0)
        is True
    )

    withdrawal_2 = RouteWithdrawal(
        destination=destination,
        path=RoutePath(("cluster-b", "cluster-c")),
        advertiser="cluster-b",
        withdrawn_at=103.0,
        epoch=3,
    )
    table.apply_withdrawal(withdrawal_2, received_from="cluster-b", now=103.0)

    assert (
        table.apply_announcement(announcement, received_from="cluster-b", now=104.0)
        is False
    )
    assert table.stats.suppression_rejects == 1


def test_route_table_deterministic_tiebreakers() -> None:
    policy = RoutePolicy(
        weight_latency=0.0,
        weight_hops=0.0,
        weight_cost=0.0,
        weight_reliability=0.0,
        weight_bandwidth=0.0,
    )
    table = RouteTable(local_cluster="cluster-a", policy=policy)
    destination = RouteDestination(cluster_id="cluster-x")

    higher_hops = RouteAnnouncement(
        destination=destination,
        path=RoutePath(("adv-a", "cluster-x")),
        metrics=RouteMetrics(hop_count=2, latency_ms=5.0, reliability_score=0.9),
        advertiser="adv-a",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )
    higher_latency = RouteAnnouncement(
        destination=destination,
        path=RoutePath(("adv-b", "cluster-x")),
        metrics=RouteMetrics(hop_count=1, latency_ms=40.0, reliability_score=0.5),
        advertiser="adv-b",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )
    lower_latency = RouteAnnouncement(
        destination=destination,
        path=RoutePath(("adv-c", "cluster-x")),
        metrics=RouteMetrics(hop_count=1, latency_ms=15.0, reliability_score=0.9),
        advertiser="adv-c",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )

    assert (
        table.apply_announcement(higher_hops, received_from="adv-a", now=100.0) is True
    )
    assert (
        table.apply_announcement(higher_latency, received_from="adv-b", now=100.0)
        is True
    )
    assert (
        table.apply_announcement(lower_latency, received_from="adv-c", now=100.0)
        is True
    )

    selected = table.select_route(destination, now=100.0)
    assert selected is not None
    assert selected.advertiser == "adv-c"

    destination_2 = RouteDestination(cluster_id="cluster-y")
    tie_a = RouteAnnouncement(
        destination=destination_2,
        path=RoutePath(("adv-a", "cluster-y")),
        metrics=RouteMetrics(hop_count=1, latency_ms=10.0, reliability_score=0.9),
        advertiser="adv-a",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )
    tie_b = RouteAnnouncement(
        destination=destination_2,
        path=RoutePath(("adv-b", "cluster-y")),
        metrics=RouteMetrics(hop_count=1, latency_ms=10.0, reliability_score=0.9),
        advertiser="adv-b",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )

    assert table.apply_announcement(tie_a, received_from="adv-a", now=100.0) is True
    assert table.apply_announcement(tie_b, received_from="adv-b", now=100.0) is True
    selected = table.select_route(destination_2, now=100.0)
    assert selected is not None
    assert selected.advertiser == "adv-a"


def test_route_metrics_snapshot_reports_convergence() -> None:
    policy = RoutePolicy(
        weight_latency=0.0,
        weight_hops=0.0,
        weight_cost=0.0,
        weight_reliability=0.0,
        weight_bandwidth=0.0,
    )
    table = RouteTable(local_cluster="cluster-a", policy=policy)
    destination = RouteDestination(cluster_id="cluster-c")
    initial = RouteAnnouncement(
        destination=destination,
        path=RoutePath(("adv-a", "cluster-c")),
        metrics=RouteMetrics(hop_count=1, latency_ms=40.0, reliability_score=0.9),
        advertiser="adv-a",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )
    improved = RouteAnnouncement(
        destination=destination,
        path=RoutePath(("adv-b", "cluster-c")),
        metrics=RouteMetrics(hop_count=1, latency_ms=10.0, reliability_score=0.9),
        advertiser="adv-b",
        advertised_at=110.0,
        ttl_seconds=30.0,
        epoch=2,
    )

    assert table.apply_announcement(initial, received_from="adv-a", now=100.0) is True
    snapshot = table.metrics_snapshot(now=100.0)
    assert snapshot["routes_active_total"] == 1
    assert snapshot["destinations_tracked"] == 1
    assert snapshot["routes_per_destination"]["cluster-c"] == 1
    assert snapshot["convergence_seconds_avg"] == 0.0

    assert table.apply_announcement(improved, received_from="adv-b", now=110.0) is True
    snapshot = table.metrics_snapshot(now=110.0)
    assert snapshot["routes_active_total"] == 2
    assert snapshot["routes_per_destination"]["cluster-c"] == 2
    assert snapshot["convergence_seconds_avg"] == 10.0
