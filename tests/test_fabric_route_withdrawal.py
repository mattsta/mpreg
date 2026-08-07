import pytest

from mpreg.fabric.route_control import (
    RouteAnnouncement,
    RouteDestination,
    RouteMetrics,
    RoutePath,
    RouteTable,
)
from mpreg.fabric.route_withdrawal import RouteWithdrawalCoordinator


class StubPublisher:
    def __init__(self) -> None:
        self.withdrawals = []

    async def publish_withdrawal(self, withdrawal) -> None:
        self.withdrawals.append(withdrawal)


@pytest.mark.asyncio
async def test_withdrawal_coordinator_publishes() -> None:
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

    publisher = StubPublisher()
    coordinator = RouteWithdrawalCoordinator(
        local_cluster="cluster-a",
        route_table=table,
        publisher=publisher,
        cluster_resolver=lambda _: "cluster-b",
    )

    count = await coordinator.withdraw_routes_for_cluster("cluster-b", now=105.0)

    assert count == 1
    assert table.select_route(destination, now=105.0) is None
    assert len(publisher.withdrawals) == 1
    withdrawal = publisher.withdrawals[0]
    assert withdrawal.destination == destination
    assert withdrawal.path.hops == ("cluster-a", "cluster-b", "cluster-c")
    assert withdrawal.advertiser == "cluster-a"
