import pytest

from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport
from mpreg.fabric.route_announcer import (
    RouteAnnouncementProcessor,
    RouteAnnouncementPublisher,
    RouteAnnouncer,
)
from mpreg.fabric.route_control import (
    RouteAnnouncement,
    RouteDestination,
    RouteMetrics,
    RoutePath,
    RoutePolicy,
    RouteTable,
)


class StubPublisher:
    def __init__(self) -> None:
        self.announcements: list[RouteAnnouncement] = []

    async def publish(self, announcement: RouteAnnouncement) -> None:
        self.announcements.append(announcement)


@pytest.mark.asyncio
async def test_route_announcement_processor_relays_updates() -> None:
    table = RouteTable(local_cluster="cluster-a")
    publisher = StubPublisher()
    processor = RouteAnnouncementProcessor(
        local_cluster="cluster-a",
        route_table=table,
        sender_cluster_resolver=lambda _: "cluster-b",
        publisher=publisher,
    )
    announcement = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-c"),
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(hop_count=1, latency_ms=2.0),
        advertiser="cluster-b",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )

    updated = await processor.handle_announcement(
        announcement, sender_id="node-b", now=100.0
    )

    assert updated is True
    assert publisher.announcements
    relayed = publisher.announcements[0]
    assert relayed.advertiser == "cluster-a"
    assert relayed.path.hops == ("cluster-a", "cluster-b", "cluster-c")


@pytest.mark.asyncio
async def test_route_announcer_publishes_local() -> None:
    table = RouteTable(local_cluster="cluster-a")
    publisher = StubPublisher()
    announcer = RouteAnnouncer(
        local_cluster="cluster-a",
        route_table=table,
        publisher=publisher,
        ttl_seconds=25.0,
        interval_seconds=10.0,
    )

    await announcer.announce_once(now=50.0)

    assert len(publisher.announcements) == 1
    announcement = publisher.announcements[0]
    assert announcement.advertiser == "cluster-a"
    assert announcement.destination.cluster_id == "cluster-a"
    assert announcement.path.hops == ("cluster-a",)


@pytest.mark.asyncio
async def test_route_publisher_export_policy_blocks() -> None:
    transport = InProcessGossipTransport()
    gossip = GossipProtocol(node_id="node-a", transport=transport)
    transport.register(gossip)

    policy = RoutePolicy(allowed_tags={"gold"})
    publisher = RouteAnnouncementPublisher(gossip, export_policy=policy)

    blocked = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-b"),
        path=RoutePath(("cluster-a", "cluster-b")),
        metrics=RouteMetrics(),
        advertiser="cluster-a",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
        route_tags=("silver",),
    )

    result = await publisher.publish(blocked)
    assert result is None
    assert len(gossip.pending_messages) == 0

    allowed = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-b"),
        path=RoutePath(("cluster-a", "cluster-b")),
        metrics=RouteMetrics(),
        advertiser="cluster-a",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=2,
        route_tags=("gold",),
    )

    result = await publisher.publish(allowed)
    assert result is not None
    assert len(gossip.pending_messages) == 1
