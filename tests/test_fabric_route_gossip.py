import time

import pytest

from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport
from mpreg.fabric.route_announcer import (
    RouteAnnouncementProcessor,
    RouteAnnouncementPublisher,
    RouteAnnouncer,
)
from mpreg.fabric.route_control import (
    RouteDestination,
    RoutePath,
    RouteTable,
    RouteWithdrawal,
)


@pytest.mark.asyncio
async def test_route_gossip_propagates_routes() -> None:
    transport = InProcessGossipTransport()
    gossip_a = GossipProtocol(node_id="node-a", transport=transport)
    gossip_b = GossipProtocol(node_id="node-b", transport=transport)
    transport.register(gossip_a)
    transport.register(gossip_b)

    resolver = {"node-a": "cluster-a", "node-b": "cluster-b"}
    table_a = RouteTable(local_cluster="cluster-a")
    table_b = RouteTable(local_cluster="cluster-b")

    publisher_a = RouteAnnouncementPublisher(gossip_a)
    publisher_b = RouteAnnouncementPublisher(gossip_b)

    processor_a = RouteAnnouncementProcessor(
        local_cluster="cluster-a",
        route_table=table_a,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
        publisher=publisher_a,
    )
    processor_b = RouteAnnouncementProcessor(
        local_cluster="cluster-b",
        route_table=table_b,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
        publisher=publisher_b,
    )
    gossip_a.route_applier = processor_a
    gossip_b.route_applier = processor_b

    announcer_b = RouteAnnouncer(
        local_cluster="cluster-b",
        route_table=table_b,
        publisher=publisher_b,
        ttl_seconds=30.0,
        interval_seconds=10.0,
    )

    now = time.time()
    await announcer_b.announce_once(now=now)
    await gossip_b._perform_gossip_cycle()

    route = table_a.select_route(RouteDestination(cluster_id="cluster-b"), now=now)
    assert route is not None
    assert route.next_hop == "cluster-b"
    assert route.path.hops == ("cluster-a", "cluster-b")


@pytest.mark.asyncio
async def test_route_gossip_propagates_withdrawals() -> None:
    transport = InProcessGossipTransport()
    gossip_a = GossipProtocol(node_id="node-a", transport=transport)
    gossip_b = GossipProtocol(node_id="node-b", transport=transport)
    transport.register(gossip_a)
    transport.register(gossip_b)

    resolver = {"node-a": "cluster-a", "node-b": "cluster-b"}
    table_a = RouteTable(local_cluster="cluster-a")
    table_b = RouteTable(local_cluster="cluster-b")

    publisher_b = RouteAnnouncementPublisher(gossip_b)
    processor_a = RouteAnnouncementProcessor(
        local_cluster="cluster-a",
        route_table=table_a,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
        publisher=None,
    )
    processor_b = RouteAnnouncementProcessor(
        local_cluster="cluster-b",
        route_table=table_b,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
        publisher=publisher_b,
    )
    gossip_a.route_applier = processor_a
    gossip_b.route_applier = processor_b

    announcer_b = RouteAnnouncer(
        local_cluster="cluster-b",
        route_table=table_b,
        publisher=publisher_b,
        ttl_seconds=30.0,
        interval_seconds=10.0,
    )

    now = time.time()
    await announcer_b.announce_once(now=now)
    await gossip_b._perform_gossip_cycle()
    assert table_a.select_route(RouteDestination(cluster_id="cluster-b"), now=now)

    withdrawal = RouteWithdrawal(
        destination=RouteDestination(cluster_id="cluster-b"),
        path=RoutePath(("cluster-b",)),
        advertiser="cluster-b",
        withdrawn_at=now + 1.0,
        epoch=2,
    )
    await publisher_b.publish_withdrawal(withdrawal)
    await gossip_b._perform_gossip_cycle()

    assert (
        table_a.select_route(RouteDestination(cluster_id="cluster-b"), now=now + 2.0)
        is None
    )
