import time

import pytest

from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport
from mpreg.fabric.route_announcer import (
    RouteAnnouncementProcessor,
    RouteAnnouncementPublisher,
)
from mpreg.fabric.route_control import (
    RouteAnnouncement,
    RouteDestination,
    RouteMetrics,
    RoutePath,
    RouteStabilityPolicy,
    RouteTable,
    RouteWithdrawal,
)


@pytest.mark.asyncio
async def test_route_churn_triggers_suppression() -> None:
    transport = InProcessGossipTransport()
    gossip_a = GossipProtocol(node_id="node-a", transport=transport)
    gossip_b = GossipProtocol(node_id="node-b", transport=transport)
    transport.register(gossip_a)
    transport.register(gossip_b)

    resolver = {"node-a": "cluster-a", "node-b": "cluster-b"}

    table_a = RouteTable(
        local_cluster="cluster-a",
        stability_policy=RouteStabilityPolicy(
            flap_threshold=2,
            suppression_window_seconds=10.0,
        ),
    )
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

    announcement = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-b"),
        path=RoutePath(("cluster-b",)),
        metrics=RouteMetrics(),
        advertiser="cluster-b",
        advertised_at=time.time(),
        ttl_seconds=30.0,
        epoch=1,
    )

    await publisher_b.publish(announcement)
    await gossip_b._perform_gossip_cycle()

    destination = RouteDestination(cluster_id="cluster-b")
    assert table_a.select_route(destination, now=time.time()) is not None

    withdrawal = RouteWithdrawal(
        destination=destination,
        path=RoutePath(("cluster-b",)),
        advertiser="cluster-b",
        withdrawn_at=time.time(),
        epoch=2,
    )
    await publisher_b.publish_withdrawal(withdrawal)
    await gossip_b._perform_gossip_cycle()

    await publisher_b.publish(announcement)
    await gossip_b._perform_gossip_cycle()

    withdrawal_2 = RouteWithdrawal(
        destination=destination,
        path=RoutePath(("cluster-b",)),
        advertiser="cluster-b",
        withdrawn_at=time.time(),
        epoch=3,
    )
    await publisher_b.publish_withdrawal(withdrawal_2)
    await gossip_b._perform_gossip_cycle()

    await publisher_b.publish(announcement)
    await gossip_b._perform_gossip_cycle()

    assert table_a.select_route(destination, now=time.time()) is None
    assert table_a.stats.suppression_rejects >= 1
