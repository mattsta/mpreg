import asyncio
import time

import pytest

from mpreg.core.connection_events import ConnectionEvent, ConnectionEventBus
from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport
from mpreg.fabric.route_announcer import (
    RouteAnnouncementProcessor,
    RouteAnnouncementPublisher,
    RouteAnnouncer,
)
from mpreg.fabric.route_control import RouteDestination, RouteTable
from mpreg.fabric.route_withdrawal import RouteWithdrawalCoordinator


@pytest.mark.asyncio
async def test_withdrawal_on_connection_loss_propagates() -> None:
    transport = InProcessGossipTransport()
    gossip_a = GossipProtocol(node_id="node-a", transport=transport)
    gossip_b = GossipProtocol(node_id="node-b", transport=transport)
    gossip_c = GossipProtocol(node_id="node-c", transport=transport)
    transport.register(gossip_a)
    transport.register(gossip_b)
    transport.register(gossip_c)

    resolver = {
        "node-a": "cluster-a",
        "node-b": "cluster-b",
        "node-c": "cluster-c",
    }

    table_a = RouteTable(local_cluster="cluster-a")
    table_b = RouteTable(local_cluster="cluster-b")
    table_c = RouteTable(local_cluster="cluster-c")

    publisher_a = RouteAnnouncementPublisher(gossip_a)
    publisher_b = RouteAnnouncementPublisher(gossip_b)
    publisher_c = RouteAnnouncementPublisher(gossip_c)

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
    processor_c = RouteAnnouncementProcessor(
        local_cluster="cluster-c",
        route_table=table_c,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
        publisher=publisher_c,
    )
    gossip_a.route_applier = processor_a
    gossip_b.route_applier = processor_b
    gossip_c.route_applier = processor_c

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
    await gossip_a._perform_gossip_cycle()

    destination = RouteDestination(cluster_id="cluster-b")
    routes = table_c.routes_for(destination, now=now)
    assert routes
    assert any(record.learned_from == "cluster-b" for record in routes)
    assert any(record.learned_from == "cluster-a" for record in routes)

    event_bus = ConnectionEventBus()
    coordinator = RouteWithdrawalCoordinator(
        local_cluster="cluster-a",
        route_table=table_a,
        publisher=publisher_a,
        cluster_resolver=lambda node_url: resolver.get(node_url),
    )
    event_bus.subscribe(coordinator)
    event_bus.publish(ConnectionEvent.lost("node-b", local_node_url="node-a"))

    await asyncio.sleep(0)
    await gossip_a._perform_gossip_cycle()

    routes_after = table_c.routes_for(destination, now=now + 1.0)
    assert routes_after
    assert any(record.learned_from == "cluster-b" for record in routes_after)
    assert not any(record.learned_from == "cluster-a" for record in routes_after)
