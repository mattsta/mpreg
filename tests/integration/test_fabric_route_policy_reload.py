import time

import pytest

from mpreg.fabric.control_plane import FabricControlPlane
from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport
from mpreg.fabric.route_control import RouteDestination, RoutePolicy


@pytest.mark.asyncio
async def test_route_policy_reload_keeps_existing_routes() -> None:
    transport = InProcessGossipTransport()
    gossip_a = GossipProtocol(node_id="node-a", transport=transport)
    gossip_b = GossipProtocol(node_id="node-b", transport=transport)
    transport.register(gossip_a)
    transport.register(gossip_b)

    resolver = {"node-a": "cluster-a", "node-b": "cluster-b"}

    control_a = FabricControlPlane.create(
        local_cluster="cluster-a",
        gossip=gossip_a,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
    )
    control_b = FabricControlPlane.create(
        local_cluster="cluster-b",
        gossip=gossip_b,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
    )

    if control_b.route_announcer is None:
        raise RuntimeError("Route announcer not initialized")

    await control_b.route_announcer.announce_once(now=time.time())
    await gossip_b._perform_gossip_cycle()
    await gossip_a._perform_gossip_cycle()

    destination = RouteDestination(cluster_id="cluster-b")
    assert control_a.route_table.select_route(destination, now=time.time()) is not None

    control_a.update_route_configuration(
        route_policy=RoutePolicy(allowed_tags={"gold"}), force=True
    )

    assert control_a.route_table.select_route(destination, now=time.time()) is not None

    await control_b.route_announcer.announce_once(now=time.time())
    await gossip_b._perform_gossip_cycle()
    await gossip_a._perform_gossip_cycle()

    assert control_a.route_table.select_route(destination, now=time.time()) is not None
    assert control_a.route_table.stats.announcements_rejected >= 1
