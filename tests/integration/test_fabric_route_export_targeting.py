import time

import pytest

from mpreg.fabric.control_plane import FabricControlPlane
from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport
from mpreg.fabric.route_control import RouteDestination, RoutePolicy
from mpreg.fabric.route_policy_directory import (
    RouteNeighborPolicy,
    RoutePolicyDirectory,
)


@pytest.mark.asyncio
async def test_route_export_targeting_filters_neighbors() -> None:
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

    export_policies = RoutePolicyDirectory(
        default_policy=RoutePolicy(allowed_destinations=set())
    )
    export_policies.register(
        RouteNeighborPolicy(
            cluster_id="cluster-b",
            policy=RoutePolicy(allowed_destinations={"cluster-a"}),
        )
    )

    control_a = FabricControlPlane.create(
        local_cluster="cluster-a",
        gossip=gossip_a,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
        route_export_neighbor_policy_resolver=export_policies.resolver(),
    )
    control_b = FabricControlPlane.create(
        local_cluster="cluster-b",
        gossip=gossip_b,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
    )
    control_c = FabricControlPlane.create(
        local_cluster="cluster-c",
        gossip=gossip_c,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
    )

    if control_a.route_announcer is None:
        raise RuntimeError("Route announcer not initialized")
    if control_b.route_processor:
        control_b.route_processor.publisher = None
    if control_c.route_processor:
        control_c.route_processor.publisher = None

    await control_a.route_announcer.announce_once(now=time.time())
    await gossip_a._perform_gossip_cycle()

    destination = RouteDestination(cluster_id="cluster-a")
    assert control_b.route_table.select_route(destination, now=time.time()) is not None
    assert control_c.route_table.select_route(destination, now=time.time()) is None
