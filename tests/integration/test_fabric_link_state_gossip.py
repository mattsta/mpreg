import time

import pytest

from mpreg.fabric.federation_graph import GraphBasedFederationRouter
from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport
from mpreg.fabric.link_state import (
    LinkStateNeighbor,
    LinkStateProcessor,
    LinkStatePublisher,
    LinkStateTable,
    LinkStateUpdate,
)


@pytest.mark.asyncio
async def test_link_state_gossip_builds_multi_hop_path() -> None:
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

    router_a = GraphBasedFederationRouter()
    router_b = GraphBasedFederationRouter()
    router_c = GraphBasedFederationRouter()

    processor_a = LinkStateProcessor(
        local_cluster="cluster-a",
        table=LinkStateTable(local_cluster="cluster-a"),
        router=router_a,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
    )
    processor_b = LinkStateProcessor(
        local_cluster="cluster-b",
        table=LinkStateTable(local_cluster="cluster-b"),
        router=router_b,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
    )
    processor_c = LinkStateProcessor(
        local_cluster="cluster-c",
        table=LinkStateTable(local_cluster="cluster-c"),
        router=router_c,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
    )
    gossip_a.link_state_applier = processor_a
    gossip_b.link_state_applier = processor_b
    gossip_c.link_state_applier = processor_c

    publisher_a = LinkStatePublisher(gossip_a)
    publisher_b = LinkStatePublisher(gossip_b)

    now = time.time()
    update_a = LinkStateUpdate(
        origin="cluster-a",
        neighbors=(LinkStateNeighbor(cluster_id="cluster-b", latency_ms=4.0),),
        advertised_at=now,
        ttl_seconds=30.0,
        sequence=1,
    )
    update_b = LinkStateUpdate(
        origin="cluster-b",
        neighbors=(LinkStateNeighbor(cluster_id="cluster-c", latency_ms=6.0),),
        advertised_at=now,
        ttl_seconds=30.0,
        sequence=1,
    )

    await publisher_a.publish(update_a)
    await publisher_b.publish(update_b)

    await gossip_a._perform_gossip_cycle()
    await gossip_b._perform_gossip_cycle()

    path = router_a.find_optimal_path("cluster-a", "cluster-c", max_hops=5)
    assert path == ["cluster-a", "cluster-b", "cluster-c"]
