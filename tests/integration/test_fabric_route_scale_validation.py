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
from mpreg.fabric.route_announcer import (
    RouteAnnouncementProcessor,
    RouteAnnouncementPublisher,
)
from mpreg.fabric.route_control import RouteDestination, RouteTable, RouteWithdrawal


@pytest.mark.asyncio
async def test_path_vector_churn_converges_small_mesh() -> None:
    transport = InProcessGossipTransport()
    node_ids = [f"node-{idx}" for idx in range(6)]
    cluster_ids = [f"cluster-{idx}" for idx in range(6)]
    resolver = dict(zip(node_ids, cluster_ids))

    gossips: dict[str, GossipProtocol] = {}
    tables: dict[str, RouteTable] = {}
    publishers: dict[str, RouteAnnouncementPublisher] = {}

    for node_id, cluster_id in zip(node_ids, cluster_ids):
        gossip = GossipProtocol(node_id=node_id, transport=transport)
        transport.register(gossip)
        table = RouteTable(local_cluster=cluster_id)
        publisher = RouteAnnouncementPublisher(gossip)
        processor = RouteAnnouncementProcessor(
            local_cluster=cluster_id,
            route_table=table,
            sender_cluster_resolver=lambda node, mapping=resolver: mapping.get(node),
            publisher=None,
        )
        gossip.route_applier = processor
        gossips[cluster_id] = gossip
        tables[cluster_id] = table
        publishers[cluster_id] = publisher

    now = time.time()
    for cluster_id, publisher in publishers.items():
        announcement = tables[cluster_id].build_local_announcement(
            ttl_seconds=30.0,
            epoch=1,
            now=now,
        )
        await publisher.publish(announcement)

    for _ in range(3):
        for gossip in gossips.values():
            await gossip._perform_gossip_cycle()

    destination = RouteDestination(cluster_id="cluster-5")
    for cluster_id, table in tables.items():
        if cluster_id == "cluster-5":
            continue
        assert table.select_route(destination, now=now + 1) is not None

    withdrawal = RouteWithdrawal(
        destination=destination,
        path=tables["cluster-5"].build_local_announcement().path,
        advertiser="cluster-5",
        withdrawn_at=now + 2,
        epoch=2,
    )
    await publishers["cluster-5"].publish_withdrawal(withdrawal)

    for _ in range(3):
        for gossip in gossips.values():
            await gossip._perform_gossip_cycle()

    for cluster_id, table in tables.items():
        if cluster_id == "cluster-5":
            continue
        assert table.select_route(destination, now=now + 3) is None


@pytest.mark.asyncio
async def test_link_state_churn_breaks_path() -> None:
    transport = InProcessGossipTransport()
    node_ids = [f"node-{idx}" for idx in range(4)]
    cluster_ids = [f"cluster-{idx}" for idx in range(4)]
    resolver = dict(zip(node_ids, cluster_ids))

    gossips: dict[str, GossipProtocol] = {}
    routers: dict[str, GraphBasedFederationRouter] = {}
    publishers: dict[str, LinkStatePublisher] = {}

    for node_id, cluster_id in zip(node_ids, cluster_ids):
        gossip = GossipProtocol(node_id=node_id, transport=transport)
        transport.register(gossip)
        router = GraphBasedFederationRouter()
        processor = LinkStateProcessor(
            local_cluster=cluster_id,
            table=LinkStateTable(local_cluster=cluster_id),
            router=router,
            sender_cluster_resolver=lambda node, mapping=resolver: mapping.get(node),
        )
        gossip.link_state_applier = processor
        gossips[cluster_id] = gossip
        routers[cluster_id] = router
        publishers[cluster_id] = LinkStatePublisher(gossip)

    now = time.time()
    updates = [
        LinkStateUpdate(
            origin="cluster-0",
            neighbors=(LinkStateNeighbor(cluster_id="cluster-1", latency_ms=4.0),),
            advertised_at=now,
            ttl_seconds=30.0,
            sequence=1,
        ),
        LinkStateUpdate(
            origin="cluster-1",
            neighbors=(LinkStateNeighbor(cluster_id="cluster-2", latency_ms=4.0),),
            advertised_at=now,
            ttl_seconds=30.0,
            sequence=1,
        ),
        LinkStateUpdate(
            origin="cluster-2",
            neighbors=(LinkStateNeighbor(cluster_id="cluster-3", latency_ms=4.0),),
            advertised_at=now,
            ttl_seconds=30.0,
            sequence=1,
        ),
    ]

    for update in updates:
        await publishers[update.origin].publish(update)

    for _ in range(2):
        for gossip in gossips.values():
            await gossip._perform_gossip_cycle()

    path = routers["cluster-0"].find_optimal_path("cluster-0", "cluster-3", max_hops=6)
    assert path == ["cluster-0", "cluster-1", "cluster-2", "cluster-3"]

    churn_update = LinkStateUpdate(
        origin="cluster-1",
        neighbors=(),
        advertised_at=now + 10,
        ttl_seconds=30.0,
        sequence=2,
    )
    await publishers["cluster-1"].publish(churn_update)

    for _ in range(2):
        for gossip in gossips.values():
            await gossip._perform_gossip_cycle()

    path = routers["cluster-0"].find_optimal_path("cluster-0", "cluster-3", max_hops=6)
    assert path is None


@pytest.mark.asyncio
async def test_link_state_mesh_supports_ecmp_paths() -> None:
    transport = InProcessGossipTransport()
    node_ids = [f"node-{idx}" for idx in range(8)]
    cluster_ids = [f"cluster-{idx}" for idx in range(8)]
    resolver = dict(zip(node_ids, cluster_ids))

    gossips: dict[str, GossipProtocol] = {}
    routers: dict[str, GraphBasedFederationRouter] = {}
    publishers: dict[str, LinkStatePublisher] = {}

    for node_id, cluster_id in zip(node_ids, cluster_ids):
        gossip = GossipProtocol(node_id=node_id, transport=transport)
        transport.register(gossip)
        router = GraphBasedFederationRouter()
        processor = LinkStateProcessor(
            local_cluster=cluster_id,
            table=LinkStateTable(local_cluster=cluster_id),
            router=router,
            sender_cluster_resolver=lambda node, mapping=resolver: mapping.get(node),
        )
        gossip.link_state_applier = processor
        gossips[cluster_id] = gossip
        routers[cluster_id] = router
        publishers[cluster_id] = LinkStatePublisher(gossip)

    now = time.time()
    updates: list[LinkStateUpdate] = []
    for idx, cluster_id in enumerate(cluster_ids):
        neighbor_a = cluster_ids[(idx + 1) % len(cluster_ids)]
        neighbor_b = cluster_ids[(idx + 2) % len(cluster_ids)]
        updates.append(
            LinkStateUpdate(
                origin=cluster_id,
                neighbors=(
                    LinkStateNeighbor(cluster_id=neighbor_a, latency_ms=4.0),
                    LinkStateNeighbor(cluster_id=neighbor_b, latency_ms=4.0),
                ),
                advertised_at=now,
                ttl_seconds=30.0,
                sequence=1,
            )
        )

    for update in updates:
        await publishers[update.origin].publish(update)

    for _ in range(3):
        for gossip in gossips.values():
            await gossip._perform_gossip_cycle()

    paths = routers["cluster-0"].find_multiple_paths(
        "cluster-0", "cluster-5", num_paths=2, max_hops=6
    )
    assert len(paths) >= 2
