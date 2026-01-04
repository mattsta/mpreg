import time

import pytest

from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport
from mpreg.fabric.route_key_gossip import RouteKeyAnnouncer, RouteKeyProcessor
from mpreg.fabric.route_keys import RouteKeyAnnouncement, RouteKeyRegistry
from mpreg.fabric.route_security import RouteAnnouncementSigner


def test_route_key_announcement_round_trip() -> None:
    signer = RouteAnnouncementSigner.create()
    registry = RouteKeyRegistry()
    registry.register_key(
        cluster_id="cluster-a",
        public_key=signer.public_key,
        key_id="key-a",
        now=100.0,
    )
    announcement = RouteKeyAnnouncement(
        cluster_id="cluster-a",
        keys=registry.key_sets["cluster-a"].active_records(now=100.0),
        primary_key_id="key-a",
        advertised_at=100.0,
        ttl_seconds=60.0,
    )

    payload = announcement.to_dict()
    parsed = RouteKeyAnnouncement.from_dict(payload)

    assert parsed.cluster_id == "cluster-a"
    assert parsed.primary_key_id == "key-a"
    assert parsed.keys[0].public_key == signer.public_key


@pytest.mark.asyncio
async def test_route_key_gossip_propagates_keys() -> None:
    transport = InProcessGossipTransport()
    gossip_a = GossipProtocol(node_id="node-a", transport=transport)
    gossip_b = GossipProtocol(node_id="node-b", transport=transport)
    transport.register(gossip_a)
    transport.register(gossip_b)

    registry_a = RouteKeyRegistry()
    registry_b = RouteKeyRegistry()

    signer = RouteAnnouncementSigner.create()
    registry_a.register_key(
        cluster_id="cluster-a",
        public_key=signer.public_key,
        key_id="key-a",
        now=100.0,
    )

    resolver = {"node-a": "cluster-a", "node-b": "cluster-b"}
    processor_b = RouteKeyProcessor(
        registry=registry_b,
        sender_cluster_resolver=lambda node_id: resolver.get(node_id),
    )
    gossip_b.route_key_applier = processor_b

    announcer = RouteKeyAnnouncer(
        local_cluster="cluster-a",
        gossip=gossip_a,
        registry=registry_a,
        ttl_seconds=60.0,
        interval_seconds=10.0,
    )

    await announcer.announce_once(now=time.time())
    await gossip_a._perform_gossip_cycle()
    await gossip_b._perform_gossip_cycle()

    assert registry_b.resolve_public_keys("cluster-a")
