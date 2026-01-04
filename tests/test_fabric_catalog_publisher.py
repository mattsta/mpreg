import pytest

from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    SemanticVersion,
)
from mpreg.fabric import FunctionEndpoint, RoutingCatalog, RoutingCatalogApplier
from mpreg.fabric.catalog_delta import RoutingCatalogDelta
from mpreg.fabric.catalog_publisher import CatalogDeltaPublisher
from mpreg.fabric.gossip import GossipMessageType, GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport


@pytest.mark.asyncio
async def test_catalog_publisher_emits_gossip_message() -> None:
    catalog = RoutingCatalog()
    applier = RoutingCatalogApplier(catalog)
    transport = InProcessGossipTransport()
    gossip = GossipProtocol(
        node_id="node-a", transport=transport, catalog_applier=applier
    )
    publisher = CatalogDeltaPublisher(gossip)

    endpoint = FunctionEndpoint(
        identity=FunctionIdentity(
            name="echo",
            function_id="func-echo",
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset({"cpu"}),
        node_id="node-1",
        cluster_id="cluster-a",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    delta = RoutingCatalogDelta(
        update_id="update-1",
        cluster_id="cluster-a",
        sent_at=100.0,
        functions=(endpoint,),
    )

    message = await publisher.publish(delta)

    assert message.message_type is GossipMessageType.CATALOG_UPDATE
    assert message.payload == delta.to_dict()
    assert message.message_id in gossip.recent_messages
    assert gossip.protocol_stats.messages_created == 1
