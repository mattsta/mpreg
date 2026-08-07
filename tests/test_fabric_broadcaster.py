import pytest

from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    SemanticVersion,
)
from mpreg.fabric.broadcaster import CatalogBroadcaster
from mpreg.fabric.catalog import FunctionEndpoint, RoutingCatalog
from mpreg.fabric.catalog_delta import RoutingCatalogApplier, RoutingCatalogDelta
from mpreg.fabric.catalog_publisher import CatalogDeltaPublisher
from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport


@pytest.mark.asyncio
async def test_catalog_broadcaster_applies_and_publishes() -> None:
    catalog = RoutingCatalog()
    applier = RoutingCatalogApplier(catalog)
    transport = InProcessGossipTransport()
    gossip = GossipProtocol(
        node_id="node-a", transport=transport, catalog_applier=applier
    )
    publisher = CatalogDeltaPublisher(gossip)
    broadcaster = CatalogBroadcaster(
        catalog=catalog, applier=applier, publisher=publisher
    )

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

    result = await broadcaster.broadcast(delta, now=100.0)

    assert result.counts["functions_added"] == 1
    assert catalog.functions.entry_count() == 1
    assert result.message.message_id in gossip.recent_messages
