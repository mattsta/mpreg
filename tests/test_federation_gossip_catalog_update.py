import pytest

from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    SemanticVersion,
)
from mpreg.fabric import FunctionEndpoint, RoutingCatalog, RoutingCatalogApplier
from mpreg.fabric.catalog_delta import RoutingCatalogDelta
from mpreg.fabric.gossip import (
    GossipMessage,
    GossipMessageType,
    GossipProtocol,
)
from mpreg.fabric.gossip_transport import InProcessGossipTransport


@pytest.mark.asyncio
async def test_gossip_catalog_update_applies_delta() -> None:
    catalog = RoutingCatalog()
    applier = RoutingCatalogApplier(catalog)
    transport = InProcessGossipTransport()
    protocol = GossipProtocol(
        node_id="node-a", transport=transport, catalog_applier=applier
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
    message = GossipMessage(
        message_id="msg-1",
        message_type=GossipMessageType.CATALOG_UPDATE,
        sender_id="node-b",
        payload=delta,
    )

    await protocol.handle_received_message(message)

    assert catalog.functions.entry_count() == 1
    assert "msg-1" in protocol.recent_messages


@pytest.mark.asyncio
async def test_gossip_catalog_update_applies_dict_payload() -> None:
    catalog = RoutingCatalog()
    applier = RoutingCatalogApplier(catalog)
    transport = InProcessGossipTransport()
    protocol = GossipProtocol(
        node_id="node-a", transport=transport, catalog_applier=applier
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
        update_id="update-2",
        cluster_id="cluster-a",
        sent_at=100.0,
        functions=(endpoint,),
    )
    message = GossipMessage(
        message_id="msg-2",
        message_type=GossipMessageType.CATALOG_UPDATE,
        sender_id="node-b",
        payload=delta.to_dict(),
    )

    await protocol.handle_received_message(message)

    assert catalog.functions.entry_count() == 1
    assert "msg-2" in protocol.recent_messages


@pytest.mark.asyncio
async def test_gossip_catalog_update_converges_two_nodes() -> None:
    catalog_a = RoutingCatalog()
    catalog_b = RoutingCatalog()
    applier_a = RoutingCatalogApplier(catalog_a)
    applier_b = RoutingCatalogApplier(catalog_b)
    transport = InProcessGossipTransport()
    protocol_a = GossipProtocol(
        node_id="node-a", transport=transport, catalog_applier=applier_a
    )
    protocol_b = GossipProtocol(
        node_id="node-b", transport=transport, catalog_applier=applier_b
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
        update_id="update-3",
        cluster_id="cluster-a",
        sent_at=100.0,
        functions=(endpoint,),
    )

    applier_a.apply(delta, now=delta.sent_at)
    message = GossipMessage(
        message_id="msg-3",
        message_type=GossipMessageType.CATALOG_UPDATE,
        sender_id="node-a",
        payload=delta,
    )

    await protocol_b.handle_received_message(message)

    assert catalog_a.functions.entry_count() == 1
    assert catalog_b.functions.entry_count() == 1
