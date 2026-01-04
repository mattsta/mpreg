import pytest

from mpreg.core.model import PubSubSubscription, TopicPattern
from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    SemanticVersion,
)
from mpreg.fabric.adapters.function_registry import LocalFunctionCatalogAdapter
from mpreg.fabric.adapters.topic_exchange import TopicExchangeCatalogAdapter
from mpreg.fabric.announcers import FabricQueueAnnouncer, FabricTopicAnnouncer
from mpreg.fabric.broadcaster import CatalogBroadcaster
from mpreg.fabric.catalog import QueueEndpoint, QueueHealth, RoutingCatalog
from mpreg.fabric.catalog_delta import RoutingCatalogApplier
from mpreg.fabric.catalog_publisher import CatalogDeltaPublisher
from mpreg.fabric.function_registry import LocalFunctionRegistry
from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport
from mpreg.fabric.message import DeliveryGuarantee


@pytest.mark.asyncio
async def test_function_registry_delta_gossips_to_remote_catalog() -> None:
    registry = LocalFunctionRegistry(
        node_id="node-a",
        cluster_id="cluster-a",
        ttl_seconds=30.0,
    )
    identity = FunctionIdentity(
        name="echo",
        function_id="func-echo",
        version=SemanticVersion.parse("1.0.0"),
    )
    registry.register(identity, resources=frozenset({"cpu"}), now=100.0)

    adapter = LocalFunctionCatalogAdapter(
        registry=registry,
        node_resources=frozenset({"cpu"}),
        node_capabilities=frozenset({"rpc"}),
    )
    delta = adapter.build_delta(now=100.0, update_id="update-1")

    catalog_a = RoutingCatalog()
    catalog_b = RoutingCatalog()
    transport = InProcessGossipTransport()
    gossip_a = GossipProtocol(
        node_id="node-a",
        transport=transport,
        catalog_applier=RoutingCatalogApplier(catalog_a),
    )
    gossip_b = GossipProtocol(
        node_id="node-b",
        transport=transport,
        catalog_applier=RoutingCatalogApplier(catalog_b),
    )
    publisher = CatalogDeltaPublisher(gossip_a)

    message = await publisher.publish(delta)
    await gossip_b.handle_received_message(message)

    assert catalog_b.functions.entry_count() == 1
    assert catalog_b.nodes.entry_count() == 1


@pytest.mark.asyncio
async def test_queue_advertisement_gossips_to_remote_catalog() -> None:
    catalog_a = RoutingCatalog()
    catalog_b = RoutingCatalog()
    applier_a = RoutingCatalogApplier(catalog_a)
    applier_b = RoutingCatalogApplier(catalog_b)
    transport = InProcessGossipTransport()
    gossip_a = GossipProtocol(
        node_id="node-a", transport=transport, catalog_applier=applier_a
    )
    gossip_b = GossipProtocol(
        node_id="node-b", transport=transport, catalog_applier=applier_b
    )
    broadcaster = CatalogBroadcaster(
        catalog=catalog_a,
        applier=applier_a,
        publisher=CatalogDeltaPublisher(gossip_a),
    )
    announcer = FabricQueueAnnouncer(broadcaster=broadcaster)

    endpoint = QueueEndpoint(
        queue_name="jobs",
        cluster_id="cluster-a",
        node_id="node-a",
        delivery_guarantees=frozenset({DeliveryGuarantee.AT_LEAST_ONCE}),
        health=QueueHealth.HEALTHY,
        current_subscribers=0,
        metadata={},
        ttl_seconds=30.0,
    )

    result = await announcer.advertise(endpoint, now=100.0)
    await gossip_b.handle_received_message(result.message)

    assert catalog_b.queues.entry_count() == 1


@pytest.mark.asyncio
async def test_topic_subscription_gossips_to_remote_catalog() -> None:
    catalog_a = RoutingCatalog()
    catalog_b = RoutingCatalog()
    applier_a = RoutingCatalogApplier(catalog_a)
    applier_b = RoutingCatalogApplier(catalog_b)
    transport = InProcessGossipTransport()
    gossip_a = GossipProtocol(
        node_id="node-a", transport=transport, catalog_applier=applier_a
    )
    gossip_b = GossipProtocol(
        node_id="node-b", transport=transport, catalog_applier=applier_b
    )
    broadcaster = CatalogBroadcaster(
        catalog=catalog_a,
        applier=applier_a,
        publisher=CatalogDeltaPublisher(gossip_a),
    )
    adapter = TopicExchangeCatalogAdapter(
        node_id="node-a",
        cluster_id="cluster-a",
        ttl_seconds=30.0,
    )
    announcer = FabricTopicAnnouncer(adapter=adapter, broadcaster=broadcaster)

    subscription = PubSubSubscription(
        subscription_id="sub-1",
        patterns=(TopicPattern(pattern="alerts.#"),),
        subscriber="client-1",
        created_at=100.0,
        get_backlog=False,
        backlog_seconds=0,
    )

    result = await announcer.announce_subscription(subscription, now=100.0)
    await gossip_b.handle_received_message(result.message)

    assert catalog_b.topics.entry_count() == 1
