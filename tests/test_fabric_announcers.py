import pytest

from mpreg.core.model import PubSubSubscription, TopicPattern
from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    SemanticVersion,
)
from mpreg.fabric.adapters.cache_federation import CacheFederationCatalogAdapter
from mpreg.fabric.adapters.function_registry import LocalFunctionCatalogAdapter
from mpreg.fabric.adapters.topic_exchange import TopicExchangeCatalogAdapter
from mpreg.fabric.announcers import (
    FabricCacheRoleAnnouncer,
    FabricFunctionAnnouncer,
    FabricQueueAnnouncer,
    FabricTopicAnnouncer,
)
from mpreg.fabric.broadcaster import CatalogBroadcaster
from mpreg.fabric.catalog import CacheRole, QueueEndpoint, QueueHealth, RoutingCatalog
from mpreg.fabric.catalog_delta import RoutingCatalogApplier
from mpreg.fabric.catalog_publisher import CatalogDeltaPublisher
from mpreg.fabric.function_registry import LocalFunctionRegistry
from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport
from mpreg.fabric.message import DeliveryGuarantee


@pytest.mark.asyncio
async def test_fabric_function_announcer_broadcasts() -> None:
    registry = LocalFunctionRegistry(
        node_id="node-a",
        cluster_id="cluster-a",
        ttl_seconds=42.0,
    )
    identity = FunctionIdentity(
        name="echo",
        function_id="func-echo",
        version=SemanticVersion.parse("1.0.0"),
    )
    registry.register(identity, resources=frozenset({"cpu"}), now=100.0)

    catalog = RoutingCatalog()
    applier = RoutingCatalogApplier(catalog)
    transport = InProcessGossipTransport()
    gossip = GossipProtocol(
        node_id="node-a", transport=transport, catalog_applier=applier
    )
    broadcaster = CatalogBroadcaster(
        catalog=catalog,
        applier=applier,
        publisher=CatalogDeltaPublisher(gossip),
    )
    announcer = FabricFunctionAnnouncer(
        adapter=LocalFunctionCatalogAdapter(
            registry=registry,
            node_resources=frozenset({"cpu"}),
            node_capabilities=frozenset({"rpc"}),
        ),
        broadcaster=broadcaster,
    )

    result = await announcer.announce(now=100.0)

    assert result.counts["functions_added"] == 1
    assert catalog.functions.entry_count() == 1


@pytest.mark.asyncio
async def test_fabric_queue_announcer_broadcasts() -> None:
    advertisement = QueueEndpoint(
        cluster_id="cluster-a",
        queue_name="jobs",
        node_id="node-a",
        delivery_guarantees=frozenset({DeliveryGuarantee.AT_LEAST_ONCE}),
        health=QueueHealth.HEALTHY,
        current_subscribers=0,
        metadata={},
        ttl_seconds=120.0,
    )
    catalog = RoutingCatalog()
    applier = RoutingCatalogApplier(catalog)
    transport = InProcessGossipTransport()
    gossip = GossipProtocol(
        node_id="node-a", transport=transport, catalog_applier=applier
    )
    broadcaster = CatalogBroadcaster(
        catalog=catalog,
        applier=applier,
        publisher=CatalogDeltaPublisher(gossip),
    )
    announcer = FabricQueueAnnouncer(broadcaster=broadcaster)

    result = await announcer.advertise(advertisement, now=100.0)

    assert result.counts["queues_added"] == 1
    assert catalog.queues.entry_count() == 1


@pytest.mark.asyncio
async def test_fabric_cache_role_announcer_broadcasts() -> None:
    catalog = RoutingCatalog()
    applier = RoutingCatalogApplier(catalog)
    transport = InProcessGossipTransport()
    gossip = GossipProtocol(
        node_id="node-a", transport=transport, catalog_applier=applier
    )
    broadcaster = CatalogBroadcaster(
        catalog=catalog,
        applier=applier,
        publisher=CatalogDeltaPublisher(gossip),
    )
    adapter = CacheFederationCatalogAdapter(
        node_id="node-a",
        cluster_id="cluster-a",
        role=CacheRole.SYNC,
        ttl_seconds=30.0,
    )
    announcer = FabricCacheRoleAnnouncer(adapter=adapter, broadcaster=broadcaster)

    result = await announcer.announce(now=100.0)

    assert result.counts["caches_added"] == 1
    assert catalog.caches.entry_count() == 1


@pytest.mark.asyncio
async def test_fabric_topic_announcer_broadcasts() -> None:
    catalog = RoutingCatalog()
    applier = RoutingCatalogApplier(catalog)
    transport = InProcessGossipTransport()
    gossip = GossipProtocol(
        node_id="node-a", transport=transport, catalog_applier=applier
    )
    broadcaster = CatalogBroadcaster(
        catalog=catalog,
        applier=applier,
        publisher=CatalogDeltaPublisher(gossip),
    )
    adapter = TopicExchangeCatalogAdapter(
        node_id="node-a",
        cluster_id="cluster-a",
        ttl_seconds=30.0,
    )
    announcer = FabricTopicAnnouncer(adapter=adapter, broadcaster=broadcaster)

    subscription = PubSubSubscription(
        subscription_id="sub-1",
        patterns=(TopicPattern(pattern="metrics.*"),),
        subscriber="client-1",
        created_at=100.0,
        get_backlog=False,
        backlog_seconds=0,
    )

    result = await announcer.announce_subscription(subscription, now=100.0)

    assert result.counts["topics_added"] == 1
    assert catalog.topics.entry_count() == 1
