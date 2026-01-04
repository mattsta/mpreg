import pytest

from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    FunctionSelector,
    SemanticVersion,
)
from mpreg.fabric.catalog import (
    CacheCatalog,
    CacheNodeProfile,
    CacheProfileCatalog,
    CacheRole,
    CacheRoleEntry,
    FunctionCatalog,
    FunctionEndpoint,
    NodeCatalog,
    NodeDescriptor,
    QueueCatalog,
    QueueEndpoint,
    QueueHealth,
    RoutingCatalog,
    TopicCatalog,
    TopicSubscription,
    TransportEndpoint,
)
from mpreg.fabric.federation_graph import GeographicCoordinate
from mpreg.fabric.message import DeliveryGuarantee


def test_function_catalog_register_and_find() -> None:
    catalog = FunctionCatalog()
    identity = FunctionIdentity(
        name="echo",
        function_id="func-echo",
        version=SemanticVersion.parse("1.0.0"),
    )
    endpoint = FunctionEndpoint(
        identity=identity,
        resources=frozenset({"cpu"}),
        node_id="node-1",
        cluster_id="cluster-a",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    assert catalog.register(endpoint, now=100.0)

    selector = FunctionSelector(name="echo")
    matches = catalog.find(
        selector,
        resources=frozenset({"cpu"}),
        cluster_id="cluster-a",
        node_id="node-1",
        now=105.0,
    )
    assert matches == [endpoint]


def test_function_catalog_name_conflict() -> None:
    catalog = FunctionCatalog()
    first = FunctionEndpoint(
        identity=FunctionIdentity(
            name="echo",
            function_id="func-echo-a",
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset(),
        node_id="node-1",
        cluster_id="cluster-a",
    )
    second = FunctionEndpoint(
        identity=FunctionIdentity(
            name="echo",
            function_id="func-echo-b",
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset(),
        node_id="node-2",
        cluster_id="cluster-a",
    )
    assert catalog.register(first)
    with pytest.raises(ValueError):
        catalog.register(second)


def test_function_catalog_function_id_conflict() -> None:
    catalog = FunctionCatalog()
    first = FunctionEndpoint(
        identity=FunctionIdentity(
            name="echo",
            function_id="func-echo",
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset(),
        node_id="node-1",
        cluster_id="cluster-a",
    )
    second = FunctionEndpoint(
        identity=FunctionIdentity(
            name="echo_alt",
            function_id="func-echo",
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset(),
        node_id="node-2",
        cluster_id="cluster-a",
    )
    assert catalog.register(first)
    with pytest.raises(ValueError):
        catalog.register(second)


def test_function_catalog_prune_expired() -> None:
    catalog = FunctionCatalog()
    endpoint = FunctionEndpoint(
        identity=FunctionIdentity(
            name="echo",
            function_id="func-echo",
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset(),
        node_id="node-1",
        cluster_id="cluster-a",
        advertised_at=10.0,
        ttl_seconds=5.0,
    )
    assert catalog.register(endpoint, now=10.0)
    assert catalog.prune_expired(now=20.0) == 1
    assert catalog.entry_count() == 0


def test_topic_catalog_register_and_match() -> None:
    catalog = TopicCatalog()
    subscription = TopicSubscription(
        subscription_id="sub-1",
        node_id="node-1",
        cluster_id="cluster-a",
        patterns=("foo.*", "bar.#"),
        advertised_at=10.0,
        ttl_seconds=20.0,
    )
    assert catalog.register(subscription, now=10.0)
    matches = catalog.match("foo.test", now=15.0)
    assert matches == [subscription]

    matches = catalog.match("bar.any.depth", now=15.0)
    assert matches == [subscription]


def test_topic_catalog_updates_patterns() -> None:
    catalog = TopicCatalog()
    original = TopicSubscription(
        subscription_id="sub-1",
        node_id="node-1",
        cluster_id="cluster-a",
        patterns=("foo.*",),
    )
    updated = TopicSubscription(
        subscription_id="sub-1",
        node_id="node-1",
        cluster_id="cluster-a",
        patterns=("baz.#",),
    )
    assert catalog.register(original)
    assert catalog.match("foo.one") == [original]
    assert catalog.register(updated)
    assert catalog.match("foo.one") == []
    assert catalog.match("baz.any.depth") == [updated]


def test_queue_catalog_register_and_find() -> None:
    catalog = QueueCatalog()
    endpoint = QueueEndpoint(
        queue_name="jobs",
        cluster_id="cluster-a",
        node_id="node-1",
        delivery_guarantees=frozenset({DeliveryGuarantee.AT_LEAST_ONCE}),
        health=QueueHealth.HEALTHY,
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    assert catalog.register(endpoint, now=100.0)
    matches = catalog.find("jobs", cluster_id="cluster-a", now=105.0)
    assert matches == [endpoint]


def test_cache_catalog_register_and_find() -> None:
    catalog = CacheCatalog()
    entry = CacheRoleEntry(
        role=CacheRole.COORDINATOR,
        node_id="node-1",
        cluster_id="cluster-a",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    assert catalog.register(entry, now=100.0)
    matches = catalog.find(CacheRole.COORDINATOR, cluster_id="cluster-a", now=105.0)
    assert matches == [entry]


def test_cache_profile_catalog_register_and_find() -> None:
    catalog = CacheProfileCatalog()
    profile = CacheNodeProfile(
        node_id="node-1",
        cluster_id="cluster-a",
        region="us-west",
        coordinates=GeographicCoordinate(latitude=37.0, longitude=-122.0),
        capacity_mb=512,
        utilization_percent=25.0,
        avg_latency_ms=10.0,
        reliability_score=0.98,
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    assert catalog.register(profile, now=100.0)
    matches = catalog.find(cluster_id="cluster-a", node_id="node-1", now=105.0)
    assert matches == [profile]


def test_node_catalog_register_and_find() -> None:
    catalog = NodeCatalog()
    node = NodeDescriptor(
        node_id="node-1",
        cluster_id="cluster-a",
        resources=frozenset({"cpu", "gpu"}),
        capabilities=frozenset({"rpc", "pubsub"}),
        advertised_at=50.0,
        ttl_seconds=10.0,
    )
    assert catalog.register(node, now=50.0)
    matches = catalog.find(cluster_id="cluster-a", now=55.0)
    assert matches == [node]


def test_node_descriptor_round_trip_with_transport_endpoints() -> None:
    node = NodeDescriptor(
        node_id="node-2",
        cluster_id="cluster-b",
        resources=frozenset({"cpu"}),
        capabilities=frozenset({"rpc"}),
        transport_endpoints=(
            TransportEndpoint(
                connection_type="internal",
                protocol="ws",
                host="127.0.0.1",
                port=9101,
            ),
            TransportEndpoint(
                connection_type="client",
                protocol="tcp",
                host="127.0.0.1",
                port=9102,
            ),
        ),
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    payload = node.to_dict()
    round_trip = NodeDescriptor.from_dict(payload)
    assert round_trip == node


def test_routing_catalog_prune_expired() -> None:
    catalog = RoutingCatalog()
    node = NodeDescriptor(
        node_id="node-1",
        cluster_id="cluster-a",
        advertised_at=10.0,
        ttl_seconds=5.0,
    )
    queue = QueueEndpoint(
        queue_name="jobs",
        cluster_id="cluster-a",
        node_id="node-1",
        advertised_at=10.0,
        ttl_seconds=5.0,
    )
    catalog.nodes.register(node, now=10.0)
    catalog.queues.register(queue, now=10.0)

    pruned = catalog.prune_expired(now=20.0)
    assert pruned["nodes"] == 1
    assert pruned["queues"] == 1
    assert catalog.nodes.entry_count() == 0
    assert catalog.queues.entry_count() == 0
