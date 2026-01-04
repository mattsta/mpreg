import time

from mpreg.datastructures.function_identity import FunctionIdentity, SemanticVersion
from mpreg.fabric.catalog import (
    CacheNodeProfile,
    CacheRole,
    CacheRoleEntry,
    FunctionEndpoint,
    NodeDescriptor,
    QueueEndpoint,
    QueueHealth,
    RoutingCatalog,
    TopicSubscription,
    TransportEndpoint,
)
from mpreg.fabric.federation_graph import GeographicCoordinate
from mpreg.fabric.message import DeliveryGuarantee
from mpreg.fabric.route_keys import RouteKeyRegistry


def test_routing_catalog_snapshot_roundtrip() -> None:
    catalog = RoutingCatalog()
    now = time.time()
    identity = FunctionIdentity(
        name="snapshot.function",
        function_id="snapshot.function",
        version=SemanticVersion.parse("1.2.3"),
    )
    endpoint = FunctionEndpoint(
        identity=identity,
        resources=frozenset({"cpu"}),
        node_id="node-a",
        cluster_id="cluster-a",
        advertised_at=now,
        ttl_seconds=300.0,
    )
    assert catalog.functions.register(endpoint, now=now)
    subscription = TopicSubscription(
        subscription_id="sub-a",
        node_id="node-a",
        cluster_id="cluster-a",
        patterns=("orders.*",),
        advertised_at=now,
        ttl_seconds=300.0,
    )
    assert catalog.topics.register(subscription, now=now)
    queue = QueueEndpoint(
        queue_name="jobs",
        cluster_id="cluster-a",
        node_id="node-a",
        delivery_guarantees=frozenset({DeliveryGuarantee.AT_LEAST_ONCE}),
        health=QueueHealth.HEALTHY,
        current_subscribers=2,
        metadata={"tier": "gold"},
        advertised_at=now,
        ttl_seconds=300.0,
    )
    assert catalog.queues.register(queue, now=now)
    cache_entry = CacheRoleEntry(
        role=CacheRole.COORDINATOR,
        node_id="node-a",
        cluster_id="cluster-a",
        advertised_at=now,
        ttl_seconds=300.0,
    )
    assert catalog.caches.register(cache_entry, now=now)
    cache_profile = CacheNodeProfile(
        node_id="node-a",
        cluster_id="cluster-a",
        region="us-east",
        coordinates=GeographicCoordinate(latitude=37.0, longitude=-122.0),
        capacity_mb=1024,
        utilization_percent=22.5,
        avg_latency_ms=8.3,
        reliability_score=0.98,
        advertised_at=now,
        ttl_seconds=300.0,
    )
    assert catalog.cache_profiles.register(cache_profile, now=now)
    node = NodeDescriptor(
        node_id="node-a",
        cluster_id="cluster-a",
        resources=frozenset({"cpu"}),
        capabilities=frozenset({"fast"}),
        transport_endpoints=(
            TransportEndpoint(
                connection_type="ws",
                protocol="ws",
                host="127.0.0.1",
                port=1234,
            ),
        ),
        advertised_at=now,
        ttl_seconds=300.0,
    )
    assert catalog.nodes.register(node, now=now)

    payload = catalog.to_dict(now=now)
    restored = RoutingCatalog()
    counts = restored.load_from_dict(payload, now=now)
    assert counts["functions"] == 1
    assert counts["topics"] == 1
    assert counts["queues"] == 1
    assert counts["caches"] == 1
    assert counts["cache_profiles"] == 1
    assert counts["nodes"] == 1
    assert restored.functions.entry_count() == 1
    assert restored.topics.entry_count() == 1
    assert restored.queues.entry_count() == 1
    assert restored.caches.entry_count() == 1
    assert restored.cache_profiles.entry_count() == 1
    assert restored.nodes.entry_count() == 1


def test_route_key_registry_snapshot_roundtrip() -> None:
    registry = RouteKeyRegistry()
    now = time.time()
    registry.register_key(
        cluster_id="cluster-a",
        public_key=b"snapshot-key",
        now=now,
    )
    payload = registry.to_dict(now=now)
    restored = RouteKeyRegistry()
    restored.load_from_dict(payload, now=now)
    assert restored.resolve_public_keys("cluster-a", now=now) == (b"snapshot-key",)
