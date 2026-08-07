from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    SemanticVersion,
)
from mpreg.fabric import (
    CacheNodeProfile,
    CacheRole,
    CacheRoleEntry,
    DeliveryGuarantee,
    FunctionEndpoint,
    NodeDescriptor,
    QueueEndpoint,
    QueueHealth,
    RoutingCatalog,
    RoutingCatalogApplier,
    RoutingCatalogDelta,
    TopicSubscription,
)
from mpreg.fabric.catalog_policy import CatalogFilterPolicy
from mpreg.fabric.federation_graph import GeographicCoordinate
from mpreg.fabric.peer_directory import PeerDirectory


def test_catalog_delta_apply_adds_entries() -> None:
    catalog = RoutingCatalog()
    applier = RoutingCatalogApplier(catalog)
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
    subscription = TopicSubscription(
        subscription_id="sub-1",
        node_id="node-1",
        cluster_id="cluster-a",
        patterns=("events.*",),
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    queue = QueueEndpoint(
        queue_name="jobs",
        cluster_id="cluster-a",
        node_id="node-1",
        delivery_guarantees=frozenset({DeliveryGuarantee.AT_LEAST_ONCE}),
        health=QueueHealth.HEALTHY,
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    cache = CacheRoleEntry(
        role=CacheRole.COORDINATOR,
        node_id="node-1",
        cluster_id="cluster-a",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    cache_profile = CacheNodeProfile(
        node_id="node-1",
        cluster_id="cluster-a",
        region="us-west",
        coordinates=GeographicCoordinate(latitude=37.0, longitude=-122.0),
        capacity_mb=512,
        utilization_percent=12.0,
        avg_latency_ms=8.0,
        reliability_score=0.99,
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    node = NodeDescriptor(
        node_id="node-1",
        cluster_id="cluster-a",
        resources=frozenset({"cpu"}),
        capabilities=frozenset({"rpc"}),
        advertised_at=100.0,
        ttl_seconds=10.0,
    )

    delta = RoutingCatalogDelta(
        update_id="u1",
        cluster_id="cluster-a",
        functions=(endpoint,),
        topics=(subscription,),
        queues=(queue,),
        caches=(cache,),
        cache_profiles=(cache_profile,),
        nodes=(node,),
    )
    counts = applier.apply(delta, now=100.0)

    assert counts["functions_added"] == 1
    assert counts["topics_added"] == 1
    assert counts["queues_added"] == 1
    assert counts["caches_added"] == 1
    assert counts["cache_profiles_added"] == 1
    assert counts["nodes_added"] == 1
    assert catalog.functions.entry_count() == 1
    assert catalog.topics.entry_count() == 1
    assert catalog.queues.entry_count() == 1
    assert catalog.caches.entry_count() == 1
    assert catalog.cache_profiles.entry_count() == 1
    assert catalog.nodes.entry_count() == 1


def test_catalog_delta_apply_removes_entries() -> None:
    catalog = RoutingCatalog()
    applier = RoutingCatalogApplier(catalog)
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
    subscription = TopicSubscription(
        subscription_id="sub-1",
        node_id="node-1",
        cluster_id="cluster-a",
        patterns=("events.*",),
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    queue = QueueEndpoint(
        queue_name="jobs",
        cluster_id="cluster-a",
        node_id="node-1",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    cache = CacheRoleEntry(
        role=CacheRole.INVALIDATOR,
        node_id="node-1",
        cluster_id="cluster-a",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    cache_profile = CacheNodeProfile(
        node_id="node-1",
        cluster_id="cluster-a",
        region="us-west",
        coordinates=GeographicCoordinate(latitude=37.0, longitude=-122.0),
        capacity_mb=512,
        utilization_percent=15.0,
        avg_latency_ms=12.0,
        reliability_score=0.95,
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    node = NodeDescriptor(
        node_id="node-1",
        cluster_id="cluster-a",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )

    catalog.functions.register(endpoint, now=100.0)
    catalog.topics.register(subscription, now=100.0)
    catalog.queues.register(queue, now=100.0)
    catalog.caches.register(cache, now=100.0)
    catalog.cache_profiles.register(cache_profile, now=100.0)
    catalog.nodes.register(node, now=100.0)

    delta = RoutingCatalogDelta(
        update_id="u2",
        cluster_id="cluster-a",
        function_removals=(endpoint.key(),),
        topic_removals=(subscription.subscription_id,),
        queue_removals=(queue.key(),),
        cache_removals=(cache.key(),),
        cache_profile_removals=(cache_profile.key(),),
        node_removals=(node.key(),),
    )
    counts = applier.apply(delta, now=105.0)

    assert counts["functions_removed"] == 1
    assert counts["topics_removed"] == 1
    assert counts["queues_removed"] == 1
    assert counts["caches_removed"] == 1
    assert counts["cache_profiles_removed"] == 1
    assert counts["nodes_removed"] == 1
    assert catalog.functions.entry_count() == 0
    assert catalog.topics.entry_count() == 0
    assert catalog.queues.entry_count() == 0
    assert catalog.caches.entry_count() == 0
    assert catalog.cache_profiles.entry_count() == 0
    assert catalog.nodes.entry_count() == 0


def test_catalog_delta_observer_respects_policy_filter() -> None:
    catalog = RoutingCatalog()
    directory = PeerDirectory(
        local_node_id="local-node",
        local_cluster_id="cluster-a",
    )
    policy = CatalogFilterPolicy(
        local_cluster="cluster-a",
        node_filter=lambda node_id, cluster_id: node_id != "blocked-node",
    )
    applier = RoutingCatalogApplier(catalog, policy=policy, observers=(directory,))
    allowed_node = NodeDescriptor(
        node_id="allowed-node",
        cluster_id="cluster-a",
        resources=frozenset({"cpu"}),
        capabilities=frozenset({"rpc"}),
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    blocked_node = NodeDescriptor(
        node_id="blocked-node",
        cluster_id="cluster-a",
        resources=frozenset({"cpu"}),
        capabilities=frozenset({"rpc"}),
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    delta = RoutingCatalogDelta(
        update_id="u3",
        cluster_id="cluster-a",
        sent_at=100.0,
        nodes=(allowed_node, blocked_node),
    )

    counts = applier.apply(delta, now=100.0)

    assert counts["nodes_added"] == 1
    assert catalog.nodes.entry_count() == 1
    assert directory.node_for_id("allowed-node") is not None
    assert directory.node_for_id("blocked-node") is None


def test_catalog_delta_round_trip() -> None:
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
    subscription = TopicSubscription(
        subscription_id="sub-1",
        node_id="node-1",
        cluster_id="cluster-a",
        patterns=("events.*",),
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    queue = QueueEndpoint(
        queue_name="jobs",
        cluster_id="cluster-a",
        node_id="node-1",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    cache = CacheRoleEntry(
        role=CacheRole.SYNC,
        node_id="node-1",
        cluster_id="cluster-a",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    cache_profile = CacheNodeProfile(
        node_id="node-1",
        cluster_id="cluster-a",
        region="us-west",
        coordinates=GeographicCoordinate(latitude=37.0, longitude=-122.0),
        capacity_mb=512,
        utilization_percent=30.0,
        avg_latency_ms=9.0,
        reliability_score=0.97,
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    node = NodeDescriptor(
        node_id="node-1",
        cluster_id="cluster-a",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    delta = RoutingCatalogDelta(
        update_id="u3",
        cluster_id="cluster-a",
        sent_at=100.0,
        functions=(endpoint,),
        topics=(subscription,),
        queues=(queue,),
        caches=(cache,),
        cache_profiles=(cache_profile,),
        nodes=(node,),
        function_removals=(endpoint.key(),),
        topic_removals=(subscription.subscription_id,),
        queue_removals=(queue.key(),),
        cache_removals=(cache.key(),),
        cache_profile_removals=(cache_profile.key(),),
        node_removals=(node.key(),),
    )

    payload = delta.to_dict()
    round_trip = RoutingCatalogDelta.from_dict(payload)
    assert round_trip.to_dict() == payload
