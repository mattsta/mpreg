from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    FunctionSelector,
    SemanticVersion,
)
from mpreg.fabric import (
    CacheNodeProfile,
    CacheProfileQuery,
    FunctionEndpoint,
    FunctionQuery,
    NodeDescriptor,
    NodeQuery,
    RoutingIndex,
    TopicQuery,
    TopicSubscription,
)
from mpreg.fabric.federation_graph import GeographicCoordinate


def test_routing_index_function_query_filters_resources() -> None:
    index = RoutingIndex()
    cpu_endpoint = FunctionEndpoint(
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
    gpu_endpoint = FunctionEndpoint(
        identity=FunctionIdentity(
            name="echo",
            function_id="func-echo",
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset({"gpu"}),
        node_id="node-2",
        cluster_id="cluster-a",
        advertised_at=100.0,
        ttl_seconds=10.0,
    )
    index.catalog.functions.register(cpu_endpoint, now=100.0)
    index.catalog.functions.register(gpu_endpoint, now=100.0)

    query = FunctionQuery(
        selector=FunctionSelector(name="echo"),
        resources=frozenset({"gpu"}),
    )
    matches = index.find_functions(query, now=105.0)
    assert matches == [gpu_endpoint]


def test_routing_index_topic_query_match() -> None:
    index = RoutingIndex()
    subscription = TopicSubscription(
        subscription_id="sub-1",
        node_id="node-1",
        cluster_id="cluster-a",
        patterns=("foo.*",),
        advertised_at=10.0,
        ttl_seconds=20.0,
    )
    index.catalog.topics.register(subscription, now=10.0)

    matches = index.match_topics(TopicQuery(topic="foo.bar"), now=15.0)
    assert matches == [subscription]


def test_routing_index_node_query_filters_resources_and_capabilities() -> None:
    index = RoutingIndex()
    node = NodeDescriptor(
        node_id="node-1",
        cluster_id="cluster-a",
        resources=frozenset({"cpu", "gpu"}),
        capabilities=frozenset({"rpc", "pubsub"}),
        advertised_at=50.0,
        ttl_seconds=20.0,
    )
    index.catalog.nodes.register(node, now=50.0)

    matches = index.find_nodes(
        NodeQuery(resources=frozenset({"gpu"}), capabilities=frozenset({"rpc"})),
        now=55.0,
    )
    assert matches == [node]

    none_matches = index.find_nodes(NodeQuery(resources=frozenset({"fpga"})), now=55.0)
    assert none_matches == []


def test_routing_index_cache_profile_query() -> None:
    index = RoutingIndex()
    profile = CacheNodeProfile(
        node_id="node-1",
        cluster_id="cluster-a",
        region="us-west",
        coordinates=GeographicCoordinate(latitude=37.0, longitude=-122.0),
        capacity_mb=512,
        utilization_percent=20.0,
        avg_latency_ms=9.0,
        reliability_score=0.98,
        advertised_at=100.0,
        ttl_seconds=30.0,
    )
    index.catalog.cache_profiles.register(profile, now=100.0)

    matches = index.find_cache_profiles(
        CacheProfileQuery(cluster_id="cluster-a", node_id="node-1"),
        now=105.0,
    )
    assert matches == [profile]
