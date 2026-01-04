from mpreg.datastructures.function_identity import FunctionIdentity, SemanticVersion
from mpreg.fabric.catalog import (
    CacheNodeProfile,
    FunctionEndpoint,
    NodeDescriptor,
    RoutingCatalog,
)
from mpreg.fabric.catalog_delta import RoutingCatalogApplier, RoutingCatalogDelta
from mpreg.fabric.catalog_policy import CatalogFilterPolicy
from mpreg.fabric.federation_graph import GeographicCoordinate


def _endpoint(*, name: str, function_id: str, cluster_id: str) -> FunctionEndpoint:
    return FunctionEndpoint(
        identity=FunctionIdentity(
            name=name,
            function_id=function_id,
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset({"resource"}),
        node_id=f"{cluster_id}-node",
        cluster_id=cluster_id,
        advertised_at=100.0,
        ttl_seconds=30.0,
    )


def _apply(delta: RoutingCatalogDelta, policy: CatalogFilterPolicy) -> RoutingCatalog:
    catalog = RoutingCatalog()
    applier = RoutingCatalogApplier(catalog, policy=policy)
    applier.apply(delta, now=100.0)
    return catalog


def test_catalog_policy_blocks_other_clusters() -> None:
    delta = RoutingCatalogDelta(
        update_id="update-1",
        cluster_id="cluster-b",
        sent_at=100.0,
        functions=(
            _endpoint(
                name="remote_fun",
                function_id="func-remote",
                cluster_id="cluster-b",
            ),
        ),
    )
    policy = CatalogFilterPolicy(
        local_cluster="cluster-a", allowed_clusters=frozenset({"cluster-a"})
    )
    catalog = _apply(delta, policy)
    assert catalog.functions.entry_count() == 0


def test_catalog_policy_allows_configured_cluster() -> None:
    delta = RoutingCatalogDelta(
        update_id="update-1",
        cluster_id="cluster-b",
        sent_at=100.0,
        functions=(
            _endpoint(
                name="remote_fun",
                function_id="func-remote",
                cluster_id="cluster-b",
            ),
        ),
    )
    policy = CatalogFilterPolicy(
        local_cluster="cluster-a",
        allowed_clusters=frozenset({"cluster-a", "cluster-b"}),
    )
    catalog = _apply(delta, policy)
    assert catalog.functions.entry_count() == 1


def test_catalog_policy_filters_allowed_functions() -> None:
    delta = RoutingCatalogDelta(
        update_id="update-2",
        cluster_id="cluster-b",
        sent_at=100.0,
        functions=(
            _endpoint(
                name="allowed_fun",
                function_id="func-allowed",
                cluster_id="cluster-b",
            ),
            _endpoint(
                name="blocked_fun",
                function_id="func-blocked",
                cluster_id="cluster-b",
            ),
        ),
    )
    policy = CatalogFilterPolicy(
        local_cluster="cluster-a",
        allowed_clusters=frozenset({"cluster-a", "cluster-b"}),
        allowed_functions=frozenset({"allowed_fun"}),
    )
    catalog = _apply(delta, policy)
    assert catalog.functions.entry_count() == 1


def test_catalog_policy_filters_blocked_functions() -> None:
    delta = RoutingCatalogDelta(
        update_id="update-3",
        cluster_id="cluster-b",
        sent_at=100.0,
        functions=(
            _endpoint(
                name="allowed_fun",
                function_id="func-allowed",
                cluster_id="cluster-b",
            ),
            _endpoint(
                name="blocked_fun",
                function_id="func-blocked",
                cluster_id="cluster-b",
            ),
        ),
    )
    policy = CatalogFilterPolicy(
        local_cluster="cluster-a",
        allowed_clusters=frozenset({"cluster-a", "cluster-b"}),
        blocked_functions=frozenset({"blocked_fun"}),
    )
    catalog = _apply(delta, policy)
    assert catalog.functions.entry_count() == 1


def test_catalog_policy_allows_local_functions_even_if_blocked() -> None:
    delta = RoutingCatalogDelta(
        update_id="update-4",
        cluster_id="cluster-a",
        sent_at=100.0,
        functions=(
            _endpoint(
                name="local_fun",
                function_id="func-local",
                cluster_id="cluster-a",
            ),
        ),
    )
    policy = CatalogFilterPolicy(
        local_cluster="cluster-a",
        allowed_clusters=frozenset({"cluster-a"}),
        blocked_functions=frozenset({"local_fun"}),
    )
    catalog = _apply(delta, policy)
    assert catalog.functions.entry_count() == 1


def test_catalog_policy_filters_cache_profiles() -> None:
    profile = CacheNodeProfile(
        node_id="node-1",
        cluster_id="cluster-b",
        region="us-east",
        coordinates=GeographicCoordinate(latitude=40.0, longitude=-73.0),
        capacity_mb=256,
        utilization_percent=40.0,
        avg_latency_ms=12.0,
        reliability_score=0.9,
        advertised_at=100.0,
        ttl_seconds=30.0,
    )
    delta = RoutingCatalogDelta(
        update_id="update-5",
        cluster_id="cluster-b",
        sent_at=100.0,
        cache_profiles=(profile,),
    )
    policy = CatalogFilterPolicy(
        local_cluster="cluster-a", allowed_clusters=frozenset({"cluster-a"})
    )
    catalog = _apply(delta, policy)
    assert catalog.cache_profiles.entry_count() == 0


def test_catalog_policy_blocks_nodes_via_filter() -> None:
    node = NodeDescriptor(
        node_id="node-1",
        cluster_id="cluster-a",
        resources=frozenset(),
        capabilities=frozenset(),
        advertised_at=100.0,
        ttl_seconds=30.0,
    )
    endpoint = FunctionEndpoint(
        identity=FunctionIdentity(
            name="local_fun",
            function_id="func-local",
            version=SemanticVersion.parse("1.0.0"),
        ),
        resources=frozenset({"resource"}),
        node_id="node-1",
        cluster_id="cluster-a",
        advertised_at=100.0,
        ttl_seconds=30.0,
    )
    delta = RoutingCatalogDelta(
        update_id="update-6",
        cluster_id="cluster-a",
        sent_at=100.0,
        functions=(endpoint,),
        nodes=(node,),
    )
    policy = CatalogFilterPolicy(
        local_cluster="cluster-a",
        node_filter=lambda node_id, cluster_id: node_id != "node-1",
    )
    catalog = _apply(delta, policy)
    assert catalog.functions.entry_count() == 0
    assert catalog.nodes.entry_count() == 0
