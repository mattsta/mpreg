from mpreg.fabric.adapters.cache_federation import CacheFederationCatalogAdapter
from mpreg.fabric.catalog import CacheRole


def test_cache_federation_adapter_builds_delta() -> None:
    adapter = CacheFederationCatalogAdapter(
        node_id="node-1",
        cluster_id="cluster-a",
        role=CacheRole.INVALIDATOR,
        ttl_seconds=25.0,
    )
    delta = adapter.build_delta(now=100.0, update_id="update-1")

    assert delta.cluster_id == "cluster-a"
    assert delta.update_id == "update-1"
    assert len(delta.caches) == 1
    entry = delta.caches[0]
    assert entry.node_id == "node-1"
    assert entry.cluster_id == "cluster-a"
    assert entry.role is CacheRole.INVALIDATOR
    assert entry.advertised_at == 100.0
    assert entry.ttl_seconds == 25.0
