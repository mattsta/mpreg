from mpreg.fabric.adapters.cache_profile import CacheProfileCatalogAdapter
from mpreg.fabric.federation_graph import GeographicCoordinate


def test_cache_profile_adapter_builds_delta() -> None:
    adapter = CacheProfileCatalogAdapter(
        node_id="node-1",
        cluster_id="cluster-a",
        region="us-west",
        coordinates=GeographicCoordinate(latitude=37.0, longitude=-122.0),
        capacity_mb=512,
        utilization_percent=22.0,
        avg_latency_ms=8.0,
        reliability_score=0.99,
        ttl_seconds=25.0,
    )
    delta = adapter.build_delta(now=100.0, update_id="update-1")

    assert delta.cluster_id == "cluster-a"
    assert delta.update_id == "update-1"
    assert len(delta.cache_profiles) == 1
    profile = delta.cache_profiles[0]
    assert profile.node_id == "node-1"
    assert profile.cluster_id == "cluster-a"
    assert profile.region == "us-west"
    assert profile.coordinates.latitude == 37.0
    assert profile.coordinates.longitude == -122.0
    assert profile.capacity_mb == 512
    assert profile.utilization_percent == 22.0
    assert profile.avg_latency_ms == 8.0
    assert profile.reliability_score == 0.99
    assert profile.advertised_at == 100.0
    assert profile.ttl_seconds == 25.0
