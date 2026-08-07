from mpreg.core.global_cache import CacheMetadata, ReplicationStrategy
from mpreg.fabric.cache_selection import CachePeerSelector
from mpreg.fabric.catalog import CacheNodeProfile
from mpreg.fabric.federation_graph import GeographicCoordinate


def _profile(
    node_id: str,
    *,
    region: str,
    latitude: float,
    longitude: float,
    utilization: float,
    latency_ms: float,
    reliability: float,
) -> CacheNodeProfile:
    return CacheNodeProfile(
        node_id=node_id,
        cluster_id="cluster-a",
        region=region,
        coordinates=GeographicCoordinate(latitude=latitude, longitude=longitude),
        capacity_mb=512,
        utilization_percent=utilization,
        avg_latency_ms=latency_ms,
        reliability_score=reliability,
        advertised_at=100.0,
        ttl_seconds=30.0,
    )


def test_cache_peer_selector_prefers_geographic_hints() -> None:
    selector = CachePeerSelector(
        local_region="us-west",
        local_coordinates=GeographicCoordinate(latitude=37.0, longitude=-122.0),
    )
    profiles = (
        _profile(
            "node-west",
            region="us-west",
            latitude=37.0,
            longitude=-122.0,
            utilization=20.0,
            latency_ms=8.0,
            reliability=0.98,
        ),
        _profile(
            "node-eu",
            region="eu-west",
            latitude=51.0,
            longitude=0.0,
            utilization=10.0,
            latency_ms=20.0,
            reliability=0.98,
        ),
    )
    metadata = CacheMetadata(
        replication_policy=ReplicationStrategy.GEOGRAPHIC,
        geographic_hints=["eu-west"],
    )
    peers = selector.select_peers(profiles, metadata=metadata, max_peers=1)
    assert peers == ("node-eu",)


def test_cache_peer_selector_prefers_low_utilization_for_load_based() -> None:
    selector = CachePeerSelector(
        local_region="us-west",
        local_coordinates=GeographicCoordinate(latitude=37.0, longitude=-122.0),
    )
    profiles = (
        _profile(
            "node-hot",
            region="us-west",
            latitude=37.0,
            longitude=-122.0,
            utilization=90.0,
            latency_ms=5.0,
            reliability=0.99,
        ),
        _profile(
            "node-cool",
            region="us-west",
            latitude=37.1,
            longitude=-122.1,
            utilization=30.0,
            latency_ms=12.0,
            reliability=0.95,
        ),
    )
    metadata = CacheMetadata(replication_policy=ReplicationStrategy.LOAD_BASED)
    peers = selector.select_peers(profiles, metadata=metadata, max_peers=1)
    assert peers == ("node-cool",)


def test_cache_peer_selector_respects_replication_none() -> None:
    selector = CachePeerSelector(
        local_region="us-west",
        local_coordinates=GeographicCoordinate(latitude=37.0, longitude=-122.0),
    )
    profiles = (
        _profile(
            "node-1",
            region="us-west",
            latitude=37.0,
            longitude=-122.0,
            utilization=20.0,
            latency_ms=8.0,
            reliability=0.98,
        ),
    )
    metadata = CacheMetadata(replication_policy=ReplicationStrategy.NONE)
    peers = selector.select_peers(profiles, metadata=metadata, max_peers=1)
    assert peers == ()
