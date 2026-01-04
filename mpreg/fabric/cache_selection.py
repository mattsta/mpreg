"""Cache peer selection logic for fabric cache federation."""

from __future__ import annotations

from dataclasses import dataclass, field

from mpreg.core.cache_models import CacheMetadata, ReplicationStrategy
from mpreg.datastructures.type_aliases import NodeId, RegionName

from .catalog import CacheNodeProfile
from .federation_graph import GeographicCoordinate


@dataclass(frozen=True, slots=True)
class CacheSelectionWeights:
    geographic: float = 0.4
    latency: float = 0.2
    reliability: float = 0.2
    capacity: float = 0.2


@dataclass(slots=True)
class CachePeerSelector:
    local_region: RegionName
    local_coordinates: GeographicCoordinate
    weights: CacheSelectionWeights = field(default_factory=CacheSelectionWeights)
    min_reliability: float = 0.5
    max_utilization_percent: float = 95.0

    def select_peers(
        self,
        profiles: tuple[CacheNodeProfile, ...],
        *,
        metadata: CacheMetadata | None,
        max_peers: int | None = None,
    ) -> tuple[NodeId, ...]:
        if metadata and metadata.replication_policy is ReplicationStrategy.NONE:
            return ()
        eligible = [
            profile
            for profile in profiles
            if profile.reliability_score >= self.min_reliability
            and profile.utilization_percent <= self.max_utilization_percent
        ]
        if not eligible:
            eligible = list(profiles)
        if not eligible:
            return ()

        scored = [
            (
                self._score_profile(profile, metadata),
                profile.node_id,
                profile,
            )
            for profile in eligible
        ]
        scored.sort(key=lambda item: (-item[0], item[1]))
        limit = max_peers if max_peers is not None else len(scored)
        return tuple(item[2].node_id for item in scored[:limit])

    def _score_profile(
        self, profile: CacheNodeProfile, metadata: CacheMetadata | None
    ) -> float:
        policy = (
            metadata.replication_policy
            if metadata is not None
            else ReplicationStrategy.GEOGRAPHIC
        )
        geo_score = self._geographic_score(profile, metadata)
        latency_score = 1.0 / (1.0 + max(profile.avg_latency_ms, 0.0) / 50.0)
        reliability_score = max(0.0, min(profile.reliability_score, 1.0))
        capacity_score = max(0.0, 1.0 - (profile.utilization_percent / 100.0))

        if policy is ReplicationStrategy.GEOGRAPHIC:
            weights = CacheSelectionWeights(
                geographic=0.6, latency=0.2, reliability=0.1, capacity=0.1
            )
        elif policy is ReplicationStrategy.PROXIMITY:
            weights = CacheSelectionWeights(
                geographic=0.5, latency=0.3, reliability=0.1, capacity=0.1
            )
        elif policy is ReplicationStrategy.LOAD_BASED:
            weights = CacheSelectionWeights(
                geographic=0.1, latency=0.1, reliability=0.2, capacity=0.6
            )
        elif policy is ReplicationStrategy.HYBRID:
            weights = self.weights
        else:
            weights = self.weights

        return (
            geo_score * weights.geographic
            + latency_score * weights.latency
            + reliability_score * weights.reliability
            + capacity_score * weights.capacity
        )

    def _geographic_score(
        self, profile: CacheNodeProfile, metadata: CacheMetadata | None
    ) -> float:
        hints = metadata.geographic_hints if metadata is not None else []
        if hints:
            if profile.region in hints:
                return 1.0
            return 0.2

        if self._has_unknown_coordinates(
            self.local_coordinates
        ) or self._has_unknown_coordinates(profile.coordinates):
            return 0.5

        distance_km = self.local_coordinates.distance_to(profile.coordinates)
        return 1.0 / (1.0 + (distance_km / 1000.0))

    @staticmethod
    def _has_unknown_coordinates(coordinates: GeographicCoordinate) -> bool:
        return coordinates.latitude == 0.0 and coordinates.longitude == 0.0
