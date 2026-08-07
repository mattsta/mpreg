"""Adapter to emit cache node profiles into routing catalog deltas."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

from mpreg.datastructures.type_aliases import (
    ClusterId,
    DurationSeconds,
    NodeId,
    RegionName,
    Timestamp,
)

from ..catalog import CacheNodeProfile
from ..catalog_delta import RoutingCatalogDelta
from ..federation_graph import GeographicCoordinate


@dataclass(slots=True)
class CacheProfileCatalogAdapter:
    node_id: NodeId
    cluster_id: ClusterId
    region: RegionName
    coordinates: GeographicCoordinate
    capacity_mb: int
    utilization_percent: float
    avg_latency_ms: float
    reliability_score: float
    ttl_seconds: DurationSeconds = 30.0

    def build_delta(
        self,
        *,
        now: Timestamp | None = None,
        update_id: str | None = None,
    ) -> RoutingCatalogDelta:
        timestamp = now if now is not None else time.time()
        profile = CacheNodeProfile(
            node_id=self.node_id,
            cluster_id=self.cluster_id,
            region=self.region,
            coordinates=self.coordinates,
            capacity_mb=self.capacity_mb,
            utilization_percent=self.utilization_percent,
            avg_latency_ms=self.avg_latency_ms,
            reliability_score=self.reliability_score,
            advertised_at=timestamp,
            ttl_seconds=self.ttl_seconds,
        )
        return RoutingCatalogDelta(
            update_id=update_id or str(uuid.uuid4()),
            cluster_id=self.cluster_id,
            sent_at=timestamp,
            cache_profiles=(profile,),
        )
