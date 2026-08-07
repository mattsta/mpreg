"""Adapter to emit cache role announcements into routing catalog deltas."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

from mpreg.datastructures.type_aliases import (
    ClusterId,
    DurationSeconds,
    NodeId,
    Timestamp,
)

from ..catalog import CacheRole, CacheRoleEntry
from ..catalog_delta import RoutingCatalogDelta


@dataclass(slots=True)
class CacheFederationCatalogAdapter:
    node_id: NodeId
    cluster_id: ClusterId
    role: CacheRole = CacheRole.SYNC
    ttl_seconds: DurationSeconds = 30.0

    def build_delta(
        self,
        *,
        now: Timestamp | None = None,
        update_id: str | None = None,
    ) -> RoutingCatalogDelta:
        timestamp = now if now is not None else time.time()
        entry = CacheRoleEntry(
            role=self.role,
            node_id=self.node_id,
            cluster_id=self.cluster_id,
            advertised_at=timestamp,
            ttl_seconds=self.ttl_seconds,
        )
        return RoutingCatalogDelta(
            update_id=update_id or str(uuid.uuid4()),
            cluster_id=self.cluster_id,
            sent_at=timestamp,
            caches=(entry,),
        )
