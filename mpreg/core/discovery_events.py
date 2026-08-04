from __future__ import annotations

import time
from dataclasses import dataclass, field

from mpreg.core.payloads import (
    PAYLOAD_FLOAT,
    PAYLOAD_KEEP_EMPTY,
    PAYLOAD_LIST,
    Payload,
)
from mpreg.datastructures.type_aliases import ClusterId, NodeId, Timestamp
from mpreg.fabric.catalog_delta import RoutingCatalogDelta

DISCOVERY_DELTA_TOPIC = "mpreg.discovery.delta"

@dataclass(frozen=True, slots=True)
class CatalogDeltaCounts:
    """Counts summary for catalog delta application."""

    functions_added: int = 0
    functions_removed: int = 0
    topics_added: int = 0
    topics_removed: int = 0
    queues_added: int = 0
    queues_removed: int = 0
    services_added: int = 0
    services_removed: int = 0
    caches_added: int = 0
    caches_removed: int = 0
    cache_profiles_added: int = 0
    cache_profiles_removed: int = 0
    nodes_added: int = 0
    nodes_removed: int = 0

    @classmethod
    def from_dict(cls, payload: dict[str, int]) -> CatalogDeltaCounts:
        return cls(
            functions_added=int(payload.get("functions_added", 0) or 0),
            functions_removed=int(payload.get("functions_removed", 0) or 0),
            topics_added=int(payload.get("topics_added", 0) or 0),
            topics_removed=int(payload.get("topics_removed", 0) or 0),
            queues_added=int(payload.get("queues_added", 0) or 0),
            queues_removed=int(payload.get("queues_removed", 0) or 0),
            services_added=int(payload.get("services_added", 0) or 0),
            services_removed=int(payload.get("services_removed", 0) or 0),
            caches_added=int(payload.get("caches_added", 0) or 0),
            caches_removed=int(payload.get("caches_removed", 0) or 0),
            cache_profiles_added=int(payload.get("cache_profiles_added", 0) or 0),
            cache_profiles_removed=int(payload.get("cache_profiles_removed", 0) or 0),
            nodes_added=int(payload.get("nodes_added", 0) or 0),
            nodes_removed=int(payload.get("nodes_removed", 0) or 0),
        )

    def to_dict(self) -> Payload:
        # Flat int counters — never route through payload_from_dataclass.
        return {
            "functions_added": self.functions_added,
            "functions_removed": self.functions_removed,
            "topics_added": self.topics_added,
            "topics_removed": self.topics_removed,
            "queues_added": self.queues_added,
            "queues_removed": self.queues_removed,
            "services_added": self.services_added,
            "services_removed": self.services_removed,
            "caches_added": self.caches_added,
            "caches_removed": self.caches_removed,
            "cache_profiles_added": self.cache_profiles_added,
            "cache_profiles_removed": self.cache_profiles_removed,
            "nodes_added": self.nodes_added,
            "nodes_removed": self.nodes_removed,
        }

@dataclass(frozen=True, slots=True)
class DiscoveryDeltaMessage:
    """Discovery delta message published to the catalog watch topic."""

    delta: RoutingCatalogDelta
    counts: CatalogDeltaCounts
    namespaces: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    source_node: NodeId = ""
    source_cluster: ClusterId = ""
    published_at: Timestamp = field(
        default_factory=time.time, metadata={PAYLOAD_FLOAT: True}
    )

    def to_dict(self) -> Payload:
        """Single-pass wire dict.

        Must not use ``payload_from_dataclass`` here: that path finds
        ``delta.to_dict()`` then re-walks the entire nested result through
        ``payload_mapping`` / ``payload_to_dict``, doubling CPU and peak
        memory on every gossip catalog apply (live-profiled under 50-node
        multi-hub discovery at ~99% CPU / multi-GB RSS).
        """
        return {
            "delta": self.delta.to_dict(),
            "counts": self.counts.to_dict(),
            "namespaces": list(self.namespaces),
            "source_node": self.source_node,
            "source_cluster": self.source_cluster,
            "published_at": float(self.published_at),
        }
