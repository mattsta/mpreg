from __future__ import annotations

import time
from dataclasses import dataclass, field

from mpreg.core.payloads import (
    PAYLOAD_FLOAT,
    PAYLOAD_KEEP_EMPTY,
    PAYLOAD_LIST,
    Payload,
    payload_from_dataclass,
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
        return payload_from_dataclass(self)


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
        return payload_from_dataclass(self)
