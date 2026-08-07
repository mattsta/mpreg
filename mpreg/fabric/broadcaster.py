"""Broadcast routing catalog deltas across the gossip fabric."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - type-only import
    from mpreg.fabric.gossip import GossipMessage

from .catalog import RoutingCatalog
from .catalog_delta import RoutingCatalogApplier, RoutingCatalogDelta
from .catalog_publisher import CatalogDeltaPublisher


@dataclass(frozen=True, slots=True)
class CatalogBroadcastResult:
    counts: dict[str, int]
    message: GossipMessage


@dataclass(slots=True)
class CatalogBroadcaster:
    catalog: RoutingCatalog
    applier: RoutingCatalogApplier
    publisher: CatalogDeltaPublisher

    async def broadcast(
        self, delta: RoutingCatalogDelta, *, now: float | None = None
    ) -> CatalogBroadcastResult:
        counts = self.applier.apply(delta, now=now)
        message = await self.publisher.publish(delta)
        return CatalogBroadcastResult(counts=counts, message=message)
