"""High-level announcers for fabric catalog updates."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Protocol

from mpreg.datastructures.type_aliases import Timestamp

from .adapters.cache_federation import CacheFederationCatalogAdapter
from .adapters.cache_profile import CacheProfileCatalogAdapter
from .adapters.topic_exchange import TopicExchangeCatalogAdapter
from .broadcaster import CatalogBroadcaster, CatalogBroadcastResult
from .catalog import QueueEndpoint
from .catalog_delta import RoutingCatalogDelta


class FunctionCatalogAdapter(Protocol):
    def build_delta(
        self,
        *,
        now: Timestamp | None = None,
        include_node: bool = True,
        update_id: str | None = None,
    ) -> RoutingCatalogDelta: ...


class ServiceCatalogAdapter(Protocol):
    def build_delta(
        self,
        *,
        now: Timestamp | None = None,
        include_node: bool = True,
        update_id: str | None = None,
    ) -> RoutingCatalogDelta: ...


@dataclass(slots=True)
class FabricFunctionAnnouncer:
    adapter: FunctionCatalogAdapter
    broadcaster: CatalogBroadcaster

    async def announce(self, *, now: float | None = None) -> CatalogBroadcastResult:
        delta = self.adapter.build_delta(now=now, include_node=True)
        return await self.broadcaster.broadcast(delta, now=now)


@dataclass(slots=True)
class FabricQueueAnnouncer:
    broadcaster: CatalogBroadcaster

    async def advertise(
        self, endpoint: QueueEndpoint, *, now: float | None = None
    ) -> CatalogBroadcastResult:
        delta = RoutingCatalogDelta(
            update_id=str(uuid.uuid4()),
            cluster_id=endpoint.cluster_id,
            sent_at=now if now is not None else time.time(),
            queues=(endpoint,),
        )
        return await self.broadcaster.broadcast(delta, now=now)


@dataclass(slots=True)
class FabricServiceAnnouncer:
    adapter: ServiceCatalogAdapter
    broadcaster: CatalogBroadcaster

    async def announce(self, *, now: float | None = None) -> CatalogBroadcastResult:
        delta = self.adapter.build_delta(now=now, include_node=True)
        return await self.broadcaster.broadcast(delta, now=now)


@dataclass(slots=True)
class FabricCacheRoleAnnouncer:
    adapter: CacheFederationCatalogAdapter
    broadcaster: CatalogBroadcaster

    async def announce(self, *, now: float | None = None) -> CatalogBroadcastResult:
        delta = self.adapter.build_delta(now=now)
        return await self.broadcaster.broadcast(delta, now=now)


@dataclass(slots=True)
class FabricCacheProfileAnnouncer:
    adapter: CacheProfileCatalogAdapter
    broadcaster: CatalogBroadcaster

    async def announce(self, *, now: float | None = None) -> CatalogBroadcastResult:
        delta = self.adapter.build_delta(now=now)
        return await self.broadcaster.broadcast(delta, now=now)


@dataclass(slots=True)
class FabricTopicAnnouncer:
    adapter: TopicExchangeCatalogAdapter
    broadcaster: CatalogBroadcaster

    async def announce_subscription(
        self, subscription, *, now: float | None = None
    ) -> CatalogBroadcastResult:
        delta = self.adapter.build_delta(subscription, now=now)
        return await self.broadcaster.broadcast(delta, now=now)

    async def remove_subscription(
        self, subscription_id: str, *, now: float | None = None
    ) -> CatalogBroadcastResult:
        delta = self.adapter.build_removal(subscription_id, now=now)
        return await self.broadcaster.broadcast(delta, now=now)

    async def announce_all(
        self, exchange, *, now: float | None = None
    ) -> CatalogBroadcastResult:
        delta = self.adapter.build_full_delta(exchange, now=now)
        return await self.broadcaster.broadcast(delta, now=now)
