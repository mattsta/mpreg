"""Persistence helpers for fabric metadata snapshots."""

from __future__ import annotations

import json
from dataclasses import dataclass

from loguru import logger

from mpreg.core.persistence.kv_store import KeyValueStore
from mpreg.datastructures.type_aliases import JsonDict

from .catalog import RoutingCatalog
from .route_keys import RouteKeyRegistry

CATALOG_SNAPSHOT_KEY = "fabric.catalog.snapshot"
ROUTE_KEYS_SNAPSHOT_KEY = "fabric.route_keys.snapshot"


@dataclass(slots=True)
class FabricSnapshotStore:
    kv_store: KeyValueStore
    catalog_key: str = CATALOG_SNAPSHOT_KEY
    route_keys_key: str = ROUTE_KEYS_SNAPSHOT_KEY

    async def load_catalog(
        self, catalog: RoutingCatalog, *, now: float | None = None
    ) -> dict[str, int]:
        payload = await self._load_payload(self.catalog_key)
        if payload is None:
            return {}
        return catalog.load_from_dict(payload, now=now)

    async def save_catalog(
        self, catalog: RoutingCatalog, *, now: float | None = None
    ) -> None:
        payload = catalog.to_dict(now=now)
        await self._save_payload(self.catalog_key, payload)

    async def load_route_keys(
        self, registry: RouteKeyRegistry, *, now: float | None = None
    ) -> int:
        payload = await self._load_payload(self.route_keys_key)
        if payload is None:
            return 0
        return registry.load_from_dict(payload, now=now)

    async def save_route_keys(
        self, registry: RouteKeyRegistry, *, now: float | None = None
    ) -> None:
        payload = registry.to_dict(now=now)
        await self._save_payload(self.route_keys_key, payload)

    async def _load_payload(self, key: str) -> JsonDict | None:
        raw = await self.kv_store.get(key)
        if raw is None:
            return None
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            logger.warning("Fabric snapshot decode failed for {}: {}", key, exc)
            return None
        if not isinstance(payload, dict):
            logger.warning("Fabric snapshot payload invalid for {}", key)
            return None
        return payload

    async def _save_payload(self, key: str, payload: JsonDict) -> None:
        data = json.dumps(payload, sort_keys=True).encode("utf-8")
        await self.kv_store.put(key, data)
