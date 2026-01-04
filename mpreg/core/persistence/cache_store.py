from __future__ import annotations

from dataclasses import dataclass

from mpreg.core.cache_models import GlobalCacheEntry, GlobalCacheKey
from mpreg.core.cache_protocol import CacheEntryMessage
from mpreg.core.serialization import JsonSerializer

from .kv_store import KeyValueStore


@dataclass(slots=True)
class CacheL2Store:
    """L2 cache persistence adapter backed by a KeyValueStore."""

    store: KeyValueStore
    serializer: JsonSerializer

    async def get(self, key: GlobalCacheKey) -> GlobalCacheEntry | None:
        raw = await self.store.get(str(key))
        if raw is None:
            return None
        payload = self.serializer.deserialize(raw)
        entry = CacheEntryMessage.from_dict(payload).to_global_cache_entry()
        if entry.is_expired():
            await self.delete(key)
            return None
        return entry

    async def put(self, entry: GlobalCacheEntry) -> None:
        expires_at: float | None = None
        if entry.metadata.ttl_seconds is not None:
            expires_at = entry.creation_time + entry.metadata.ttl_seconds
        payload = CacheEntryMessage.from_global_cache_entry(entry).to_dict()
        await self.store.put(
            str(entry.key), self.serializer.serialize(payload), expires_at=expires_at
        )

    async def delete(self, key: GlobalCacheKey) -> bool:
        await self.store.delete(str(key))
        return True
