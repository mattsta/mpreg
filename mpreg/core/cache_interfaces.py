"""Protocols for cache manager interoperability."""

from __future__ import annotations

from typing import Any, Protocol

from .cache_models import (
    CacheMetadata,
    CacheOperationResult,
    CacheOptions,
    GlobalCacheKey,
)


class CacheManagerProtocol(Protocol):
    async def get(
        self, key: GlobalCacheKey, options: CacheOptions | None = None
    ) -> CacheOperationResult: ...

    async def put(
        self,
        key: GlobalCacheKey,
        value: Any,
        metadata: CacheMetadata | None = None,
        options: CacheOptions | None = None,
    ) -> CacheOperationResult: ...

    async def delete(
        self, key: GlobalCacheKey, options: CacheOptions | None = None
    ) -> CacheOperationResult: ...

    async def invalidate(
        self, pattern: str, options: CacheOptions | None = None
    ) -> CacheOperationResult: ...

    def get_namespace_keys(
        self, namespace: str, pattern: str | None = None
    ) -> list[GlobalCacheKey]: ...
