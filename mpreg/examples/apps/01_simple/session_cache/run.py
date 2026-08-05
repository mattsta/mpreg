"""L1 session_cache — TTL-aware session put/get/invalidate."""

from __future__ import annotations

import asyncio

from mpreg.core.global_cache import (
    CacheMetadata,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.examples.apps._shared.runtime import ensure, ok, step
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport

async def main() -> None:
    transport = InProcessCacheTransport()
    protocol = FabricCacheProtocol("session-node", transport=transport, gossip_interval=60.0)
    cache = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        ),
        cache_protocol=protocol,
    )
    try:
        key = GlobalCacheKey.from_data("session", {"sid": "abc"})
        step("store session with TTL")
        await cache.put(
            key,
            {"sid": "abc", "user": "bob", "roles": ["reader"]},
            CacheMetadata(computation_cost_ms=1.0, ttl_seconds=300.0),
        )
        hit = await cache.get(key)
        ensure(hit.success and hit.entry is not None, "session miss")
        ensure(hit.entry.value.get("user") == "bob", f"bad {hit.entry.value}")
        ok("session hit")

        # Overwrite / rotate session
        await cache.put(
            key,
            {"sid": "abc", "user": "bob", "roles": ["reader", "writer"]},
            CacheMetadata(computation_cost_ms=1.0, ttl_seconds=300.0),
        )
        hit2 = await cache.get(key)
        ensure(
            hit2.success
            and hit2.entry is not None
            and hit2.entry.value.get("roles") == ["reader", "writer"],
            "session rotate failed",
        )
        ok(f"session rotated roles={hit2.entry.value.get('roles')}")
    finally:
        await cache.shutdown()
        await protocol.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
