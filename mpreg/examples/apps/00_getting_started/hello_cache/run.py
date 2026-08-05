"""L0 hello_cache — put/get, miss, overwrite, TTL metadata (cache.*)."""

from __future__ import annotations

import asyncio

from mpreg.core.global_cache import (
    CacheMetadata,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport

async def main() -> None:
    with app_run("hello_cache", "Hello Cache — L1 put/get drill-down", level="L0"):
        transport = InProcessCacheTransport()
        protocol = FabricCacheProtocol(
            "hello-cache", transport=transport, gossip_interval=60.0
        )
        cache = GlobalCacheManager(
            GlobalCacheConfiguration(
                enable_l2_persistent=False,
                enable_l3_distributed=False,
                enable_l4_federation=False,
            ),
            cache_protocol=protocol,
        )
        try:
            key = GlobalCacheKey.from_data("hello.cache", {"user": "alice"})
            missing_key = GlobalCacheKey.from_data("hello.cache", {"user": "nobody"})

            with scenario("put + get happy path", "cache.put_get", "cache.l1", "cache.ttl"):
                step("put session payload with TTL metadata")
                await cache.put(
                    key,
                    {"session": "s-1", "user": "alice"},
                    CacheMetadata(computation_cost_ms=1.0, ttl_seconds=60.0),
                )
                got = await cache.get(key)
                ensure(got.success and got.entry is not None, "cache miss after put")
                ensure(
                    got.entry.value.get("session") == "s-1",
                    f"bad value {got.entry.value}",
                )
                ok(f"cache hit value={got.entry.value}")

            with scenario("cold miss", "cache.put_get"):
                miss = await cache.get(missing_key)
                ensure(
                    (not miss.success) or miss.entry is None,
                    f"expected miss for nobody, got {miss!r}",
                )
                ok("cold key correctly missed")

            with scenario("overwrite same key", "cache.put_get"):
                await cache.put(
                    key,
                    {"session": "s-2", "user": "alice", "rev": 2},
                    CacheMetadata(computation_cost_ms=2.0, ttl_seconds=120.0),
                )
                got2 = await cache.get(key)
                ensure(got2.success and got2.entry is not None, "miss after overwrite")
                ensure(
                    got2.entry.value.get("session") == "s-2"
                    and got2.entry.value.get("rev") == 2,
                    f"overwrite failed {got2.entry.value}",
                )
                ok(f"overwrite value={got2.entry.value}")

            with scenario("namespace-shaped key identity", "cache.put_get"):
                other = GlobalCacheKey.from_data("hello.cache", {"user": "bob"})
                await cache.put(
                    other,
                    {"session": "bob-1"},
                    CacheMetadata(computation_cost_ms=1.0, ttl_seconds=30.0),
                )
                a = await cache.get(key)
                b = await cache.get(other)
                ensure(
                    a.entry is not None
                    and b.entry is not None
                    and a.entry.value.get("user") == "alice"
                    and b.entry.value.get("session") == "bob-1",
                    "key identity collision",
                )
                ok("distinct keys isolated")
        finally:
            await cache.shutdown()
            await protocol.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
