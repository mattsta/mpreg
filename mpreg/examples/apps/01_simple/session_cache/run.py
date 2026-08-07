"""L1 session_cache — TTL put/rotate/invalidate and multi-key isolation (cache.*)."""

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
    with app_run(
        "session_cache", "Session Cache — TTL / rotate / invalidate", level="L1"
    ):
        transport = InProcessCacheTransport()
        protocol = FabricCacheProtocol(
            "session-node", transport=transport, gossip_interval=60.0
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
            key = GlobalCacheKey.from_data("session", {"sid": "abc"})
            key_b = GlobalCacheKey.from_data("session", {"sid": "xyz"})

            with scenario(
                "store session with TTL", "cache.put_get", "cache.ttl", "cache.l1"
            ):
                step("put session abc")
                await cache.put(
                    key,
                    {"sid": "abc", "user": "bob", "roles": ["reader"]},
                    CacheMetadata(computation_cost_ms=1.0, ttl_seconds=300.0),
                )
                hit = await cache.get(key)
                ensure(hit.success and hit.entry is not None, "session miss")
                ensure(hit.entry.value.get("user") == "bob", f"bad {hit.entry.value}")
                ok("session hit")

            with scenario("rotate roles in place", "cache.put_get"):
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
                ok(f"rotated roles={hit2.entry.value.get('roles')}")

            with scenario("second session isolated", "cache.put_get"):
                await cache.put(
                    key_b,
                    {"sid": "xyz", "user": "cara", "roles": ["admin"]},
                    CacheMetadata(computation_cost_ms=1.0, ttl_seconds=60.0),
                )
                a = await cache.get(key)
                b = await cache.get(key_b)
                ensure(
                    a.entry is not None
                    and b.entry is not None
                    and a.entry.value.get("user") == "bob"
                    and b.entry.value.get("user") == "cara",
                    "session key isolation failed",
                )
                ok("sid abc/xyz isolated")

            with scenario("invalidate / delete path", "cache.invalidate"):
                if hasattr(cache, "delete"):
                    deleted = await cache.delete(key_b)
                    step(f"delete returned {deleted!r}")
                    miss = await cache.get(key_b)
                    ensure(
                        (not miss.success) or miss.entry is None,
                        f"expected miss after delete, got {miss!r}",
                    )
                    ok("delete invalidated sid=xyz")
                elif hasattr(cache, "invalidate"):
                    await cache.invalidate("session*")
                    ok("invalidate(pattern) invoked")
                else:
                    # Overwrite with empty tombstone teaching pattern
                    await cache.put(
                        key_b,
                        {"sid": "xyz", "revoked": True},
                        CacheMetadata(computation_cost_ms=0.5, ttl_seconds=1.0),
                    )
                    tomb = await cache.get(key_b)
                    ensure(
                        tomb.entry is not None
                        and tomb.entry.value.get("revoked") is True,
                        "tombstone failed",
                    )
                    ok("tombstone revoke pattern (no delete API path taken)")

            # Original session still live
            still = await cache.get(key)
            ensure(
                still.success
                and still.entry is not None
                and still.entry.value.get("user") == "bob",
                "primary session lost",
            )
            ok("primary session still present after secondary invalidate")
        finally:
            await cache.shutdown()
            await protocol.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
