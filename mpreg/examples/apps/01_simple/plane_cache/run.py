"""L1 plane_cache — L1/L3/L4 + fabric sync + geo hints (cache.* tour)."""

from __future__ import annotations

import asyncio

from mpreg.core.global_cache import (
    CacheLevel,
    CacheMetadata,
    CacheOptions,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport

async def main() -> None:
    with app_run("plane_cache", "Plane Cache — L1/L3/L4 federation tour", level="L1"):
        transport = InProcessCacheTransport()
        protocol_a = FabricCacheProtocol(
            "node-a", transport=transport, gossip_interval=60.0
        )
        protocol_b = FabricCacheProtocol(
            "node-b", transport=transport, gossip_interval=60.0
        )
        cache_a = GlobalCacheManager(
            GlobalCacheConfiguration(
                enable_l2_persistent=False,
                enable_l3_distributed=True,
                enable_l4_federation=True,
                local_cluster_id="cluster-a",
                local_region="us-west",
            ),
            cache_protocol=protocol_a,
        )
        cache_b = GlobalCacheManager(
            GlobalCacheConfiguration(
                enable_l2_persistent=False,
                enable_l3_distributed=True,
                enable_l4_federation=True,
                local_cluster_id="cluster-b",
                local_region="eu-west",
            ),
            cache_protocol=protocol_b,
        )
        try:
            key = GlobalCacheKey.from_data("plane.cache", {"customer": "alice"})
            metadata = CacheMetadata(
                computation_cost_ms=250.0,
                ttl_seconds=60.0,
                geographic_hints=["eu-west"],
            )

            with scenario("L1 put on node-a", "cache.put_get", "cache.l1", "cache.ttl", "cache.geo_hints"):
                await cache_a.put(key, {"payload": "cached-value"}, metadata)
                local = await cache_a.get(key)
                ensure(local.success and local.entry is not None, "L1 miss on writer")
                ensure(
                    local.entry.value.get("payload") == "cached-value",
                    f"bad L1 value {local.entry.value}",
                )
                ok("L1 writer hit")

            with scenario("fabric sync + L3 fetch", "cache.sync", "cache.l3", "cache.fabric_protocol"):
                await protocol_a.sync_cache_state("node-b")
                await asyncio.sleep(0.15)
                l3 = await cache_b.get(
                    key, CacheOptions(cache_levels=frozenset([CacheLevel.L3]))
                )
                ensure(l3.success, f"L3 fetch failed: {l3}")
                ok(f"L3 success={l3.success}")

            with scenario("L4 federation-scope fetch", "cache.l4"):
                l4 = await cache_b.get(
                    key, CacheOptions(cache_levels=frozenset([CacheLevel.L4]))
                )
                ensure(l4.success, f"L4 fetch failed: {l4}")
                ok(f"L4 success={l4.success}")

            with scenario("second key isolation", "cache.put_get"):
                other = GlobalCacheKey.from_data("plane.cache", {"customer": "bob"})
                await cache_a.put(
                    other,
                    {"payload": "bob"},
                    CacheMetadata(computation_cost_ms=1.0, ttl_seconds=30.0),
                )
                a = await cache_a.get(key)
                b = await cache_a.get(other)
                ensure(
                    a.entry is not None
                    and b.entry is not None
                    and a.entry.value.get("payload") == "cached-value"
                    and b.entry.value.get("payload") == "bob",
                    "key isolation failed",
                )
                ok("customer keys isolated")
        finally:
            await cache_a.shutdown()
            await cache_b.shutdown()
            await protocol_a.shutdown()
            await protocol_b.shutdown()
        step("plane_cache tour complete")

if __name__ == "__main__":
    asyncio.run(main())
