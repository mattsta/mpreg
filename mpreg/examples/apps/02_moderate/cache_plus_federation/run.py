"""L2 cache_plus_federation — L4 federated cache across fabric nodes."""

from __future__ import annotations

import asyncio

from mpreg.core.global_cache import (
    CacheLevel,
    CacheMetadata,
    CacheOptions,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
    ReplicationStrategy,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport

async def main() -> None:
    with app_run(
        "cache_plus_federation",
        "Cache + Federation L4",
        level="L2",
    ):
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
                enable_l3_distributed=False,
                enable_l4_federation=True,
                local_cluster_id="cluster-a",
            ),
            cache_protocol=protocol_a,
        )
        cache_b = GlobalCacheManager(
            GlobalCacheConfiguration(
                enable_l2_persistent=False,
                enable_l3_distributed=False,
                enable_l4_federation=True,
                local_cluster_id="cluster-b",
            ),
            cache_protocol=protocol_b,
        )
        try:
            key = GlobalCacheKey.from_data("federated.cache", {"order": "a-100"})
            options = CacheOptions(
                cache_levels=frozenset([CacheLevel.L1, CacheLevel.L4])
            )

            with scenario(
                "put on A with L4 + geo replication metadata",
                "cache.l4",
                "cache.fabric_protocol",
                "cache.replication",
                "cache.geo_hints",
            ):
                await cache_a.put(
                    key,
                    {"status": "ready", "order": "a-100"},
                    CacheMetadata(
                        computation_cost_ms=5.0,
                        ttl_seconds=120.0,
                        replication_policy=ReplicationStrategy.GEOGRAPHIC,
                        geographic_hints=["eu-west"],
                    ),
                    options=options,
                )
                local = await cache_a.get(key, options=options)
                ensure(local.success and local.entry is not None, "writer miss")
                ok(f"A local status={local.entry.value.get('status')}")

            with scenario("B fetches via L4 federation", "cache.l4", "cache.sync"):
                # Give fabric gossip a beat; also try explicit sync when available
                if hasattr(protocol_a, "sync_cache_state"):
                    await protocol_a.sync_cache_state("node-b")
                await asyncio.sleep(0.25)
                result = await cache_b.get(key, options=options)
                ensure(result.success, f"L4 fetch failed: {result}")
                if result.entry is not None:
                    ensure(
                        result.entry.value.get("status") == "ready",
                        f"bad federated value {result.entry.value}",
                    )
                ok(f"B L4 hit success={result.success}")

            with scenario("unrelated key remains cold on B", "cache.put_get"):
                other = GlobalCacheKey.from_data(
                    "federated.cache", {"order": "missing"}
                )
                miss = await cache_b.get(other, options=options)
                ensure(
                    (not miss.success) or miss.entry is None,
                    f"expected miss got {miss}",
                )
                # Writer still has original
                still_a = await cache_a.get(key, options=options)
                ensure(
                    still_a.success
                    and still_a.entry is not None
                    and still_a.entry.value.get("status") == "ready",
                    f"writer lost key after federation {still_a}",
                )
                ok("cold key isolated; writer still hot")
                step("non-claim: not multi-master conflict resolution demo")
        finally:
            await cache_a.shutdown()
            await cache_b.shutdown()
            await protocol_a.shutdown()
            await protocol_b.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
