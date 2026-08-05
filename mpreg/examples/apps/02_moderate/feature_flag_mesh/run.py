"""L2 feature_flag_mesh — flags in cache with federated L4 visibility."""

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
from mpreg.examples.apps._shared.runtime import ensure, ok, step
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport

async def main() -> None:
    transport = InProcessCacheTransport()
    proto_a = FabricCacheProtocol("flags-a", transport=transport, gossip_interval=60.0)
    proto_b = FabricCacheProtocol("flags-b", transport=transport, gossip_interval=60.0)
    cache_a = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=True,
            local_cluster_id="cluster-a",
        ),
        cache_protocol=proto_a,
    )
    cache_b = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=True,
            local_cluster_id="cluster-b",
        ),
        cache_protocol=proto_b,
    )
    try:
        key = GlobalCacheKey.from_data("flags", {"name": "new_checkout"})
        opts = CacheOptions(cache_levels=frozenset([CacheLevel.L1, CacheLevel.L4]))
        step("publish flag on cluster-a")
        await cache_a.put(
            key,
            {"name": "new_checkout", "enabled": True, "pct": 25},
            CacheMetadata(
                computation_cost_ms=1.0,
                ttl_seconds=120.0,
                replication_policy=ReplicationStrategy.GEOGRAPHIC,
                geographic_hints=["eu-west"],
            ),
            options=opts,
        )
        await asyncio.sleep(0.25)
        step("read flag on cluster-b via L4")
        hit = await cache_b.get(key, options=opts)
        ensure(hit.success and hit.entry is not None, "flag not visible on B")
        ensure(hit.entry.value.get("enabled") is True, f"bad flag {hit.entry.value}")
        ok(f"feature flag mesh value={hit.entry.value}")
    finally:
        await cache_a.shutdown()
        await cache_b.shutdown()
        await proto_a.shutdown()
        await proto_b.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
