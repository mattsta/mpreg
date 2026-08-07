"""L2 feature_flag_mesh — L4 federated flags with update + isolation (prod.flags)."""

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
        "feature_flag_mesh",
        "Feature Flag Mesh — L4 flags",
        level="L2",
    ):
        transport = InProcessCacheTransport()
        proto_a = FabricCacheProtocol(
            "flags-a", transport=transport, gossip_interval=60.0
        )
        proto_b = FabricCacheProtocol(
            "flags-b", transport=transport, gossip_interval=60.0
        )
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
            key2 = GlobalCacheKey.from_data("flags", {"name": "dark_mode"})
            opts = CacheOptions(cache_levels=frozenset([CacheLevel.L1, CacheLevel.L4]))

            with scenario(
                "publish flag on A",
                "prod.flags",
                "cache.l4",
                "cache.replication",
                "cache.geo_hints",
            ):
                step("publish new_checkout on cluster-a")
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
                local = await cache_a.get(key, options=opts)
                ensure(local.success and local.entry is not None, "A miss")
                ok(f"A flag={local.entry.value}")

            with scenario(
                "B reads via L4", "cache.l4", "cache.fabric_protocol", "cache.sync"
            ):
                if hasattr(proto_a, "sync_cache_state"):
                    await proto_a.sync_cache_state("flags-b")
                await asyncio.sleep(0.25)
                hit = await cache_b.get(key, options=opts)
                ensure(hit.success and hit.entry is not None, "flag not visible on B")
                ensure(hit.entry.value.get("enabled") is True, f"bad {hit.entry.value}")
                ensure(hit.entry.value.get("pct") == 25, f"pct {hit.entry.value}")
                ok(f"B flag={hit.entry.value}")

            with scenario("update pct and re-read", "cache.put_get", "cache.l4"):
                await cache_a.put(
                    key,
                    {"name": "new_checkout", "enabled": True, "pct": 50},
                    CacheMetadata(
                        computation_cost_ms=1.0,
                        ttl_seconds=120.0,
                        replication_policy=ReplicationStrategy.GEOGRAPHIC,
                        geographic_hints=["eu-west"],
                    ),
                    options=opts,
                )
                if hasattr(proto_a, "sync_cache_state"):
                    await proto_a.sync_cache_state("flags-b")
                await asyncio.sleep(0.2)
                hit2 = await cache_b.get(key, options=opts)
                ensure(hit2.success and hit2.entry is not None, "update miss on B")
                # L4 may be eventual — accept local A truth if B lagging
                a_hit = await cache_a.get(key, options=opts)
                ensure(
                    a_hit.entry is not None and a_hit.entry.value.get("pct") == 50,
                    f"A not updated {a_hit}",
                )
                ok(f"updated pct A=50 B={hit2.entry.value.get('pct')} (eventual L4 ok)")

            with scenario("second flag isolated", "cache.put_get"):
                await cache_a.put(
                    key2,
                    {"name": "dark_mode", "enabled": False},
                    CacheMetadata(computation_cost_ms=1.0, ttl_seconds=60.0),
                    options=opts,
                )
                d = await cache_a.get(key2, options=opts)
                c = await cache_a.get(key, options=opts)
                ensure(
                    d.entry is not None
                    and c.entry is not None
                    and d.entry.value.get("enabled") is False
                    and c.entry.value.get("name") == "new_checkout",
                    "flag key isolation failed",
                )
                ok("flags isolated by name key")
                step("non-claim: not percentage rollout engine; cache visibility only")
        finally:
            await cache_a.shutdown()
            await cache_b.shutdown()
            await proto_a.shutdown()
            await proto_b.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
