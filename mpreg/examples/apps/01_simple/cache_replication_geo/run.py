"""L1 cache_replication_geo — geo hints, replication, invalidate, L2/pers mode."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

from mpreg.core.cache_models import CacheMetadata, ReplicationStrategy
from mpreg.core.global_cache import (
    CacheReplicationPolicy,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.core.persistence.config import PersistenceConfig, PersistenceMode
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

async def main() -> None:
    with app_run(
        "cache_replication_geo",
        "Cache Replication + Geo + L2 + Invalidate",
        level="L1",
    ):
        with scenario(
            "replication policy + geo hints types",
            "cache.replication",
            "cache.geo_hints",
        ):
            policy = CacheReplicationPolicy(
                strategy=ReplicationStrategy.GEOGRAPHIC,
                min_replicas=2,
                max_replicas=4,
                preferred_regions=["us-west", "eu-west"],
            )
            ensure(policy.strategy is ReplicationStrategy.GEOGRAPHIC, "strategy")
            meta = CacheMetadata(
                replication_policy=ReplicationStrategy.PROXIMITY,
                geographic_hints=["us-west", "us-east"],
                created_by="cache_replication_geo",
            )
            ensure(meta.geographic_hints == ["us-west", "us-east"], "hints")
            ensure(
                meta.replication_policy is ReplicationStrategy.PROXIMITY,
                "meta policy",
            )
            # Enum surface for all strategies
            for s in ReplicationStrategy:
                ensure(isinstance(s.value, str), f"bad {s}")
            ok(
                f"policy regions={policy.preferred_regions} "
                f"hints={meta.geographic_hints}"
            )

        with scenario(
            "persistence mode config",
            "pers.mode",
            "pers.cache_l2",
        ):
            mem = PersistenceConfig(mode=PersistenceMode.MEMORY)
            ensure(mem.mode is PersistenceMode.MEMORY, "memory mode")
            with tempfile.TemporaryDirectory() as td:
                sql = PersistenceConfig(
                    mode=PersistenceMode.SQLITE,
                    data_dir=Path(td),
                    sqlite_filename="cache-lab.sqlite",
                )
                ensure(sql.mode is PersistenceMode.SQLITE, "sqlite mode")
                ensure(
                    sql.sqlite_path().name == "cache-lab.sqlite",
                    f"path {sql.sqlite_path()}",
                )
            ok("PersistenceMode MEMORY + SQLITE")

        with tempfile.TemporaryDirectory() as td:
            cfg = GlobalCacheConfiguration(
                enable_l2_persistent=True,
                enable_l3_distributed=False,
                enable_l4_federation=False,
                persistent_cache_dir=Path(td) / "l2",
                persistent_cache_size_mb=32,
                default_replication_policy=CacheReplicationPolicy(
                    strategy=ReplicationStrategy.GEOGRAPHIC,
                    preferred_regions=["lab"],
                ),
                local_cluster_id="lab-cluster",
                local_region="lab",
            )
            cache = GlobalCacheManager(cfg)
            try:
                with scenario(
                    "L2-enabled put/get",
                    "cache.l2",
                    "cache.put_get",
                    "pers.cache_l2",
                ):
                    ensure(cfg.enable_l2_persistent is True, "l2 flag")
                    key = GlobalCacheKey(namespace="geo", identifier="widget-1")
                    put = await cache.put(
                        key,
                        {"sku": "w1", "region": "us-west"},
                        metadata=CacheMetadata(
                            geographic_hints=["us-west"],
                            replication_policy=ReplicationStrategy.GEOGRAPHIC,
                        ),
                    )
                    ensure(put.success, f"put failed: {put.error_message}")
                    got = await cache.get(key)
                    ensure(got.success, f"get failed: {got.error_message}")
                    ensure(got.entry is not None, "get entry None")
                    val = got.entry.value
                    ensure(
                        isinstance(val, dict) and val.get("sku") == "w1",
                        f"value {val!r}",
                    )
                    ok(f"L2 path put/get value={val}")

                with scenario(
                    "pattern invalidate",
                    "cache.invalidate",
                ):
                    key2 = GlobalCacheKey(namespace="geo", identifier="widget-2")
                    await cache.put(key2, {"sku": "w2"})
                    inv = await cache.invalidate("geo")
                    ensure(inv.success, f"invalidate failed: {inv.error_message}")
                    # After namespace invalidate, entries should miss or count > 0
                    step(
                        f"invalidate success count="
                        f"{getattr(inv, 'invalidated_count', getattr(inv, 'value', '?'))}"
                    )
                    ok("invalidate pattern accepted")

                with scenario(
                    "invalidate bad kwargs fail closed (F8)",
                    "cache.invalidate",
                ):
                    raised = False
                    try:
                        await cache.invalidate("geo", namespace="nope")  # type: ignore[call-arg]
                    except TypeError as exc:
                        raised = True
                        step(f"TypeError: {exc}")
                    ensure(raised, "expected TypeError on bad kwargs")
                    ok("invalidate keyword contract")
            finally:
                shutdown = getattr(cache, "shutdown", None) or getattr(
                    cache, "close", None
                )
                if callable(shutdown):
                    res = shutdown()
                    if asyncio.iscoroutine(res):
                        await res

        ok("cache_replication_geo complete")

if __name__ == "__main__":
    asyncio.run(main())
