"""L2 rpc_plus_cache — RPC compute cached and reused (integration drill)."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.global_cache import (
    CacheMetadata,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
)
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport
from mpreg.server import MPREGServer

async def main() -> None:
    with app_run("rpc_plus_cache", "RPC + Cache integration", level="L2"):
        with port_range_context(1, "servers") as ports:
            port = ports[0]
            settings = [
                MPREGSettings(
                    port=port,
                    name="RPC-Cache",
                    resources={"compute"},
                    log_level="WARNING",
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                calls = {"n": 0}

                def expensive(x: int) -> dict[str, Any]:
                    calls["n"] += 1
                    time.sleep(0.02)
                    return {"value": x * x, "computed_at": time.time(), "n": calls["n"]}

                servers[0].register_command("expensive", expensive, ["compute"])

                protocol = FabricCacheProtocol(
                    "local", transport=InProcessCacheTransport(), gossip_interval=60.0
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
                    key = GlobalCacheKey.from_function_call(
                        "rpc.cache", "expensive", args=(12,), kwargs={}
                    )
                    hub = f"ws://127.0.0.1:{port}"

                    with scenario("cold miss then RPC fill", "rpc.call", "cache.put_get"):
                        async with MPREGClientAPI(hub) as client:
                            cached = await cache.get(key)
                            ensure(
                                (not cached.success) or cached.entry is None,
                                "expected cold miss",
                            )
                            result = await client.call(
                                "expensive", 12, locs=frozenset(["compute"])
                            )
                            ensure(result.get("value") == 144, f"bad rpc {result}")
                            await cache.put(
                                key,
                                result,
                                CacheMetadata(computation_cost_ms=20.0, ttl_seconds=300.0),
                            )
                        ok(f"filled cache calls={calls['n']}")

                    with scenario("cache hit avoids second RPC", "cache.put_get", "cache.ttl"):
                        before = calls["n"]
                        hit = await cache.get(key)
                        ensure(hit.success and hit.entry is not None, "cache miss")
                        ensure(hit.entry.value.get("value") == 144, f"bad hit {hit.entry.value}")
                        ensure(calls["n"] == before, "RPC should not re-run on cache hit")
                        ok(f"cache hit value={hit.entry.value} rpc_calls={calls['n']}")

                    with scenario("different args distinct key", "rpc.call", "cache.put_get"):
                        key2 = GlobalCacheKey.from_function_call(
                            "rpc.cache", "expensive", args=(5,), kwargs={}
                        )
                        async with MPREGClientAPI(hub) as client:
                            r2 = await client.call(
                                "expensive", 5, locs=frozenset(["compute"])
                            )
                            await cache.put(
                                key2,
                                r2,
                                CacheMetadata(computation_cost_ms=20.0, ttl_seconds=60.0),
                            )
                        h1 = await cache.get(key)
                        h2 = await cache.get(key2)
                        ensure(
                            h1.entry is not None
                            and h2.entry is not None
                            and h1.entry.value.get("value") == 144
                            and h2.entry.value.get("value") == 25,
                            "key isolation failed",
                        )
                        ok(f"distinct keys 144 vs 25; total rpc_calls={calls['n']}")
                        step("non-claim: not distributed invalidation")
                finally:
                    await cache.shutdown()
                    await protocol.shutdown()

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
