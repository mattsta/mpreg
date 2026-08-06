"""L1 url_shortener_rpc — CRUD RPC + cache mirror + miss path (rpc.* + cache.*)."""

from __future__ import annotations

import asyncio
import hashlib
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
    with app_run(
        "url_shortener_rpc",
        "URL Shortener — RPC CRUD + cache mirror",
        level="L1",
    ):
        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="URL-Shortener",
                    resources={"urls", "api"},
                    log_level="WARNING",
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]
                store: dict[str, str] = {}

                def shorten(url: str) -> dict[str, Any]:
                    code = hashlib.sha256(url.encode()).hexdigest()[:8]
                    store[code] = url
                    return {"code": code, "url": url}

                def resolve(code: str) -> dict[str, Any]:
                    if code not in store:
                        return {"found": False, "code": code}
                    return {"found": True, "code": code, "url": store[code]}

                def list_codes() -> dict[str, Any]:
                    return {"codes": sorted(store.keys()), "count": len(store)}

                server.register_command("shorten", shorten, ["urls", "api"])
                server.register_command("resolve", resolve, ["urls", "api"])
                server.register_command("list_codes", list_codes, ["urls", "api"])

                protocol = FabricCacheProtocol(
                    "url-node",
                    transport=InProcessCacheTransport(),
                    gossip_interval=60.0,
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
                    hub = f"ws://127.0.0.1:{ports[0]}"
                    async with MPREGClientAPI(hub) as client:
                        with scenario(
                            "shorten + resolve happy path", "rpc.call", "rpc.register"
                        ):
                            step("shorten URL")
                            short = await client.call(
                                "shorten",
                                "https://example.com/docs",
                                locs=frozenset(["urls", "api"]),
                            )
                            ensure(
                                isinstance(short, dict) and short.get("code"),
                                f"bad {short}",
                            )
                            code = str(short["code"])
                            resolved = await client.call(
                                "resolve", code, locs=frozenset(["urls", "api"])
                            )
                            ensure(
                                resolved.get("found") is True
                                and resolved.get("url") == "https://example.com/docs",
                                f"resolve failed {resolved}",
                            )
                            ok(f"code={code} resolved")

                        with scenario("resolve miss path", "rpc.call"):
                            miss = await client.call(
                                "resolve",
                                "deadbeef",
                                locs=frozenset(["urls", "api"]),
                            )
                            ensure(
                                miss.get("found") is False
                                and miss.get("code") == "deadbeef",
                                f"expected miss got {miss}",
                            )
                            ok("unknown code returns found=False")

                        with scenario("idempotent shorten same URL", "rpc.call"):
                            again = await client.call(
                                "shorten",
                                "https://example.com/docs",
                                locs=frozenset(["urls", "api"]),
                            )
                            ensure(
                                again.get("code") == code,
                                f"code drift {again.get('code')} vs {code}",
                            )
                            listed = await client.call(
                                "list_codes", locs=frozenset(["urls", "api"])
                            )
                            ensure(
                                isinstance(listed, dict) and listed.get("count") == 1,
                                f"expected 1 code got {listed}",
                            )
                            ok(f"idempotent code + list count={listed.get('count')}")

                        with scenario(
                            "cache mirror of short record", "cache.put_get", "cache.l1"
                        ):
                            key = GlobalCacheKey.from_data("urls.code", {"code": code})
                            await cache.put(
                                key,
                                short,
                                CacheMetadata(
                                    computation_cost_ms=1.0, ttl_seconds=120.0
                                ),
                            )
                            hit = await cache.get(key)
                            ensure(hit.success and hit.entry is not None, "cache miss")
                            ensure(
                                hit.entry.value.get("url")
                                == "https://example.com/docs",
                                f"cache value bad {hit.entry.value}",
                            )
                            ok(f"cache mirror code={code}")

                        with scenario("second URL distinct code", "rpc.call"):
                            other = await client.call(
                                "shorten",
                                "https://example.com/api",
                                locs=frozenset(["urls", "api"]),
                            )
                            ensure(
                                other.get("code") and other.get("code") != code,
                                f"codes should differ: {other} vs {code}",
                            )
                            ok(f"second code={other.get('code')}")
                finally:
                    await cache.shutdown()
                    await protocol.shutdown()

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
