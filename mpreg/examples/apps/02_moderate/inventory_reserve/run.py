"""L2 inventory_reserve — atomic-ish stock reserve via cache + RPC."""

from __future__ import annotations

import asyncio
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
        "inventory_reserve",
        "Inventory Reserve — stock cache + reserve RPC",
        level="L2",
    ):
        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Inventory-API",
                    resources={"inventory", "api"},
                    log_level="WARNING",
                    gossip_interval=30.0,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]
                stock: dict[str, int] = {"SKU-1": 10, "SKU-2": 1}
                reservations: dict[str, dict[str, Any]] = {}

                def available(sku: str) -> dict[str, Any]:
                    return {"sku": sku, "qty": stock.get(sku, 0)}

                def reserve(sku: str, qty: int, hold_id: str) -> dict[str, Any]:
                    if hold_id in reservations:
                        return {**reservations[hold_id], "replay": True}
                    have = stock.get(sku, 0)
                    if have < qty:
                        return {
                            "ok": False,
                            "sku": sku,
                            "requested": qty,
                            "available": have,
                            "replay": False,
                        }
                    stock[sku] = have - qty
                    rec = {
                        "ok": True,
                        "sku": sku,
                        "qty": qty,
                        "hold_id": hold_id,
                        "remaining": stock[sku],
                        "replay": False,
                    }
                    reservations[hold_id] = rec
                    return rec

                def release(hold_id: str) -> dict[str, Any]:
                    rec = reservations.pop(hold_id, None)
                    if rec is None:
                        return {"ok": False, "reason": "unknown_hold"}
                    stock[rec["sku"]] = stock.get(rec["sku"], 0) + int(rec["qty"])
                    return {"ok": True, "released": rec, "stock": stock[rec["sku"]]}

                server.register_command("available", available, ["inventory", "api"])
                server.register_command("reserve", reserve, ["inventory", "api"])
                server.register_command("release", release, ["inventory", "api"])

                transport = InProcessCacheTransport()
                protocol = FabricCacheProtocol(
                    "inv-cache", transport=transport, gossip_interval=60.0
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
                    url = f"ws://127.0.0.1:{ports[0]}"
                    async with MPREGClientAPI(url) as client:
                        with scenario(
                            "seed stock + cache snapshot",
                            "prod.inventory",
                            "cache.put_get",
                            "rpc.call",
                        ):
                            avail = await client.call(
                                "available",
                                "SKU-1",
                                locs=frozenset(["inventory", "api"]),
                            )
                            ensure(avail.get("qty") == 10, f"avail {avail}")
                            key = GlobalCacheKey.from_data(
                                "inv.stock", {"sku": "SKU-1"}
                            )
                            await cache.put(
                                key,
                                avail,
                                CacheMetadata(computation_cost_ms=1.0, ttl_seconds=60.0),
                            )
                            got = await cache.get(key)
                            ensure(
                                got.success and got.entry is not None,
                                "stock cache miss",
                            )
                            ok(f"SKU-1 qty={avail['qty']} cached")

                        with scenario(
                            "reserve reduces stock",
                            "prod.inventory",
                            "rpc.call",
                        ):
                            hold = await client.call(
                                "reserve",
                                "SKU-1",
                                3,
                                "hold-a",
                                locs=frozenset(["inventory", "api"]),
                            )
                            ensure(hold.get("ok") is True, f"reserve {hold}")
                            ensure(hold.get("remaining") == 7, f"remaining {hold}")
                            ok(f"reserved hold-a remaining={hold['remaining']}")

                        with scenario(
                            "idempotent reserve replay",
                            "rpc.call",
                        ):
                            again = await client.call(
                                "reserve",
                                "SKU-1",
                                3,
                                "hold-a",
                                locs=frozenset(["inventory", "api"]),
                            )
                            ensure(again.get("replay") is True, f"replay {again}")
                            ensure(again.get("remaining") == 7, "double-spend on replay")
                            ok("idempotent hold-a")

                        with scenario(
                            "insufficient stock fails closed",
                            "prod.inventory",
                            "rpc.call",
                        ):
                            fail = await client.call(
                                "reserve",
                                "SKU-2",
                                5,
                                "hold-b",
                                locs=frozenset(["inventory", "api"]),
                            )
                            ensure(fail.get("ok") is False, f"should fail {fail}")
                            ensure(fail.get("available") == 1, f"avail {fail}")
                            ok(f"insufficient stock: {fail}")

                        with scenario(
                            "release returns stock + refresh cache",
                            "cache.put_get",
                            "rpc.call",
                        ):
                            rel = await client.call(
                                "release",
                                "hold-a",
                                locs=frozenset(["inventory", "api"]),
                            )
                            ensure(rel.get("ok") is True, f"release {rel}")
                            ensure(rel.get("stock") == 10, f"stock after release {rel}")
                            key = GlobalCacheKey.from_data(
                                "inv.stock", {"sku": "SKU-1"}
                            )
                            await cache.put(
                                key,
                                {"sku": "SKU-1", "qty": rel["stock"]},
                                CacheMetadata(computation_cost_ms=1.0, ttl_seconds=60.0),
                            )
                            got = await cache.get(key)
                            ensure(
                                got.entry is not None
                                and got.entry.value.get("qty") == 10,
                                f"cache refresh {got}",
                            )
                            ok("released hold-a; cache qty=10")
                finally:
                    await cache.shutdown()
                    await protocol.shutdown()

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
