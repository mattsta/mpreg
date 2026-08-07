"""L3 multi_region_shop — two-cluster fabric shop path (prod.shop)."""

from __future__ import annotations

import asyncio
import time

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
    wait_until,
)
from mpreg.fabric.federation_config import create_permissive_bridging_config
from mpreg.server import MPREGServer


async def main() -> None:
    with app_run(
        "multi_region_shop",
        "Multi-Region Shop — west inventory + east checkout",
        level="L3",
    ):
        config_west = create_permissive_bridging_config("region-west")
        config_east = create_permissive_bridging_config("region-east")

        with port_range_context(2, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Shop-West",
                    resources={"west", "inventory"},
                    cluster_id="region-west",
                    federation_config=config_west,
                    log_level="WARNING",
                ),
                MPREGSettings(
                    port=ports[1],
                    name="Shop-East",
                    resources={"east", "checkout"},
                    cluster_id="region-east",
                    peers=[f"ws://127.0.0.1:{ports[0]}"],
                    federation_config=config_east,
                    log_level="WARNING",
                ),
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                west, east = servers

                def inventory_lookup(sku: str) -> dict[str, object]:
                    stock = {"SKU-42": 10, "SKU-7": 3}
                    return {
                        "sku": sku,
                        "region": "west",
                        "qty": stock.get(sku, 0),
                        "found": sku in stock,
                    }

                def checkout_quote(sku: str, qty: int) -> dict[str, object]:
                    return {
                        "sku": sku,
                        "qty": qty,
                        "region": "east",
                        "total_cents": int(qty) * 499,
                    }

                west.register_command(
                    "inventory_lookup", inventory_lookup, ["west", "inventory"]
                )
                east.register_command(
                    "checkout_quote", checkout_quote, ["east", "checkout"]
                )
                step("waiting for fabric function visibility")

                async def _fabric_ready() -> bool:
                    peers = west.cluster.peer_urls_for_cluster("region-east")
                    return bool(peers) or "checkout_quote" in getattr(
                        west.cluster, "funtimes", {}
                    )

                try:
                    await wait_until(
                        _fabric_ready,
                        timeout_s=10.0,
                        what="fabric peer/function visibility",
                    )
                except Exception:
                    await asyncio.sleep(0.5)

                hub = f"ws://127.0.0.1:{ports[0]}"

                with scenario(
                    "cross-region inventory + quote DAG",
                    "prod.shop",
                    "fabric.permissive",
                    "fabric.cross_rpc",
                    "fabric.cluster_id",
                    "rpc.dag",
                    "rpc.locs",
                ):
                    async with MPREGClientAPI(hub) as client:
                        result = await client.request(
                            [
                                RPCCommand(
                                    name="inv",
                                    fun="inventory_lookup",
                                    args=("SKU-42",),
                                    locs=frozenset(["west", "inventory"]),
                                ),
                                RPCCommand(
                                    name="quote",
                                    fun="checkout_quote",
                                    args=("SKU-42", 2),
                                    locs=frozenset(["east", "checkout"]),
                                ),
                            ]
                        )
                    ensure(isinstance(result, dict), "expected dict result")
                    ensure(
                        "inv" in result and "quote" in result, f"missing keys {result}"
                    )
                    inv, quote = result["inv"], result["quote"]
                    ensure(
                        isinstance(inv, dict)
                        and inv.get("region") == "west"
                        and inv.get("qty") == 10,
                        f"bad inv {inv}",
                    )
                    ensure(
                        isinstance(quote, dict)
                        and quote.get("region") == "east"
                        and quote.get("total_cents") == 998,
                        f"bad quote {quote}",
                    )
                    ok(f"inv={inv} quote={quote}")

                with scenario("unknown SKU inventory", "rpc.call", "rpc.locs"):
                    async with MPREGClientAPI(hub) as client:
                        miss = await client.call(
                            "inventory_lookup",
                            "SKU-MISSING",
                            locs=frozenset(["west", "inventory"]),
                        )
                    ensure(
                        isinstance(miss, dict)
                        and miss.get("found") is False
                        and miss.get("qty") == 0,
                        f"miss inv {miss}",
                    )
                    ok(f"missing sku={miss}")

                with scenario("local-only west call", "rpc.call"):
                    async with MPREGClientAPI(hub) as client:
                        only = await client.call(
                            "inventory_lookup",
                            "SKU-7",
                            locs=frozenset(["west", "inventory"]),
                        )
                    ensure(
                        only.get("qty") == 3 and only.get("found") is True, f"{only}"
                    )
                    ok(f"west-only SKU-7={only}")
                    step(
                        "non-claim: routing availability ≠ global linearizability "
                        f"(t={time.time():.0f})"
                    )

            await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())
