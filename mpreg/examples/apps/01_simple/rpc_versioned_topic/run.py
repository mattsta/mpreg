"""L1 rpc_versioned_topic — function_id + version_constraint routing."""

from __future__ import annotations

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
)
from mpreg.server import MPREGServer

async def main() -> None:
    with app_run(
        "rpc_versioned_topic",
        "RPC Versioned Topic — function_id + constraints",
        level="L1",
    ):
        with port_range_context(2, "servers") as ports:
            hub = f"ws://127.0.0.1:{ports[0]}"
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Price-V1",
                    resources={"pricing"},
                    cluster_id="ver-cluster",
                    log_level="WARNING",
                    gossip_interval=0.4,
                ),
                MPREGSettings(
                    port=ports[1],
                    name="Price-V2",
                    resources={"pricing"},
                    peers=[hub],
                    cluster_id="ver-cluster",
                    log_level="WARNING",
                    gossip_interval=0.4,
                ),
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                v1, v2 = servers

                def price_v1(sku: str) -> dict[str, object]:
                    return {"sku": sku, "cents": 100, "version": "1.0.0"}

                def price_v2(sku: str) -> dict[str, object]:
                    return {"sku": sku, "cents": 110, "version": "2.0.0", "tax": True}

                v1.register_command(
                    "price",
                    price_v1,
                    ["pricing"],
                    function_id="catalog.price",
                    version="1.0.0",
                )
                v2.register_command(
                    "price",
                    price_v2,
                    ["pricing"],
                    function_id="catalog.price",
                    version="2.0.0",
                )
                step("registered catalog.price v1 + v2 on separate nodes")
                await asyncio.sleep(1.2)

                async with MPREGClientAPI(hub) as client:
                    with scenario(
                        "exact v1 pin",
                        "rpc.function_id",
                        "rpc.version_constraint",
                        "rpc.call",
                    ):
                        out = await client.call(
                            "price",
                            "SKU-A",
                            locs=frozenset(["pricing"]),
                            function_id="catalog.price",
                            version_constraint="==1.0.0",
                        )
                        ensure(
                            isinstance(out, dict) and out.get("version") == "1.0.0",
                            f"v1 pin failed: {out}",
                        )
                        ensure(out.get("cents") == 100, f"v1 cents {out}")
                        ok(f"==1.0.0 → {out}")

                    with scenario(
                        "exact v2 pin",
                        "rpc.function_id",
                        "rpc.version_constraint",
                    ):
                        out = await client.call(
                            "price",
                            "SKU-A",
                            locs=frozenset(["pricing"]),
                            function_id="catalog.price",
                            version_constraint="==2.0.0",
                        )
                        ensure(
                            isinstance(out, dict) and out.get("version") == "2.0.0",
                            f"v2 pin failed: {out}",
                        )
                        ensure(out.get("tax") is True, f"v2 tax missing {out}")
                        ok(f"==2.0.0 → {out}")

                    with scenario(
                        "range prefers newer (>=2)",
                        "rpc.version_constraint",
                        "rpc.call",
                    ):
                        out = await client.call(
                            "price",
                            "SKU-B",
                            locs=frozenset(["pricing"]),
                            function_id="catalog.price",
                            version_constraint=">=2.0.0",
                        )
                        ensure(
                            isinstance(out, dict) and out.get("version") == "2.0.0",
                            f">=2 expected v2 got {out}",
                        )
                        ok(f">=2.0.0 → {out}")

                    with scenario(
                        "upper-bound selects v1 (<2)",
                        "rpc.version_constraint",
                    ):
                        out = await client.call(
                            "price",
                            "SKU-C",
                            locs=frozenset(["pricing"]),
                            function_id="catalog.price",
                            version_constraint="<2.0.0",
                        )
                        ensure(
                            isinstance(out, dict) and out.get("version") == "1.0.0",
                            f"<2 expected v1 got {out}",
                        )
                        ok(f"<2.0.0 → {out}")

                    with scenario(
                        "impossible constraint fails closed",
                        "rpc.version_constraint",
                        "rpc.register",
                    ):
                        failed = False
                        try:
                            await client.call(
                                "price",
                                "SKU-X",
                                locs=frozenset(["pricing"]),
                                function_id="catalog.price",
                                version_constraint="==9.9.9",
                            )
                        except Exception as exc:
                            failed = True
                            step(f"expected failure: {type(exc).__name__}: {exc}")
                        ensure(failed, "impossible version should not succeed")
                        ok("==9.9.9 fails closed")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
