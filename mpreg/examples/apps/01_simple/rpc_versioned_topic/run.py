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
    with (
        app_run(
            "rpc_versioned_topic",
            "RPC Versioned Topic — function_id + constraints",
            level="L1",
        ),
        port_range_context(2, "servers") as ports,
    ):
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

            # Phase H F5: multi-version same-node (registry coexists by version).
            v1.register_command(
                "price",
                price_v1,
                ["pricing"],
                function_id="catalog.price",
                version="1.0.0",
            )
            v1.register_command(
                "price",
                price_v2,
                ["pricing"],
                function_id="catalog.price",
                version="2.0.0",
            )
            # Loud collision on same version (not multi-version).
            collided = False
            try:
                v1.register_command(
                    "price",
                    price_v1,
                    ["pricing"],
                    function_id="catalog.price",
                    version="1.0.0",
                )
            except ValueError as exc:
                collided = True
                step(f"same-version collision: {exc}")
            ensure(collided, "same version must raise ValueError")
            ok("same-node multi-version OK; same-version collision loud")

            # Peer still hosts v2 only for cross-node range demos.
            v2.register_command(
                "price",
                price_v2,
                ["pricing"],
                function_id="catalog.price",
                version="2.0.0",
            )
            step("registered catalog.price v1+v2 on hub; v2 on peer")
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
                    "impossible constraint → version_mismatch",
                    "rpc.version_constraint",
                    "rpc.register",
                ):
                    from mpreg.core.errors import MpregError, MpregErrorCode

                    failed = False
                    got_mismatch = False
                    try:
                        await client.call(
                            "price",
                            "SKU-X",
                            locs=frozenset(["pricing"]),
                            function_id="catalog.price",
                            version_constraint="==9.9.9",
                        )
                    except MpregError as exc:
                        failed = True
                        got_mismatch = int(exc.code) == int(
                            MpregErrorCode.VERSION_MISMATCH
                        )
                        step(
                            f"expected failure: code={exc.code} "
                            f"mismatch={got_mismatch}: {exc}"
                        )
                    except Exception as exc:
                        failed = True
                        step(f"expected failure: {type(exc).__name__}: {exc}")
                    ensure(failed, "impossible version should not succeed")
                    ensure(
                        got_mismatch,
                        "expected VERSION_MISMATCH (1002), not generic not-found",
                    )
                    ok("==9.9.9 → VERSION_MISMATCH")

                with scenario(
                    "bare name + opaque function_id (Phase W)",
                    "rpc.function_id",
                    "rpc.fqn",
                    "rpc.call",
                ):
                    # function_id is a capability key — not namespace-qualified.
                    # Bare fun leaf still routes when paired with the opaque id.
                    out = await client.call(
                        "price",
                        "SKU-W",
                        locs=frozenset(["pricing"]),
                        function_id="catalog.price",
                        version_constraint="==1.0.0",
                    )
                    ensure(
                        isinstance(out, dict) and out.get("version") == "1.0.0",
                        f"bare+opaque id failed: {out}",
                    )
                    # Explicit FQN fun must also work with the same opaque id.
                    out_fqn = await client.call(
                        "app.price",
                        "SKU-W2",
                        locs=frozenset(["pricing"]),
                        function_id="catalog.price",
                        version_constraint="==1.0.0",
                    )
                    ensure(
                        isinstance(out_fqn, dict) and out_fqn.get("version") == "1.0.0",
                        f"FQN+opaque id failed: {out_fqn}",
                    )
                    ok("bare leaf + opaque function_id; FQN + same id")

        await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())
