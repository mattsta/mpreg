"""L3 chaos_checkout — M2 deadlines + FaultInjector partition (chaos.*)."""

from __future__ import annotations

import asyncio

from mpreg.client.call_policy import ClientCallPolicy, RpcExecutionMode
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
from mpreg.testing.faults import FaultInjector

async def main() -> None:
    with app_run(
        "chaos_checkout",
        "Chaos Checkout — deadlines + partition model",
        level="L3",
    ):
        injector = FaultInjector(seed=42, data_drop_rate=0.0)

        with port_range_context(2, "servers") as ports:
            url_api = f"ws://127.0.0.1:{ports[0]}"
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Checkout-API",
                    resources={"api", "checkout"},
                    log_level="WARNING",
                ),
                MPREGSettings(
                    port=ports[1],
                    name="Payments",
                    resources={"payments"},
                    peers=[url_api],
                    log_level="WARNING",
                ),
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                api, pay = servers

                def create_cart(sku: str) -> dict[str, object]:
                    return {"sku": sku, "cart_id": "c-1", "total_cents": 499}

                def charge(cart: dict[str, object]) -> dict[str, object]:
                    return {
                        "cart_id": cart.get("cart_id"),
                        "charged": True,
                        "total_cents": cart.get("total_cents"),
                    }

                api.register_command("create_cart", create_cart, ["api", "checkout"])
                pay.register_command("charge", charge, ["payments"])
                step("checkout + payments registered")

                with scenario(
                    "happy path with M2 soft-RT policy",
                    "client.policy.m2",
                    "rpc.deadline",
                    "rpc.call",
                ):
                    m2 = ClientCallPolicy.for_mode(
                        RpcExecutionMode.M2_SOFT_RT, deadline_seconds=5.0
                    )
                    ensure(
                        m2.share_deadline_across_attempts is True, "M2 shares deadline"
                    )
                    async with MPREGClientAPI(url_api, call_policy=m2) as client:
                        cart = await client.call(
                            "create_cart",
                            "SKU-1",
                            locs=frozenset(["api", "checkout"]),
                            timeout=5.0,
                        )
                        ensure(
                            isinstance(cart, dict) and cart.get("cart_id") == "c-1",
                            f"cart {cart}",
                        )
                        charged = await client.call(
                            "charge",
                            cart,
                            locs=frozenset(["payments"]),
                            timeout=5.0,
                        )
                        ensure(
                            isinstance(charged, dict)
                            and charged.get("charged") is True,
                            f"charge {charged}",
                        )
                    ok(f"happy-path charged={charged}")

                with scenario(
                    "partition api|payments fail-closed model",
                    "chaos.partition",
                    "chaos.heal",
                ):
                    step("inject partition api|{payments}")
                    injector.partition({"api"}, {"payments"})
                    view = injector.view()
                    ensure(
                        not view.can_communicate("api", "payments"),
                        "partition should block",
                    )
                    ensure(
                        not injector.can_deliver("api", "payments", plane="data"),
                        "data plane drop under partition",
                    )
                    ok("injector blocks api→payments")

                    injector.heal()
                    ensure(
                        injector.view().can_communicate("api", "payments"),
                        "heal failed",
                    )
                    ok(f"healed; fault_events={len(injector.decisions)}")

                with scenario(
                    "post-heal checkout still works", "rpc.call", "chaos.heal"
                ):
                    async with MPREGClientAPI(url_api) as client:
                        cart = await client.call(
                            "create_cart",
                            "SKU-2",
                            locs=frozenset(["api", "checkout"]),
                        )
                        charged = await client.call(
                            "charge", cart, locs=frozenset(["payments"])
                        )
                    ensure(charged.get("charged") is True, f"post-heal {charged}")
                    ok(f"post-heal charged={charged}")
                    step(
                        "non-claim: injector models delivery; live WS may still route "
                        "until server-side partition hooks are wired"
                    )

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
