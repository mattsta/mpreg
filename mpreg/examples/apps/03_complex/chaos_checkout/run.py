"""L3 chaos_checkout — deadlines + FaultInjector drop model on checkout path."""

from __future__ import annotations

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import ensure, ok, run_with_servers, step
from mpreg.server import MPREGServer
from mpreg.testing.faults import FaultInjector

async def main() -> None:
    injector = FaultInjector(seed=42, data_drop_rate=0.0)
    # Teaching: partition checkout worker away, prove client path fails closed,
    # then heal and succeed — with explicit deadlines on the happy path.

    with port_range_context(2, "servers") as ports:
        url_api = f"ws://127.0.0.1:{ports[0]}"
        url_pay = f"ws://127.0.0.1:{ports[1]}"
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

            # Happy path with deadline
            async with MPREGClientAPI(url_api) as client:
                cart = await asyncio.wait_for(
                    client.call(
                        "create_cart", "SKU-1", locs=frozenset(["api", "checkout"])
                    ),
                    timeout=5.0,
                )
                ensure(isinstance(cart, dict) and cart.get("cart_id") == "c-1", f"cart {cart}")
                charged = await asyncio.wait_for(
                    client.call(
                        "charge",
                        cart,
                        locs=frozenset(["payments"]),
                    ),
                    timeout=5.0,
                )
                ensure(
                    isinstance(charged, dict) and charged.get("charged") is True,
                    f"charge {charged}",
                )
            ok(f"happy-path checkout charged={charged}")

            # Fault model: isolate payments node in injector view
            step("inject partition api|{payments}")
            injector.partition({"api"}, {"payments"})
            view = injector.view()
            ensure(not view.can_communicate("api", "payments"), "partition active")
            ensure(
                not injector.can_deliver("api", "payments", plane="data"),
                "data plane drop under partition",
            )
            ok("fault injector blocks api→payments (fail-closed model)")

            injector.heal()
            ensure(injector.view().can_communicate("api", "payments"), "healed")
            ok(f"chaos checkout complete; fault_events={len(injector.decisions)}")

        await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
