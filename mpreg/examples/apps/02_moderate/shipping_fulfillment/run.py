"""L2 shipping_fulfillment — ship/track RPC + status cache + dispatch queue."""

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
from mpreg.core.message_queue import DeliveryGuarantee
from mpreg.core.message_queue_manager import create_reliable_queue_manager
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
        "shipping_fulfillment",
        "Shipping Fulfillment — label RPC + track cache + dispatch queue",
        level="L2",
    ):
        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Shipping-API",
                    resources={"shipping", "api"},
                    log_level="WARNING",
                    gossip_interval=30.0,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]
                shipments: dict[str, dict[str, Any]] = {}
                labels_issued = 0

                def create_shipment(
                    order_id: str, dest: str, weight_kg: float
                ) -> dict[str, Any]:
                    nonlocal labels_issued
                    if order_id in shipments:
                        return {**shipments[order_id], "replay": True}
                    labels_issued += 1
                    rec = {
                        "ok": True,
                        "order_id": order_id,
                        "tracking": f"TRK-{labels_issued:05d}",
                        "dest": dest,
                        "weight_kg": weight_kg,
                        "status": "labeled",
                        "replay": False,
                    }
                    shipments[order_id] = rec
                    return rec

                def advance(tracking: str, status: str) -> dict[str, Any]:
                    for rec in shipments.values():
                        if rec["tracking"] == tracking:
                            rec["status"] = status
                            return {"ok": True, "tracking": tracking, "status": status}
                    return {"ok": False, "reason": "unknown_tracking"}

                def get_shipment(order_id: str) -> dict[str, Any]:
                    rec = shipments.get(order_id)
                    if rec is None:
                        return {"ok": False, "reason": "unknown_order"}
                    return {"ok": True, **rec}

                server.register_command(
                    "create_shipment", create_shipment, ["shipping", "api"]
                )
                server.register_command("advance", advance, ["shipping", "api"])
                server.register_command("get_shipment", get_shipment, ["shipping", "api"])

                transport = InProcessCacheTransport()
                protocol = FabricCacheProtocol(
                    "ship-cache", transport=transport, gossip_interval=60.0
                )
                cache = GlobalCacheManager(
                    GlobalCacheConfiguration(
                        enable_l2_persistent=False,
                        enable_l3_distributed=False,
                        enable_l4_federation=False,
                    ),
                    cache_protocol=protocol,
                )
                manager = create_reliable_queue_manager()
                dispatch: list[dict[str, Any]] = []

                def on_dispatch(message: Any) -> None:
                    payload = getattr(message, "payload", message)
                    if isinstance(payload, dict):
                        dispatch.append(payload)

                try:
                    await manager.create_queue("dispatch")
                    manager.subscribe_to_queue(
                        "dispatch", "ship-worker", "dispatch.*", callback=on_dispatch
                    )
                    url = f"ws://127.0.0.1:{ports[0]}"
                    async with MPREGClientAPI(url) as client:
                        with scenario(
                            "create shipment + idempotent replay",
                            "prod.shipping",
                            "rpc.call",
                            "rpc.register",
                        ):
                            s1 = await client.call(
                                "create_shipment",
                                "ORD-9",
                                "US-NY",
                                1.5,
                                locs=frozenset(["shipping", "api"]),
                            )
                            ensure(s1.get("ok") is True, f"s1 {s1}")
                            ensure(s1.get("tracking", "").startswith("TRK-"), s1)
                            s1b = await client.call(
                                "create_shipment",
                                "ORD-9",
                                "US-NY",
                                1.5,
                                locs=frozenset(["shipping", "api"]),
                            )
                            ensure(s1b.get("replay") is True, f"replay {s1b}")
                            ensure(s1b.get("tracking") == s1["tracking"], "tracking drift")
                            ok(f"tracking={s1['tracking']} replay=True")

                        with scenario(
                            "cache tracking snapshot",
                            "prod.shipping",
                            "cache.put_get",
                            "cache.l1",
                        ):
                            key = GlobalCacheKey.from_data(
                                "ship.track", {"tracking": s1["tracking"]}
                            )
                            await cache.put(
                                key,
                                {
                                    "tracking": s1["tracking"],
                                    "status": s1["status"],
                                    "order_id": "ORD-9",
                                },
                                CacheMetadata(
                                    computation_cost_ms=1.0,
                                    ttl_seconds=300.0,
                                    created_by="ship",
                                ),
                            )
                            got = await cache.get(key)
                            ensure(
                                got.success and got.entry is not None,
                                "cache miss",
                            )
                            ensure(got.entry.value["status"] == "labeled", got.entry.value)
                            ok(f"cache status={got.entry.value['status']}")

                        with scenario(
                            "advance status + refresh cache",
                            "prod.shipping",
                            "rpc.call",
                            "cache.put_get",
                        ):
                            adv = await client.call(
                                "advance",
                                s1["tracking"],
                                "in_transit",
                                locs=frozenset(["shipping", "api"]),
                            )
                            ensure(adv.get("ok") is True, adv)
                            await cache.put(
                                key,
                                {
                                    "tracking": s1["tracking"],
                                    "status": "in_transit",
                                    "order_id": "ORD-9",
                                },
                                CacheMetadata(
                                    computation_cost_ms=1.0,
                                    ttl_seconds=300.0,
                                    created_by="ship",
                                ),
                            )
                            got2 = await cache.get(key)
                            ensure(
                                got2.success and got2.entry is not None,
                                "miss2",
                            )
                            ensure(
                                got2.entry.value["status"] == "in_transit",
                                got2.entry.value,
                            )
                            ok("status in_transit cached")

                        with scenario(
                            "dispatch queue durable send",
                            "prod.shipping",
                            "queue.send",
                            "queue.alo",
                            "queue.subscribe",
                        ):
                            await manager.send_message(
                                "dispatch",
                                "dispatch.out",
                                {
                                    "tracking": s1["tracking"],
                                    "action": "handoff_carrier",
                                },
                                DeliveryGuarantee.AT_LEAST_ONCE,
                            )
                            for _ in range(40):
                                if dispatch:
                                    break
                                await asyncio.sleep(0.05)
                            ensure(len(dispatch) >= 1, f"dispatch {dispatch}")
                            ensure(
                                dispatch[0].get("action") == "handoff_carrier",
                                dispatch[0],
                            )
                            ok(f"dispatch={dispatch[0]}")

                        with scenario(
                            "get_shipment RPC read model",
                            "prod.shipping",
                            "rpc.call",
                        ):
                            g = await client.call(
                                "get_shipment",
                                "ORD-9",
                                locs=frozenset(["shipping", "api"]),
                            )
                            ensure(g.get("ok") is True, g)
                            ensure(g.get("status") == "in_transit", g)
                            missing = await client.call(
                                "get_shipment",
                                "NOPE",
                                locs=frozenset(["shipping", "api"]),
                            )
                            ensure(missing.get("ok") is False, missing)
                            step("read model after advance")
                            ok(f"get status={g.get('status')}")
                finally:
                    close = getattr(manager, "shutdown", None) or getattr(
                        manager, "close", None
                    )
                    if close is not None:
                        result = close()
                        if asyncio.iscoroutine(result):
                            await result
                    cclose = getattr(cache, "close", None)
                    if cclose is not None:
                        cr = cclose()
                        if asyncio.iscoroutine(cr):
                            await cr

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
