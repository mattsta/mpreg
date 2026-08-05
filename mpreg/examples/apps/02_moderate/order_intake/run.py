"""L2 order_intake — multi-plane product: RPC + cache + pubsub + queue (prod.order)."""

from __future__ import annotations

import asyncio
import time
import uuid
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
from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.core.port_allocator import port_range_context
from mpreg.core.topic_exchange import TopicExchange
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
        "order_intake",
        "Order Intake — RPC + cache + pubsub + queue",
        level="L2",
    ):
        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Order-API",
                    resources={"orders", "api"},
                    log_level="WARNING",
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]
                orders: dict[str, dict[str, Any]] = {}

                def create_order(sku: str, qty: int, idem_key: str) -> dict[str, Any]:
                    if idem_key in orders:
                        return {**orders[idem_key], "replay": True}
                    order_id = f"ord-{uuid.uuid4().hex[:8]}"
                    rec = {
                        "order_id": order_id,
                        "sku": sku,
                        "qty": qty,
                        "idem_key": idem_key,
                        "replay": False,
                        "status": "created",
                    }
                    orders[idem_key] = rec
                    return rec

                def get_order(order_id: str) -> dict[str, Any]:
                    for rec in orders.values():
                        if rec["order_id"] == order_id:
                            return {"found": True, **rec}
                    return {"found": False, "order_id": order_id}

                server.register_command("create_order", create_order, ["orders", "api"])
                server.register_command("get_order", get_order, ["orders", "api"])

                cache_transport = InProcessCacheTransport()
                cache_protocol = FabricCacheProtocol(
                    "order-node", transport=cache_transport, gossip_interval=60.0
                )
                cache = GlobalCacheManager(
                    GlobalCacheConfiguration(
                        enable_l2_persistent=False,
                        enable_l3_distributed=False,
                        enable_l4_federation=False,
                    ),
                    cache_protocol=cache_protocol,
                )

                exchange = TopicExchange("ws://local-order", "order_cluster")
                exchange.add_subscription(
                    PubSubSubscription(
                        subscription_id="fulfill-notify",
                        patterns=(TopicPattern(pattern="orders.*.created"),),
                        subscriber="notify-svc",
                        created_at=time.time(),
                    )
                )
                exchange.add_subscription(
                    PubSubSubscription(
                        subscription_id="audit",
                        patterns=(TopicPattern(pattern="orders.#"),),
                        subscriber="audit-svc",
                        created_at=time.time(),
                    )
                )

                manager = create_reliable_queue_manager()
                await manager.create_queue("fulfill")
                fulfilled: list[str] = []

                def fulfill_worker(message: Any) -> None:
                    payload = message.payload
                    fulfilled.append(str(payload.get("order_id", payload)))

                manager.subscribe_to_queue(
                    "fulfill", "worker-1", "fulfill.*", callback=fulfill_worker
                )

                try:
                    hub = f"ws://127.0.0.1:{ports[0]}"
                    idem = f"idem-{uuid.uuid4().hex[:10]}"
                    order_id = ""

                    with scenario(
                        "idempotent RPC create_order",
                        "prod.order",
                        "rpc.call",
                        "rpc.register",
                    ):
                        async with MPREGClientAPI(hub) as client:
                            first = await client.call(
                                "create_order",
                                "SKU-1",
                                2,
                                idem,
                                locs=frozenset(["orders", "api"]),
                            )
                            second = await client.call(
                                "create_order",
                                "SKU-1",
                                2,
                                idem,
                                locs=frozenset(["orders", "api"]),
                            )
                        ensure(isinstance(first, dict), "first order not dict")
                        ensure(first.get("replay") is False, "first should not be replay")
                        ensure(second.get("replay") is True, "second should be replay")
                        ensure(
                            first.get("order_id") == second.get("order_id"),
                            "idempotent order_id mismatch",
                        )
                        order_id = str(first["order_id"])
                        ok(f"order_id={order_id} idempotent replay ok")

                    with scenario("get_order lookup", "rpc.call"):
                        async with MPREGClientAPI(hub) as client:
                            got = await client.call(
                                "get_order",
                                order_id,
                                locs=frozenset(["orders", "api"]),
                            )
                            miss = await client.call(
                                "get_order",
                                "ord-missing",
                                locs=frozenset(["orders", "api"]),
                            )
                        ensure(got.get("found") is True and got.get("sku") == "SKU-1", f"get {got}")
                        ensure(miss.get("found") is False, f"miss {miss}")
                        ok("get_order hit + miss")

                    with scenario("cache idempotency mirror", "cache.put_get", "cache.l1"):
                        key = GlobalCacheKey.from_data("orders.idem", {"key": idem})
                        await cache.put(
                            key,
                            first,
                            CacheMetadata(computation_cost_ms=5.0, ttl_seconds=300.0),
                        )
                        cached = await cache.get(key)
                        ensure(cached.success and cached.entry is not None, "cache miss")
                        ensure(
                            cached.entry.value.get("order_id") == order_id,
                            "cached order mismatch",
                        )
                        cold = await cache.get(
                            GlobalCacheKey.from_data("orders.idem", {"key": "nope"})
                        )
                        ensure(
                            (not cold.success) or cold.entry is None,
                            "expected cold miss",
                        )
                        ok("cache mirror + cold miss")

                    with scenario(
                        "pubsub created + audit fan-out",
                        "pubsub.exchange",
                        "pubsub.wildcard_hash",
                        "pubsub.fanout",
                    ):
                        msg = PubSubMessage(
                            topic=f"orders.{order_id}.created",
                            payload={"order_id": order_id, "sku": "SKU-1"},
                            timestamp=time.time(),
                            message_id=uuid.uuid4().hex,
                            publisher="order-api",
                            headers={"x-idem": idem},
                        )
                        matched = exchange.publish_message(msg)
                        ensure(len(matched) >= 2, f"expected notify+audit got {len(matched)}")
                        noise = exchange.publish_message(
                            PubSubMessage(
                                topic="inventory.tick",
                                payload={},
                                timestamp=time.time(),
                                message_id=uuid.uuid4().hex,
                                publisher="order-api",
                            )
                        )
                        ensure(len(noise) == 0, "inventory should not match orders.#")
                        ok(f"pubsub matched={len(matched)}")

                    with scenario("fulfill queue ALO", "queue.alo", "queue.subscribe", "queue.send"):
                        await manager.send_message(
                            "fulfill",
                            "fulfill.new",
                            {"order_id": order_id},
                            DeliveryGuarantee.AT_LEAST_ONCE,
                        )
                        await asyncio.sleep(0.4)
                        ensure(order_id in fulfilled, f"not fulfilled: {fulfilled}")
                        ok(f"fulfill processed {fulfilled}")

                    with scenario("second distinct order", "rpc.call"):
                        idem2 = f"idem-{uuid.uuid4().hex[:10]}"
                        async with MPREGClientAPI(hub) as client:
                            other = await client.call(
                                "create_order",
                                "SKU-2",
                                1,
                                idem2,
                                locs=frozenset(["orders", "api"]),
                            )
                        ensure(
                            other.get("order_id") != order_id and other.get("replay") is False,
                            f"second order bad {other}",
                        )
                        ok(f"second order_id={other.get('order_id')}")

                finally:
                    await cache.shutdown()
                    await cache_protocol.shutdown()
                    await manager.shutdown()

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
