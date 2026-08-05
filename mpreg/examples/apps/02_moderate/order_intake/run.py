"""L2 order_intake — RPC + cache idempotency + pubsub notify + fulfill queue."""

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
from mpreg.examples.apps._shared.runtime import ensure, ok, run_with_servers, step
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport
from mpreg.server import MPREGServer

async def main() -> None:
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
                }
                orders[idem_key] = rec
                return rec

            server.register_command("create_order", create_order, ["orders", "api"])

            # Cache plane — idempotency mirror
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

            # Pub/sub plane — order notifications
            exchange = TopicExchange("ws://local-order", "order_cluster")
            notifications: list[str] = []
            exchange.add_subscription(
                PubSubSubscription(
                    subscription_id="fulfill-notify",
                    patterns=(TopicPattern(pattern="orders.*.created"),),
                    subscriber="notify-svc",
                    created_at=time.time(),
                )
            )

            # Queue plane — fulfill workers
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
                step("RPC create_order")
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

                # Cache the order record
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
                ok("cache idempotency mirror stored")

                # Notify via topic exchange
                msg = PubSubMessage(
                    topic=f"orders.{order_id}.created",
                    payload={"order_id": order_id, "sku": "SKU-1"},
                    timestamp=time.time(),
                    message_id=uuid.uuid4().hex,
                    publisher="order-api",
                )
                matched = exchange.publish_message(msg)
                ensure(len(matched) >= 1, "no pubsub subscribers matched")
                notifications.append(order_id)
                ok(f"pubsub matched={len(matched)}")

                # Enqueue fulfill
                await manager.send_message(
                    "fulfill",
                    "fulfill.new",
                    {"order_id": order_id},
                    DeliveryGuarantee.AT_LEAST_ONCE,
                )
                await asyncio.sleep(0.4)
                ensure(order_id in fulfilled, f"not fulfilled: {fulfilled}")
                ok(f"fulfill queue processed {fulfilled}")

            finally:
                await cache.shutdown()
                await cache_protocol.shutdown()
                await manager.shutdown()

        await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
