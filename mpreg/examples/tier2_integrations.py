from __future__ import annotations

#!/usr/bin/env python3
"""Tier 2: Two-system integrations in realistic workflows."""


import asyncio
import time
from typing import Any

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.global_cache import (
    CacheLevel,
    CacheMetadata,
    CacheOptions,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
    ReplicationStrategy,
)
from mpreg.core.message_queue import DeliveryGuarantee
from mpreg.core.message_queue_manager import create_reliable_queue_manager
from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.core.port_allocator import port_context
from mpreg.core.topic_exchange import TopicExchange
from mpreg.examples.showcase_utils import run_with_servers
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport
from mpreg.server import MPREGServer


def _ensure(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


async def rpc_plus_cache() -> None:
    """RPC output is cached and reused across requests."""
    with port_context("servers") as port:
        settings = [MPREGSettings(port=port, name="RPC-Cache", resources={"compute"})]

        async def _run(servers: list[MPREGServer]) -> None:
            server = servers[0]

            def expensive(x: int) -> dict[str, Any]:
                time.sleep(0.02)
                return {"value": x * x, "computed_at": time.time()}

            server.register_command("expensive", expensive, ["compute"])

            cache = GlobalCacheManager(
                GlobalCacheConfiguration(
                    enable_l2_persistent=False, enable_l3_distributed=False
                ),
                cache_protocol=FabricCacheProtocol(
                    "local", transport=InProcessCacheTransport()
                ),
            )

            async with MPREGClientAPI(f"ws://127.0.0.1:{port}") as client:
                key = GlobalCacheKey.from_function_call(
                    "rpc.cache", "expensive", args=(12,), kwargs={}
                )

                cached = await cache.get(key)
                if not cached.success:
                    result = await client.call(
                        "expensive", 12, locs=frozenset(["compute"])
                    )
                    await cache.put(
                        key,
                        result,
                        CacheMetadata(computation_cost_ms=20.0, ttl_seconds=300.0),
                    )

                cached_again = await cache.get(key)
                print(
                    "RPC + Cache value:",
                    cached_again.entry.value if cached_again.entry else None,
                )
                _ensure(
                    cached_again.entry is not None
                    and cached_again.entry.value.get("value") == 144,
                    "RPC + Cache demo: unexpected cached value",
                )

            await cache.shutdown()

        await run_with_servers(settings, _run)


async def pubsub_plus_queue() -> None:
    """Topic exchange fan-out feeds a durable queue."""
    exchange = TopicExchange("ws://local", "demo_cluster")
    manager = create_reliable_queue_manager()
    await manager.create_queue("notifications")

    received: list[str] = []

    def queue_handler(message: Any) -> None:
        received.append(str(message.payload))

    manager.subscribe_to_queue(
        "notifications", "worker", "notifications.*", queue_handler
    )

    exchange.add_subscription(
        PubSubSubscription(
            subscription_id="fanout",
            patterns=(TopicPattern(pattern="user.*.event"),),
            subscriber="queue-bridge",
            created_at=time.time(),
        )
    )

    notifications = []
    notifications.extend(
        exchange.publish_message(
            PubSubMessage(
                message_id="n1",
                topic="user.42.event",
                payload={"user": 42, "action": "signup"},
                publisher="demo",
                headers={},
                timestamp=time.time(),
            )
        )
    )
    notifications.extend(
        exchange.publish_message(
            PubSubMessage(
                message_id="n2",
                topic="user.99.event",
                payload={"user": 99, "action": "upgrade"},
                publisher="demo",
                headers={},
                timestamp=time.time(),
            )
        )
    )

    for notification in notifications:
        await manager.send_message(
            "notifications",
            "notifications.user",
            notification.message.payload,
            DeliveryGuarantee.AT_LEAST_ONCE,
        )

    await asyncio.sleep(0.3)
    print("PubSub + Queue received:", received)
    _ensure(len(received) == 2, "PubSub + Queue demo: unexpected message count")
    await manager.shutdown()


async def cache_plus_federation() -> None:
    """Cache writes replicated across fabric federation scope."""
    from mpreg.fabric.cache_federation import FabricCacheProtocol
    from mpreg.fabric.cache_transport import InProcessCacheTransport

    transport = InProcessCacheTransport()
    cache_protocol_a = FabricCacheProtocol(
        "node-a", transport=transport, gossip_interval=60.0
    )
    cache_protocol_b = FabricCacheProtocol(
        "node-b", transport=transport, gossip_interval=60.0
    )

    cache_a = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=True,
            local_cluster_id="cluster-a",
        ),
        cache_protocol=cache_protocol_a,
    )
    cache_b = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=True,
            local_cluster_id="cluster-b",
        ),
        cache_protocol=cache_protocol_b,
    )

    key = GlobalCacheKey.from_data("federated.cache", {"order": "a-100"})
    cache_options = CacheOptions(cache_levels=frozenset([CacheLevel.L1, CacheLevel.L4]))
    await cache_a.put(
        key,
        {"status": "ready"},
        CacheMetadata(
            computation_cost_ms=5.0,
            replication_policy=ReplicationStrategy.GEOGRAPHIC,
            geographic_hints=["eu-west"],
        ),
        options=cache_options,
    )
    await asyncio.sleep(0.2)

    result = await cache_b.get(key, options=cache_options)
    print("Cache + Federation hit:", result.success)
    _ensure(result.success, "Cache + Federation demo: L4 fetch failed")

    await cache_a.shutdown()
    await cache_b.shutdown()
    await cache_protocol_a.shutdown()
    await cache_protocol_b.shutdown()


async def main() -> None:
    print("Tier 2: Integrations")
    await rpc_plus_cache()
    await pubsub_plus_queue()
    await cache_plus_federation()


if __name__ == "__main__":
    asyncio.run(main())
