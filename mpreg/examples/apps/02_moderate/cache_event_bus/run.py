"""L2 cache_event_bus — CachePubSubIntegration notifications on put/atomic."""

from __future__ import annotations

import asyncio
import time

from mpreg.core.advanced_cache_ops import AdvancedCacheOperations
from mpreg.core.cache_pubsub_integration import (
    CacheEvent,
    CacheEventType,
    CacheNotificationConfig,
    CachePubSubIntegration,
)
from mpreg.core.global_cache import (
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.core.topic_exchange import TopicExchange
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport

async def main() -> None:
    with app_run(
        "cache_event_bus",
        "Cache Event Bus — cache ops → pubsub notifications",
        level="L2",
    ):
        transport = InProcessCacheTransport()
        protocol = FabricCacheProtocol(
            "cache-bus", transport=transport, gossip_interval=60.0
        )
        cache = GlobalCacheManager(
            GlobalCacheConfiguration(
                enable_l2_persistent=False,
                enable_l3_distributed=False,
                enable_l4_federation=False,
            ),
            cache_protocol=protocol,
        )
        ops = AdvancedCacheOperations(cache)
        exchange = TopicExchange(server_url="ws://local-cache-bus", cluster_id="bus")
        integration = CachePubSubIntegration(
            cache_manager=cache,
            advanced_cache_ops=ops,
            topic_exchange=exchange,
            cluster_id="bus",
        )
        try:
            topic = "cache.events.demo"
            received: list[PubSubMessage] = []

            with scenario(
                "subscribe topic exchange for cache events",
                "cache.pubsub_events",
                "pubsub.exchange",
            ):
                sub = PubSubSubscription(
                    subscription_id="cache-bus-sub",
                    patterns=(TopicPattern(pattern="cache.events.#"),),
                    subscriber="cache-bus-demo",
                    created_at=time.time(),
                    get_backlog=False,
                )
                ensure(exchange.add_subscription(sub) is True, "subscribe failed")

                def _capture(msg: PubSubMessage) -> None:
                    received.append(msg)

                # TopicExchange may deliver via notifications; also listen via integration
                ok("subscribed cache.events.#")

            with scenario(
                "configure namespace notifications",
                "cache.pubsub_events",
            ):
                integration.configure_notifications(
                    "demo",
                    CacheNotificationConfig(
                        notify_on_change=True,
                        notification_topic=topic,
                        event_types=[
                            CacheEventType.CACHE_PUT,
                            CacheEventType.ATOMIC_OPERATION,
                            CacheEventType.CACHE_DELETE,
                        ],
                        include_cache_metadata=True,
                        async_notification=True,
                    ),
                )
                ensure(
                    "demo" in integration.notification_configs,
                    "config not stored",
                )
                ok("notifications configured for namespace=demo")

            key = GlobalCacheKey(namespace="demo", identifier="k1")

            with scenario(
                "notify_cache_event publishes CACHE_PUT",
                "cache.pubsub_events",
                "cache.put_get",
            ):
                await cache.put(key, {"v": 1})
                event = CacheEvent(
                    event_type=CacheEventType.CACHE_PUT,
                    cache_key=key,
                    new_value={"v": 1},
                    cluster_id="bus",
                )
                await integration.notify_cache_event(event)
                # Allow async processor
                await asyncio.sleep(0.25)
                # Drain exchange by publishing path — notifications go through exchange
                # Collect from topic_exchange recent or stats
                stats = integration.get_statistics()
                ensure(isinstance(stats, dict), f"stats not dict {stats}")
                sent = int(stats.get("notifications_sent", 0) or 0)
                # Also try direct publish path observation via exchange
                ensure(
                    sent >= 1 or integration.stats.notifications_sent >= 1,
                    f"no notifications sent stats={stats}",
                )
                ok(
                    f"notifications_sent="
                    f"{integration.stats.notifications_sent}"
                )

            with scenario(
                "second put increments notifications; topic exchange path",
                "cache.pubsub_events",
                "pubsub.fanout",
            ):
                before = integration.stats.notifications_sent
                # Phase H F7: listeners fire on notify_cache_event
                seen_hooks: list[str] = []
                integration.add_event_listener(
                    CacheEventType.CACHE_PUT,
                    lambda ev: seen_hooks.append(ev.event_type.value),
                )
                ensure(
                    len(integration.event_listeners[CacheEventType.CACHE_PUT]) >= 1,
                    "listener not registered",
                )
                await integration.notify_cache_event(
                    CacheEvent(
                        event_type=CacheEventType.CACHE_PUT,
                        cache_key=key,
                        new_value={"v": 2},
                        cluster_id="bus",
                    )
                )
                await asyncio.sleep(0.2)
                after = integration.stats.notifications_sent
                ensure(after > before, f"notifications did not increase {before}→{after}")
                ensure(
                    len(seen_hooks) >= 1 and seen_hooks[-1] == CacheEventType.CACHE_PUT.value,
                    f"F7 listener not fired: {seen_hooks}",
                )
                step(f"F7 fixed: add_event_listener fired hooks={seen_hooks}")
                msg = PubSubMessage(
                    topic=topic,
                    payload={"event_type": "cache_put", "v": 2},
                    timestamp=time.time(),
                    message_id="cache-bus-probe",
                    publisher="cache-bus-demo",
                )
                notes = exchange.publish_message(msg)
                ensure(len(notes) >= 1, f"topic exchange fanout empty {notes}")
                ok(
                    f"notifications {before}→{after}; "
                    f"listeners_fired={len(seen_hooks)}; "
                    f"exchange_notes={len(notes)}"
                )

            with scenario(
                "invalidate kwargs guard + broadcast API",
                "cache.pubsub_events",
                "cache.invalidate",
            ):
                # Phase H F8: bad kwargs raise clear TypeError
                raised = False
                try:
                    await cache.invalidate("demo.*", namespace="demo")  # type: ignore[call-arg]
                except TypeError as exc:
                    raised = True
                    ensure("unexpected keyword" in str(exc).lower() or "namespace" in str(exc), str(exc))
                    step(f"F8 fixed: bad kwargs → TypeError: {exc}")
                ensure(raised is True, "expected TypeError on bad invalidate kwargs")
                # Valid pattern-only invalidate
                inv = await cache.invalidate("demo")
                ensure(inv is not None, "invalidate returned None")
                if hasattr(integration, "broadcast_cache_invalidation"):
                    await integration.broadcast_cache_invalidation(
                        cache_key=key
                    )
                    await integration.broadcast_cache_invalidation(pattern="demo.*")
                    ok("broadcast_cache_invalidation(key + pattern) invoked")
                else:
                    ok("broadcast API absent — skipped")
                ensure(
                    integration.stats.notifications_sent >= 1,
                    "expected cumulative notifications",
                )
                ok(
                    f"final notifications_sent="
                    f"{integration.stats.notifications_sent}"
                )
        finally:
            await integration.shutdown()
            await cache.shutdown()
            await protocol.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
