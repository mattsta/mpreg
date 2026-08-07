"""L2 pubsub_plus_queue — topic fan-out bridges into durable queue."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from mpreg.core.message_queue import DeliveryGuarantee
from mpreg.core.message_queue_manager import create_reliable_queue_manager
from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.core.topic_exchange import TopicExchange
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step


async def main() -> None:
    with app_run(
        "pubsub_plus_queue",
        "PubSub + Queue bridge",
        level="L2",
    ):
        exchange = TopicExchange("ws://local-psq", "demo_cluster")
        manager = create_reliable_queue_manager()
        try:
            received: list[Any] = []

            def queue_handler(message: Any) -> None:
                received.append(message.payload)

            with scenario(
                "subscribe exchange + queue worker",
                "pubsub.exchange",
                "queue.create",
                "queue.subscribe",
            ):
                await manager.create_queue("notifications")
                manager.subscribe_to_queue(
                    "notifications", "worker", "notifications.*", callback=queue_handler
                )
                exchange.add_subscription(
                    PubSubSubscription(
                        subscription_id="fanout",
                        patterns=(TopicPattern(pattern="user.*.event"),),
                        subscriber="queue-bridge",
                        created_at=time.time(),
                    )
                )
                ok("bridge ready")

            with scenario(
                "matched topics enqueued ALO",
                "pubsub.wildcard_star",
                "queue.alo",
                "queue.topic_route",
            ):
                notifications: list = []
                for mid, topic, payload in (
                    ("n1", "user.42.event", {"user": 42, "action": "signup"}),
                    ("n2", "user.99.event", {"user": 99, "action": "upgrade"}),
                ):
                    hits = exchange.publish_message(
                        PubSubMessage(
                            message_id=mid,
                            topic=topic,
                            payload=payload,
                            publisher="demo",
                            headers={"bridge": "true"},
                            timestamp=time.time(),
                        )
                    )
                    ensure(len(hits) >= 1, f"{topic} unmatched")
                    notifications.extend(hits)

                for notification in notifications:
                    await manager.send_message(
                        "notifications",
                        "notifications.user",
                        notification.message.payload,
                        DeliveryGuarantee.AT_LEAST_ONCE,
                    )
                await asyncio.sleep(0.35)
                ensure(len(received) == 2, f"expected 2 queue msgs got {received}")
                actions = {
                    str(p.get("action")) for p in received if isinstance(p, dict)
                }
                ensure(actions == {"signup", "upgrade"}, f"actions {actions}")
                ok(f"bridged payloads={received}")

            with scenario("unmatched topic not enqueued", "pubsub.exchange"):
                before = len(received)
                hits = exchange.publish_message(
                    PubSubMessage(
                        message_id="noise",
                        topic="system.tick",
                        payload={"t": 1},
                        publisher="demo",
                        headers={},
                        timestamp=time.time(),
                    )
                )
                ensure(len(hits) == 0, "system.tick should not match user.*.event")
                # Intentionally do not enqueue unmatched
                ensure(len(received) == before, "queue grew without match")
                ok("negative path holds")
                step("non-claim: not exactly-once; bridge is app-level")
        finally:
            await manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
