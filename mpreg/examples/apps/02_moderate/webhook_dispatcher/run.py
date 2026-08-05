"""L2 webhook_dispatcher — events.# → durable egress queue (prod.webhooks)."""

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
        "webhook_dispatcher",
        "Webhook Dispatcher — events to egress",
        level="L2",
    ):
        exchange = TopicExchange("ws://local-webhooks", "webhooks")
        manager = create_reliable_queue_manager()
        try:
            delivered: list[dict[str, Any]] = []

            def worker(message: Any) -> None:
                payload = message.payload
                if isinstance(payload, dict):
                    delivered.append(payload)
                else:
                    delivered.append({"raw": payload})

            with scenario(
                "install bridge + egress worker",
                "prod.webhooks",
                "pubsub.exchange",
                "queue.create",
                "queue.subscribe",
            ):
                await manager.create_queue("egress")
                manager.subscribe_to_queue(
                    "egress", "webhook-worker", "egress.*", callback=worker
                )
                exchange.add_subscription(
                    PubSubSubscription(
                        subscription_id="hook-bridge",
                        patterns=(TopicPattern(pattern="events.#"),),
                        subscriber="dispatcher",
                        created_at=time.time(),
                    )
                )
                ok("events.# → egress queue")

            with scenario(
                "dispatch signup + paid",
                "pubsub.wildcard_hash",
                "queue.alo",
                "queue.topic_route",
                "pubsub.headers",
            ):
                events = [
                    ("events.user.signup", {"type": "signup", "user": "u1"}),
                    ("events.order.paid", {"type": "paid", "order": "o9"}),
                ]
                matched_total = 0
                for topic, payload in events:
                    hits = exchange.publish_message(
                        PubSubMessage(
                            topic=topic,
                            payload=payload,
                            timestamp=time.time(),
                            message_id=topic,
                            publisher="api",
                            headers={"x-webhook": "v1"},
                        )
                    )
                    matched_total += len(hits)
                    for _ in hits:
                        await manager.send_message(
                            "egress",
                            "egress.webhook",
                            payload,
                            DeliveryGuarantee.AT_LEAST_ONCE,
                        )
                await asyncio.sleep(0.4)
                ensure(matched_total >= 2, f"pubsub matches {matched_total}")
                ensure(len(delivered) == 2, f"queue delivered {delivered}")
                types = {d.get("type") for d in delivered}
                ensure(types == {"signup", "paid"}, f"bad types {types}")
                ok(f"delivered={delivered}")

            with scenario("noise topic not dispatched", "pubsub.exchange"):
                before = len(delivered)
                hits = exchange.publish_message(
                    PubSubMessage(
                        topic="metrics.tick",
                        payload={"type": "tick"},
                        timestamp=time.time(),
                        message_id="tick",
                        publisher="api",
                    )
                )
                ensure(len(hits) == 0, "metrics should not match events.#")
                ensure(len(delivered) == before, "egress grew without match")
                ok("negative path holds")
                step("non-claim: not HTTP delivery; queue is the egress boundary")
        finally:
            await manager.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
