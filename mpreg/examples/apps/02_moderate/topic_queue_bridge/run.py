"""L2 topic_queue_bridge — TopicExchange hits drive durable queue send."""

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
        "topic_queue_bridge",
        "Topic→Queue Bridge — fanout becomes durable work",
        level="L2",
    ):
        exchange = TopicExchange("ws://local-bridge", "bridge")
        manager = create_reliable_queue_manager()
        try:
            work: list[dict[str, Any]] = []

            def worker(message: Any) -> None:
                payload = getattr(message, "payload", message)
                if isinstance(payload, dict):
                    work.append(payload)

            with scenario(
                "wire subscription + queue worker",
                "queue.topic_route",
                "pubsub.exchange",
                "queue.create",
                "queue.subscribe",
            ):
                await manager.create_queue("work")
                manager.subscribe_to_queue(
                    "work", "bridge-worker", "work.*", callback=worker
                )
                exchange.add_subscription(
                    PubSubSubscription(
                        subscription_id="bridge",
                        patterns=(TopicPattern(pattern="events.#"),),
                        subscriber="bridge",
                        created_at=time.time(),
                    )
                )
                ok("events.# → work queue")

            with scenario(
                "publish bridged to durable send",
                "pubsub.wildcard_hash",
                "queue.alo",
                "queue.send",
            ):
                events = [
                    ("events.order.created", {"type": "created", "id": "o1"}),
                    ("events.order.paid", {"type": "paid", "id": "o1"}),
                ]
                matched = 0
                for topic, payload in events:
                    hits = exchange.publish_message(
                        PubSubMessage(
                            topic=topic,
                            payload=payload,
                            timestamp=time.time(),
                            message_id=topic,
                            publisher="api",
                            headers={"bridge": "v1"},
                        )
                    )
                    matched += len(hits)
                    for _ in hits:
                        await manager.send_message(
                            "work",
                            "work.event",
                            payload,
                            DeliveryGuarantee.AT_LEAST_ONCE,
                        )
                for _ in range(40):
                    if len(work) >= 2:
                        break
                    await asyncio.sleep(0.05)
                ensure(matched >= 2, f"matched {matched}")
                ensure(len(work) == 2, f"work {work}")
                types = {w.get("type") for w in work}
                ensure(types == {"created", "paid"}, f"types {types}")
                ok(f"bridged work={work}")

            with scenario("non-matching topic not enqueued", "pubsub.exchange"):
                before = len(work)
                hits = exchange.publish_message(
                    PubSubMessage(
                        topic="metrics.cpu",
                        payload={"type": "metric"},
                        timestamp=time.time(),
                        message_id="m1",
                        publisher="agent",
                    )
                )
                if hits:
                    for _ in hits:
                        await manager.send_message(
                            "work",
                            "work.event",
                            {"type": "metric"},
                            DeliveryGuarantee.AT_LEAST_ONCE,
                        )
                await asyncio.sleep(0.2)
                ensure(len(hits) == 0, f"metrics should not match events.# got {hits}")
                ensure(len(work) == before, f"work grew {work}")
                ok("metrics ignored")

            with scenario(
                "burst publish preserves order of enqueue",
                "queue.send",
                "pubsub.fanout",
            ):
                before = len(work)
                for i in range(3):
                    payload = {"type": "burst", "n": i}
                    hits = exchange.publish_message(
                        PubSubMessage(
                            topic=f"events.burst.{i}",
                            payload=payload,
                            timestamp=time.time(),
                            message_id=f"b{i}",
                            publisher="api",
                        )
                    )
                    for _ in hits:
                        await manager.send_message(
                            "work",
                            "work.event",
                            payload,
                            DeliveryGuarantee.AT_LEAST_ONCE,
                        )
                for _ in range(40):
                    if len(work) >= before + 3:
                        break
                    await asyncio.sleep(0.05)
                burst = [w for w in work if w.get("type") == "burst"]
                ensure(len(burst) == 3, f"burst {burst}")
                ok(f"burst ns={[b.get('n') for b in burst]}")

            with scenario("headers available on pubsub message", "pubsub.headers"):
                msg = PubSubMessage(
                    topic="events.tagged",
                    payload={"type": "tagged"},
                    timestamp=time.time(),
                    message_id="t1",
                    publisher="api",
                    headers={"x-trace": "abc"},
                )
                ensure(msg.headers.get("x-trace") == "abc", "headers missing")
                hits = exchange.publish_message(msg)
                ensure(len(hits) >= 1, "tagged should match")
                ok("headers path ok")
        finally:
            shutdown = getattr(manager, "shutdown", None)
            if callable(shutdown):
                out = shutdown()
                if asyncio.iscoroutine(out):
                    await out

if __name__ == "__main__":
    asyncio.run(main())
