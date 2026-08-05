"""L1 plane_pubsub — wildcards, multi-sub fan-out, headers, negatives."""

from __future__ import annotations

import asyncio
import time

from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.core.topic_exchange import TopicExchange
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

def _pub(
    exchange: TopicExchange,
    topic: str,
    payload: dict,
    *,
    headers: dict | None = None,
) -> list:
    return list(
        exchange.publish_message(
            PubSubMessage(
                topic=topic,
                payload=payload,
                timestamp=time.time(),
                message_id=f"{topic}-{time.time()}",
                publisher="plane_pubsub",
                headers=headers or {},
            )
        )
    )

async def main() -> None:
    with app_run("plane_pubsub", "Plane PubSub — full topic tour", level="L1"):
        exchange = TopicExchange("ws://local-plane-pubsub", "plane_pubsub")

        with scenario(
            "install multi-pattern subscriptions",
            "pubsub.exchange",
            "pubsub.wildcard_star",
            "pubsub.wildcard_hash",
        ):
            exchange.add_subscription(
                PubSubSubscription(
                    subscription_id="auth",
                    patterns=(
                        TopicPattern(pattern="user.*.login"),
                        TopicPattern(pattern="user.*.logout"),
                    ),
                    subscriber="auth-svc",
                    created_at=time.time(),
                )
            )
            exchange.add_subscription(
                PubSubSubscription(
                    subscription_id="orders",
                    patterns=(
                        TopicPattern(pattern="order.#"),
                        TopicPattern(pattern="payment.*.completed"),
                    ),
                    subscriber="orders-svc",
                    created_at=time.time(),
                )
            )
            exchange.add_subscription(
                PubSubSubscription(
                    subscription_id="audit",
                    patterns=(TopicPattern(pattern="user.*.login"),),
                    subscriber="audit-svc",
                    created_at=time.time(),
                )
            )
            ok("auth + orders + audit subscriptions")

        with scenario("login fan-out to auth+audit", "pubsub.fanout", "pubsub.wildcard_star"):
            hits = _pub(exchange, "user.123.login", {"user": "alice"})
            ensure(len(hits) >= 2, f"fan-out expected >=2 got {len(hits)}")
            ok(f"login matches={len(hits)}")

        with scenario("order hash + payment star", "pubsub.wildcard_hash"):
            h1 = _pub(exchange, "order.us.456.created", {"order": 456})
            h2 = _pub(exchange, "payment.card.completed", {"order": 456})
            ensure(len(h1) >= 1 and len(h2) >= 1, f"order/payment {h1} {h2}")
            ok("hash and payment patterns matched")

        with scenario("negative + headers", "pubsub.headers", "pubsub.exchange"):
            noise = _pub(exchange, "system.tick", {"n": 1})
            ensure(len(noise) == 0, f"noise matched {noise}")
            hdr = _pub(
                exchange,
                "user.7.logout",
                {"user": "z"},
                headers={"x-trace": "plane-1"},
            )
            ensure(len(hdr) >= 1, "logout should match auth")
            ok("negative + headers ok")

        # Aggregate count mirrors tier1 demo expectation for core topics
        total = 0
        for topic, payload in (
            ("user.1.login", {}),
            ("order.x.y", {}),
            ("payment.x.completed", {}),
        ):
            total += len(_pub(exchange, topic, payload))
        ensure(total >= 3, f"core tour total matches {total}")
        step(f"core tour total matches={total}")
        ok("plane_pubsub tour complete")

if __name__ == "__main__":
    asyncio.run(main())
