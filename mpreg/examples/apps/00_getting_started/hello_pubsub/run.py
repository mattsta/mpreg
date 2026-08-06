"""L0 hello_pubsub — wildcards, fan-out, negative match (pubsub.*)."""

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
                publisher="hello_pubsub",
                headers=headers or {},
            )
        )
    )

async def main() -> None:
    with app_run("hello_pubsub", "Hello PubSub — wildcards + fan-out", level="L0"):
        exchange = TopicExchange("ws://local-hello-pubsub", "hello_pubsub")

        with scenario("subscribe patterns", "pubsub.exchange", "pubsub.wildcard_star"):
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
            # Second subscriber on same login pattern → fan-out
            exchange.add_subscription(
                PubSubSubscription(
                    subscription_id="audit",
                    patterns=(TopicPattern(pattern="user.*.login"),),
                    subscriber="audit-svc",
                    created_at=time.time(),
                )
            )
            ok("subscriptions: auth, orders, audit")

        with scenario(
            "star wildcard + fan-out", "pubsub.wildcard_star", "pubsub.fanout"
        ):
            hits = _pub(exchange, "user.123.login", {"user": "alice"})
            ensure(
                len(hits) >= 2, f"login fan-out expected >=2 got {len(hits)}: {hits}"
            )
            step(f"user.123.login -> {len(hits)} match(es)")
            ok(f"fan-out matches={len(hits)}")

        with scenario("hash multi-segment", "pubsub.wildcard_hash"):
            hits = _pub(exchange, "order.us.456.created", {"order": 456})
            ensure(len(hits) >= 1, f"order.# expected match, got {hits}")
            hits2 = _pub(exchange, "payment.card.completed", {"order": 456})
            ensure(len(hits2) >= 1, f"payment.*.completed expected match, got {hits2}")
            ok("hash + payment star matched")

        with scenario("negative match (no subscriber)", "pubsub.exchange"):
            hits = _pub(exchange, "noise.unrelated.event", {"x": 1})
            ensure(len(hits) == 0, f"noise should not match, got {hits}")
            ok("unrelated topic correctly unmatched")

        with scenario("headers pass-through shape", "pubsub.headers"):
            hits = _pub(
                exchange,
                "user.9.logout",
                {"user": "zoe"},
                headers={"x-trace": "t-1", "tenant": "demo"},
            )
            ensure(len(hits) >= 1, "logout should match auth")
            ok("publish with headers accepted")

if __name__ == "__main__":
    asyncio.run(main())
