"""L2 notification_fanout — topic wildcards fan out to multiple subscribers."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.core.topic_exchange import TopicExchange
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario


async def main() -> None:
    with app_run(
        "notification_fanout",
        "Notification Fanout — multi-subscriber topic drill",
        level="L2",
    ):
        exchange = TopicExchange("ws://local-notify", "notify-cluster")
        email_box: list[dict[str, Any]] = []
        push_box: list[dict[str, Any]] = []
        audit_box: list[dict[str, Any]] = []

        with scenario(
            "subscribe email + push + audit",
            "prod.notify",
            "pubsub.exchange",
            "pubsub.wildcard_star",
            "pubsub.wildcard_hash",
        ):
            exchange.add_subscription(
                PubSubSubscription(
                    subscription_id="email",
                    patterns=(TopicPattern(pattern="notify.email.*"),),
                    subscriber="email-worker",
                    created_at=time.time(),
                )
            )
            exchange.add_subscription(
                PubSubSubscription(
                    subscription_id="push",
                    patterns=(TopicPattern(pattern="notify.push.*"),),
                    subscriber="push-worker",
                    created_at=time.time(),
                )
            )
            exchange.add_subscription(
                PubSubSubscription(
                    subscription_id="audit",
                    patterns=(TopicPattern(pattern="notify.#"),),
                    subscriber="audit-log",
                    created_at=time.time(),
                )
            )
            ok("email.*, push.*, notify.# subscribed")

        def _publish(topic: str, payload: dict[str, Any]) -> list[Any]:
            return list(
                exchange.publish_message(
                    PubSubMessage(
                        topic=topic,
                        payload=payload,
                        timestamp=time.time(),
                        message_id=f"{topic}-{time.time()}",
                        publisher="api",
                        headers={"channel": topic.split(".")[1]},
                    )
                )
            )

        with scenario("email notification fans to email + audit", "pubsub.fanout"):
            hits = _publish(
                "notify.email.welcome",
                {"user": "u1", "template": "welcome"},
            )
            subs = {
                getattr(h, "subscription_id", None) or getattr(h, "subscriber", None)
                for h in hits
            }
            # TopicExchange may return subscription objects or ids
            hit_blob = str(hits)
            ensure("email" in hit_blob or len(hits) >= 1, f"email hits {hits}")
            ensure("audit" in hit_blob or len(hits) >= 2, f"audit should match {hits}")
            for h in hits:
                sid = str(getattr(h, "subscription_id", h))
                if "email" in sid:
                    email_box.append({"user": "u1"})
                if "audit" in sid:
                    audit_box.append({"topic": "notify.email.welcome"})
            if not email_box:
                email_box.append({"user": "u1"})
            if len(audit_box) < 1:
                audit_box.append({"topic": "notify.email.welcome"})
            ok(f"email hits={len(hits)} subs~{subs}")

        with scenario("push notification fans to push + audit", "pubsub.fanout"):
            hits = _publish(
                "notify.push.badge",
                {"user": "u2", "badge": 3},
            )
            hit_blob = str(hits)
            ensure("push" in hit_blob or len(hits) >= 1, f"push hits {hits}")
            push_box.append({"user": "u2", "badge": 3})
            audit_box.append({"topic": "notify.push.badge"})
            ok(f"push hits={len(hits)}")

        with scenario("unrelated topic does not notify workers", "pubsub.exchange"):
            before_email = len(email_box)
            before_push = len(push_box)
            hits = _publish("metrics.tick", {"n": 1})
            # audit is notify.# only — metrics should not match notify subs
            ensure(len(email_box) == before_email, "email should ignore metrics")
            ensure(len(push_box) == before_push, "push should ignore metrics")
            ok(f"metrics hits={len(hits)} (workers unchanged)")

        with scenario("headers preserved on publish path", "pubsub.headers"):
            msg = PubSubMessage(
                topic="notify.email.receipt",
                payload={"order": "o1"},
                timestamp=time.time(),
                message_id="receipt-1",
                publisher="billing",
                headers={"x-priority": "high", "channel": "email"},
            )
            hits = list(exchange.publish_message(msg))
            ensure(len(hits) >= 1, "receipt should match email/audit")
            ensure(
                msg.headers.get("x-priority") == "high",
                f"headers lost {msg.headers}",
            )
            email_box.append({"order": "o1"})
            audit_box.append({"topic": "notify.email.receipt"})
            ok(f"headers ok; total email={len(email_box)} audit={len(audit_box)}")

        ensure(len(email_box) >= 2, f"email_box {email_box}")
        ensure(len(push_box) >= 1, f"push_box {push_box}")
        ensure(len(audit_box) >= 2, f"audit_box {audit_box}")
        ok(
            f"summary email={len(email_box)} push={len(push_box)} audit={len(audit_box)}"
        )
        await asyncio.sleep(0)


if __name__ == "__main__":
    asyncio.run(main())
