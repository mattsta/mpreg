"""L1 sensor_ingest_pubsub — multi-pattern sensor topic fan-out."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.core.topic_exchange import TopicExchange
from mpreg.examples.apps._shared.runtime import ensure, ok, step

async def main() -> None:
    exchange = TopicExchange("ws://local-sensors", "sensors")
    buckets: dict[str, list[Any]] = {"temp": [], "pressure": [], "all": []}

    exchange.add_subscription(
        PubSubSubscription(
            subscription_id="temp-watch",
            patterns=(TopicPattern(pattern="sensor.temp.*"),),
            subscriber="temp-svc",
            created_at=time.time(),
        )
    )
    exchange.add_subscription(
        PubSubSubscription(
            subscription_id="pressure-watch",
            patterns=(TopicPattern(pattern="sensor.pressure.*"),),
            subscriber="pressure-svc",
            created_at=time.time(),
        )
    )
    exchange.add_subscription(
        PubSubSubscription(
            subscription_id="all-sensors",
            patterns=(TopicPattern(pattern="sensor.#"),),
            subscriber="all-svc",
            created_at=time.time(),
        )
    )
    step("watching sensor.temp.*, sensor.pressure.*, sensor.#")

    events = [
        ("sensor.temp.rack-a", {"c": 22.5}),
        ("sensor.temp.rack-b", {"c": 23.1}),
        ("sensor.pressure.line-1", {"kpa": 101.3}),
        ("sensor.humidity.room", {"pct": 40}),
    ]
    total_hits = 0
    for topic, payload in events:
        hits = exchange.publish_message(
            PubSubMessage(
                topic=topic,
                payload=payload,
                timestamp=time.time(),
                message_id=topic,
                publisher="ingest",
            )
        )
        total_hits += len(hits)
        if "temp" in topic:
            buckets["temp"].append(payload)
        if "pressure" in topic:
            buckets["pressure"].append(payload)
        if topic.startswith("sensor."):
            buckets["all"].append(payload)
        step(f"{topic} -> {len(hits)} match(es)")

    # temp:2 topics * (temp-watch + all) = 4; pressure:1*(pressure+all)=2; humidity:1*all=1 → 7
    ensure(total_hits == 7, f"expected 7 matches got {total_hits}")
    ensure(len(buckets["temp"]) == 2, "temp bucket")
    ensure(len(buckets["pressure"]) == 1, "pressure bucket")
    ensure(len(buckets["all"]) == 4, "all bucket")
    ok(f"sensor ingest matches={total_hits} buckets={ {k: len(v) for k, v in buckets.items()} }")

if __name__ == "__main__":
    asyncio.run(main())
