"""L1 sensor_ingest_pubsub — multi-pattern fan-out and negative matches (pubsub.*)."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.core.topic_exchange import TopicExchange
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

def _publish(exchange: TopicExchange, topic: str, payload: dict[str, Any]) -> list:
    return list(
        exchange.publish_message(
            PubSubMessage(
                topic=topic,
                payload=payload,
                timestamp=time.time(),
                message_id=f"{topic}-{time.time()}",
                publisher="ingest",
                headers={"source": "sensor_ingest"},
            )
        )
    )

async def main() -> None:
    with app_run(
        "sensor_ingest_pubsub",
        "Sensor Ingest — multi-pattern bus",
        level="L1",
    ):
        exchange = TopicExchange("ws://local-sensors", "sensors")
        buckets: dict[str, list[Any]] = {"temp": [], "pressure": [], "all": []}

        with scenario(
            "install pattern subscriptions",
            "pubsub.exchange",
            "pubsub.wildcard_star",
            "pubsub.wildcard_hash",
        ):
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
            ok("watching sensor.temp.*, sensor.pressure.*, sensor.#")

        with scenario("typed sensor fan-out counts", "pubsub.fanout", "pubsub.wildcard_star"):
            events = [
                ("sensor.temp.rack-a", {"c": 22.5}),
                ("sensor.temp.rack-b", {"c": 23.1}),
                ("sensor.pressure.line-1", {"kpa": 101.3}),
                ("sensor.humidity.room", {"pct": 40}),
            ]
            total_hits = 0
            for topic, payload in events:
                hits = _publish(exchange, topic, payload)
                total_hits += len(hits)
                if "temp" in topic:
                    buckets["temp"].append(payload)
                if "pressure" in topic:
                    buckets["pressure"].append(payload)
                if topic.startswith("sensor."):
                    buckets["all"].append(payload)
                step(f"{topic} -> {len(hits)} match(es)")

            # temp:2*(temp+all)=4; pressure:1*(pressure+all)=2; humidity:1*all=1 → 7
            ensure(total_hits == 7, f"expected 7 matches got {total_hits}")
            ensure(len(buckets["temp"]) == 2, "temp bucket")
            ensure(len(buckets["pressure"]) == 1, "pressure bucket")
            ensure(len(buckets["all"]) == 4, "all bucket")
            ok(
                f"matches={total_hits} buckets="
                f"{ {k: len(v) for k, v in buckets.items()} }"
            )

        with scenario("negative match outside sensor tree", "pubsub.exchange"):
            hits = _publish(exchange, "plant.actuator.valve", {"open": True})
            ensure(len(hits) == 0, f"actuator should not match sensors: {hits}")
            ok("non-sensor topic unmatched")

        with scenario("headers on sensor publish", "pubsub.headers"):
            hits = _publish(exchange, "sensor.temp.rack-c", {"c": 21.0, "hdr": True})
            ensure(len(hits) >= 2, f"temp+all expected >=2 got {len(hits)}")
            ok("publish with headers matched temp+all")

if __name__ == "__main__":
    asyncio.run(main())
