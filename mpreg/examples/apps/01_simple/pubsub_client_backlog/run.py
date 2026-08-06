"""L1 pubsub_client_backlog — MPREGPubSubClient + get_backlog (client.pubsub)."""

from __future__ import annotations

import asyncio
import time

from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.pubsub_client import MPREGPubSubClient
from mpreg.core.config import MPREGSettings
from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.core.port_allocator import port_range_context
from mpreg.core.statistics import MessageHeaders
from mpreg.core.topic_exchange import TopicExchange
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
)
from mpreg.server import MPREGServer

async def main() -> None:
    with app_run(
        "pubsub_client_backlog",
        "PubSub Client Backlog — wire client + backlog",
        level="L1",
    ):
        with scenario(
            "in-process exchange backlog API",
            "pubsub.backlog",
            "pubsub.exchange",
            "pubsub.headers",
        ):
            exchange = TopicExchange("ws://local-backlog", "backlog-lab")
            exchange.add_subscription(
                PubSubSubscription(
                    subscription_id="early",
                    patterns=(TopicPattern(pattern="lab.event.*"),),
                    subscriber="early-svc",
                    created_at=time.time(),
                    get_backlog=True,
                    backlog_seconds=60,
                )
            )
            # Publish before a late subscriber would attach
            for i in range(3):
                exchange.publish_message(
                    PubSubMessage(
                        topic=f"lab.event.{i}",
                        payload={"i": i},
                        timestamp=time.time(),
                        message_id=f"pre-{i}",
                        publisher="seed",
                        headers={"x-seed": "1", "seq": str(i)},
                    )
                )
            backlog = exchange.backlog.get_backlog("lab.event.*", 60)
            ensure(len(backlog) >= 3, f"backlog short {len(backlog)}")
            ensure(
                all(getattr(m, "headers", None) is not None for m in backlog),
                "headers missing on backlog msgs",
            )
            ok(f"exchange backlog n={len(backlog)} with headers")

        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="PubSub-Backlog",
                    resources={"ps"},
                    log_level="WARNING",
                    gossip_interval=30.0,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                url = f"ws://127.0.0.1:{ports[0]}"
                got: list[object] = []

                def _on(msg: object) -> None:
                    got.append(msg)

                with scenario(
                    "MPREGPubSubClient subscribe + publish",
                    "client.pubsub",
                    "pubsub.client_wire",
                    "pubsub.headers",
                ):
                    async with MPREGClientAPI(url) as base:
                        ps = MPREGPubSubClient(base_client=base)
                        await ps.start()
                        try:
                            sub_id = await ps.subscribe(
                                ["demo.sensor.*"],
                                _on,
                                get_backlog=True,
                                backlog_seconds=30,
                            )
                            ensure(bool(sub_id), "empty sub id")
                            ok_pub = await ps.publish(
                                "demo.sensor.temp",
                                {"c": 21.5},
                                headers=MessageHeaders(
                                    correlation_id="backlog-lab",
                                    custom_headers={"x-trace": "backlog-lab"},
                                ),
                            )
                            ensure(ok_pub is True, f"publish returned {ok_pub}")
                            # F22: bare dict headers are coerced (no MessageHeaders required)
                            got.clear()
                            ok_dict = await ps.publish(
                                "demo.sensor.humidity",
                                {"rh": 0.4},
                                headers={
                                    "correlation_id": "dict-headers",
                                    "x-trace": "dict-path",
                                },
                            )
                            ensure(ok_dict is True, f"dict publish returned {ok_dict}")
                            for _ in range(60):
                                if got:
                                    break
                                await asyncio.sleep(0.05)
                            ensure(got, "no notification received")
                            step(f"received n={len(got)} sub={sub_id[:12]}…")
                            ok("wire pubsub client deliver (MessageHeaders + dict)")
                        finally:
                            await ps.stop()

                with scenario(
                    "subscribe get_backlog flag surface",
                    "pubsub.backlog",
                    "client.pubsub",
                ):
                    # Flag is part of the subscribe contract; empty backlog is OK
                    async with MPREGClientAPI(url) as base:
                        ps = MPREGPubSubClient(base_client=base)
                        await ps.start()
                        try:
                            sid = await ps.subscribe(
                                ["demo.empty.*"],
                                lambda _m: None,
                                get_backlog=False,
                                backlog_seconds=5,
                            )
                            ensure(bool(sid), "sub without backlog failed")
                            ok("get_backlog=False accepted")
                        finally:
                            await ps.stop()

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
