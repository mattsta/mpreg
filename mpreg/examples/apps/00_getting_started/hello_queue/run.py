"""L0 hello_queue — create, send, subscribe, ack-style delivery."""

from __future__ import annotations

import asyncio
from typing import Any

from mpreg.core.message_queue import (
    DeliveryGuarantee,
    MessageQueue,
    QueueConfiguration,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step


async def main() -> None:
    with app_run("hello_queue", "Hello Queue — send + subscribe drill", level="L0"):
        config = QueueConfiguration(
            name="hello-jobs",
            default_acknowledgment_timeout_seconds=2.0,
            max_retries=1,
            enable_dead_letter_queue=False,
        )
        queue = MessageQueue(config)
        try:
            delivered: list[Any] = []

            def worker(message: Any) -> None:
                payload = getattr(message, "payload", message)
                delivered.append(payload)
                step(f"worker got {payload!r}")

            with scenario("subscribe worker", "queue.subscribe", "queue.create"):
                queue.subscribe(
                    "hello-worker",
                    "jobs.*",
                    callback=worker,
                    auto_acknowledge=True,
                )
                ok("worker subscribed on jobs.*")

            with scenario("send at-least-once job", "queue.send", "queue.alo"):
                result = await queue.send_message(
                    "jobs.greet",
                    {"hello": "queue", "n": 1},
                    DeliveryGuarantee.AT_LEAST_ONCE,
                )
                ensure(result.success, f"send failed: {result}")
                ok(f"sent id={getattr(result, 'message_id', result)}")

            with scenario("worker delivers payload", "queue.alo"):
                for _ in range(40):
                    if delivered:
                        break
                    await asyncio.sleep(0.05)
                ensure(len(delivered) >= 1, f"no delivery: {delivered}")
                ensure(
                    isinstance(delivered[0], dict)
                    and delivered[0].get("hello") == "queue",
                    f"bad payload {delivered[0]!r}",
                )
                ok(f"delivered={delivered[0]}")

            with scenario("second message still delivers", "queue.send"):
                before = len(delivered)
                r2 = await queue.send_message(
                    "jobs.greet",
                    {"hello": "again", "n": 2},
                    DeliveryGuarantee.AT_LEAST_ONCE,
                )
                ensure(r2.success, f"second send failed: {r2}")
                for _ in range(40):
                    if len(delivered) > before:
                        break
                    await asyncio.sleep(0.05)
                ensure(len(delivered) == before + 1, f"expected +1 got {delivered}")
                ok(f"second payload={delivered[-1]}")
        finally:
            # MessageQueue may not expose shutdown; best-effort
            stop = getattr(queue, "shutdown", None) or getattr(queue, "stop", None)
            if callable(stop):
                out = stop()
                if asyncio.iscoroutine(out):
                    await out


if __name__ == "__main__":
    asyncio.run(main())
