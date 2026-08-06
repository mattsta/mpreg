"""L1 job_queue_dlq — poison message retries then dead-letter queue."""

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
    with app_run(
        "job_queue_dlq",
        "Job Queue DLQ — retries then dead-letter",
        level="L1",
    ):
        config = QueueConfiguration(
            name="poison-jobs",
            default_acknowledgment_timeout_seconds=0.35,
            max_retries=2,
            enable_dead_letter_queue=True,
            dead_letter_max_receives=3,
        )
        queue = MessageQueue(config)
        try:
            attempts = {"n": 0}
            dlq_hooks = {"n": 0}

            def on_dlq(_count: int = 1) -> None:
                dlq_hooks["n"] += 1

            queue.on_dlq = on_dlq

            with scenario(
                "subscribe failing worker (no ack)",
                "queue.subscribe",
                "queue.create",
            ):

                def failing_worker(message: Any) -> None:
                    attempts["n"] += 1
                    step(
                        f"worker attempt={attempts['n']} id={getattr(message, 'id', '?')}"
                    )
                    # auto_acknowledge=False path: do not ack → timeout → retry

                queue.subscribe(
                    "poison-worker",
                    "jobs.*",
                    callback=failing_worker,
                    auto_acknowledge=False,
                )
                ok("failing worker subscribed (no auto-ack)")

            with scenario(
                "send poison message with max_retries=2",
                "queue.send",
                "queue.alo",
            ):
                result = await queue.send_message(
                    "jobs.poison",
                    {"task": "bad-payload", "n": 1},
                    DeliveryGuarantee.AT_LEAST_ONCE,
                    max_retries=2,
                )
                ensure(result.success, f"send failed: {result}")
                ok(f"sent message_id={getattr(result, 'message_id', result)}")

            with scenario(
                "retries exhaust then DLQ receives message",
                "queue.dlq",
            ):
                # initial + 2 retries ≈ 3 attempts; ack timeout 0.35s each
                await asyncio.sleep(2.5)
                ensure(
                    attempts["n"] >= 3,
                    f"expected ≥3 delivery attempts got {attempts['n']}",
                )
                dlq = list(queue.dead_letter_queue)
                ensure(
                    len(dlq) >= 1, f"DLQ empty after poison; attempts={attempts['n']}"
                )
                payload = getattr(dlq[0], "payload", None)
                ensure(
                    isinstance(payload, dict) and payload.get("task") == "bad-payload",
                    f"bad DLQ payload {payload}",
                )
                ok(
                    f"DLQ size={len(dlq)} attempts={attempts['n']} "
                    f"on_dlq_hooks={dlq_hooks['n']}"
                )

            with scenario(
                "statistics reflect failures/requeues",
                "queue.dlq",
            ):
                stats = queue.get_statistics()
                ensure(
                    stats.messages_requeued >= 2, f"requeued={stats.messages_requeued}"
                )
                ensure(
                    stats.messages_failed >= 1 or len(queue.dead_letter_queue) >= 1,
                    f"no failure accounting stats={stats}",
                )
                ok(f"requeued={stats.messages_requeued} failed={stats.messages_failed}")

            with scenario(
                "healthy message still delivers after poison",
                "queue.alo",
                "queue.send",
            ):
                good: list[str] = []

                def good_worker(message: Any) -> None:
                    payload = getattr(message, "payload", message)
                    if isinstance(payload, dict):
                        good.append(str(payload.get("task", payload)))
                    else:
                        good.append(str(payload))

                # Second subscriber with auto-ack for healthy path
                queue.subscribe(
                    "healthy-worker",
                    "jobs.healthy.*",
                    callback=good_worker,
                    auto_acknowledge=True,
                )
                sent = await queue.send_message(
                    "jobs.healthy.ok",
                    {"task": "ok-job"},
                    DeliveryGuarantee.AT_LEAST_ONCE,
                )
                ensure(sent.success, f"healthy send failed {sent}")
                for _ in range(40):
                    if good:
                        break
                    await asyncio.sleep(0.05)
                ensure(any("ok-job" in g for g in good), f"healthy miss {good}")
                ok(f"healthy delivered={good}")
        finally:
            await queue.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
