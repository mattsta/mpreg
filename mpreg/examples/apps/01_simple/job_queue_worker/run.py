"""L1 job_queue_worker — ALO, quorum, broadcast, FNF delivery guarantees (queue.*)."""

from __future__ import annotations

import asyncio
from typing import Any

from mpreg.core.message_queue import DeliveryGuarantee
from mpreg.core.message_queue_manager import create_reliable_queue_manager
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

async def main() -> None:
    with app_run(
        "job_queue_worker",
        "Job Queue — ALO / quorum / broadcast / FNF",
        level="L1",
    ):
        manager = create_reliable_queue_manager()
        try:
            with scenario("create queue + dual workers", "queue.create", "queue.subscribe", "queue.factories"):
                await manager.create_queue("jobs")
                received: list[str] = []

                def worker(message: Any) -> None:
                    payload = message.payload
                    if isinstance(payload, dict):
                        received.append(str(payload.get("task", payload)))
                    else:
                        received.append(str(payload))

                manager.subscribe_to_queue("jobs", "worker-1", "jobs.*", callback=worker)
                manager.subscribe_to_queue("jobs", "worker-2", "jobs.*", callback=worker)
                step("two workers on jobs.*")
                ok("queue jobs ready")

            with scenario("at-least-once delivery", "queue.send", "queue.alo"):
                before = len(received)
                await manager.send_message(
                    "jobs",
                    "jobs.batch",
                    {"task": "build-index"},
                    DeliveryGuarantee.AT_LEAST_ONCE,
                )
                await asyncio.sleep(0.4)
                ensure(
                    any("build-index" in r for r in received[before:]),
                    f"ALO missing build-index in {received}",
                )
                ok(f"ALO delivered; received={received}")

            with scenario("quorum requires N acks", "queue.quorum", "queue.send"):
                before = len(received)
                await manager.send_message(
                    "jobs",
                    "jobs.batch",
                    {"task": "refresh-cache"},
                    DeliveryGuarantee.QUORUM,
                    required_acknowledgments=2,
                )
                await asyncio.sleep(0.5)
                # Quorum with 2 workers typically fans to both
                ensure(
                    any("refresh-cache" in r for r in received[before:]),
                    f"quorum missing refresh-cache in {received}",
                )
                ok(f"quorum path delivered; slice={received[before:]}")

            with scenario("broadcast to all subscribers", "queue.broadcast"):
                before = len(received)
                await manager.send_message(
                    "jobs",
                    "jobs.batch",
                    {"task": "fanout-stats"},
                    DeliveryGuarantee.BROADCAST,
                )
                await asyncio.sleep(0.4)
                fan = [r for r in received[before:] if "fanout-stats" in r]
                ensure(len(fan) >= 2, f"broadcast expected >=2 deliveries got {fan}")
                ok(f"broadcast deliveries={fan}")

            with scenario("fire-and-forget", "queue.fnf"):
                before = len(received)
                await manager.send_message(
                    "jobs",
                    "jobs.batch",
                    {"task": "metrics-tick"},
                    DeliveryGuarantee.FIRE_AND_FORGET,
                )
                await asyncio.sleep(0.4)
                # FNF may or may not hit workers depending on implementation;
                # prove the send API accepts the guarantee without error.
                ok(
                    f"FNF send accepted; new receipts={received[before:]} "
                    "(non-claim: durability)"
                )

            ensure(
                "build-index" in str(received) and "refresh-cache" in str(received),
                f"core tasks missing from {received}",
            )
            ok(f"final received={received}")
        finally:
            await manager.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
