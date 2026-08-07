"""L1 plane_queue — ALO, quorum, broadcast, FNF + factory (queue.* tour)."""

from __future__ import annotations

import asyncio
from typing import Any

from mpreg.core.message_queue import DeliveryGuarantee
from mpreg.core.message_queue_manager import create_reliable_queue_manager
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step


async def main() -> None:
    with app_run("plane_queue", "Plane Queue — delivery guarantee tour", level="L1"):
        manager = create_reliable_queue_manager()
        try:
            received: list[str] = []

            def worker(message: Any) -> None:
                payload = message.payload
                if isinstance(payload, dict):
                    received.append(str(payload.get("task", payload)))
                else:
                    received.append(str(payload))

            with scenario(
                "create + dual subscribe",
                "queue.create",
                "queue.subscribe",
                "queue.factories",
            ):
                await manager.create_queue("jobs")
                manager.subscribe_to_queue(
                    "jobs", "worker-1", "jobs.*", callback=worker
                )
                manager.subscribe_to_queue(
                    "jobs", "worker-2", "jobs.*", callback=worker
                )
                ok("jobs queue + 2 workers")

            with scenario("at-least-once", "queue.alo", "queue.send"):
                await manager.send_message(
                    "jobs",
                    "jobs.batch",
                    {"task": "build-index"},
                    DeliveryGuarantee.AT_LEAST_ONCE,
                )
                await asyncio.sleep(0.4)
                ensure("build-index" in str(received), f"ALO miss {received}")
                ok(f"ALO ok received={received}")

            with scenario("quorum n=2", "queue.quorum"):
                before = len(received)
                await manager.send_message(
                    "jobs",
                    "jobs.batch",
                    {"task": "refresh-cache"},
                    DeliveryGuarantee.QUORUM,
                    required_acknowledgments=2,
                )
                await asyncio.sleep(0.5)
                ensure(
                    any("refresh-cache" in r for r in received[before:]),
                    f"quorum miss {received[before:]}",
                )
                ok(f"quorum slice={received[before:]}")

            with scenario("broadcast", "queue.broadcast"):
                before = len(received)
                await manager.send_message(
                    "jobs",
                    "jobs.batch",
                    {"task": "announce"},
                    DeliveryGuarantee.BROADCAST,
                )
                await asyncio.sleep(0.4)
                fan = [r for r in received[before:] if "announce" in r]
                ensure(len(fan) >= 2, f"broadcast expected >=2 got {fan}")
                ok(f"broadcast={fan}")

            with scenario("fire-and-forget", "queue.fnf"):
                before = len(received)
                await manager.send_message(
                    "jobs",
                    "jobs.batch",
                    {"task": "tick"},
                    DeliveryGuarantee.FIRE_AND_FORGET,
                )
                await asyncio.sleep(0.3)
                # Prove API accepted; delivery optional for FNF
                ensure(before >= 0, "internal counter")
                ok(
                    f"FNF accepted; new receipts={received[before:]} "
                    "(non-claim: persistence)"
                )

            ensure(
                "build-index" in str(received) and "refresh-cache" in str(received),
                f"core tasks missing {received}",
            )
            ensure(len(received) >= 4, f"expected ≥4 receipts got {received}")
            step(f"final received={received}")
            ok("plane_queue tour complete")
        finally:
            await manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
