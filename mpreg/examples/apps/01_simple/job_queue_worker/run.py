"""L1 job_queue_worker — at-least-once and quorum queue deliveries."""

from __future__ import annotations

import asyncio
from typing import Any

from mpreg.core.message_queue import DeliveryGuarantee
from mpreg.core.message_queue_manager import create_reliable_queue_manager
from mpreg.examples.apps._shared.runtime import ensure, ok, step

async def main() -> None:
    manager = create_reliable_queue_manager()
    await manager.create_queue("jobs")
    received: list[str] = []

    def worker(message: Any) -> None:
        received.append(str(message.payload))

    manager.subscribe_to_queue("jobs", "worker-1", "jobs.*", callback=worker)
    manager.subscribe_to_queue("jobs", "worker-2", "jobs.*", callback=worker)
    step("two workers subscribed on jobs.*")

    await manager.send_message(
        "jobs",
        "jobs.batch",
        {"task": "build-index"},
        DeliveryGuarantee.AT_LEAST_ONCE,
    )
    await manager.send_message(
        "jobs",
        "jobs.batch",
        {"task": "refresh-cache"},
        DeliveryGuarantee.QUORUM,
        required_acknowledgments=2,
    )
    await asyncio.sleep(0.5)
    ensure(
        len(received) == 4
        and "build-index" in str(received)
        and "refresh-cache" in str(received),
        f"unexpected deliveries: {received}",
    )
    ok(f"queue deliveries={received}")
    await manager.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
