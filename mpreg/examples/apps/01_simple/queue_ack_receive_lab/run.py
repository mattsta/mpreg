"""L1 queue_ack_receive_lab — poll receive + explicit ack (queue.receive/ack)."""

from __future__ import annotations

import asyncio

from mpreg.core.message_queue import DeliveryGuarantee
from mpreg.core.message_queue_manager import create_reliable_queue_manager
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step


async def main() -> None:
    with app_run(
        "queue_ack_receive_lab",
        "Queue Ack/Receive — poll + explicit acknowledge",
        level="L1",
    ):
        manager = create_reliable_queue_manager()
        try:
            with scenario(
                "create queue + ALO send",
                "queue.create",
                "queue.send",
                "queue.alo",
            ):
                await manager.create_queue("ack-lab")
                sent = await manager.send_message(
                    "ack-lab",
                    "jobs.paint",
                    {"task": "paint", "n": 1},
                    DeliveryGuarantee.AT_LEAST_ONCE,
                )
                ensure(sent.success, f"send failed: {sent.error_message}")
                mid = str(sent.message_id)
                ensure(bool(mid), "empty message_id")
                ok(f"sent message_id={mid[:24]}…")

            with scenario(
                "receive_message poll path",
                "queue.receive",
                "queue.subscribe",
            ):
                msg = await manager.receive_message(
                    "ack-lab",
                    subscriber_id="poller-1",
                    topic_pattern="jobs.*",
                    timeout_seconds=3.0,
                    auto_acknowledge=False,
                )
                ensure(msg is not None, "receive timed out")
                payload = getattr(msg, "payload", msg)
                if isinstance(payload, dict):
                    ensure(payload.get("task") == "paint", f"payload {payload}")
                msg_id = str(getattr(msg, "id", None) or getattr(msg, "message_id", ""))
                ensure(bool(msg_id), f"no id on {type(msg).__name__}")
                step(f"received type={type(msg).__name__} id={msg_id[:24]}…")
                ok("poll receive delivered message")

            with scenario(
                "acknowledge_message explicit ack",
                "queue.ack",
            ):
                # Re-send so we have a known in-flight for explicit ack path
                received: list[object] = []

                def _cb(message: object) -> None:
                    received.append(message)

                await manager.send_message(
                    "ack-lab",
                    "jobs.seal",
                    {"task": "seal"},
                    DeliveryGuarantee.AT_LEAST_ONCE,
                )
                sub = manager.subscribe_to_queue(
                    "ack-lab",
                    "ack-worker",
                    "jobs.*",
                    callback=_cb,
                    auto_acknowledge=False,
                )
                ensure(sub is not None, "subscribe failed")
                for _ in range(40):
                    if received:
                        break
                    await asyncio.sleep(0.05)
                ensure(received, "callback never fired")
                m0 = received[0]
                ack_id = str(getattr(m0, "id", None) or getattr(m0, "message_id", ""))
                ensure(bool(ack_id), "missing message id for ack")
                acked = await manager.acknowledge_message(
                    "ack-lab", ack_id, "ack-worker"
                )
                ensure(acked is True, f"ack returned {acked}")
                ok(f"explicit ack ok id={ack_id[:24]}…")

            with scenario(
                "broadcast + FNF still accepted",
                "queue.broadcast",
                "queue.fnf",
            ):
                hits: list[str] = []

                def fan(message: object) -> None:
                    p = getattr(message, "payload", message)
                    hits.append(str(p))

                manager.subscribe_to_queue(
                    "ack-lab", "fan-a", "jobs.*", callback=fan, auto_acknowledge=True
                )
                manager.subscribe_to_queue(
                    "ack-lab", "fan-b", "jobs.*", callback=fan, auto_acknowledge=True
                )
                await manager.send_message(
                    "ack-lab",
                    "jobs.announce",
                    {"task": "announce"},
                    DeliveryGuarantee.BROADCAST,
                )
                await manager.send_message(
                    "ack-lab",
                    "jobs.tick",
                    {"task": "tick"},
                    DeliveryGuarantee.FIRE_AND_FORGET,
                )
                await asyncio.sleep(0.4)
                ensure(
                    any("announce" in h for h in hits),
                    f"broadcast miss hits={hits}",
                )
                ok(f"broadcast+fnf surface; hits={len(hits)}")

            ok("queue_ack_receive_lab complete")
        finally:
            await manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
