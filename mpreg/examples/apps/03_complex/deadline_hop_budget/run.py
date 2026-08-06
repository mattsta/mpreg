"""L3 deadline_hop_budget — DeadlineBudget hop decrement on MessageHeaders."""

from __future__ import annotations

import asyncio
import time

from mpreg.core.rpc_deadline import DeadlineBudget, decrement_deadline_headers
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.fabric.message import MessageHeaders

async def main() -> None:
    with app_run(
        "deadline_hop_budget",
        "Deadline Hop Budget — fabric header remaining_ms",
        level="L3",
    ):
        with scenario("from_seconds budget", "rpc.deadline", "fabric.deadline_hop"):
            b = DeadlineBudget.from_seconds(1.0)
            ensure(b.remaining_ms >= 999.0, f"remaining_ms {b.remaining_ms}")
            ensure(b.exhausted() is False, "fresh budget exhausted?")
            ok(f"budget remaining_ms={b.remaining_ms:.1f}")

        with scenario("apply_to_headers stamps deadline", "fabric.deadline_hop"):
            b = DeadlineBudget.from_seconds(0.8)
            h = MessageHeaders(correlation_id="corr-1")
            h2 = b.apply_to_headers(h)
            ensure(h2.deadline_remaining_ms is not None, "missing deadline field")
            ensure(h2.deadline_remaining_ms <= 800.0, f"ms {h2.deadline_remaining_ms}")
            ensure(
                "mpreg.deadline_remaining_ms" in h2.metadata
                or h2.deadline_remaining_ms is not None,
                f"meta {h2.metadata}",
            )
            ok(f"header deadline_remaining_ms={h2.deadline_remaining_ms:.1f}")

        with scenario(
            "decrement per hop latency", "fabric.deadline_hop", "rpc.deadline"
        ):
            b = DeadlineBudget.from_seconds(1.0)
            h = b.apply_to_headers(MessageHeaders(correlation_id="corr-2"))
            before = float(h.deadline_remaining_ms or 0)
            h2 = decrement_deadline_headers(h, hop_latency_ms=40.0)
            after = float(h2.deadline_remaining_ms or 0)
            ensure(after < before, f"expected decrement {before}→{after}")
            ensure(before - after >= 39.0, f"delta {before - after}")
            ok(f"hop -40ms: {before:.1f}→{after:.1f}")

        with scenario("multi-hop until near exhaust", "fabric.deadline_hop"):
            b = DeadlineBudget.from_seconds(0.15)
            h = b.apply_to_headers(MessageHeaders(correlation_id="corr-3"))
            hops = 0
            while hops < 20 and (h.deadline_remaining_ms or 0) > 5:
                h = decrement_deadline_headers(h, hop_latency_ms=20.0)
                hops += 1
            ensure(hops >= 1, "no hops")
            ok(f"hops={hops} remaining={h.deadline_remaining_ms}")

        with scenario("raise_if_exhausted fail-closed", "rpc.deadline"):
            b = DeadlineBudget(remaining_ms=1.0, started_mono=time.monotonic() - 1.0)
            ensure(b.exhausted() is True, "should be exhausted")
            failed = False
            try:
                b.raise_if_exhausted(details="demo hop")
            except Exception as exc:
                failed = True
                step(f"expected: {type(exc).__name__}: {exc}")
            ensure(failed, "exhausted must raise")
            ok("raise_if_exhausted fail-closed")

        with scenario("from_headers round-trip", "fabric.deadline_hop"):
            b = DeadlineBudget.from_seconds(0.5)
            h = b.apply_to_headers(MessageHeaders(correlation_id="corr-4"))
            b2 = DeadlineBudget.from_headers(h)
            ensure(b2 is not None, "from_headers None")
            ensure(b2.remaining_ms > 0, f"b2 {b2}")
            ok(f"from_headers remaining_ms={b2.remaining_ms:.1f}")

        await asyncio.sleep(0)

if __name__ == "__main__":
    asyncio.run(main())
