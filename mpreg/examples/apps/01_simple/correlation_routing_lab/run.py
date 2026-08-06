"""L1 correlation_routing_lab — CorrelationTracker + no-loop assert (Phase K)."""

from __future__ import annotations

import asyncio

from mpreg.core.transport.correlation import (
    CorrelationConfig,
    CorrelationTracker,
    create_correlation_tracker,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.testing.faults import FaultInjector, assert_no_routing_loop

async def main() -> None:
    with app_run(
        "correlation_routing_lab",
        "Correlation + Routing Loop Lab",
        level="L1",
    ):
        with scenario(
            "create_correlation_tracker + start/complete",
            "tx.correlation",
            "mon.correlation",
        ):
            tracker = create_correlation_tracker()
            ensure(isinstance(tracker, CorrelationTracker), str(type(tracker)))
            cid = tracker.start_correlation()
            ensure(cid in tracker.active_correlations, "not active")
            result = tracker.complete_correlation(
                cid,
                response_data=b"ok",
                endpoint="ws://127.0.0.1:1",
                connection_id="c1",
                success=True,
            )
            ensure(result.success is True, f"success {result.success}")
            ensure(result.correlation_id == cid, "id mismatch")
            ensure(cid not in tracker.active_correlations, "still active")
            ensure(len(tracker.correlation_history) >= 1, "no history")
            ok(f"cid={cid[:8]}… latency_ms={result.latency_ms:.3f}")

        with scenario(
            "CorrelationConfig knobs",
            "tx.correlation",
        ):
            cfg = CorrelationConfig(
                correlation_timeout_ms=5000.0,
                max_correlation_history=100,
            )
            t2 = CorrelationTracker(config=cfg)
            ensure(t2.config.correlation_timeout_ms == 5000.0, "timeout")
            ok("config applied")

        with scenario(
            "assert_no_routing_loop INV-R2",
            "chaos.no_loop",
            "oracle.routing",
        ):
            assert_no_routing_loop(["us", "core", "eu"])
            ok("unique hops pass")
            raised = False
            try:
                assert_no_routing_loop(["us", "core", "us"])
            except AssertionError as exc:
                raised = True
                step(f"loop detected: {exc}")
            ensure(raised, "duplicate hop must raise")
            ok("loop fail-closed")

        with scenario(
            "FaultInjector path + no-loop compose",
            "chaos.no_loop",
            "chaos.transport",
        ):
            inj = FaultInjector(seed=5)
            path = ("a", "b", "c")
            assert_no_routing_loop(path)
            inj.partition({"a"}, {"c"})
            ensure(not inj.view().can_communicate("a", "c"), "partition")
            ok("oracle helper + injector compose")

        with scenario(
            "generate_correlation_id uniqueness",
            "tx.correlation",
        ):
            ids = {tracker.generate_correlation_id() for _ in range(20)}
            ensure(len(ids) == 20, f"dup ids {len(ids)}")
            ok("20 unique ids")

        await asyncio.sleep(0)

if __name__ == "__main__":
    asyncio.run(main())
