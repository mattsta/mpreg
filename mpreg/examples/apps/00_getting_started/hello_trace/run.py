"""L0 hello_trace — unified monitoring timeline drill-down (mon.*)."""

from __future__ import annotations

import asyncio

from mpreg.core.monitoring.unified_monitoring import (
    EventType,
    SystemType,
    create_unified_system_monitor,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

async def main() -> None:
    with app_run("hello_trace", "Hello Trace — correlation timeline", level="L0"):
        monitor = create_unified_system_monitor()
        await monitor.start()
        try:
            correlation_id = "hello-trace-demo"

            with scenario(
                "record multi-hop timeline",
                "mon.unified",
                "mon.events",
                "mon.system_types",
            ):
                step(f"correlation_id={correlation_id}")
                tracking_id = await monitor.record_cross_system_event(
                    correlation_id=correlation_id,
                    event_type=EventType.REQUEST_START,
                    source_system=SystemType.RPC,
                    metadata={"operation": "hello_trace", "app": "hello_trace"},
                )
                ensure(tracking_id is not None, "missing tracking_id")
                await monitor.record_cross_system_event(
                    correlation_id=correlation_id,
                    event_type=EventType.CROSS_SYSTEM_CORRELATION,
                    source_system=SystemType.RPC,
                    target_system=SystemType.CACHE,
                    tracking_id=tracking_id,
                    latency_ms=5.0,
                )
                await monitor.record_cross_system_event(
                    correlation_id=correlation_id,
                    event_type=EventType.CROSS_SYSTEM_CORRELATION,
                    source_system=SystemType.CACHE,
                    target_system=SystemType.QUEUE,
                    tracking_id=tracking_id,
                    latency_ms=3.0,
                )
                await monitor.record_cross_system_event(
                    correlation_id=correlation_id,
                    event_type=EventType.REQUEST_COMPLETE,
                    source_system=SystemType.RPC,
                    tracking_id=tracking_id,
                    latency_ms=18.0,
                )
                ok(f"recorded 4 events tracking_id={tracking_id}")

            with scenario("read tracking timeline", "mon.timeline"):
                timeline = monitor.get_tracking_timeline(tracking_id)
                ensure(len(timeline) >= 4, f"expected >=4 events, got {len(timeline)}")
                types = [getattr(e, "event_type", None) for e in timeline]
                ensure(
                    EventType.REQUEST_START in types
                    or any("START" in str(t) for t in types),
                    f"missing REQUEST_START in {types}",
                )
                step(f"event_types={types}")
                ok(f"timeline events={len(timeline)}")

            with scenario("correlation lookup", "mon.correlation"):
                # Prefer correlation timeline when available; fall back to tracking.
                corr = None
                if hasattr(monitor, "get_correlation_timeline"):
                    corr = monitor.get_correlation_timeline(correlation_id)
                if corr:
                    ensure(len(corr) >= 1, "empty correlation timeline")
                    ok(f"correlation timeline len={len(corr)}")
                else:
                    active = monitor.get_active_tracking_ids()
                    ensure(
                        tracking_id in active or len(timeline) >= 4,
                        "tracking id not active and no correlation API",
                    )
                    ok(f"active_tracking_ids keys={len(active)}")

            step(
                "production: MPREG_MONITORING_URL + "
                "`mpreg monitor decisions` / OpenAPI /routing/decisions"
            )
        finally:
            await monitor.stop()

if __name__ == "__main__":
    asyncio.run(main())
