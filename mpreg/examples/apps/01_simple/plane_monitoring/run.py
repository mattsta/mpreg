"""L1 plane_monitoring — unified monitor timeline + health (mon.* tour)."""

from __future__ import annotations

import asyncio

from mpreg.core.monitoring.unified_monitoring import (
    EventType,
    SystemType,
    create_unified_system_monitor,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

async def main() -> None:
    with app_run(
        "plane_monitoring",
        "Plane Monitoring — timeline + metrics",
        level="L1",
    ):
        monitor = create_unified_system_monitor()
        await monitor.start()
        try:
            corr = "plane-monitoring-flow"

            with scenario(
                "multi-system event chain",
                "mon.unified",
                "mon.events",
                "mon.system_types",
            ):
                tid = await monitor.record_cross_system_event(
                    correlation_id=corr,
                    event_type=EventType.REQUEST_START,
                    source_system=SystemType.RPC,
                    metadata={"operation": "plane_monitoring"},
                )
                for src, dst, lat in (
                    (SystemType.RPC, SystemType.CACHE, 12.0),
                    (SystemType.CACHE, SystemType.QUEUE, 8.0),
                    (SystemType.QUEUE, SystemType.PUBSUB, 4.0),
                ):
                    await monitor.record_cross_system_event(
                        correlation_id=corr,
                        event_type=EventType.CROSS_SYSTEM_CORRELATION,
                        source_system=src,
                        target_system=dst,
                        tracking_id=tid,
                        latency_ms=lat,
                    )
                await monitor.record_cross_system_event(
                    correlation_id=corr,
                    event_type=EventType.REQUEST_COMPLETE,
                    source_system=SystemType.RPC,
                    tracking_id=tid,
                    latency_ms=40.0,
                )
                ok(f"tracking_id={tid}")

            with scenario("tracking + correlation timelines", "mon.timeline", "mon.correlation"):
                timeline = monitor.get_tracking_timeline(tid)
                ensure(len(timeline) >= 5, f"timeline short {len(timeline)}")
                corr_tl = monitor.get_correlation_timeline(corr)
                ensure(len(corr_tl) >= 1, "empty correlation timeline")
                ok(f"tracking={len(timeline)} correlation={len(corr_tl)}")

            with scenario("unified metrics / health", "mon.health"):
                if hasattr(monitor, "get_unified_metrics"):
                    metrics = await monitor.get_unified_metrics()
                    ensure(metrics is not None, "metrics None")
                    ok(f"metrics type={type(metrics).__name__}")
                else:
                    active = monitor.get_active_tracking_ids()
                    ensure(tid in active or len(timeline) >= 5, "no health surface")
                    ok(f"active ids={len(active)}")

            step("production: attach transport adapters + OpenAPI /routing/decisions")
        finally:
            await monitor.stop()

if __name__ == "__main__":
    asyncio.run(main())
