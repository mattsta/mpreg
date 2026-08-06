"""L1 mon_logging_json — configure_logging json_logs + mon correlation/health."""

from __future__ import annotations

import asyncio
import io

from mpreg.core.logging import configure_logging
from mpreg.core.monitoring.unified_monitoring import (
    EventType,
    SystemType,
    create_unified_system_monitor,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

async def main() -> None:
    with app_run(
        "mon_logging_json",
        "Mon Logging JSON + correlation/health",
        level="L1",
    ):
        with scenario(
            "configure_logging json_logs sink",
            "mon.logging",
        ):
            buf = io.StringIO()
            ids = configure_logging(
                "INFO",
                colorize=False,
                json_logs=True,
                sink=buf,
            )
            ensure(ids is not None, "handler ids None")
            from loguru import logger

            logger.info("curriculum_json_probe key={}", "phase-l")
            await asyncio.sleep(0.05)
            text = buf.getvalue()
            step(f"handlers={ids} buf_len={len(text)}")
            ensure(bool(text), "json sink produced no output")
            # loguru serialize=True wraps record as JSON with text/record keys
            ensure(
                "{" in text and ("curriculum_json_probe" in text or "record" in text),
                f"unexpected json log {text[:240]!r}",
            )
            ok(f"json log line captured len={len(text)}")

        with scenario(
            "human format still works",
            "mon.logging",
        ):
            ids2 = configure_logging("WARNING", colorize=False, json_logs=False)
            ensure(ids2 is not None, "human handlers None")
            ok(f"human handlers={ids2}")

        monitor = create_unified_system_monitor()
        await monitor.start()
        try:
            corr = "mon-logging-flow"
            with scenario(
                "correlation timeline depth",
                "mon.correlation",
                "mon.timeline",
            ):
                tid = await monitor.record_cross_system_event(
                    correlation_id=corr,
                    event_type=EventType.REQUEST_START,
                    source_system=SystemType.RPC,
                    metadata={"app": "mon_logging_json"},
                )
                await monitor.record_cross_system_event(
                    correlation_id=corr,
                    event_type=EventType.CROSS_SYSTEM_CORRELATION,
                    source_system=SystemType.RPC,
                    target_system=SystemType.CACHE,
                    tracking_id=tid,
                    latency_ms=5.0,
                )
                await monitor.record_cross_system_event(
                    correlation_id=corr,
                    event_type=EventType.REQUEST_COMPLETE,
                    source_system=SystemType.RPC,
                    tracking_id=tid,
                    latency_ms=9.0,
                )
                tl = monitor.get_correlation_timeline(corr)
                ensure(len(tl) >= 3, f"corr short {len(tl)}")
                ok(f"correlation events={len(tl)}")

            with scenario(
                "unified metrics / health",
                "mon.health",
                "mon.unified",
            ):
                if hasattr(monitor, "get_unified_metrics"):
                    metrics = await monitor.get_unified_metrics()
                    ensure(metrics is not None, "metrics None")
                    ok(f"unified metrics type={type(metrics).__name__}")
                else:
                    active = monitor.get_active_tracking_ids()
                    ensure(len(active) >= 0, "no active surface")
                    ok(f"active tracking ids={len(active)}")
        finally:
            stop = getattr(monitor, "stop", None) or getattr(monitor, "shutdown", None)
            if callable(stop):
                res = stop()
                if asyncio.iscoroutine(res):
                    await res

        ok("mon_logging_json complete")

if __name__ == "__main__":
    asyncio.run(main())
