"""L1 transport_health_attach — TransportHealthAggregator + mon.attach (Phase J)."""

from __future__ import annotations

import asyncio

from mpreg.core.monitoring.unified_monitoring import (
    MonitoringConfig,
    create_unified_system_monitor,
)
from mpreg.core.transport.enhanced_health import (
    ConnectionHealthMonitor,
    TransportHealthAggregator,
    TransportHealthSnapshot,
    create_transport_health_aggregator,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

async def main() -> None:
    with app_run(
        "transport_health_attach",
        "Transport Health Attach — mon.transport surface",
        level="L1",
    ):
        with scenario(
            "create_transport_health_aggregator",
            "mon.transport",
            "tx.security",
        ):
            agg = create_transport_health_aggregator("ws://127.0.0.1:9999")
            ensure(isinstance(agg, TransportHealthAggregator), f"type {type(agg)}")
            ensure(agg.endpoint == "ws://127.0.0.1:9999", f"ep {agg.endpoint}")
            ok(f"aggregator endpoint={agg.endpoint}")

        with scenario(
            "record operations → snapshot scores",
            "mon.transport",
            "mon.health",
        ):
            cm = ConnectionHealthMonitor(
                connection_id="c1",
                endpoint="ws://127.0.0.1:9999",
            )
            for lat, ok_op in ((5.0, True), (8.0, True), (40.0, False), (6.0, True)):
                cm.record_operation(lat, ok_op)
            agg.add_connection_monitor(cm)
            snap = agg.get_transport_health_snapshot()
            ensure(isinstance(snap, TransportHealthSnapshot), f"snap {type(snap)}")
            ensure(snap.endpoint == "ws://127.0.0.1:9999", f"snap ep {snap.endpoint}")
            ensure(
                isinstance(snap.overall_health_score, (int, float)),
                f"score {snap.overall_health_score}",
            )
            ensure(snap.total_connections >= 1, f"conns {snap.total_connections}")
            ok(
                f"score={snap.overall_health_score} "
                f"total={snap.total_connections} "
                f"err_rate={snap.error_rate_percent}"
            )

        with scenario(
            "UnifiedSystemMonitor.attach_transport_adapter",
            "mon.transport",
            "mon.unified",
        ):
            monitor = create_unified_system_monitor()
            await monitor.start()
            try:
                ensure(
                    hasattr(monitor, "attach_transport_adapter"),
                    "attach_transport_adapter missing",
                )

                class _FakeAdapter:
                    """Minimal stand-in exposing health like an enhanced adapter."""

                    def __init__(self) -> None:
                        self.health_aggregators = {
                            "ws://127.0.0.1:9999": agg,
                        }

                monitor.attach_transport_adapter(_FakeAdapter())
                ensure(
                    getattr(monitor, "transport_adapter", None) is not None,
                    "adapter not attached",
                )
                step("attached fake adapter with one health aggregator")
                ok("attach_transport_adapter stored adapter")
            finally:
                await monitor.stop()

        with scenario(
            "MonitoringConfig surface present",
            "mon.unified",
        ):
            cfg = MonitoringConfig()
            ensure(cfg is not None, "config missing")
            ok(f"MonitoringConfig type={type(cfg).__name__}")
            step(
                "production: wire EnhancedMultiProtocolAdapter into "
                "UnifiedSystemMonitor for live connection scores"
            )

        with scenario(
            "remove connection monitor",
            "mon.transport",
        ):
            before = len(agg.connection_monitors)
            agg.remove_connection_monitor("c1")
            after = len(agg.connection_monitors)
            ensure(after == before - 1 or after == 0, f"before={before} after={after}")
            ok(f"removed monitor; remaining={after}")

        await asyncio.sleep(0)

if __name__ == "__main__":
    asyncio.run(main())
