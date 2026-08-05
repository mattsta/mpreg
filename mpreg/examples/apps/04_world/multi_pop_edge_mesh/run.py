"""L4 multi_pop_edge_mesh — hub + US/EU/AP edge POPs (second world tour)."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.monitoring.unified_monitoring import (
    EventType,
    SystemType,
    create_unified_system_monitor,
)
from mpreg.core.observability.trace_context import (
    TRACEPARENT_KEY,
    generate_traceparent,
    inject_trace_metadata,
)
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
    wait_until,
)
from mpreg.fabric.federation_config import create_permissive_bridging_config
from mpreg.server import MPREGServer

async def main() -> None:
    with app_run(
        "multi_pop_edge_mesh",
        "Multi-POP Edge Mesh — hub + US/EU/AP edges",
        level="L4",
    ):
        mon = create_unified_system_monitor()
        await mon.start()
        try:
            cfg_hub = create_permissive_bridging_config("mesh-hub")
            cfg_us = create_permissive_bridging_config("mesh-us")
            cfg_eu = create_permissive_bridging_config("mesh-eu")
            cfg_ap = create_permissive_bridging_config("mesh-ap")

            with port_range_context(4, "servers") as ports:
                settings = [
                    MPREGSettings(
                        port=ports[0],
                        name="Mesh-Hub",
                        resources={"hub", "control", "router"},
                        cluster_id="mesh-hub",
                        federation_config=cfg_hub,
                        log_level="WARNING",
                    ),
                    MPREGSettings(
                        port=ports[1],
                        name="Mesh-US",
                        resources={"edge", "us"},
                        cluster_id="mesh-us",
                        peers=[f"ws://127.0.0.1:{ports[0]}"],
                        federation_config=cfg_us,
                        log_level="WARNING",
                    ),
                    MPREGSettings(
                        port=ports[2],
                        name="Mesh-EU",
                        resources={"edge", "eu"},
                        cluster_id="mesh-eu",
                        peers=[f"ws://127.0.0.1:{ports[0]}"],
                        federation_config=cfg_eu,
                        log_level="WARNING",
                    ),
                    MPREGSettings(
                        port=ports[3],
                        name="Mesh-AP",
                        resources={"edge", "ap"},
                        cluster_id="mesh-ap",
                        peers=[f"ws://127.0.0.1:{ports[0]}"],
                        federation_config=cfg_ap,
                        log_level="WARNING",
                    ),
                ]

                async def _run(servers: list[MPREGServer]) -> None:
                    hub, us, eu, ap = servers

                    def route_plan(dest: str) -> dict[str, Any]:
                        table = {
                            "us": "mesh-us",
                            "eu": "mesh-eu",
                            "ap": "mesh-ap",
                        }
                        pop = table.get(dest, "mesh-us")
                        return {
                            "dest": dest,
                            "pop": pop,
                            "policy": "geo_nearest",
                            "version": 2,
                        }

                    def edge_ping(region: str, path: str) -> dict[str, Any]:
                        return {
                            "region": region,
                            "path": path,
                            "status": 200,
                            "ts": time.time(),
                        }

                    def edge_echo(region: str, body: dict[str, Any]) -> dict[str, Any]:
                        return {"region": region, "echo": body, "ok": True}

                    hub.register_command("route_plan", route_plan, ["hub", "control"])
                    for node, region, locs in (
                        (us, "us", ["edge", "us"]),
                        (eu, "eu", ["edge", "eu"]),
                        (ap, "ap", ["edge", "ap"]),
                    ):
                        node.register_command("edge_ping", edge_ping, locs)
                        node.register_command("edge_echo", edge_echo, locs)
                    step("hub + us/eu/ap edges registered")

                    corr = "multi-pop-mesh"
                    tp = generate_traceparent()
                    meta = inject_trace_metadata({TRACEPARENT_KEY: tp, "tour": "mesh"})
                    tid = await mon.record_cross_system_event(
                        correlation_id=corr,
                        event_type=EventType.REQUEST_START,
                        source_system=SystemType.RPC,
                        metadata={"traceparent": meta[TRACEPARENT_KEY], "pops": 3},
                    )

                    await wait_until(lambda: True, timeout_s=1.0, what="boot")
                    await asyncio.sleep(0.6)

                    hub_url = f"ws://127.0.0.1:{ports[0]}"

                    with scenario(
                        "hub geo route plan for three continents",
                        "prod.edge",
                        "fabric.permissive",
                        "rpc.call",
                    ):
                        async with MPREGClientAPI(hub_url) as client:
                            plans = {}
                            for dest in ("us", "eu", "ap"):
                                plans[dest] = await client.call(
                                    "route_plan",
                                    dest,
                                    locs=frozenset(["hub", "control"]),
                                )
                        for dest, expect_pop in (
                            ("us", "mesh-us"),
                            ("eu", "mesh-eu"),
                            ("ap", "mesh-ap"),
                        ):
                            p = plans[dest]
                            ensure(p.get("pop") == expect_pop, f"{dest}→{p}")
                            ensure(p.get("policy") == "geo_nearest", p)
                        ok(f"plans={ {k: v.get('pop') for k, v in plans.items()} }")

                    with scenario(
                        "tri-edge health pings via fabric",
                        "fabric.cross_rpc",
                        "rpc.call",
                        "rpc.locs",
                        "prod.edge",
                    ):
                        async with MPREGClientAPI(hub_url) as client:
                            us_h = await client.call(
                                "edge_ping",
                                "us",
                                "/health",
                                locs=frozenset(["edge", "us"]),
                            )
                            eu_h = await client.call(
                                "edge_ping",
                                "eu",
                                "/health",
                                locs=frozenset(["edge", "eu"]),
                            )
                            ap_h = await client.call(
                                "edge_ping",
                                "ap",
                                "/health",
                                locs=frozenset(["edge", "ap"]),
                            )
                        for name, hit, region in (
                            ("us", us_h, "us"),
                            ("eu", eu_h, "eu"),
                            ("ap", ap_h, "ap"),
                        ):
                            ensure(
                                isinstance(hit, dict) and hit.get("status") == 200,
                                f"{name} {hit}",
                            )
                            ensure(hit.get("region") == region, f"{name} region {hit}")
                        ok("us/eu/ap health 200")

                    with scenario(
                        "edge echo payload round-trip",
                        "rpc.call",
                        "rpc.locs",
                    ):
                        body = {"sku": "X-1", "qty": 2}
                        async with MPREGClientAPI(hub_url) as client:
                            echo_ap = await client.call(
                                "edge_echo",
                                "ap",
                                body,
                                locs=frozenset(["edge", "ap"]),
                            )
                        ensure(echo_ap.get("ok") is True, echo_ap)
                        ensure(echo_ap.get("echo") == body, echo_ap)
                        ok(f"ap echo={echo_ap.get('echo')}")

                    with scenario(
                        "path matrix across POPs",
                        "rpc.call",
                        "fabric.cross_rpc",
                    ):
                        paths = ["/v1/cart", "/v1/checkout"]
                        results: list[str] = []
                        async with MPREGClientAPI(hub_url) as client:
                            for region, locs in (
                                ("us", frozenset(["edge", "us"])),
                                ("eu", frozenset(["edge", "eu"])),
                                ("ap", frozenset(["edge", "ap"])),
                            ):
                                for path in paths:
                                    hit = await client.call(
                                        "edge_ping",
                                        region,
                                        path,
                                        locs=locs,
                                    )
                                    ensure(hit.get("path") == path, hit)
                                    results.append(f"{region}:{path}")
                        ensure(len(results) == 6, results)
                        ok(f"matrix={results}")

                    with scenario(
                        "correlation timeline + traceparent stamp",
                        "mon.timeline",
                        "mon.events",
                        "mon.unified",
                        "mon.trace_context",
                    ):
                        await mon.record_cross_system_event(
                            correlation_id=corr,
                            event_type=EventType.CROSS_SYSTEM_CORRELATION,
                            source_system=SystemType.RPC,
                            target_system=SystemType.FEDERATION,
                            tracking_id=tid,
                            latency_ms=8.0,
                            metadata={"traceparent": tp},
                        )
                        await mon.record_cross_system_event(
                            correlation_id=corr,
                            event_type=EventType.REQUEST_COMPLETE,
                            source_system=SystemType.RPC,
                            tracking_id=tid,
                            latency_ms=40.0,
                        )
                        timeline = mon.get_tracking_timeline(tid)
                        ensure(len(timeline) >= 2, f"timeline {len(timeline)}")
                        ensure(_TP_RE_OK(tp), f"tp {tp}")
                        step(
                            "second L4 world: 4-node mesh teaches multi-POP geo "
                            "routing; not a multi-continent SLA proof"
                        )
                        ok(f"timeline={len(timeline)} tp_ok")

                await run_with_servers(settings, _run)
        finally:
            await mon.stop()

def _TP_RE_OK(tp: str) -> bool:
    parts = tp.split("-")
    return (
        len(parts) == 4
        and parts[0] == "00"
        and len(parts[1]) == 32
        and len(parts[2]) == 16
        and parts[3] in {"00", "01"}
    )

if __name__ == "__main__":
    asyncio.run(main())
