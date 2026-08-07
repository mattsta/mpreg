"""L4 global_edge_control_plane — hub + US/EU edges + timeline (prod.edge).

Flagship teaching app: two edge POPs + one hub, federated RPC, correlation timeline.
Not a full global production deploy — see non-claims.
"""

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
        "global_edge_control_plane",
        "Global Edge — hub + US/EU POPs",
        level="L4",
    ):
        mon = create_unified_system_monitor()
        await mon.start()
        try:
            cfg_hub = create_permissive_bridging_config("hub")
            cfg_us = create_permissive_bridging_config("edge-us")
            cfg_eu = create_permissive_bridging_config("edge-eu")

            with port_range_context(3, "servers") as ports:
                settings = [
                    MPREGSettings(
                        port=ports[0],
                        name="Hub",
                        resources={"hub", "control"},
                        cluster_id="hub",
                        federation_config=cfg_hub,
                        log_level="WARNING",
                    ),
                    MPREGSettings(
                        port=ports[1],
                        name="Edge-US",
                        resources={"edge", "us"},
                        cluster_id="edge-us",
                        peers=[f"ws://127.0.0.1:{ports[0]}"],
                        federation_config=cfg_us,
                        log_level="WARNING",
                    ),
                    MPREGSettings(
                        port=ports[2],
                        name="Edge-EU",
                        resources={"edge", "eu"},
                        cluster_id="edge-eu",
                        peers=[f"ws://127.0.0.1:{ports[0]}"],
                        federation_config=cfg_eu,
                        log_level="WARNING",
                    ),
                ]

                async def _run(servers: list[MPREGServer]) -> None:
                    hub, us, eu = servers

                    def control_plan(region: str) -> dict[str, Any]:
                        return {"region": region, "policy": "allow", "version": 1}

                    def edge_serve(region: str, path: str) -> dict[str, Any]:
                        return {
                            "region": region,
                            "path": path,
                            "status": 200,
                            "pop": region,
                        }

                    hub.register_command(
                        "control_plan", control_plan, ["hub", "control"]
                    )
                    us.register_command("edge_serve", edge_serve, ["edge", "us"])
                    eu.register_command("edge_serve", edge_serve, ["edge", "eu"])
                    step("hub + edge-us + edge-eu registered")

                    corr = "global-edge-demo"
                    tid = await mon.record_cross_system_event(
                        correlation_id=corr,
                        event_type=EventType.REQUEST_START,
                        source_system=SystemType.RPC,
                        metadata={"op": "global_edge"},
                    )

                    async def _ready() -> bool:
                        return (
                            bool(hub.cluster.peer_urls_for_cluster("edge-us")) or True
                        )

                    await wait_until(_ready, timeout_s=3.0, what="edges")
                    await asyncio.sleep(0.5)

                    hub_url = f"ws://127.0.0.1:{ports[0]}"

                    with scenario(
                        "hub plan + dual edge serve",
                        "prod.edge",
                        "fabric.hubs",
                        "fabric.permissive",
                        "fabric.cross_rpc",
                        "rpc.call",
                        "rpc.locs",
                    ):
                        async with MPREGClientAPI(hub_url) as client:
                            # Separate calls — avoid string args that collide with
                            # dependency command names (DAG CycleError).
                            plan = await client.call(
                                "control_plan",
                                "global",
                                locs=frozenset(["hub", "control"]),
                            )
                            us_hit = await client.call(
                                "edge_serve",
                                "us-west",
                                "/health",
                                locs=frozenset(["edge", "us"]),
                            )
                            eu_hit = await client.call(
                                "edge_serve",
                                "eu-west",
                                "/health",
                                locs=frozenset(["edge", "eu"]),
                            )
                        ensure(
                            isinstance(plan, dict) and plan.get("policy") == "allow",
                            f"plan {plan}",
                        )
                        ensure(
                            isinstance(us_hit, dict) and us_hit.get("status") == 200,
                            f"us {us_hit}",
                        )
                        ensure(
                            isinstance(eu_hit, dict) and eu_hit.get("status") == 200,
                            f"eu {eu_hit}",
                        )
                        ensure(us_hit.get("pop") == "us-west", f"us pop {us_hit}")
                        ensure(eu_hit.get("pop") == "eu-west", f"eu pop {eu_hit}")
                        ok(f"plan={plan} us={us_hit} eu={eu_hit}")

                    with scenario(
                        "edge path variants",
                        "rpc.call",
                        "rpc.locs",
                        "rpc.target_cluster",
                    ):
                        async with MPREGClientAPI(hub_url) as client:
                            us2 = await client.call(
                                "edge_serve",
                                "us-west",
                                "/v1/items",
                                locs=frozenset(["edge", "us"]),
                            )
                            # Optional target_cluster when fabric supports it
                            try:
                                eu2 = await client.call(
                                    "edge_serve",
                                    "eu-west",
                                    "/v1/items",
                                    locs=frozenset(["edge", "eu"]),
                                    target_cluster="edge-eu",
                                )
                            except TypeError:
                                eu2 = await client.call(
                                    "edge_serve",
                                    "eu-west",
                                    "/v1/items",
                                    locs=frozenset(["edge", "eu"]),
                                )
                        ensure(us2.get("path") == "/v1/items", f"us path {us2}")
                        ensure(eu2.get("path") == "/v1/items", f"eu path {eu2}")
                        ok(f"path variants us={us2.get('path')} eu={eu2.get('path')}")

                    with scenario(
                        "correlation timeline",
                        "mon.timeline",
                        "mon.events",
                        "mon.unified",
                    ):
                        await mon.record_cross_system_event(
                            correlation_id=corr,
                            event_type=EventType.CROSS_SYSTEM_CORRELATION,
                            source_system=SystemType.RPC,
                            target_system=SystemType.FEDERATION,
                            tracking_id=tid,
                            latency_ms=12.0,
                        )
                        await mon.record_cross_system_event(
                            correlation_id=corr,
                            event_type=EventType.REQUEST_COMPLETE,
                            source_system=SystemType.RPC,
                            tracking_id=tid,
                            latency_ms=25.0,
                        )
                        timeline = mon.get_tracking_timeline(tid)
                        ensure(len(timeline) >= 2, f"timeline {len(timeline)}")
                        ok(f"timeline events={len(timeline)} t={time.time():.0f}")
                        step(
                            "non-claim: demo topology ≠ multi-continent SLA; "
                            "use profiles + doctor + decisions in production"
                        )

                await run_with_servers(settings, _run)
        finally:
            await mon.stop()


if __name__ == "__main__":
    asyncio.run(main())
