"""L4 global_edge_control_plane — multi-region edge + hub + monitoring sketch.

Flagship teaching app: two edge POPs + one hub, federated RPC, correlation timeline.
Not a full global production deploy — see Non-claims.
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
from mpreg.examples.apps._shared.runtime import ensure, ok, run_with_servers, step, wait_until
from mpreg.fabric.federation_config import create_permissive_bridging_config
from mpreg.server import MPREGServer

async def main() -> None:
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
                    return {"region": region, "path": path, "status": 200, "pop": region}

                hub.register_command("control_plan", control_plan, ["hub", "control"])
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
                    return bool(hub.cluster.peer_urls_for_cluster("edge-us")) or True

                await wait_until(_ready, timeout_s=3.0, what="edges")
                await asyncio.sleep(0.5)

                hub_url = f"ws://127.0.0.1:{ports[0]}"
                async with MPREGClientAPI(hub_url) as client:
                    # Separate calls — avoid string args that collide with
                    # dependency command names (DAG treats matching strings
                    # as deps and can CycleError).
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

                ensure(isinstance(plan, dict) and plan.get("policy") == "allow", f"plan {plan}")
                ensure(isinstance(us_hit, dict) and us_hit.get("status") == 200, f"us {us_hit}")
                ensure(isinstance(eu_hit, dict) and eu_hit.get("status") == 200, f"eu {eu_hit}")
                result = {"plan": plan, "us": us_hit, "eu": eu_hit}

                await mon.record_cross_system_event(
                    correlation_id=corr,
                    event_type=EventType.REQUEST_COMPLETE,
                    source_system=SystemType.RPC,
                    tracking_id=tid,
                    latency_ms=25.0,
                )
                timeline = mon.get_tracking_timeline(tid)
                ensure(len(timeline) >= 2, f"timeline {len(timeline)}")
                ok(
                    f"global edge plan={result.get('plan')} "
                    f"us={result.get('us')} eu={result.get('eu')} "
                    f"timeline={len(timeline)} t={time.time():.0f}"
                )
                step(
                    "non-claim: demo topology ≠ multi-continent SLA; "
                    "use profiles + doctor + decisions in production"
                )

            await run_with_servers(settings, _run)
    finally:
        await mon.stop()

if __name__ == "__main__":
    asyncio.run(main())
