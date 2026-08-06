"""L3 packet_loss_chaos — plane drop rates + live drain contrast (Phase K depth)."""

from __future__ import annotations

import asyncio

import aiohttp

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
)
from mpreg.server import MPREGServer
from mpreg.testing.faults import FaultInjector, FaultKind

async def main() -> None:
    with app_run(
        "packet_loss_chaos",
        "Packet Loss Chaos — plane drops + live admission",
        level="L3",
    ):
        with scenario(
            "data_drop_rate=1.0 always drops data plane",
            "chaos.drop",
            "chaos.transport",
        ):
            inj = FaultInjector(seed=1, data_drop_rate=1.0, control_drop_rate=0.0)
            ensure(
                inj.can_deliver("a", "b", plane="data") is False,
                "data should drop",
            )
            ensure(
                inj.can_deliver("a", "b", plane="control") is True,
                "control should pass",
            )
            kinds = {d.get("kind") for d in inj.decisions}
            ensure("drop_random" in kinds or len(inj.decisions) >= 1, f"kinds {kinds}")
            ok(f"plane-separated drop; decisions={len(inj.decisions)}")

        with scenario(
            "control_drop_rate=1.0 drops control only",
            "chaos.drop",
        ):
            inj = FaultInjector(seed=2, data_drop_rate=0.0, control_drop_rate=1.0)
            ensure(inj.can_deliver("x", "y", plane="control") is False, "ctrl drop")
            ensure(inj.can_deliver("x", "y", plane="data") is True, "data pass")
            ok("control-only loss")

        with scenario(
            "stochastic data_drop_rate sampling",
            "chaos.drop",
            "chaos.transport",
        ):
            inj = FaultInjector(seed=99, data_drop_rate=0.5, control_drop_rate=0.0)
            drops = sum(
                1
                for _ in range(40)
                if not inj.can_deliver("s", "t", plane="data")
            )
            # With rate 0.5 over 40 trials expect some drops and some delivers
            ensure(drops >= 5, f"expected some drops got {drops}")
            ensure(drops <= 35, f"expected some delivers got drops={drops}")
            ok(f"stochastic drops/40={drops}")

        with scenario(
            "FaultKind includes drop/partition surfaces",
            "chaos.transport",
        ):
            vals = {k.value for k in FaultKind}
            ensure(len(vals) >= 3, f"kinds {vals}")
            ok(f"FaultKind={sorted(vals)}")

        with port_range_context(3, "servers") as ports:
            ws_a, mon_a, ws_b = ports[0], ports[1], ports[2]
            settings = [
                MPREGSettings(
                    host="127.0.0.1",
                    port=ws_a,
                    name="Loss-A",
                    cluster_id="loss-lab",
                    resources={"alpha"},
                    log_level="WARNING",
                    gossip_interval=0.5,
                    monitoring_enabled=True,
                    monitoring_port=mon_a,
                    monitoring_enable_cors=False,
                ),
                MPREGSettings(
                    host="127.0.0.1",
                    port=ws_b,
                    name="Loss-B",
                    cluster_id="loss-lab",
                    resources={"beta"},
                    peers=[f"ws://127.0.0.1:{ws_a}"],
                    log_level="WARNING",
                    gossip_interval=0.5,
                    monitoring_enabled=False,
                ),
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                a, b = servers

                def ping_a(msg: str) -> str:
                    return f"a:{msg}"

                a.register_command("ping_a", ping_a, ["alpha"])
                await asyncio.sleep(0.5)
                base = f"http://127.0.0.1:{mon_a}"

                with scenario(
                    "live mesh still serves while lab models loss",
                    "rpc.call",
                    "chaos.drop",
                ):
                    async with MPREGClientAPI(f"ws://127.0.0.1:{ws_a}") as client:
                        out = await client.call(
                            "ping_a", "live", locs=frozenset(["alpha"]), timeout=5.0
                        )
                    ensure(out == "a:live", out)
                    ok("live RPC independent of lab drop model")

                with scenario(
                    "compose lab drop + live drain admission",
                    "chaos.drop",
                    "chaos.live_drain",
                    "ops.mgmt_drain",
                    "mon.health",
                ):
                    inj = FaultInjector(seed=3, data_drop_rate=1.0)
                    ensure(
                        not inj.can_deliver("Loss-A", "Loss-B", plane="data"),
                        "lab data loss",
                    )
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            f"{base}/mgmt/v1/nodes/drain",
                            json={
                                "draining": True,
                                "actor": "packet_loss_chaos",
                                "reason": "compose",
                            },
                        ) as resp:
                            data = await resp.json(content_type=None)
                            ensure(resp.status == 200, f"drain {resp.status}")
                        async with session.get(f"{base}/ready") as resp:
                            ensure(resp.status == 503, f"ready {resp.status}")
                        async with session.post(
                            f"{base}/mgmt/v1/nodes/drain",
                            json={"draining": False, "actor": "packet_loss_chaos"},
                        ) as resp:
                            ensure(resp.status == 200, f"clear {resp.status}")
                    step(
                        "honest: FaultInjector models delivery decisions; "
                        "raw TCP/WS socket byte-drop is not injected on the wire — "
                        "live admission uses /mgmt drain for operator fail-closed"
                    )
                    ok("lab drop + live drain composed")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
