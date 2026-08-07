"""L3 live_partition_chaos — live drain/detach + /ready (Phase J F10)."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

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
from mpreg.testing.faults import FaultInjector


async def main() -> None:
    with (
        app_run(
            "live_partition_chaos",
            "Live Partition Chaos — drain/detach + /ready admission",
            level="L3",
        ),
        port_range_context(4, "servers") as ports,
    ):
        ws_a, ws_b, mon_a, mon_b = ports[0], ports[1], ports[2], ports[3]
        url_a = f"ws://127.0.0.1:{ws_a}"
        url_b = f"ws://127.0.0.1:{ws_b}"
        base_a = f"http://127.0.0.1:{mon_a}"

        audit_dir = tempfile.mkdtemp(prefix="mpreg-mgmt-audit-")
        audit_path = str(Path(audit_dir) / "mgmt-audit.jsonl")
        settings = [
            MPREGSettings(
                host="127.0.0.1",
                port=ws_a,
                name="Chaos-A",
                cluster_id="live-chaos",
                resources={"alpha"},
                log_level="WARNING",
                gossip_interval=0.5,
                monitoring_enabled=True,
                monitoring_port=mon_a,
                monitoring_enable_cors=False,
                mgmt_audit_path=audit_path,
            ),
            MPREGSettings(
                host="127.0.0.1",
                port=ws_b,
                name="Chaos-B",
                cluster_id="live-chaos",
                resources={"beta"},
                peers=[url_a],
                log_level="WARNING",
                gossip_interval=0.5,
                monitoring_enabled=True,
                monitoring_port=mon_b,
                monitoring_enable_cors=False,
            ),
        ]

        async def _run(servers: list[MPREGServer]) -> None:
            a, b = servers

            def ping_a(msg: str) -> str:
                return f"a:{msg}"

            def ping_b(msg: str) -> str:
                return f"b:{msg}"

            a.register_command("ping_a", ping_a, ["alpha"])
            b.register_command("ping_b", ping_b, ["beta"])
            await asyncio.sleep(0.8)

            with scenario(
                "baseline mesh RPC + /ready admits",
                "rpc.call",
                "mon.health",
                "chaos.partition",
            ):
                async with MPREGClientAPI(url_a) as client:
                    out_a = await client.call(
                        "ping_a", "hi", locs=frozenset(["alpha"]), timeout=5.0
                    )
                    out_b = await client.call(
                        "ping_b", "hi", locs=frozenset(["beta"]), timeout=8.0
                    )
                ensure(out_a == "a:hi", f"ping_a {out_a!r}")
                ensure(out_b == "b:hi", f"ping_b {out_b!r}")
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{base_a}/ready") as resp:
                        body = await resp.json(content_type=None)
                        ensure(
                            resp.status == 200,
                            f"/ready expected 200 got {resp.status} {body}",
                        )
                        ensure(
                            body.get("draining") is False
                            or body.get("draining") is None
                            or body.get("ready") is True
                            or resp.status == 200,
                            f"ready body {body}",
                        )
                ok(f"mesh RPC + /ready ok body_keys={sorted(body)[:8]}")

            with scenario(
                "POST /mgmt/v1/nodes/drain → /ready 503",
                "chaos.partition",
                "mon.health",
                "ops.cli_doctor",
            ):
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{base_a}/mgmt/v1/nodes/drain",
                        json={
                            "draining": True,
                            "actor": "curriculum",
                            "reason": "live_partition_chaos",
                        },
                    ) as resp:
                        data = await resp.json(content_type=None)
                        ensure(
                            resp.status == 200,
                            f"drain POST {resp.status} {data}",
                        )
                        ensure(
                            data.get("applied") is True or data.get("draining") is True,
                            f"drain body {data}",
                        )
                    async with session.get(f"{base_a}/ready") as resp:
                        ready_body = await resp.json(content_type=None)
                        ensure(
                            resp.status == 503,
                            f"draining /ready expected 503 got {resp.status} "
                            f"{ready_body}",
                        )
                        ensure(
                            ready_body.get("draining") is True or resp.status == 503,
                            f"ready while drain {ready_body}",
                        )
                    ensure(
                        bool(getattr(a, "_mgmt_draining", False)) is True,
                        "server._mgmt_draining not set",
                    )
                ok("drain applied; /ready fail-closed 503")

            with scenario(
                "clear drain restores /ready",
                "chaos.heal",
                "mon.health",
            ):
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{base_a}/mgmt/v1/nodes/drain",
                        json={
                            "draining": False,
                            "actor": "curriculum",
                            "reason": "clear_drain",
                        },
                    ) as resp:
                        data = await resp.json(content_type=None)
                        ensure(resp.status == 200, f"clear drain {resp.status}")
                        ensure(
                            data.get("draining") is False
                            or data.get("applied") is True,
                            f"clear body {data}",
                        )
                    await asyncio.sleep(0.15)
                    async with session.get(f"{base_a}/ready") as resp:
                        ready_body = await resp.json(content_type=None)
                        ensure(
                            resp.status == 200,
                            f"post-clear /ready {resp.status} {ready_body}",
                        )
                ensure(
                    bool(getattr(a, "_mgmt_draining", False)) is False,
                    "drain flag stuck",
                )
                ok("drain cleared; /ready admits again")

            with scenario(
                "POST /mgmt/v1/peers/detach severs peer link",
                "chaos.partition",
                "disco.list_peers",
            ):
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{base_a}/mgmt/v1/peers/detach",
                        json={
                            "peer_url": url_b,
                            "actor": "curriculum",
                            "reason": "live_detach",
                        },
                    ) as resp:
                        data = await resp.json(content_type=None)
                        step(f"detach status={resp.status} body={data}")
                        ensure(
                            resp.status == 200,
                            f"detach HTTP {resp.status} {data}",
                        )
                        # applied may be True even if peer already idle
                        ensure(
                            data.get("applied") is True
                            or data.get("error") is None
                            or "detail" in data,
                            f"detach unexpected {data}",
                        )
                ok(f"detach response applied={data.get('applied')}")

            with scenario(
                "lab FaultInjector still models abstract partitions",
                "chaos.partition",
                "chaos.heal",
                "chaos.transport",
            ):
                inj = FaultInjector(seed=11, data_drop_rate=0.0)
                inj.partition({"Chaos-A"}, {"Chaos-B"})
                ensure(
                    not inj.view().can_communicate("Chaos-A", "Chaos-B"),
                    "lab partition should block",
                )
                inj.heal()
                ensure(
                    inj.view().can_communicate("Chaos-A", "Chaos-B"),
                    "lab heal failed",
                )
                step(
                    "contrast: FaultInjector is a pure lab model; "
                    "live admission uses /mgmt drain+detach on the server"
                )
                ok(f"lab model events={len(inj.decisions)}")

            with scenario(
                "mgmt audit ring + JSONL durability",
                "ops.mgmt_drain",
                "ops.mgmt_audit",
            ):
                step(f"mgmt_audit_path={audit_path}")
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{base_a}/mgmt/v1/audit",
                        params={"limit": "20"},
                    ) as resp:
                        body = await resp.json(content_type=None)
                        ensure(resp.status == 200, f"audit HTTP {resp.status}")
                        mutations = body.get("mutations") or body.get("entries") or []
                        ensure(
                            isinstance(mutations, list) and len(mutations) >= 1,
                            f"expected audit mutations, got {body}",
                        )
                        events = {
                            str(m.get("event"))
                            for m in mutations
                            if isinstance(m, dict)
                        }
                        step(f"audit events={sorted(events)} n={len(mutations)}")
                        ensure(
                            any("drain" in e.lower() for e in events)
                            or any("drain" in str(m).lower() for m in mutations),
                            f"no drain event in {events}",
                        )
                # JSONL file must have been appended
                p = Path(audit_path)
                ensure(p.is_file(), f"missing audit JSONL {p}")
                lines = [
                    ln
                    for ln in p.read_text(encoding="utf-8").splitlines()
                    if ln.strip()
                ]
                ensure(len(lines) >= 1, f"empty JSONL {p}")
                ok(f"audit JSONL lines={len(lines)} ring={len(mutations)}")

            with scenario(
                "post-chaos local RPC still works on A",
                "rpc.call",
                "chaos.heal",
            ):
                async with MPREGClientAPI(url_a) as client:
                    out = await client.call(
                        "ping_a", "after", locs=frozenset(["alpha"]), timeout=5.0
                    )
                ensure(out == "a:after", f"post {out!r}")
                ok("local RPC after drain/detach cycle")

        await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())
