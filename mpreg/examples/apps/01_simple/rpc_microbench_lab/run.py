"""L1 rpc_microbench_lab — in-process RPC latency/throughput + server snapshot (Phase P)."""

from __future__ import annotations

import asyncio
import statistics

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.obs import ExampleProbe, format_server_snapshot
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
)
from mpreg.server import MPREGServer

ROUNDS = 40

async def main() -> None:
    with app_run(
        "rpc_microbench_lab",
        "RPC Microbench Lab — probe + ServerMetricsTracker snapshot depth",
        level="L1",
    ):
        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Bench-Node",
                    resources={"bench"},
                    log_level="WARNING",
                    gossip_interval=30.0,
                    monitoring_enabled=False,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]

                def nop(x: int = 0) -> int:
                    return x + 1

                def echo(msg: str) -> str:
                    return msg

                server.register_command("nop", nop, ["bench"])
                server.register_command("echo", echo, ["bench"])
                url = f"ws://127.0.0.1:{ports[0]}"
                probe = ExampleProbe("rpc_microbench_lab")

                with scenario(
                    "warm + timed RPC loop",
                    "rpc.call",
                    "mon.metrics_snapshot",
                    "client.api",
                ):
                    async with MPREGClientAPI(url) as client:
                        # Warm path
                        for _ in range(3):
                            await client.call("nop", 0, locs=frozenset(["bench"]))

                        for i in range(ROUNDS):
                            with probe.measure("rpc.nop"):
                                out = await client.call(
                                    "nop", i, locs=frozenset(["bench"])
                                )
                            ensure(out == i + 1, f"nop got {out}")

                        # Mixed second op for multi-op snapshot
                        for i in range(10):
                            with probe.measure("rpc.echo"):
                                e = await client.call(
                                    "echo", f"m{i}", locs=frozenset(["bench"])
                                )
                            ensure(e == f"m{i}", f"echo {e!r}")

                        snap = probe.snapshot()
                        ensure(snap["total_ops"] >= ROUNDS, f"ops={snap['total_ops']}")
                        ensure(
                            snap["throughput_ops_s"] > 0,
                            f"rps={snap['throughput_ops_s']}",
                        )
                        nop_op = probe.op("rpc.nop")
                        ensure(nop_op.count == ROUNDS, f"nop count {nop_op.count}")
                        ensure(nop_op.p50_ms >= 0, "p50 missing")
                        ensure(nop_op.p95_ms >= nop_op.p50_ms, "p95 < p50")
                        step(
                            f"client probe ops={snap['total_ops']} "
                            f"rps={snap['throughput_ops_s']:.1f} "
                            f"nop_p50={nop_op.p50_ms:.3f}ms "
                            f"nop_p95={nop_op.p95_ms:.3f}ms"
                        )
                        ok("client-side microbench probe green")

                with scenario(
                    "ServerMetricsTracker snapshot depth",
                    "mon.metrics_snapshot",
                    "mon.server_tracker",
                ):
                    tracker = getattr(server, "_metrics_tracker", None)
                    ensure(tracker is not None, "no _metrics_tracker")
                    s = tracker.snapshot()
                    ensure(isinstance(s, dict), "snapshot not dict")
                    rpc = s.get("rpc") or {}
                    ensure(int(rpc.get("total", 0)) >= ROUNDS, f"rpc total {rpc}")
                    ensure("samples" in rpc, "samples key missing (Phase P depth)")
                    ensure("min_ms" in rpc and "max_ms" in rpc, "min/max missing")
                    ensure("p50_ms" in rpc and "p95_ms" in rpc, "percentiles missing")
                    ensure(float(rpc.get("rps", 0)) >= 0, "rps missing")
                    # min <= avg <= max when samples present
                    if int(rpc.get("samples", 0)) > 0:
                        ensure(
                            float(rpc["min_ms"]) <= float(rpc["avg_ms"]) + 1e-6,
                            f"min>avg {rpc}",
                        )
                        ensure(
                            float(rpc["avg_ms"]) <= float(rpc["max_ms"]) + 1e-6,
                            f"avg>max {rpc}",
                        )
                    format_server_snapshot(s)
                    probe.absorb_server_tracker(tracker, label="bench-server")
                    probe.print_report()
                    fab = s.get("fabric") or {}
                    ensure(isinstance(fab, dict), f"fabric missing {s.keys()}")
                    for k in (
                        "decisions_total",
                        "avg_hops",
                        "max_hops",
                        "blackhole_count",
                        "reachable_ratio",
                    ):
                        ensure(k in fab, f"fabric missing {k}: {fab}")
                    step(
                        f"fabric decisions={fab.get('decisions_total')} "
                        f"avg_hops={fab.get('avg_hops')} max_hops={fab.get('max_hops')}"
                    )
                    ok(
                        f"server snapshot total={rpc['total']} "
                        f"p50={rpc['p50_ms']} p95={rpc['p95_ms']} "
                        f"min={rpc['min_ms']} max={rpc['max_ms']} "
                        f"fabric_hops_max={fab.get('max_hops')}"
                    )

                with scenario(
                    "latency stability sanity",
                    "rpc.call",
                    "mon.metrics_snapshot",
                ):
                    # No pathological multi-second p95 on localhost nop
                    ensure(
                        probe.op("rpc.nop").p95_ms < 5000.0,
                        f"p95 too high {probe.op('rpc.nop').p95_ms}",
                    )
                    lat = list(probe.op("rpc.nop").latencies_ms)
                    ensure(len(lat) >= ROUNDS, "missing latency samples")
                    med = statistics.median(lat)
                    ensure(med < 2000.0, f"median {med}ms too high")
                    ok(f"stability median={med:.3f}ms n={len(lat)}")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
