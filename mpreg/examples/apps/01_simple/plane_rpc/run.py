"""L1 plane_rpc — full RPC plane tour with discovery + policy drills."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from mpreg.client.call_policy import ClientCallPolicy, RpcExecutionMode
from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import (
    EXAMPLE_RUN_EXCEPTIONS,
    app_run,
    ensure,
    get_probe,
    ok,
    run_with_servers,
    scenario,
    step,
)
from mpreg.server import MPREGServer


async def main() -> None:
    with app_run(
        "plane_rpc",
        "Plane RPC — DAG, locs, discovery, policy",
        level="L1",
        probe=True,
    ):
        probe = get_probe()
        assert probe is not None
        with port_range_context(2, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="RPC-CPU",
                    resources={"cpu", "math"},
                    log_level="WARNING",
                ),
                MPREGSettings(
                    port=ports[1],
                    name="RPC-GPU",
                    resources={"gpu", "ml"},
                    peers=[f"ws://127.0.0.1:{ports[0]}"],
                    log_level="WARNING",
                ),
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                cpu, gpu = servers

                def add(a: int, b: int) -> int:
                    return a + b

                def multiply(x: int, factor: int) -> int:
                    return x * factor

                def model_score(x: int) -> dict[str, Any]:
                    return {"score": x / 100.0, "model": "plane"}

                cpu.register_command("add", add, ["cpu", "math"])
                cpu.register_command("multiply", multiply, ["cpu", "math"])
                gpu.register_command("model_score", model_score, ["gpu", "ml"])

                hub = f"ws://127.0.0.1:{ports[0]}"

                with scenario(
                    "cross-node dependency DAG",
                    "rpc.dag",
                    "rpc.locs",
                    "rpc.register",
                ):
                    async with MPREGClientAPI(hub) as client:
                        result = await probe.measure_await(
                            "rpc.dag",
                            client.request(
                                [
                                    RPCCommand(
                                        name="sum",
                                        fun="add",
                                        args=(20, 22),
                                        locs=frozenset(["cpu", "math"]),
                                    ),
                                    RPCCommand(
                                        name="scaled",
                                        fun="multiply",
                                        args=("sum", 3),
                                        locs=frozenset(["cpu", "math"]),
                                    ),
                                    RPCCommand(
                                        name="scored",
                                        fun="model_score",
                                        args=("scaled",),
                                        locs=frozenset(["gpu", "ml"]),
                                    ),
                                ]
                            ),
                        )
                    scored = result.get("scored") if isinstance(result, dict) else None
                    ensure(isinstance(scored, dict), f"missing scored in {result!r}")
                    ensure(
                        abs(float(scored.get("score", 0.0)) - 1.26) < 1e-6,
                        f"unexpected score {scored}",
                    )
                    ok(f"DAG scored={scored}")

                with scenario(
                    "rpc_list / rpc_describe / rpc_report",
                    "rpc.list",
                    "rpc.describe",
                    "rpc.report",
                ):
                    async with MPREGClientAPI(hub) as client:
                        listed = await probe.measure_await(
                            "rpc.list", client.rpc_list()
                        )
                        ensure(listed is not None, "rpc_list returned None")
                        step(f"rpc_list type={type(listed).__name__}")
                        try:
                            desc = await probe.measure_await(
                                "rpc.describe", client.rpc_describe()
                            )
                            step(f"rpc_describe type={type(desc).__name__}")
                            ok("rpc_describe ok")
                        except EXAMPLE_RUN_EXCEPTIONS as exc:
                            step(f"rpc_describe optional path: {type(exc).__name__}")
                        try:
                            report = await probe.measure_await(
                                "rpc.report", client.rpc_report()
                            )
                            step(f"rpc_report type={type(report).__name__}")
                            ok("rpc_report ok")
                        except EXAMPLE_RUN_EXCEPTIONS as exc:
                            step(f"rpc_report optional path: {type(exc).__name__}")
                        ok(f"discovery surfaces exercised list={type(listed).__name__}")

                with scenario(
                    "M1 and M3 call policies",
                    "client.policy.m1",
                    "client.policy.m3",
                    "rpc.call",
                ):
                    m1 = ClientCallPolicy.for_mode(RpcExecutionMode.M1_ASYNC)
                    m3 = ClientCallPolicy.for_mode(RpcExecutionMode.M3_STREAMING)
                    ensure(m1.max_attempts >= 2, "M1 should retry")
                    ensure(
                        m3.share_deadline_across_attempts is True,
                        "M3 shares deadline",
                    )
                    async with MPREGClientAPI(hub, call_policy=m1) as client:
                        v = await probe.measure_await(
                            "rpc.call",
                            client.call("add", 2, 3, locs=frozenset(["cpu", "math"])),
                        )
                        ensure(v == 5, f"M1 add got {v!r}")
                    async with MPREGClientAPI(hub, call_policy=m3) as client:
                        v = await probe.measure_await(
                            "rpc.call",
                            client.call(
                                "multiply", 5, 5, locs=frozenset(["cpu", "math"])
                            ),
                        )
                        ensure(v == 25, f"M3 multiply got {v!r}")
                    ok("M1 + M3 policies on live calls")

                with scenario("concurrent independent calls", "rpc.call"):
                    async with MPREGClientAPI(hub) as client:
                        t0 = time.perf_counter()
                        a, b, c = await asyncio.gather(
                            client.call("add", 1, 1, locs=frozenset(["cpu", "math"])),
                            client.call("add", 2, 2, locs=frozenset(["cpu", "math"])),
                            client.call(
                                "model_score", 50, locs=frozenset(["gpu", "ml"])
                            ),
                        )
                        batch_ms = (time.perf_counter() - t0) * 1000.0
                        for _ in range(3):
                            probe.record("rpc.call", batch_ms / 3.0)
                    ensure(a == 2 and b == 4, f"concurrent add {a},{b}")
                    ensure(
                        isinstance(c, dict) and abs(float(c["score"]) - 0.5) < 1e-6,
                        f"concurrent score {c}",
                    )
                    ok(f"concurrent results a={a} b={b} c={c}")

                with scenario(
                    "client latency/throughput probe",
                    "mon.slo",
                ):
                    async with MPREGClientAPI(hub) as client:
                        for i in range(6):
                            await probe.measure_await(
                                "rpc.call",
                                client.call(
                                    "add", i, 1, locs=frozenset(["cpu", "math"])
                                ),
                            )
                    call_op = probe.op("rpc.call")
                    ensure(call_op.count >= 8, f"ops {call_op.count}")
                    ensure(call_op.p95_ms < 8000.0, f"p95 {call_op.p95_ms}")
                    ensure(probe.throughput_ops_s > 0.0, "throughput")
                    tracker = getattr(cpu, "_metrics_tracker", None)
                    if tracker is not None and hasattr(tracker, "snapshot"):
                        snap = tracker.snapshot()
                        probe.absorb_server_tracker(tracker, label="plane")
                        step(
                            f"server-metrics rpc.total={snap['rpc']['total']} "
                            f"p95_ms={snap['rpc']['p95_ms']} rps={snap['rpc']['rps']}"
                        )
                    ok(
                        f"probe ops={probe.total_ops} p95_ms={call_op.p95_ms:.2f} "
                        f"throughput_ops_s={probe.throughput_ops_s:.1f}"
                    )

            await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())
