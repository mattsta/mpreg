"""L0 hello_rpc — register, single call, DAG chain, locs (FEATURE_CATALOG rpc.*)."""

from __future__ import annotations

import asyncio

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
        "hello_rpc",
        "Hello RPC — call + DAG drill-down",
        level="L0",
        probe=True,
    ):
        probe = get_probe()
        assert probe is not None
        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Hello-RPC",
                    resources={"cpu", "math"},
                    log_level="WARNING",
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]

                def add(a: int, b: int) -> int:
                    return a + b

                def multiply(x: int, factor: int) -> int:
                    return x * factor

                def greet(msg: str) -> str:
                    return f"greet:{msg}"

                server.register_command("add", add, ["cpu", "math"])
                server.register_command("multiply", multiply, ["cpu", "math"])
                server.register_command("greet", greet, ["cpu"])
                step(f"ws://127.0.0.1:{ports[0]} registered add, multiply, greet")

                hub = f"ws://127.0.0.1:{ports[0]}"
                async with MPREGClientAPI(hub) as client:
                    with scenario(
                        "single call via MPREGClientAPI.call",
                        "rpc.call",
                        "rpc.register",
                    ):
                        greeted = await probe.measure_await(
                            "rpc.call",
                            client.call("greet", "hello", locs=frozenset(["cpu"])),
                        )
                        ensure(greeted == "greet:hello", f"greet got {greeted!r}")
                        summed = await probe.measure_await(
                            "rpc.call",
                            client.call("add", 10, 32, locs=frozenset(["cpu", "math"])),
                        )
                        ensure(summed == 42, f"add expected 42 got {summed!r}")
                        ok(f"call greet={greeted!r} add={summed}")

                    with scenario("dependency DAG request()", "rpc.dag", "rpc.locs"):
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
                                ]
                            ),
                        )
                        ensure(
                            isinstance(result, dict),
                            f"expected dict got {type(result)}",
                        )
                        ensure(
                            result.get("scaled") == 126,
                            f"scaled expected 126 got {result.get('scaled')!r} full={result!r}",
                        )
                        ok(f"DAG terminal scaled={result.get('scaled')} full={result}")

                    with scenario("call_dag alias", "rpc.dag"):
                        alias = await probe.measure_await(
                            "rpc.dag",
                            client.call_dag(
                                [
                                    RPCCommand(
                                        name="s",
                                        fun="add",
                                        args=(1, 2),
                                        locs=frozenset(["cpu", "math"]),
                                    ),
                                    RPCCommand(
                                        name="m",
                                        fun="multiply",
                                        args=("s", 10),
                                        locs=frozenset(["cpu", "math"]),
                                    ),
                                ]
                            ),
                        )
                        ensure(
                            isinstance(alias, dict) and alias.get("m") == 30,
                            f"call_dag unexpected {alias!r}",
                        )
                        ok(f"call_dag m={alias.get('m')}")

                    with scenario("locs routing boundary", "rpc.locs"):
                        try:
                            await client.call(
                                "add",
                                1,
                                1,
                                locs=frozenset(["gpu-only-missing"]),
                                timeout=3.0,
                            )
                            step(
                                "locs miss did not raise (implementation may broaden match)"
                            )
                        except EXAMPLE_RUN_EXCEPTIONS as exc:
                            ok(f"locs miss fail-closed: {type(exc).__name__}")

                    with scenario(
                        "client latency/throughput probe",
                        "mon.slo",
                    ):
                        # Burst a few more calls to populate histogram
                        for i in range(8):
                            await probe.measure_await(
                                "rpc.call",
                                client.call(
                                    "add", i, 1, locs=frozenset(["cpu", "math"])
                                ),
                            )
                        call_op = probe.op("rpc.call")
                        ensure(call_op.count >= 10, f"ops {call_op.count}")
                        ensure(call_op.p95_ms < 5000.0, f"p95 {call_op.p95_ms}")
                        ensure(call_op.avg_ms >= 0.0, "avg")
                        ensure(probe.throughput_ops_s > 0.0, "throughput")
                        # Server-side tracker if present
                        tracker = getattr(server, "metrics_tracker", None) or getattr(
                            server, "_metrics_tracker", None
                        )
                        if tracker is not None and hasattr(tracker, "snapshot"):
                            snap = tracker.snapshot()
                            probe.absorb_server_tracker(tracker, label="hello")
                            step(
                                f"server-metrics rpc.total={snap['rpc']['total']} "
                                f"avg_ms={snap['rpc']['avg_ms']} rps={snap['rpc']['rps']}"
                            )
                        else:
                            step(
                                "server metrics_tracker not exposed on this build; "
                                "client ExampleProbe still proves latency/throughput"
                            )
                        ok(
                            f"probe ops={probe.total_ops} p95_ms={call_op.p95_ms:.2f} "
                            f"throughput_ops_s={probe.throughput_ops_s:.1f}"
                        )

            await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())
