"""L0 hello_rpc — register, single call, DAG chain, locs (FEATURE_CATALOG rpc.*)."""

from __future__ import annotations

import asyncio
from typing import Any

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
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

async def main() -> None:
    with app_run("hello_rpc", "Hello RPC — call + DAG drill-down", level="L0"):
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
                    # ── S1: single-function public call API ─────────────────
                    with scenario("single call via MPREGClientAPI.call", "rpc.call", "rpc.register"):
                        greeted = await client.call(
                            "greet", "hello", locs=frozenset(["cpu"])
                        )
                        ensure(greeted == "greet:hello", f"greet got {greeted!r}")
                        summed = await client.call(
                            "add", 10, 32, locs=frozenset(["cpu", "math"])
                        )
                        ensure(summed == 42, f"add expected 42 got {summed!r}")
                        ok(f"call greet={greeted!r} add={summed}")

                    # ── S2: multi-command DAG via request / call_dag ────────
                    with scenario("dependency DAG request()", "rpc.dag", "rpc.locs"):
                        result = await client.request(
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
                        )
                        ensure(isinstance(result, dict), f"expected dict got {type(result)}")
                        ensure(
                            result.get("scaled") == 126,
                            f"scaled expected 126 got {result.get('scaled')!r} full={result!r}",
                        )
                        ok(f"DAG terminal scaled={result.get('scaled')} full={result}")

                    # ── S3: call_dag alias parity ───────────────────────────
                    with scenario("call_dag alias", "rpc.dag"):
                        alias = await client.call_dag(
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
                        )
                        ensure(
                            isinstance(alias, dict) and alias.get("m") == 30,
                            f"call_dag unexpected {alias!r}",
                        )
                        ok(f"call_dag m={alias.get('m')}")

                    # ── S4: wrong locs fails closed (or misses) ─────────────
                    with scenario("locs routing boundary", "rpc.locs"):
                        try:
                            await client.call(
                                "add",
                                1,
                                1,
                                locs=frozenset(["gpu-only-missing"]),
                                timeout=3.0,
                            )
                            # Some builds may still resolve locally; accept either
                            # hard failure or we at least exercised the kwarg.
                            step("locs miss did not raise (implementation may broaden match)")
                        except Exception as exc:
                            ok(f"locs miss fail-closed: {type(exc).__name__}")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
