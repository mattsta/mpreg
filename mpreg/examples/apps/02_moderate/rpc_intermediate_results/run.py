"""L2 rpc_intermediate_results — IntermediateResultCollector level progress."""

from __future__ import annotations

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.intermediate_results import (
    IntermediateResultCollector,
    RPCExecutionSummary,
    RPCIntermediateResult,
)
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
    with app_run(
        "rpc_intermediate_results",
        "RPC Intermediate Results — level collector + live DAG",
        level="L2",
    ):
        with scenario(
            "collector records per-level progress",
            "rpc.intermediate",
            "client.policy.m3",
        ):
            c = IntermediateResultCollector(request_id="req-1", total_levels=3)
            c.start_level(0)
            r0 = c.complete_level(0, {"parse": True}, {"parse": True})
            ensure(
                r0.progress_percentage == (1 / 3) * 100, f"p0 {r0.progress_percentage}"
            )
            ensure(r0.is_final_level is False, "level0 final?")
            c.start_level(1)
            c.complete_level(1, {"enrich": 2}, {"parse": True, "enrich": 2})
            c.start_level(2)
            r2 = c.complete_level(
                2, {"score": 0.9}, {"parse": True, "enrich": 2, "score": 0.9}
            )
            ensure(r2.is_final_level is True, "level2 should be final")
            ensure(r2.progress_percentage == 100.0, f"p2 {r2.progress_percentage}")
            ensure(len(c.intermediate_results) == 3, f"n={len(c.intermediate_results)}")
            ok(
                f"progress={[round(x.progress_percentage, 1) for x in c.intermediate_results]}"
            )

        with scenario("execution summary shape", "rpc.intermediate"):
            summary = RPCExecutionSummary(
                request_id="req-1",
                total_commands=3,
                total_levels=3,
                total_execution_time_ms=12.5,
                commands_per_level=[1, 1, 1],
                level_execution_times_ms=[4.0, 5.0, 3.5],
                dependency_graph_depth=3,
                parallel_execution_efficiency=1.0,
            )
            ensure(summary.average_level_time_ms > 0, "avg")
            ensure(
                summary.slowest_level_index == 1, f"slow {summary.slowest_level_index}"
            )
            ok(
                f"avg={summary.average_level_time_ms:.2f}ms "
                f"slowest_level={summary.slowest_level_index}"
            )

        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="IR-Node",
                    resources={"cpu"},
                    log_level="WARNING",
                    gossip_interval=30.0,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]

                def parse(x: int) -> int:
                    return x + 1

                def enrich(x: int) -> int:
                    return x * 3

                def score(x: int) -> dict[str, float | int]:
                    return {"value": x, "ok": 1}

                server.register_command("parse", parse, ["cpu"])
                server.register_command("enrich", enrich, ["cpu"])
                server.register_command("score", score, ["cpu"])
                url = f"ws://127.0.0.1:{ports[0]}"

                with scenario(
                    "live multi-level DAG still returns final",
                    "rpc.dag",
                    "rpc.call",
                    "rpc.intermediate",
                ):
                    async with MPREGClientAPI(url) as client:
                        result = await client.request(
                            [
                                RPCCommand(
                                    name="p",
                                    fun="parse",
                                    args=(10,),
                                    locs=frozenset(["cpu"]),
                                ),
                                RPCCommand(
                                    name="e",
                                    fun="enrich",
                                    args=("p",),
                                    locs=frozenset(["cpu"]),
                                ),
                                RPCCommand(
                                    name="s",
                                    fun="score",
                                    args=("e",),
                                    locs=frozenset(["cpu"]),
                                ),
                            ]
                        )
                    # request returns final mapping-ish
                    ensure(result is not None, "empty result")
                    ok(f"DAG final={result!r}"[:120])
                    step(
                        "note: client.request returns final values; full "
                        "return_intermediate_results wire path is server "
                        "RPCRequest flag (see IntermediateResultCollector lab above)"
                    )

                with scenario(
                    "intermediate result dataclass fields", "rpc.intermediate"
                ):
                    sample = RPCIntermediateResult(
                        request_id="r",
                        level_index=0,
                        level_results={"a": 1},
                        accumulated_results={"a": 1},
                        total_levels=1,
                        completed_levels=1,
                        execution_time_ms=1.2,
                    )
                    ensure(sample.is_final_level is True, "final")
                    ensure(sample.progress_percentage == 100.0, "pct")
                    ok("RPCIntermediateResult fields ok")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
