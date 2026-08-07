"""L1 rpc_concurrency_lab — concurrent calls + M3 streaming policy + routing_topic."""

from __future__ import annotations

import asyncio

from mpreg.client.call_policy import ClientCallPolicy, RpcExecutionMode
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


async def main() -> None:
    with app_run(
        "rpc_concurrency_lab",
        "RPC Concurrency — gather + M3 + routing_topic",
        level="L1",
    ):
        with scenario(
            "M3_STREAMING policy factory",
            "client.policy.m3",
        ):
            m3 = ClientCallPolicy.for_mode(
                RpcExecutionMode.M3_STREAMING, max_attempts=2
            )
            ensure(m3.mode is RpcExecutionMode.M3_STREAMING, f"mode={m3.mode}")
            ensure(m3.max_attempts == 2, "attempts")
            ok(f"M3 policy mode={m3.mode.value} attempts={m3.max_attempts}")

        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Conc-Node",
                    resources={"c"},
                    log_level="WARNING",
                    gossip_interval=30.0,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]

                def echo(msg: str) -> str:
                    return f"e:{msg}"

                def add(a: int, b: int) -> int:
                    return int(a) + int(b)

                def mul(a: int, b: int) -> int:
                    return int(a) * int(b)

                server.register_command("echo", echo, ["c"])
                server.register_command("add", add, ["c"])
                server.register_command("mul", mul, ["c"])
                url = f"ws://127.0.0.1:{ports[0]}"

                with scenario(
                    "concurrent independent calls",
                    "rpc.concurrency",
                    "rpc.call",
                    "client.api",
                ):
                    async with MPREGClientAPI(url) as client:
                        results = await asyncio.gather(
                            client.call("echo", "a", locs=frozenset(["c"])),
                            client.call("echo", "b", locs=frozenset(["c"])),
                            client.call("add", 2, 3, locs=frozenset(["c"])),
                            client.call("mul", 4, 5, locs=frozenset(["c"])),
                        )
                    ensure(results[0] == "e:a", f"r0={results[0]!r}")
                    ensure(results[1] == "e:b", f"r1={results[1]!r}")
                    ensure(results[2] == 5, f"r2={results[2]!r}")
                    ensure(results[3] == 20, f"r3={results[3]!r}")
                    ok(f"gather n={len(results)} → {results}")

                with scenario(
                    "routing_topic kwarg accepted",
                    "rpc.routing_topic",
                    "rpc.call",
                ):
                    async with MPREGClientAPI(url) as client:
                        # Same-cluster call; routing_topic is a policy hint
                        out = await client.call(
                            "echo",
                            "rt",
                            locs=frozenset(["c"]),
                            routing_topic="policy.route.lab",
                        )
                        ensure(out == "e:rt", f"got {out!r}")
                    ok("routing_topic kwarg on call")

                with scenario(
                    "M3 policy on client call path",
                    "client.policy.m3",
                    "rpc.call",
                ):
                    # ClientCallPolicy is cluster-client oriented; prove factory
                    # and that simple API still works under concurrent load.
                    m3 = ClientCallPolicy.for_mode(RpcExecutionMode.M3_STREAMING)
                    ensure(
                        m3.mode is RpcExecutionMode.M3_STREAMING,
                        "m3 mode",
                    )
                    async with MPREGClientAPI(url) as client:
                        batch = await asyncio.gather(
                            *[
                                client.call("add", i, i, locs=frozenset(["c"]))
                                for i in range(5)
                            ]
                        )
                    ensure(batch == [0, 2, 4, 6, 8], f"batch={batch}")
                    step(f"M3 factory ready; concurrent batch={batch}")
                    ok("M3 + concurrency compose")

            await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())
