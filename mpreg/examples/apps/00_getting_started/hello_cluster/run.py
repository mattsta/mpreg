"""L0 hello_cluster — multi-node resources, DAG, peer discovery drill-down."""

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
    wait_until,
)
from mpreg.server import MPREGServer

async def main() -> None:
    with app_run("hello_cluster", "Hello Cluster — peers + locs DAG", level="L0"):
        with port_range_context(2, "servers") as ports:
            hub = f"ws://127.0.0.1:{ports[0]}"
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Cluster-CPU",
                    resources={"cpu", "math"},
                    log_level="WARNING",
                ),
                MPREGSettings(
                    port=ports[1],
                    name="Cluster-GPU",
                    resources={"gpu", "ml"},
                    peers=[hub],
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
                    return {"score": x / 100.0, "model": "hello", "node": "gpu"}

                cpu.register_command("add", add, ["cpu", "math"])
                cpu.register_command("multiply", multiply, ["cpu", "math"])
                gpu.register_command("model_score", model_score, ["gpu", "ml"])
                step(f"peers {hub} + ws://127.0.0.1:{ports[1]}")

                async with MPREGClientAPI(hub) as client:
                    # ── S1: cross-node dependency DAG ───────────────────────
                    with scenario(
                        "cross-node RPC DAG", "rpc.dag", "rpc.locs", "boot.peers"
                    ):
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
                                RPCCommand(
                                    name="scored",
                                    fun="model_score",
                                    args=("scaled",),
                                    locs=frozenset(["gpu", "ml"]),
                                ),
                            ]
                        )
                        scored = (
                            result.get("scored") if isinstance(result, dict) else None
                        )
                        ensure(
                            isinstance(scored, dict), f"missing scored in {result!r}"
                        )
                        ensure(
                            abs(float(scored.get("score", 0.0)) - 1.26) < 1e-6,
                            f"unexpected score {scored}",
                        )
                        ensure(scored.get("node") == "gpu", "expected gpu node tag")
                        ok(f"cluster DAG scored={scored}")

                    # ── S2: direct GPU-only call via locs ───────────────────
                    with scenario("direct locs routing to GPU", "rpc.locs", "rpc.call"):
                        direct = await client.call(
                            "model_score",
                            200,
                            locs=frozenset(["gpu", "ml"]),
                        )
                        ensure(
                            isinstance(direct, dict)
                            and abs(float(direct.get("score", 0)) - 2.0) < 1e-6,
                            f"direct GPU call bad {direct!r}",
                        )
                        ok(f"direct GPU model_score={direct}")

                    # ── S3: list_peers discovery surface ────────────────────
                    with scenario("list_peers discovery", "disco.list_peers"):

                        async def _peers_ready() -> bool:
                            try:
                                peers = await client.list_peers()
                                return len(peers) >= 1
                            except Exception:
                                return False

                        await wait_until(
                            _peers_ready, timeout_s=8.0, what="peers visible"
                        )
                        peers = await client.list_peers()
                        ensure(len(peers) >= 1, f"expected peers, got {peers!r}")
                        ok(f"list_peers count={len(peers)}")
                        for p in peers[:4]:
                            step(f"peer snapshot: {p}")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
