"""L1 plane_fabric — multi-cluster permissive bridging + cross RPC (fabric.*)."""

from __future__ import annotations

import asyncio

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
from mpreg.fabric.federation_config import create_permissive_bridging_config
from mpreg.server import MPREGServer

async def main() -> None:
    with app_run("plane_fabric", "Plane Fabric — multi-cluster RPC", level="L1"):
        config_a = create_permissive_bridging_config("cluster-a")
        config_b = create_permissive_bridging_config("cluster-b")

        with port_range_context(2, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Cluster-A",
                    resources={"alpha"},
                    cluster_id="cluster-a",
                    federation_config=config_a,
                    log_level="WARNING",
                ),
                MPREGSettings(
                    port=ports[1],
                    name="Cluster-B",
                    resources={"beta"},
                    cluster_id="cluster-b",
                    peers=[f"ws://127.0.0.1:{ports[0]}"],
                    federation_config=config_b,
                    log_level="WARNING",
                ),
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server_a, server_b = servers

                def alpha_task(x: int) -> str:
                    return f"alpha-{x}"

                def beta_task(x: int) -> str:
                    return f"beta-{x}"

                server_a.register_command("alpha_task", alpha_task, ["alpha"])
                server_b.register_command("beta_task", beta_task, ["beta"])

                hub = f"ws://127.0.0.1:{ports[0]}"

                with scenario(
                    "cross-cluster DAG via permissive bridge",
                    "fabric.permissive",
                    "fabric.cluster_id",
                    "fabric.cross_rpc",
                    "rpc.dag",
                    "rpc.locs",
                ):
                    async with MPREGClientAPI(hub) as client:
                        result = await client.request(
                            [
                                RPCCommand(
                                    name="a",
                                    fun="alpha_task",
                                    args=(1,),
                                    locs=frozenset(["alpha"]),
                                ),
                                RPCCommand(
                                    name="b",
                                    fun="beta_task",
                                    args=(2,),
                                    locs=frozenset(["beta"]),
                                ),
                            ]
                        )
                    ensure(
                        isinstance(result, dict) and "a" in result and "b" in result,
                        f"missing fabric results {result!r}",
                    )
                    ensure(
                        result.get("a") == "alpha-1" and result.get("b") == "beta-2",
                        f"unexpected values {result}",
                    )
                    ok(f"fabric RPC {result}")

                with scenario("single-side call still local", "rpc.call"):
                    async with MPREGClientAPI(hub) as client:
                        only_a = await client.call(
                            "alpha_task", 9, locs=frozenset(["alpha"])
                        )
                    ensure(only_a == "alpha-9", f"local alpha {only_a!r}")
                    ok(f"local alpha_task={only_a}")

                with scenario(
                    "cluster identities advertised on settings",
                    "fabric.cluster_id",
                    "fabric.permissive",
                ):
                    ensure(
                        server_a.settings.cluster_id == "cluster-a",
                        f"A cluster_id={server_a.settings.cluster_id}",
                    )
                    ensure(
                        server_b.settings.cluster_id == "cluster-b",
                        f"B cluster_id={server_b.settings.cluster_id}",
                    )
                    ensure(
                        server_a.settings.federation_config is not None,
                        "A missing federation_config",
                    )
                    ensure(
                        server_b.settings.federation_config is not None,
                        "B missing federation_config",
                    )
                    ok("cluster_id + federation_config present on both nodes")

                step("non-claim: not global linearizability; routing availability only")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
