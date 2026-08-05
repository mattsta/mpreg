"""L1 ha_client_failover — multi-seed HA client, map, M1 policy (client.*)."""

from __future__ import annotations

import asyncio

from mpreg.client.call_policy import ClientCallPolicy, RpcExecutionMode, default_ha_policy
from mpreg.client.cluster_client import MPREGClusterClient
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
        "ha_client_failover",
        "HA Client — multi-seed + M1 policy",
        level="L1",
    ):
        with port_range_context(2, "servers") as ports:
            url_a = f"ws://127.0.0.1:{ports[0]}"
            url_b = f"ws://127.0.0.1:{ports[1]}"
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="HA-A",
                    resources={"api"},
                    log_level="WARNING",
                ),
                MPREGSettings(
                    port=ports[1],
                    name="HA-B",
                    resources={"api"},
                    peers=[url_a],
                    log_level="WARNING",
                ),
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                for s in servers:

                    def ping(msg: str = "pong") -> str:
                        return f"ok:{msg}"

                    def whoami() -> str:
                        return str(s.settings.name)

                    s.register_command("ping", ping, ["api"])
                    s.register_command("whoami", whoami, ["api"])

                step(f"seeds {url_a} , {url_b}")

                with scenario(
                    "multi-seed call with default HA policy",
                    "client.cluster",
                    "client.default_ha",
                    "rpc.call",
                ):
                    policy = default_ha_policy()
                    ensure(policy.max_attempts >= 2, "HA policy should retry")
                    async with MPREGClusterClient(
                        seed_urls=(url_a, url_b),
                        call_policy=policy,
                    ) as client:
                        result = await client.call(
                            "ping", "ha", locs=frozenset(["api"]), timeout=10.0
                        )
                    ensure(result == "ok:ha", f"unexpected result {result!r}")
                    ok(f"cluster client call succeeded: {result!r}")

                with scenario(
                    "M1_ASYNC policy factory",
                    "client.policy.m1",
                    "client.cluster",
                ):
                    m1 = ClientCallPolicy.for_mode(
                        RpcExecutionMode.M1_ASYNC, max_attempts=3
                    )
                    ensure(m1.mode is RpcExecutionMode.M1_ASYNC, "mode mismatch")
                    ensure(
                        m1.share_deadline_across_attempts is False,
                        "M1 should not share deadline by default",
                    )
                    async with MPREGClusterClient(
                        seed_urls=(url_b, url_a),
                        call_policy=m1,
                    ) as client:
                        result = await client.call(
                            "ping", "m1", locs=frozenset(["api"]), timeout=10.0
                        )
                        ensure(result == "ok:m1", f"M1 call failed {result!r}")
                        # Reverse seed order still works
                        ok(f"M1 policy call ok result={result!r}")

                with scenario(
                    "cluster_map discovery surface",
                    "client.cluster_map",
                    "disco.cluster_map",
                ):
                    async with MPREGClusterClient(seed_urls=(url_a, url_b)) as client:
                        if hasattr(client, "cluster_map"):
                            snap = await client.cluster_map()
                            ensure(snap is not None, "empty cluster_map")
                            ok(f"cluster_map type={type(snap).__name__}")
                        else:
                            # Fall back to any-seed call proving both endpoints live
                            a = await client.call(
                                "whoami", locs=frozenset(["api"]), timeout=10.0
                            )
                            ensure(isinstance(a, str) and a.startswith("HA-"), f"whoami {a!r}")
                            ok(f"whoami via multi-seed={a!r}")

                step(
                    "stronger chaos (kill seed mid-flight) → chaos_checkout / deeper HA labs"
                )

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
