"""L1 ha_client_failover — multi-seed MPREGClusterClient call path."""

from __future__ import annotations

import asyncio

from mpreg.client.cluster_client import MPREGClusterClient
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import ensure, ok, run_with_servers, step
from mpreg.server import MPREGServer

async def main() -> None:
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

                s.register_command("ping", ping, ["api"])

            step(f"seeds {url_a} , {url_b}")
            async with MPREGClusterClient(seed_urls=(url_a, url_b)) as client:
                result = await client.call(
                    "ping", "ha", locs=frozenset(["api"]), timeout=10.0
                )
            ensure(result == "ok:ha", f"unexpected result {result!r}")
            ok(f"cluster client call succeeded via multi-seed: {result!r}")
            step(
                "stronger chaos (kill seed mid-flight) is planned in chaos_checkout"
            )

        await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
