"""L3 discovery_join — third node joins two-node cluster; peers + RPC visible."""

from __future__ import annotations

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import ensure, ok, run_with_servers, step, wait_until
from mpreg.server import MPREGServer

async def main() -> None:
    with port_range_context(3, "servers") as ports:
        url_a = f"ws://127.0.0.1:{ports[0]}"
        url_b = f"ws://127.0.0.1:{ports[1]}"
        url_c = f"ws://127.0.0.1:{ports[2]}"
        settings = [
            MPREGSettings(
                port=ports[0],
                name="Join-A",
                resources={"api", "core"},
                log_level="WARNING",
            ),
            MPREGSettings(
                port=ports[1],
                name="Join-B",
                resources={"api", "edge"},
                peers=[url_a],
                log_level="WARNING",
            ),
            MPREGSettings(
                port=ports[2],
                name="Join-C",
                resources={"api", "worker"},
                peers=[url_a],
                log_level="WARNING",
            ),
        ]

        async def _run(servers: list[MPREGServer]) -> None:
            a, b, c = servers

            def ping(who: str) -> str:
                return f"pong-from-{who}"

            a.register_command("ping_a", lambda: ping("a"), ["api", "core"])
            b.register_command("ping_b", lambda: ping("b"), ["api", "edge"])
            c.register_command("ping_c", lambda: ping("c"), ["api", "worker"])
            step("A/B/C registered; C is the joining worker")

            async def _c_visible() -> bool:
                # Function table or peer list propagation
                funs = getattr(a.cluster, "funtimes", {})
                return "ping_c" in funs or bool(a.cluster.peer_info)

            try:
                await wait_until(_c_visible, timeout_s=8.0, what="join visibility")
            except Exception:
                await asyncio.sleep(0.8)

            async with MPREGClientAPI(url_a) as client:
                ra = await client.call("ping_a", locs=frozenset(["api", "core"]))
                rb = await client.call("ping_b", locs=frozenset(["api", "edge"]))
                rc = await client.call("ping_c", locs=frozenset(["api", "worker"]))
                peers = await client.list_peers()

            ensure(ra == "pong-from-a", f"a {ra}")
            ensure(rb == "pong-from-b", f"b {rb}")
            ensure(rc == "pong-from-c", f"c {rc}")
            ok(f"discovery join rpc ok peers={peers!r}")

        await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
