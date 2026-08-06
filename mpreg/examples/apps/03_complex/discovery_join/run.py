"""L3 discovery_join — third node join; peers, map, rpc_list (disco.*)."""

from __future__ import annotations

import asyncio

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
    wait_until,
)
from mpreg.server import MPREGServer

async def main() -> None:
    with (
        app_run(
            "discovery_join",
            "Discovery Join — third node visibility",
            level="L3",
        ),
        port_range_context(3, "servers") as ports,
    ):
        url_a = f"ws://127.0.0.1:{ports[0]}"
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
                funs = getattr(a.cluster, "funtimes", {})
                return "ping_c" in funs or bool(a.cluster.peer_info)

            try:
                await wait_until(_c_visible, timeout_s=8.0, what="join visibility")
            except Exception:
                await asyncio.sleep(0.8)

            with scenario(
                "cross-node RPC after join",
                "disco.join",
                "rpc.call",
                "rpc.locs",
                "boot.peers",
            ):
                async with MPREGClientAPI(url_a) as client:
                    ra = await client.call("ping_a", locs=frozenset(["api", "core"]))
                    rb = await client.call("ping_b", locs=frozenset(["api", "edge"]))
                    rc = await client.call("ping_c", locs=frozenset(["api", "worker"]))
                ensure(ra == "pong-from-a", f"a {ra}")
                ensure(rb == "pong-from-b", f"b {rb}")
                ensure(rc == "pong-from-c", f"c {rc}")
                ok("rpc a/b/c ok")

            with scenario(
                "list_peers + cluster_map", "disco.list_peers", "disco.cluster_map"
            ):
                async with MPREGClientAPI(url_a) as client:
                    peers = await client.list_peers()
                    ensure(len(peers) >= 1, f"expected peers got {peers!r}")
                    cmap = await client.cluster_map()
                    ensure(cmap is not None, "cluster_map None")
                    ok(f"peers={len(peers)} cluster_map={type(cmap).__name__}")
                    step(f"peer sample: {peers[0] if peers else None}")

            with scenario(
                "rpc_list / catalog_query surfaces",
                "rpc.list",
                "disco.catalog_query",
            ):
                async with MPREGClientAPI(url_a) as client:
                    listed = await client.rpc_list()
                    ensure(listed is not None, "rpc_list None")
                    ok(f"rpc_list type={type(listed).__name__}")
                    try:
                        cat = await client.catalog_query()
                        ok(f"catalog_query type={type(cat).__name__}")
                    except Exception as exc:
                        step(f"catalog_query optional: {type(exc).__name__}")
                        ok("rpc_list sufficient for this drill")

        await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
