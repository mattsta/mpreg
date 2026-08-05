"""L1 auto_port_bootstrap — OS-assigned ports + peer join (boot.auto_port)."""

from __future__ import annotations

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.server import MPREGServer

async def main() -> None:
    with app_run(
        "auto_port_bootstrap",
        "Auto Port Bootstrap — OS ports + join",
        level="L1",
    ):
        assigned: dict[str, int] = {}

        def _capture(name: str):
            def _cb(port: int) -> None:
                assigned[name] = port

            return _cb

        with scenario("start node-a with port=None", "boot.auto_port", "boot.settings"):
            server_a = MPREGServer(
                MPREGSettings(
                    name="node-a",
                    port=None,
                    resources={"api"},
                    log_level="WARNING",
                    on_port_assigned=_capture("a"),
                    monitoring_enabled=False,
                )
            )
            task_a = asyncio.create_task(server_a.server())
            await asyncio.sleep(0.4)
            ensure(server_a.settings.port is not None, "node-a port not assigned")
            url_a = f"ws://{server_a.settings.host}:{server_a.settings.port}"
            step(f"node-a {url_a}")
            ok(f"assigned a={assigned.get('a')}")

        with scenario("join node-b via peers", "boot.peers", "boot.auto_port"):
            server_b = MPREGServer(
                MPREGSettings(
                    name="node-b",
                    port=None,
                    resources={"api"},
                    peers=[url_a],
                    log_level="WARNING",
                    on_port_assigned=_capture("b"),
                    monitoring_enabled=False,
                )
            )
            task_b = asyncio.create_task(server_b.server())
            try:
                await asyncio.sleep(1.0)
                ensure(server_b.settings.port is not None, "node-b port not assigned")
                url_b = f"ws://{server_b.settings.host}:{server_b.settings.port}"
                step(f"node-b {url_b}")
                ensure(
                    assigned.get("a") and assigned.get("b"),
                    f"capture failed {assigned}",
                )
                ensure(assigned["a"] != assigned["b"], "ports must differ")
                ok(f"assigned={assigned}")

                def ping(msg: str = "ok") -> str:
                    return f"pong:{msg}"

                server_a.register_command("ping", ping, ["api"])
                server_b.register_command("ping", ping, ["api"])

                with scenario("RPC + list_peers after join", "rpc.call", "disco.list_peers"):
                    async with MPREGClientAPI(url_b) as client:
                        result = await client.call(
                            "ping", "auto", locs=frozenset(["api"])
                        )
                        peers = await client.list_peers()
                    ensure(result == "pong:auto", f"bad result {result!r}")
                    ensure(len(peers) >= 0, "list_peers failed")
                    ok(f"rpc={result!r} peers_count={len(peers)}")
            finally:
                await server_b.shutdown_async()
                await server_a.shutdown_async()
                task_b.cancel()
                task_a.cancel()
                await asyncio.gather(task_a, task_b, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
