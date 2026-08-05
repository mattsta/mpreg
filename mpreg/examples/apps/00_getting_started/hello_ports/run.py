"""L0 hello_ports — dynamic ports + multi-call on allocated endpoint (boot.*)."""

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
)
from mpreg.server import MPREGServer

async def main() -> None:
    with app_run("hello_ports", "Hello Ports — allocator + RPC", level="L0"):
        with scenario("allocate distinct ports", "boot.port_range", "boot.auto_port"):
            with port_range_context(2, "servers") as ports:
                ensure(len(ports) == 2, f"expected 2 ports got {ports}")
                ensure(ports[0] != ports[1], "ports should be distinct")
                ensure(all(isinstance(p, int) and p > 0 for p in ports), f"bad {ports}")
                step(f"allocator chose ports={ports}")

                settings = [
                    MPREGSettings(
                        port=ports[0],
                        name="Hello-Ports-A",
                        resources={"api"},
                        log_level="WARNING",
                    ),
                    MPREGSettings(
                        port=ports[1],
                        name="Hello-Ports-B",
                        resources={"api"},
                        peers=[f"ws://127.0.0.1:{ports[0]}"],
                        log_level="WARNING",
                    ),
                ]

                async def _run(servers: list[MPREGServer]) -> None:
                    def port_echo(msg: str) -> str:
                        return f"port-echo:{msg}"

                    def which_port() -> int:
                        return int(servers[0].settings.port)

                    for s in servers:
                        s.register_command("port_echo", port_echo, ["api"])
                    servers[0].register_command("which_port", which_port, ["api"])

                    with scenario("RPC on dynamically allocated port", "rpc.call", "rpc.register"):
                        async with MPREGClientAPI(f"ws://127.0.0.1:{ports[0]}") as client:
                            result = await client.call(
                                "port_echo", "ports", locs=frozenset(["api"])
                            )
                            ensure(result == "port-echo:ports", f"unexpected {result!r}")
                            reported = await client.call(
                                "which_port", locs=frozenset(["api"])
                            )
                            ensure(
                                int(reported) == ports[0],
                                f"server port {reported} != allocated {ports[0]}",
                            )
                            ok(f"dynamic port RPC ok on {ports[0]} (which_port={reported})")

                    with scenario("second allocated port is live", "boot.port_range", "rpc.call"):
                        async with MPREGClientAPI(f"ws://127.0.0.1:{ports[1]}") as client:
                            result = await client.call(
                                "port_echo", "b", locs=frozenset(["api"])
                            )
                        ensure(result == "port-echo:b", f"peer B unexpected {result!r}")
                        ok(f"second port {ports[1]} answered")

                await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
