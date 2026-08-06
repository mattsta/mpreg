"""L1 client_trace_bind — last_trace_context + bind_trace_context (Phase K)."""

from __future__ import annotations

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.logging import bind_trace_context, trace_context
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
        "client_trace_bind",
        "Client Trace Bind — W3C + loguru bind",
        level="L1",
    ):
        with scenario(
            "bind_trace_context returns bound logger",
            "mon.trace_bind",
            "client.trace",
        ):
            log = bind_trace_context(
                traceparent="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                correlation_id="corr-k",
                request_u="u-1",
            )
            ensure(log is not None, "logger None")
            ok(f"bound logger type={type(log).__name__}")

        with scenario(
            "trace_context context manager",
            "mon.trace_bind",
            "mon.trace_context",
        ):
            with trace_context(
                traceparent="00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                correlation_id="ctx-1",
            ):
                step("inside trace_context")
            ok("trace_context enter/exit")

        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Trace-Node",
                    resources={"t"},
                    log_level="WARNING",
                    gossip_interval=30.0,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]

                def echo(msg: str) -> str:
                    return f"e:{msg}"

                server.register_command("echo", echo, ["t"])
                url = f"ws://127.0.0.1:{ports[0]}"

                with scenario(
                    "RPC + last_trace_context surface",
                    "client.trace",
                    "rpc.call",
                    "client.api",
                ):
                    async with MPREGClientAPI(url) as client:
                        out = await client.call(
                            "echo", "hi", locs=frozenset(["t"])
                        )
                        ensure(out == "e:hi", f"got {out!r}")
                        ctx = client.last_trace_context()
                        # May be None if server did not echo traceparent — surface must exist
                        ensure(
                            ctx is None or isinstance(ctx, dict),
                            f"unexpected ctx {ctx!r}",
                        )
                        ensure(
                            callable(client.last_trace_context),
                            "last_trace_context not callable",
                        )
                        step(f"last_trace_context={ctx}")
                        ok(
                            "last_trace_context callable; "
                            f"populated={ctx is not None and bool(ctx)}"
                        )

                with scenario(
                    "empty bind is identity logger",
                    "mon.trace_bind",
                ):
                    bare = bind_trace_context()
                    ensure(bare is not None, "bare None")
                    # Second bind must still return a usable logger (idempotent surface)
                    again = bind_trace_context(correlation_id="again")
                    ensure(again is not None, "second bind None")
                    ok("bind with no fields + re-bind returns logger")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
