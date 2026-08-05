"""L2 ops_cli_tour — teach mpreg CLI surfaces via CliRunner (live server).

API discovery notes (logged as scenarios / steps):
- DNS lives under ``mpreg client dns-register|dns-list`` (not top-level ``dns``).
- ``mpreg doctor`` probes **monitoring HTTP**, not the WebSocket RPC URL.
- ``list-peers`` is ``mpreg client list-peers``.
- CLI commands call ``asyncio.run`` internally — cannot invoke them on the
  curriculum event loop; use a worker thread (``asyncio.to_thread``).
- Built-in RPC name ``echo`` is pre-registered; demos must pick other names.
"""

from __future__ import annotations

import asyncio
import os

from click.testing import CliRunner

from mpreg.cli.main import cli
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

def _invoke_sync(
    args: list[str], *, env: dict[str, str] | None = None
):
    """Run click CLI in a fresh thread (CLI uses asyncio.run internally)."""
    runner = CliRunner()
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return runner.invoke(cli, args, env=merged, catch_exceptions=False)

async def _invoke(args: list[str], *, env: dict[str, str] | None = None):
    return await asyncio.to_thread(_invoke_sync, args, env=env)

async def main() -> None:
    with app_run(
        "ops_cli_tour",
        "Ops CLI Tour — call / dns / doctor / examples",
        level="L2",
    ):
        with scenario("config-check on dev profile", "ops.cli_config"):
            result = await _invoke(
                ["config-check", "mpreg/profiles/dev.toml", "--format", "json"]
            )
            ensure(
                result.exit_code in (0, 2),
                f"config-check exit {result.exit_code}: {result.output[:300]}",
            )
            ok(f"config-check exit={result.exit_code}")

        with scenario("examples list via mpreg CLI", "ops.cli_examples"):
            result = await _invoke(["examples", "list"])
            ensure(result.exit_code == 0, f"examples list failed: {result.output}")
            ensure(
                "Total:" in result.output or "hello_rpc" in result.output,
                f"list missing apps: {result.output[:200]}",
            )
            ok("mpreg examples list ok")

        with scenario(
            "CLI group shape discovery (usability)",
            "ops.cli_call",
        ):
            top = await _invoke(["--help"])
            ensure(top.exit_code == 0, "top help failed")
            client_help = await _invoke(["client", "--help"])
            ensure(client_help.exit_code == 0, "client help failed")
            ensure(
                "dns-register" in client_help.output
                or "dns-list" in client_help.output,
                f"dns-* missing under client: {client_help.output[:400]}",
            )
            ensure(
                "call" in client_help.output,
                f"call missing under client: {client_help.output[:200]}",
            )
            step(
                "usability: DNS + call live under `mpreg client …` "
                "(not top-level `mpreg dns` / `mpreg call`)"
            )
            step(
                "friction: CLI handlers use asyncio.run — nest from async apps "
                "only via thread (asyncio.to_thread)"
            )
            ok("documented client subcommand layout")

        with port_range_context(3, "servers") as ports:
            ws_port, udp_port, tcp_port = ports[0], ports[1], ports[2]
            settings = [
                MPREGSettings(
                    host="127.0.0.1",
                    port=ws_port,
                    name="Ops-CLI-Node",
                    cluster_id="ops-cli",
                    resources={"compute"},
                    log_level="WARNING",
                    gossip_interval=30.0,
                    enable_default_cache=True,
                    dns_gateway_enabled=True,
                    dns_zones=("mpreg",),
                    dns_udp_port=udp_port,
                    dns_tcp_port=tcp_port,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]

                def ops_echo(msg: str) -> str:
                    return f"echo:{msg}"

                def ops_add(a: int, b: int) -> int:
                    return int(a) + int(b)

                # Friction: bare name "echo" collides with a built-in registry entry.
                server.register_command("ops_echo", ops_echo, ["compute"])
                server.register_command("ops_add", ops_add, ["compute"])
                step("registered ops_echo/ops_add (avoid built-in name 'echo')")

                for _ in range(80):
                    if getattr(server, "_dns_gateway", None) is not None:
                        break
                    await asyncio.sleep(0.05)
                ensure(
                    getattr(server, "_dns_gateway", None) is not None,
                    "DNS gateway missing",
                )
                url = f"ws://127.0.0.1:{ws_port}"

                with scenario(
                    "client call via CLI",
                    "ops.cli_call",
                    "rpc.call",
                ):
                    result = await _invoke(
                        [
                            "client",
                            "call",
                            "ops_echo",
                            "ops",
                            "--url",
                            url,
                            "--locs",
                            "compute",
                        ],
                    )
                    ensure(
                        result.exit_code == 0,
                        f"client call exit {result.exit_code}: {result.output}",
                    )
                    ensure(
                        "echo:ops" in result.output,
                        f"unexpected call output: {result.output}",
                    )
                    ok(f"client call → {result.output.strip()[:80]}")

                with scenario(
                    "client call ops_add with numeric args",
                    "ops.cli_call",
                ):
                    result = await _invoke(
                        [
                            "client",
                            "call",
                            "ops_add",
                            "20",
                            "22",
                            "--url",
                            url,
                            "--locs",
                            "compute",
                        ],
                    )
                    ensure(
                        result.exit_code == 0,
                        f"ops_add call failed: {result.output}",
                    )
                    ensure("42" in result.output, f"ops_add output: {result.output}")
                    ok("client call ops_add → 42")

                with scenario(
                    "client dns-register + dns-list",
                    "ops.cli_dns",
                    "disco.dns_register",
                ):
                    reg = await _invoke(
                        [
                            "client",
                            "dns-register",
                            "--url",
                            url,
                            "--name",
                            "opsfeed",
                            "--namespace",
                            "ops",
                            "--protocol",
                            "tcp",
                            "--port",
                            "9001",
                            "--target",
                            "127.0.0.1",
                        ],
                    )
                    ensure(
                        reg.exit_code == 0,
                        f"dns-register failed: {reg.output[:400]}",
                    )
                    ok(f"dns-register ok: {reg.output.strip()[:100]}")

                    listed = await _invoke(
                        [
                            "client",
                            "dns-list",
                            "--url",
                            url,
                            "--namespace",
                            "ops",
                        ],
                    )
                    ensure(
                        listed.exit_code == 0,
                        f"dns-list failed: {listed.output[:300]}",
                    )
                    ensure(
                        "opsfeed" in listed.output,
                        f"opsfeed missing from list: {listed.output[:300]}",
                    )
                    ok("dns-list shows opsfeed")

                with scenario(
                    "client list-peers",
                    "ops.cli_call",
                    "disco.list_peers",
                ):
                    peers = await _invoke(
                        ["client", "list-peers", "--url", url]
                    )
                    ensure(
                        peers.exit_code == 0,
                        f"list-peers failed: {peers.output[:300]}",
                    )
                    ok(f"list-peers ok len={len(peers.output)}")

                with scenario(
                    "doctor requires monitoring HTTP URL (friction)",
                    "ops.cli_doctor",
                ):
                    bad = await _invoke(["doctor", "--url", url])
                    ensure(
                        bad.exit_code != 0,
                        "doctor should not accept bare WS URL as monitoring base",
                    )
                    step(
                        "friction: `mpreg doctor --url` expects monitoring HTTP "
                        f"(e.g. http://127.0.0.1:PORT), not WS; exit={bad.exit_code}"
                    )
                    ok("doctor correctly rejects WS-only URL / missing monitoring")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
