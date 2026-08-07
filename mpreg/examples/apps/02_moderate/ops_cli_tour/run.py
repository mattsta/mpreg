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
import json
import os
import tempfile
from pathlib import Path

from click.testing import CliRunner

from mpreg.cli.main import cli
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.obs import ExampleProbe
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
)
from mpreg.server import MPREGServer

def _invoke_sync(args: list[str], *, env: dict[str, str] | None = None):
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

        with scenario("config-check --explain field guide", "ops.cli_config"):
            result = await _invoke(
                [
                    "config-check",
                    "mpreg/profiles/dev.toml",
                    "--format",
                    "json",
                    "--explain",
                ]
            )
            ensure(
                result.exit_code in (0, 2),
                f"explain exit {result.exit_code}: {result.output[:400]}",
            )
            out = result.output
            ensure(
                "guide" in out or "field guide" in out or "## identity" in out,
                f"explain missing guide: {out[:400]}",
            )
            ensure(
                "four-plane" in out.lower()
                or "systems" in out
                or "persistence" in out.lower(),
                f"explain thin: {out[:300]}",
            )
            # T66: strong_cache guide documents residual_ops_hint ops loop
            ensure(
                "residual_ops_hint" in out
                or "cache-strong-retry-abort" in out
                or "abort_fail" in out,
                f"explain missing STRONG residual ops loop: {out[:500]}",
            )
            ensure(
                "not" in out.lower()
                and ("auto-heal" in out.lower() or "auto heal" in out.lower()
                     or "ops-driven" in out.lower() or "best-effort" in out.lower()),
                f"explain missing CFT honesty on strong_cache: {out[:500]}",
            )
            step(
                "ERG: --explain strong_cache → residual_ops_hint → "
                "cache-strong-retry-abort (ops-driven CFT; not auto-heal)"
            )
            ok("config-check --explain guide present")

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

        with port_range_context(4, "servers") as ports:
            ws_port, udp_port, tcp_port, mon_port = (
                ports[0],
                ports[1],
                ports[2],
                ports[3],
            )
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
                    enable_default_queue=True,
                    dns_gateway_enabled=True,
                    dns_zones=("mpreg",),
                    dns_udp_port=udp_port,
                    dns_tcp_port=tcp_port,
                    monitoring_enabled=True,
                    monitoring_port=mon_port,
                    monitoring_enable_cors=False,
                    mgmt_audit_path=str(
                        Path(tempfile.mkdtemp(prefix="ops-audit-")) / "a.jsonl"
                    ),
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]

                def ops_echo(msg: str) -> str:
                    return f"echo:{msg}"

                def ops_add(a: int, b: int) -> int:
                    return int(a) + int(b)

                # FQN: bare names qualify under default namespace (app.*).
                # Platform builtins live under mpreg.* — users own everything else.
                server.register_command(
                    "ops_echo", ops_echo, ["compute"]
                )  # → app.ops_echo
                server.register_command(
                    "ops_add", ops_add, ["compute"]
                )  # → app.ops_add
                # Also prove bare "echo" is legal now (app.echo ≠ mpreg.system.echo).
                server.register_command("echo", ops_echo, ["compute"])
                step(
                    "registered app.ops_echo / app.ops_add / app.echo (mpreg.* denied to users)"
                )

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
                    peers = await _invoke(["client", "list-peers", "--url", url])
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

                # ── Phase M: plane / ns / discovery CLI surfaces ──────────
                with scenario(
                    "client cache-put + cache-get plane CLI",
                    "ops.cli_planes",
                    "cache.rpc_surface",
                ):
                    put = await _invoke(
                        [
                            "client",
                            "cache-put",
                            "--url",
                            url,
                            "--namespace",
                            "ops",
                            "--key",
                            "k1",
                            "--value",
                            '{"v":1}',
                        ],
                    )
                    ensure(
                        put.exit_code == 0,
                        f"cache-put failed: {put.output[:300]}",
                    )
                    got = await _invoke(
                        [
                            "client",
                            "cache-get",
                            "--url",
                            url,
                            "--namespace",
                            "ops",
                            "--key",
                            "k1",
                        ],
                    )
                    ensure(
                        got.exit_code == 0,
                        f"cache-get failed: {got.output[:300]}",
                    )
                    ok("cache-put/get CLI plane")

                with scenario(
                    "client cache-strong-retry-abort CLI help (ops CFT)",
                    "ops.cli_planes",
                    "cache.strong",
                ):
                    # Help-only smoke: live residual clear is DistLab/T44 e2e
                    help_r = await _invoke(
                        ["client", "cache-strong-retry-abort", "--help"],
                    )
                    ensure(
                        help_r.exit_code == 0,
                        f"cache-strong-retry-abort --help failed: "
                        f"{help_r.output[:300]}",
                    )
                    hout = help_r.output.lower()
                    ensure(
                        "--op-id" in hout or "op-id" in hout,
                        f"missing --op-id: {help_r.output[:300]}",
                    )
                    ensure(
                        "not automatic" in hout
                        or "ops-driven" in hout
                        or "cft" in hout
                        or "best-effort" in hout,
                        f"honesty missing in help: {help_r.output[:400]}",
                    )
                    ensure(
                        "--peer" in hout or "peer" in hout,
                        f"missing --peer: {help_r.output[:300]}",
                    )
                    # T55: teach ops loop metrics → residual_ops_hint → this CLI
                    step(
                        "ops loop: GET /metrics/strong residual_ops_hint + "
                        "abort_fail_op_id → cache-strong-retry-abort "
                        "(ops-driven CFT; not auto-heal; doctor does not fail "
                        "on residual candidates)"
                    )
                    ok("cache-strong-retry-abort CLI registered (ops-driven CFT)")

                with scenario(
                    "client queue-send plane CLI",
                    "ops.cli_planes",
                    "queue.rpc_surface",
                ):
                    qs = await _invoke(
                        [
                            "client",
                            "queue-send",
                            "--url",
                            url,
                            "--queue",
                            "ops-jobs",
                            "--payload",
                            '{"task":"cli"}',
                            "--topic",
                            "ops.jobs",
                        ],
                    )
                    ensure(
                        qs.exit_code == 0,
                        f"queue-send failed: {qs.output[:300]}",
                    )
                    ok("queue-send CLI plane")

                with scenario(
                    "client publish plane CLI",
                    "ops.cli_planes",
                    "pubsub.client_wire",
                ):
                    pub = await _invoke(
                        [
                            "client",
                            "publish",
                            "--url",
                            url,
                            "--topic",
                            "ops.events.tick",
                            "--payload",
                            '{"n":1}',
                        ],
                    )
                    ensure(
                        pub.exit_code == 0,
                        f"publish failed: {pub.output[:300]}",
                    )
                    ok("publish CLI plane")

                with scenario(
                    "client namespace-policy validate CLI",
                    "ops.cli_ns",
                    "ns.validate",
                ):
                    rules = [
                        {
                            "namespace": "svc.ops",
                            "visibility": ["ops-cli"],
                            "owners": ["ops-cli"],
                            "policy_version": "v1",
                        }
                    ]
                    with tempfile.TemporaryDirectory() as td:
                        rules_path = Path(td) / "ns-rules.json"
                        rules_path.write_text(json.dumps(rules), encoding="utf-8")
                        ns = await _invoke(
                            [
                                "client",
                                "namespace-policy",
                                "validate",
                                "--url",
                                url,
                                "--rules-file",
                                str(rules_path),
                                "--actor",
                                "ops-cli-tour",
                            ],
                        )
                    ensure(
                        ns.exit_code == 0,
                        f"namespace-policy validate failed: {ns.output[:400]}",
                    )
                    ok("namespace-policy validate CLI")

                with scenario(
                    "client resolver-cache-stats discovery CLI",
                    "ops.cli_discovery",
                    "disco.resolver_stats",
                ):
                    stats = await _invoke(
                        ["client", "resolver-cache-stats", "--url", url]
                    )
                    # Resolver may be off; exit 0 with stats or clear error is OK
                    ensure(
                        stats.exit_code in (0, 1),
                        f"resolver-cache-stats crash: {stats.output[:300]}",
                    )
                    step(
                        f"resolver-cache-stats exit={stats.exit_code} "
                        f"out={stats.output.strip()[:120]!r}"
                    )
                    ok("resolver-cache-stats CLI surface")

                with scenario(
                    "list-peers tagged as discovery CLI",
                    "ops.cli_discovery",
                    "disco.list_peers",
                ):
                    peers = await _invoke(["client", "list-peers", "--url", url])
                    ensure(
                        peers.exit_code == 0,
                        f"list-peers failed: {peers.output[:300]}",
                    )
                    ok("list-peers discovery CLI")

                with scenario(
                    "admin drain + audit CLI",
                    "ops.mgmt_drain",
                    "ops.mgmt_audit",
                ):
                    mon_url = f"http://127.0.0.1:{mon_port}"
                    drain = await _invoke(
                        [
                            "admin",
                            "drain",
                            "--url",
                            mon_url,
                            "--actor",
                            "ops-cli-tour",
                            "--reason",
                            "teach-audit",
                            "--json",
                        ]
                    )
                    ensure(
                        drain.exit_code == 0,
                        f"admin drain failed: {drain.output[:300]}",
                    )
                    audit = await _invoke(
                        ["admin", "audit", "--url", mon_url, "--json", "--limit", "10"]
                    )
                    ensure(
                        audit.exit_code == 0,
                        f"admin audit failed: {audit.output[:300]}",
                    )
                    ensure(
                        "node_drain" in audit.output or "mutations" in audit.output,
                        f"audit body unexpected: {audit.output[:300]}",
                    )
                    # clear drain so later calls still work
                    clear = await _invoke(
                        ["admin", "drain", "--url", mon_url, "--clear", "--json"]
                    )
                    ensure(clear.exit_code == 0, f"clear drain {clear.output[:200]}")
                    ok("admin drain→audit→clear CLI path")

                with scenario(
                    "monitor strong/audit table + doctor honesty",
                    "ops.shared_audit",
                    "cache.strong",
                    "mon.metrics_snapshot",
                ):
                    mon_url = f"http://127.0.0.1:{mon_port}"
                    env = {"MPREG_MONITORING_URL": mon_url}
                    strong_m = await _invoke(
                        ["monitor", "strong", "--format", "table"],
                        env=env,
                    )
                    ensure(
                        strong_m.exit_code == 0,
                        f"monitor strong failed: {strong_m.output[:400]}",
                    )
                    sout = strong_m.output.lower()
                    ensure(
                        "get_quorum" in sout or "strong" in sout,
                        f"strong table missing caps: {strong_m.output[:300]}",
                    )
                    ensure(
                        "v1.1" in sout or "not wan" in sout or "get_quorum=false" in sout,
                        f"strong honesty missing: {strong_m.output[:300]}",
                    )
                    # T28/T31/T36: CFT / abort / TTL honesty on monitor table
                    ensure(
                        "cft=" in sout or "abort_be=" in sout or "abort_fail=" in sout,
                        f"strong CFT honesty missing: {strong_m.output[:300]}",
                    )
                    ensure(
                        "abort_fail_peers=" in sout or "residual candidate" in sout,
                        f"strong abort_fail_peers missing: {strong_m.output[:300]}",
                    )
                    ensure(
                        "ttl_gc=" in sout
                        or "pending ttl" in sout
                        or "visible=" in sout,
                        f"strong TTL/visible honesty missing: {strong_m.output[:300]}",
                    )
                    audit_m = await _invoke(
                        ["monitor", "audit", "--format", "table"],
                        env=env,
                    )
                    ensure(
                        audit_m.exit_code == 0,
                        f"monitor audit failed: {audit_m.output[:400]}",
                    )
                    aout = audit_m.output.lower()
                    ensure(
                        "siem" in aout or "shared audit" in aout or "gset" in aout,
                        f"audit table missing caps: {audit_m.output[:300]}",
                    )
                    ensure(
                        "not siem" in aout or "bft" in aout or "siem=false" in aout,
                        f"audit honesty missing: {audit_m.output[:300]}",
                    )
                    doc = await _invoke(
                        ["doctor", "--url", mon_url, "--strong", "--audit"],
                        env=env,
                    )
                    # doctor may WARN on optional planes; exit 0 preferred
                    ensure(
                        doc.exit_code in (0, 1),
                        f"doctor crash: {doc.output[:500]}",
                    )
                    dout = doc.output.lower()
                    ensure(
                        "strong" in dout or "shared_audit" in dout or "metrics" in dout,
                        f"doctor missing strong/audit: {doc.output[:400]}",
                    )
                    ok(
                        f"monitor strong/audit table + doctor exit={doc.exit_code}"
                    )

                with scenario(
                    "ops CLI latency probe annotations",
                    "ops.cli_call",
                    "mon.metrics_snapshot",
                ):
                    probe = ExampleProbe("ops_cli_tour")
                    with probe.measure("cli.client_call"):
                        r = await _invoke(
                            [
                                "client",
                                "call",
                                "ops_echo",
                                "probe",
                                "--url",
                                url,
                                "--locs",
                                "compute",
                            ],
                        )
                    ensure(r.exit_code == 0, f"probe call failed {r.output[:200]}")
                    snap = probe.snapshot()
                    ensure(snap["total_ops"] >= 1, f"ops={snap}")
                    ensure("cli.client_call" in snap["operations"], snap)
                    op = snap["operations"]["cli.client_call"]
                    ensure(op["count"] >= 1 and "p50_ms" in op, op)
                    probe.print_report()
                    ok(
                        f"ops probe n={op['count']} p50={op['p50_ms']} "
                        f"p95={op['p95_ms']}"
                    )

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
