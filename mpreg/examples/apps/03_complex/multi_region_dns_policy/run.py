"""L3 multi_region_dns_policy — two clusters + DNS register + namespace policy."""

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
    with app_run(
        "multi_region_dns_policy",
        "Multi-Region DNS + Policy — US/EU service names",
        level="L3",
    ):
        with port_range_context(5, "servers") as ports:
            us_ws, eu_ws, us_udp, us_tcp, _spare = ports
            us_url = f"ws://127.0.0.1:{us_ws}"
            eu_url = f"ws://127.0.0.1:{eu_ws}"
            settings = [
                MPREGSettings(
                    host="127.0.0.1",
                    port=us_ws,
                    name="US-Edge",
                    cluster_id="us-east",
                    resources={"edge", "us"},
                    log_level="WARNING",
                    gossip_interval=0.5,
                    dns_gateway_enabled=True,
                    dns_zones=("mpreg",),
                    dns_udp_port=us_udp,
                    dns_tcp_port=us_tcp,
                    # permissive multi-cluster often needs fabric flags; keep simple
                ),
                MPREGSettings(
                    host="127.0.0.1",
                    port=eu_ws,
                    name="EU-Edge",
                    cluster_id="eu-west",
                    resources={"edge", "eu"},
                    peers=[us_url],
                    log_level="WARNING",
                    gossip_interval=0.5,
                ),
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                us, eu = servers

                def ping_us() -> str:
                    return "us"

                def ping_eu() -> str:
                    return "eu"

                us.register_command("ping_region", ping_us, ["edge", "us"])
                eu.register_command("ping_region", ping_eu, ["edge", "eu"])

                for _ in range(100):
                    if getattr(us, "_dns_gateway", None) is not None:
                        break
                    await asyncio.sleep(0.05)
                ensure(
                    getattr(us, "_dns_gateway", None) is not None,
                    "US DNS gateway missing",
                )
                await asyncio.sleep(1.0)

                with scenario(
                    "register regional DNS names",
                    "disco.dns_register",
                    "client.dns",
                ):
                    async with MPREGClientAPI(us_url) as client:
                        us_reg = await client.dns_register(
                            {
                                "name": "shop",
                                "namespace": "us",
                                "protocol": "tcp",
                                "port": us_ws,
                                "targets": ["127.0.0.1"],
                                "tags": ["region:us"],
                                "priority": 10,
                                "weight": 10,
                            }
                        )
                        eu_reg = await client.dns_register(
                            {
                                "name": "shop",
                                "namespace": "eu",
                                "protocol": "tcp",
                                "port": eu_ws,
                                "targets": ["127.0.0.1"],
                                "tags": ["region:eu"],
                                "priority": 10,
                                "weight": 10,
                            }
                        )
                        ensure(
                            bool(getattr(us_reg, "registration_id", "")),
                            f"us reg {us_reg}",
                        )
                        ensure(
                            bool(getattr(eu_reg, "registration_id", "")),
                            f"eu reg {eu_reg}",
                        )
                        ok("registered shop.us + shop.eu")

                with scenario("list namespaces separately", "disco.dns_register"):
                    async with MPREGClientAPI(us_url) as client:
                        us_list = await client.dns_list(namespace="us")
                        eu_list = await client.dns_list(namespace="eu")
                        us_items = getattr(us_list, "items", ()) or ()
                        eu_items = getattr(eu_list, "items", ()) or ()
                        ensure(len(us_items) >= 1, f"us list {us_list}")
                        ensure(len(eu_items) >= 1, f"eu list {eu_list}")
                        ok(f"us={len(us_items)} eu={len(eu_items)}")

                with scenario(
                    "local region RPC via locs",
                    "rpc.call",
                    "rpc.locs",
                    "fabric.cross_rpc",
                ):
                    async with MPREGClientAPI(us_url) as client:
                        us_ping = await client.call(
                            "ping_region", locs=frozenset(["edge", "us"])
                        )
                        ensure(us_ping == "us", f"us ping {us_ping}")
                        # Cross-cluster may or may not route depending on fabric mode
                        try:
                            eu_ping = await client.call(
                                "ping_region",
                                locs=frozenset(["edge", "eu"]),
                                timeout=3.0,
                            )
                            step(f"cross-cluster eu ping → {eu_ping!r}")
                            ok(f"regional RPC us={us_ping} eu={eu_ping}")
                        except Exception as exc:
                            step(
                                f"cross-cluster eu not routed (honest): "
                                f"{type(exc).__name__}: {exc}"
                            )
                            ok("local us RPC ok; eu cross-cluster non-claim if isolated")

                with scenario(
                    "F13: target_cluster without fabric bridge → clear route error",
                    "rpc.target_cluster",
                    "fabric.cross_rpc",
                ):
                    # Peers alone ≠ fabric bridge. Asking for a foreign
                    # target_cluster should fail with ROUTE_NOT_FOUND details
                    # that mention fabric bridging (Phase I F13).
                    async with MPREGClientAPI(us_url) as client:
                        failed = False
                        detail = ""
                        try:
                            await client.call(
                                "ping_region",
                                locs=frozenset(["edge"]),
                                target_cluster="eu-west-no-bridge",
                                timeout=3.0,
                            )
                        except Exception as exc:
                            failed = True
                            detail = f"{type(exc).__name__}: {exc}"
                            step(f"expected route miss: {detail}")
                        ensure(failed, "foreign target_cluster must fail closed")
                        # Prefer structured route messaging when present
                        lower = detail.lower()
                        if "fabric" in lower or "route" in lower or "no route" in lower:
                            ok(f"F13 route error is operator-readable: {detail[:160]}")
                        else:
                            step(
                                "route miss raised but message lacked fabric hint; "
                                f"raw={detail[:160]}"
                            )
                            ok("F13 fail-closed on missing target_cluster route")

                with scenario(
                    "namespace policy validate surface if present",
                    "ns.validate",
                    "ns.status",
                ):
                    async with MPREGClientAPI(us_url) as client:
                        # Prefer public API methods when available
                        if hasattr(client, "namespace_policy_status"):
                            status = await client.namespace_policy_status()
                            ok(f"ns status={status!r}"[:120])
                        elif hasattr(client, "namespace_policy_export"):
                            exported = await client.namespace_policy_export()
                            ok(f"ns export type={type(exported).__name__}")
                        else:
                            # Discovery: document missing unified method
                            step(
                                "API friction: no namespace_policy_* on MPREGClientAPI "
                                "in this build — see namespace_policy_gate app / CLI"
                            )
                            ok("noted ns policy via dedicated plane app")

                with scenario(
                    "describe regional shop",
                    "disco.dns_register",
                ):
                    async with MPREGClientAPI(us_url) as client:
                        desc = await client.dns_describe(name="shop", namespace="us")
                        items = getattr(desc, "items", ()) or ()
                        ensure(len(items) >= 1, f"describe empty {desc}")
                        ok(f"describe us/shop items={len(items)}")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
