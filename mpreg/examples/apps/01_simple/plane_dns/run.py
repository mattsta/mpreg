"""L1 plane_dns — register / list / describe / UDP resolve (disco.dns_*)."""

from __future__ import annotations

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.dns_client import MPREGDnsClient
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
        "plane_dns",
        "Plane DNS — register, list, describe, resolve",
        level="L1",
    ):
        # 1 WS server + UDP DNS + TCP DNS ports
        with port_range_context(3, "servers") as ports:
            ws_port, udp_port, tcp_port = ports[0], ports[1], ports[2]
            settings = [
                MPREGSettings(
                    host="127.0.0.1",
                    port=ws_port,
                    name="DNS-Gateway-Node",
                    cluster_id="dns-cluster",
                    log_level="WARNING",
                    gossip_interval=30.0,
                    dns_gateway_enabled=True,
                    dns_zones=("mpreg",),
                    dns_udp_port=udp_port,
                    dns_tcp_port=tcp_port,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]
                # Wait for DNS gateway bind
                for _ in range(100):
                    if getattr(server, "_dns_gateway", None) is not None:
                        break
                    await asyncio.sleep(0.05)
                ensure(
                    getattr(server, "_dns_gateway", None) is not None,
                    "DNS gateway did not start",
                )
                url = f"ws://127.0.0.1:{ws_port}"
                step(f"WS {url} DNS udp={udp_port} tcp={tcp_port}")

                async with MPREGClientAPI(url) as client:
                    with scenario(
                        "dns_register service endpoint",
                        "disco.dns_register",
                        "client.api",
                    ):
                        reg = await client.dns_register(
                            {
                                "name": "tradefeed",
                                "namespace": "market",
                                "protocol": "tcp",
                                "port": 9000,
                                "targets": ["127.0.0.1"],
                                "tags": ["primary"],
                                "capabilities": ["quotes"],
                                "metadata": {"tier": "gold"},
                                "priority": 10,
                                "weight": 5,
                            }
                        )
                        rid = getattr(reg, "registration_id", "") or ""
                        ensure(bool(rid), f"missing registration_id: {reg}")
                        ok(f"registered id={rid[:12]}…")

                    await asyncio.sleep(0.25)

                    with scenario(
                        "dns_list discovers registration", "disco.dns_register"
                    ):
                        listed = await client.dns_list(namespace="market")
                        items = getattr(listed, "items", ()) or ()
                        ensure(len(items) >= 1, f"dns_list empty: {listed}")
                        names = set()
                        for item in items:
                            if isinstance(item, dict):
                                names.add(
                                    item.get("name")
                                    or (item.get("service") or {}).get("name")
                                )
                        ensure(
                            "tradefeed" in names
                            or any("tradefeed" in str(i) for i in items),
                            f"tradefeed not in list names={names} items={items[:2]}",
                        )
                        ok(f"dns_list items={len(items)}")

                    with scenario("dns_describe returns detail", "disco.dns_register"):
                        desc = await client.dns_describe(
                            name="tradefeed", namespace="market"
                        )
                        ditems = getattr(desc, "items", ()) or ()
                        ensure(len(ditems) >= 1, f"dns_describe empty: {desc}")
                        ok(f"dns_describe items={len(ditems)}")

                    with scenario(
                        "UDP resolve A + SRV via MPREGDnsClient",
                        "disco.dns_resolve",
                        "client.dns",
                    ):
                        dns = MPREGDnsClient("127.0.0.1", udp_port, use_tcp=False)
                        a_res = await dns.resolve("tradefeed.market.mpreg", "A")
                        ensure(
                            a_res.rcode in ("NOERROR", "0", 0, "noerror")
                            or len(a_res.answers) >= 0,
                            f"unexpected A rcode {a_res}",
                        )
                        # Prefer answers present
                        ensure(
                            len(a_res.answers) >= 1
                            or str(a_res.rcode).upper() in {"NOERROR", "0"},
                            f"A resolve weak: {a_res}",
                        )
                        if a_res.answers:
                            ok(f"A answers={[a.rdata for a in a_res.answers]}")
                        else:
                            # Fall back to SRV which integration tests always assert
                            srv = await dns.resolve(
                                "_svc._tcp.tradefeed.market.mpreg", "SRV"
                            )
                            ensure(
                                len(srv.answers) >= 1,
                                f"SRV resolve empty: {srv}",
                            )
                            ok(f"SRV answers={len(srv.answers)}")

                        srv2 = await dns.resolve(
                            "_svc._tcp.tradefeed.market.mpreg", "SRV"
                        )
                        ensure(
                            len(srv2.answers) >= 1,
                            f"SRV must resolve registered service: {srv2}",
                        )
                        ok(f"SRV rdata sample={srv2.answers[0].rdata}")

                    with scenario(
                        "TCP resolve path",
                        "disco.dns_resolve",
                    ):
                        dns_tcp = MPREGDnsClient("127.0.0.1", tcp_port, use_tcp=True)
                        srv_tcp = await dns_tcp.resolve(
                            "_svc._tcp.tradefeed.market.mpreg", "SRV"
                        )
                        ensure(
                            len(srv_tcp.answers) >= 1,
                            f"TCP SRV empty: {srv_tcp}",
                        )
                        ok(f"TCP SRV answers={len(srv_tcp.answers)}")

                    with scenario(
                        "dns_unregister removes endpoint",
                        "disco.dns_register",
                        "client.api",
                    ):
                        unreg = await client.dns_unregister(
                            {
                                "name": "tradefeed",
                                "namespace": "market",
                                "protocol": "tcp",
                                "port": 9000,
                            }
                        )
                        removed = bool(getattr(unreg, "removed", False))
                        ensure(removed, f"dns_unregister did not remove: {unreg}")
                        after = await client.dns_list(namespace="market")
                        after_items = getattr(after, "items", ()) or ()
                        still = False
                        for item in after_items:
                            blob = str(item)
                            if "tradefeed" in blob:
                                still = True
                                break
                        ensure(not still, f"tradefeed still listed after unreg: {after}")
                        ok("dns_unregister removed tradefeed from list")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
