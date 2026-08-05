"""L0 hello_dns — minimal DNS register + list + resolve."""

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
    with app_run("hello_dns", "Hello DNS — register + resolve", level="L0"):
        with port_range_context(3, "servers") as ports:
            ws_port, udp_port, tcp_port = ports[0], ports[1], ports[2]
            settings = [
                MPREGSettings(
                    host="127.0.0.1",
                    port=ws_port,
                    name="Hello-DNS",
                    cluster_id="hello-dns",
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
                for _ in range(100):
                    if getattr(server, "_dns_gateway", None) is not None:
                        break
                    await asyncio.sleep(0.05)
                ensure(
                    getattr(server, "_dns_gateway", None) is not None,
                    "DNS gateway did not start",
                )
                url = f"ws://127.0.0.1:{ws_port}"
                step(f"DNS gateway udp={udp_port} tcp={tcp_port}")

                async with MPREGClientAPI(url) as client:
                    with scenario("register hello service", "disco.dns_register"):
                        reg = await client.dns_register(
                            {
                                "name": "hello",
                                "namespace": "demo",
                                "protocol": "tcp",
                                "port": 8080,
                                "targets": ["127.0.0.1"],
                                "tags": ["l0"],
                                "priority": 10,
                                "weight": 1,
                            }
                        )
                        rid = getattr(reg, "registration_id", "") or ""
                        ensure(bool(rid), f"no registration_id: {reg}")
                        ok(f"registered {rid[:12]}…")

                    await asyncio.sleep(0.2)

                    with scenario("list finds hello", "disco.dns_register"):
                        listed = await client.dns_list(namespace="demo")
                        items = getattr(listed, "items", ()) or ()
                        ensure(len(items) >= 1, f"list empty: {listed}")
                        blob = str(items)
                        ensure("hello" in blob, f"hello missing in {blob[:200]}")
                        ok(f"list items={len(items)}")

                    with scenario("UDP SRV resolve", "disco.dns_resolve", "client.dns"):
                        dns = MPREGDnsClient("127.0.0.1", udp_port, use_tcp=False)
                        srv = await dns.resolve(
                            "_svc._tcp.hello.demo.mpreg", "SRV"
                        )
                        ensure(
                            len(srv.answers) >= 1,
                            f"SRV resolve empty: {srv}",
                        )
                        ok(f"SRV rdata={srv.answers[0].rdata}")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
