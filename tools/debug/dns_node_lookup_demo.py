from __future__ import annotations

import asyncio

from mpreg.client.dns_client import MPREGDnsClient
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_context
from mpreg.dns import encode_node_id
from mpreg.server import MPREGServer


def _build_node_qname(node_id: str, zone: str) -> str:
    label = encode_node_id(node_id)
    return f"{label}.node.{zone}"


async def main() -> None:
    with port_context("servers") as server_port, port_context("dns-udp") as dns_port:
        settings = MPREGSettings(
            host="127.0.0.1",
            port=server_port,
            name="dns-node-demo",
            cluster_id="demo-cluster",
            dns_gateway_enabled=True,
            dns_zones=("mpreg",),
            dns_udp_port=dns_port,
        )
        server = MPREGServer(settings=settings)
        server_task = asyncio.create_task(server.server())
        await asyncio.sleep(0.5)

        node_id = server.cluster.local_url
        qname = _build_node_qname(node_id, "mpreg")
        dns = MPREGDnsClient(host="127.0.0.1", port=dns_port)
        result = await dns.resolve(qname, qtype="A")
        print("Query:", qname)
        print(result.to_dict())

        await server.shutdown_async()
        await asyncio.wait_for(server_task, timeout=2.0)


if __name__ == "__main__":
    asyncio.run(main())
