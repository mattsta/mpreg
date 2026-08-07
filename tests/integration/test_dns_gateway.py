import asyncio
import struct

import pytest
from dnslib import QTYPE, DNSRecord

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import wait_for_condition


async def _udp_query(host: str, port: int, qname: str, qtype: str) -> DNSRecord:
    loop = asyncio.get_running_loop()
    query = DNSRecord.question(qname, qtype)
    response_future: asyncio.Future[bytes] = loop.create_future()

    class ClientProtocol(asyncio.DatagramProtocol):
        def __init__(self) -> None:
            super().__init__()
            self.transport: asyncio.DatagramTransport | None = None

        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport  # type: ignore[assignment]
            self.transport.sendto(query.pack())

        def datagram_received(self, data: bytes, addr) -> None:  # type: ignore[override]
            if not response_future.done():
                response_future.set_result(data)
            if self.transport is not None:
                self.transport.close()

    transport, _ = await loop.create_datagram_endpoint(
        ClientProtocol, remote_addr=(host, port)
    )
    try:
        data = await asyncio.wait_for(response_future, timeout=2.0)
    finally:
        transport.close()
    return DNSRecord.parse(data)


async def _tcp_query(host: str, port: int, qname: str, qtype: str) -> DNSRecord:
    reader, writer = await asyncio.open_connection(host, port)
    query = DNSRecord.question(qname, qtype).pack()
    writer.write(struct.pack("!H", len(query)) + query)
    await writer.drain()
    length_data = await reader.readexactly(2)
    length = struct.unpack("!H", length_data)[0]
    data = await reader.readexactly(length)
    writer.close()
    await writer.wait_closed()
    return DNSRecord.parse(data)


class TestDnsGatewayIntegration:
    @pytest.mark.asyncio
    async def test_dns_gateway_resolves_service_and_rpc(
        self,
        test_context: AsyncTestContext,
        server_port: int,
        port_allocator,
    ) -> None:
        udp_port = port_allocator.allocate_port()
        tcp_port = port_allocator.allocate_port()
        settings = MPREGSettings(
            host="127.0.0.1",
            port=server_port,
            name="dns-server",
            cluster_id="dns-cluster",
            gossip_interval=0.5,
            dns_gateway_enabled=True,
            dns_zones=("mpreg",),
            dns_udp_port=udp_port,
            dns_tcp_port=tcp_port,
        )
        server = MPREGServer(settings=settings)
        test_context.servers.append(server)
        task = asyncio.create_task(server.server())
        test_context.tasks.append(task)

        await asyncio.sleep(0.5)
        await wait_for_condition(
            lambda: server._dns_gateway is not None,
            timeout=5.0,
            error_message="DNS gateway did not start",
        )

        client = MPREGClientAPI(f"ws://127.0.0.1:{server.settings.port}")
        test_context.clients.append(client)
        await client.connect()

        await client.dns_register(
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

        await asyncio.sleep(0.2)

        response_srv = await _udp_query(
            "127.0.0.1", udp_port, "_svc._tcp.tradefeed.market.mpreg", "SRV"
        )
        assert any(rr.rtype == QTYPE.SRV for rr in response_srv.rr)
        srv_record = next(rr for rr in response_srv.rr if rr.rtype == QTYPE.SRV)
        assert int(srv_record.rdata.port) == 9000

        response_a = await _udp_query(
            "127.0.0.1", udp_port, "tradefeed.market.mpreg", "A"
        )
        assert any(rr.rtype == QTYPE.A for rr in response_a.rr)

        response_rpc = await _udp_query(
            "127.0.0.1", udp_port, "_mpreg._tcp.echo.mpreg", "SRV"
        )
        assert any(rr.rtype == QTYPE.SRV for rr in response_rpc.rr)

        response_tcp = await _tcp_query(
            "127.0.0.1", tcp_port, "_svc._tcp.tradefeed.market.mpreg", "SRV"
        )
        assert any(rr.rtype == QTYPE.SRV for rr in response_tcp.rr)
