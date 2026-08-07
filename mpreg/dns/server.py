from __future__ import annotations

import asyncio
import struct
import time
from dataclasses import dataclass, field

from dnslib import QTYPE, RCODE, DNSRecord
from loguru import logger

from mpreg.datastructures.type_aliases import JsonDict

from .resolver import DnsResolver


class DnsUdpProtocol(asyncio.DatagramProtocol):
    def __init__(self, gateway: DnsGateway) -> None:
        super().__init__()
        self._gateway = gateway
        self._transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr) -> None:
        if self._transport is None:
            return

        async def _handle() -> None:
            response = await self._gateway.handle_query(data, via="udp")
            if response:
                self._transport.sendto(response, addr)

        asyncio.create_task(_handle())


@dataclass(slots=True)
class DnsGatewayMetrics:
    total_queries: int = 0
    udp_queries: int = 0
    tcp_queries: int = 0
    nxdomain_responses: int = 0
    error_responses: int = 0
    last_query_at: float | None = None
    last_error: str | None = None
    total_latency_ms: float = 0.0
    max_latency_ms: float = 0.0

    def snapshot(self) -> JsonDict:
        avg_latency = (
            self.total_latency_ms / self.total_queries if self.total_queries else 0.0
        )
        return {
            "total_queries": self.total_queries,
            "udp_queries": self.udp_queries,
            "tcp_queries": self.tcp_queries,
            "nxdomain_responses": self.nxdomain_responses,
            "error_responses": self.error_responses,
            "last_query_at": self.last_query_at,
            "last_error": self.last_error,
            "avg_latency_ms": avg_latency,
            "max_latency_ms": self.max_latency_ms,
        }


@dataclass(slots=True)
class DnsGateway:
    resolver: DnsResolver
    host: str
    udp_port: int
    tcp_port: int
    _udp_transport: asyncio.DatagramTransport | None = field(init=False, default=None)
    _tcp_server: asyncio.AbstractServer | None = field(init=False, default=None)
    _metrics: DnsGatewayMetrics = field(init=False, default_factory=DnsGatewayMetrics)
    _udp_response_limit_bytes: int = field(init=False, default=512)

    @property
    def bound_udp_port(self) -> int:
        if self._udp_transport is None:
            return self.udp_port
        sock = self._udp_transport.get_extra_info("sockname")
        if not sock:
            return self.udp_port
        return int(sock[1])

    @property
    def bound_tcp_port(self) -> int:
        if self._tcp_server is None:
            return self.tcp_port
        sockets = self._tcp_server.sockets
        if not sockets:
            return self.tcp_port
        return int(sockets[0].getsockname()[1])

    def metrics_snapshot(self) -> JsonDict:
        return self._metrics.snapshot()

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._udp_transport, _ = await loop.create_datagram_endpoint(
            lambda: DnsUdpProtocol(self), local_addr=(self.host, self.udp_port)
        )
        self._tcp_server = await asyncio.start_server(
            self._handle_tcp_client, host=self.host, port=self.tcp_port
        )

    async def stop(self) -> None:
        if self._udp_transport is not None:
            self._udp_transport.close()
            self._udp_transport = None
        if self._tcp_server is not None:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()
            self._tcp_server = None

    async def handle_query(self, data: bytes, *, via: str) -> bytes | None:
        started = time.time()
        try:
            request = DNSRecord.parse(data)
        except Exception as exc:
            self._metrics.error_responses += 1
            self._metrics.last_error = str(exc)
            logger.debug(f"DNS parse failed: {exc}")
            return None

        if not request.questions:
            return None
        question = request.questions[0]
        qname = str(question.qname).rstrip(".")
        try:
            qtype_name = str(QTYPE[question.qtype]).upper()
        except Exception:
            qtype_name = "A"
        result = self.resolver.resolve(qname, qtype_name)
        elapsed_ms = (time.time() - started) * 1000.0
        self._metrics.total_queries += 1
        if via == "udp":
            self._metrics.udp_queries += 1
        elif via == "tcp":
            self._metrics.tcp_queries += 1
        self._metrics.last_query_at = time.time()
        self._metrics.total_latency_ms += elapsed_ms
        self._metrics.max_latency_ms = max(self._metrics.max_latency_ms, elapsed_ms)
        if result.rcode == RCODE.NXDOMAIN:
            self._metrics.nxdomain_responses += 1
        response = request.reply()
        response.header.rcode = result.rcode
        for record in result.records:
            response.add_answer(record.to_rr())
        packet = response.pack()
        if via == "udp" and len(packet) > self._udp_response_limit_bytes:
            response.header.tc = 1
            while response.rr and len(response.pack()) > self._udp_response_limit_bytes:
                response.rr.pop()
            packet = response.pack()
        return packet

    async def _handle_tcp_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            length_data = await reader.readexactly(2)
            message_length = struct.unpack("!H", length_data)[0]
            if message_length <= 0:
                writer.close()
                await writer.wait_closed()
                return
            data = await reader.readexactly(message_length)
            response = await self.handle_query(data, via="tcp")
            if response:
                writer.write(struct.pack("!H", len(response)) + response)
                await writer.drain()
        except asyncio.IncompleteReadError:
            return
        except Exception as exc:
            self._metrics.error_responses += 1
            self._metrics.last_error = str(exc)
            logger.debug(f"DNS TCP handler error: {exc}")
        finally:
            writer.close()
            await writer.wait_closed()
