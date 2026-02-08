from __future__ import annotations

import asyncio
from dataclasses import dataclass

from dnslib import QTYPE, RCODE, DNSRecord

from mpreg.datastructures.type_aliases import JsonDict


@dataclass(frozen=True, slots=True)
class DnsAnswer:
    name: str
    rtype: str
    ttl: int
    rdata: str

    def to_dict(self) -> JsonDict:
        return {
            "name": self.name,
            "rtype": self.rtype,
            "ttl": int(self.ttl),
            "rdata": self.rdata,
        }


@dataclass(frozen=True, slots=True)
class DnsResolveResult:
    qname: str
    qtype: str
    rcode: str
    answers: tuple[DnsAnswer, ...]

    def to_dict(self) -> JsonDict:
        return {
            "qname": self.qname,
            "qtype": self.qtype,
            "rcode": self.rcode,
            "answers": [answer.to_dict() for answer in self.answers],
        }


class MPREGDnsClient:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        use_tcp: bool = False,
        timeout: float = 2.0,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._use_tcp = use_tcp
        self._timeout = float(timeout)

    async def resolve(
        self,
        qname: str,
        qtype: str = "A",
        *,
        timeout: float | None = None,
        use_tcp: bool | None = None,
    ) -> DnsResolveResult:
        qtype_name = (qtype or "A").strip().upper()
        try:
            getattr(QTYPE, qtype_name)
        except Exception as exc:
            raise ValueError(f"Unsupported qtype: {qtype_name}") from exc
        selected_timeout = self._timeout if timeout is None else float(timeout)
        selected_tcp = self._use_tcp if use_tcp is None else use_tcp
        if selected_tcp:
            response = await _tcp_dns_query(
                self._host, self._port, qname, qtype_name, selected_timeout
            )
        else:
            response = await _udp_dns_query(
                self._host, self._port, qname, qtype_name, selected_timeout
            )
        rcode = RCODE.get(response.header.rcode)
        answers = _dns_answers_from_response(response)
        return DnsResolveResult(
            qname=qname,
            qtype=qtype_name,
            rcode=str(rcode),
            answers=answers,
        )


def _dns_answers_from_response(response: DNSRecord) -> tuple[DnsAnswer, ...]:
    answers: list[DnsAnswer] = []
    for rr in response.rr:
        answers.append(
            DnsAnswer(
                name=str(rr.rname).rstrip("."),
                rtype=str(QTYPE[rr.rtype]),
                ttl=int(rr.ttl),
                rdata=str(rr.rdata),
            )
        )
    return tuple(answers)


async def _udp_dns_query(
    host: str, port: int, qname: str, qtype: str, timeout: float
) -> DNSRecord:
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
        data = await asyncio.wait_for(response_future, timeout=timeout)
    finally:
        transport.close()
    return DNSRecord.parse(data)


async def _tcp_dns_query(
    host: str, port: int, qname: str, qtype: str, timeout: float
) -> DNSRecord:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=timeout
    )
    query = DNSRecord.question(qname, qtype).pack()
    writer.write(len(query).to_bytes(2, byteorder="big") + query)
    await writer.drain()
    length_data = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
    length = int.from_bytes(length_data, byteorder="big")
    data = await asyncio.wait_for(reader.readexactly(length), timeout=timeout)
    writer.close()
    await writer.wait_closed()
    return DNSRecord.parse(data)
