from __future__ import annotations

import pytest
from dnslib import DNSRecord

from mpreg.dns.records import TxtRecord
from mpreg.dns.resolver import DnsResolutionResult
from mpreg.dns.server import DnsGateway


class _StubResolver:
    def __init__(self, records: list[TxtRecord]) -> None:
        self._records = records

    def resolve(self, qname: str, qtype: str) -> DnsResolutionResult:
        return DnsResolutionResult(records=list(self._records), rcode=0)


@pytest.mark.asyncio
async def test_dns_gateway_udp_truncates_large_response() -> None:
    records = [
        TxtRecord(name="example.com", ttl=60, value="x" * 200) for _ in range(10)
    ]
    gateway = DnsGateway(
        resolver=_StubResolver(records),
        host="127.0.0.1",
        udp_port=0,
        tcp_port=0,
    )
    query = DNSRecord.question("example.com", "TXT")
    response_bytes = await gateway.handle_query(query.pack(), via="udp")
    assert response_bytes is not None
    response = DNSRecord.parse(response_bytes)
    assert response.header.tc == 1
    assert response.header.rcode == 0
    assert len(response.rr) > 0


@pytest.mark.asyncio
async def test_dns_gateway_tcp_does_not_truncate() -> None:
    records = [
        TxtRecord(name="example.com", ttl=60, value="x" * 200) for _ in range(10)
    ]
    gateway = DnsGateway(
        resolver=_StubResolver(records),
        host="127.0.0.1",
        udp_port=0,
        tcp_port=0,
    )
    query = DNSRecord.question("example.com", "TXT")
    response_bytes = await gateway.handle_query(query.pack(), via="tcp")
    assert response_bytes is not None
    response = DNSRecord.parse(response_bytes)
    assert response.header.tc == 0
    assert response.header.rcode == 0
    assert len(response.rr) == len(records)
