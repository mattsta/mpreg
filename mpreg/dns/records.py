from __future__ import annotations

from dataclasses import dataclass

from dnslib import AAAA, CNAME, QTYPE, RR, SRV, TXT, A


@dataclass(frozen=True, slots=True)
class DnsRecord:
    name: str
    ttl: int

    def to_rr(self) -> RR:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class ARecord(DnsRecord):
    address: str

    def to_rr(self) -> RR:
        return RR(self.name, rtype=QTYPE.A, rdata=A(self.address), ttl=self.ttl)


@dataclass(frozen=True, slots=True)
class AAAARecord(DnsRecord):
    address: str

    def to_rr(self) -> RR:
        return RR(self.name, rtype=QTYPE.AAAA, rdata=AAAA(self.address), ttl=self.ttl)


@dataclass(frozen=True, slots=True)
class CnameRecord(DnsRecord):
    target: str

    def to_rr(self) -> RR:
        return RR(self.name, rtype=QTYPE.CNAME, rdata=CNAME(self.target), ttl=self.ttl)


@dataclass(frozen=True, slots=True)
class SrvRecord(DnsRecord):
    priority: int
    weight: int
    port: int
    target: str

    def to_rr(self) -> RR:
        return RR(
            self.name,
            rtype=QTYPE.SRV,
            rdata=SRV(self.priority, self.weight, self.port, self.target),
            ttl=self.ttl,
        )


@dataclass(frozen=True, slots=True)
class TxtRecord(DnsRecord):
    value: str

    def to_rr(self) -> RR:
        return RR(self.name, rtype=QTYPE.TXT, rdata=TXT(self.value), ttl=self.ttl)
