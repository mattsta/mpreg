from dnslib import QTYPE, RR, SRV, DNSRecord

from mpreg.fabric.auto_discovery import (
    DiscoveryConfiguration,
    DiscoveryProtocol,
    DNSDiscoveryBackend,
)


def test_dns_auto_discovery_srv_parsing() -> None:
    config = DiscoveryConfiguration(
        protocol=DiscoveryProtocol.DNS_SRV, dns_domain="example.com"
    )
    backend = DNSDiscoveryBackend(config)
    response = DNSRecord.question("_mpreg._tcp.example.com", "SRV")
    response.add_answer(
        RR(
            "_mpreg._tcp.example.com",
            rtype=QTYPE.SRV,
            rdata=SRV(10, 5, 9000, "node.example.com"),
            ttl=60,
        )
    )
    records = backend._srv_records_from_response(response)
    assert records
    record = records[0]
    assert record.priority == 10
    assert record.weight == 5
    assert record.port == 9000
    assert record.host == "node.example.com"


def test_dns_auto_discovery_resolver_override() -> None:
    config = DiscoveryConfiguration(
        protocol=DiscoveryProtocol.DNS_SRV,
        dns_domain="example.com",
        dns_resolver_host="127.0.0.1",
        dns_resolver_port=54,
    )
    backend = DNSDiscoveryBackend(config)
    assert backend._resolve_nameservers() == [("127.0.0.1", 54)]
