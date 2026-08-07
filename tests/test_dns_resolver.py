import time

from dnslib import QTYPE, RCODE

from mpreg.core.cluster_map import CatalogQueryRequest
from mpreg.dns.resolver import DnsResolver, DnsResolverConfig
from mpreg.fabric.catalog import RoutingCatalog, ServiceEndpoint
from mpreg.fabric.index import RoutingIndex, ServiceQuery


def _build_index() -> RoutingIndex:
    catalog = RoutingCatalog()
    endpoint = ServiceEndpoint(
        name="tradefeed",
        namespace="market",
        protocol="tcp",
        port=9000,
        targets=("127.0.0.1",),
        scope="zone",
        tags=frozenset(),
        capabilities=frozenset(),
        metadata={},
        priority=0,
        weight=0,
        node_id="node-a",
        cluster_id="cluster-a",
        advertised_at=time.time(),
        ttl_seconds=60.0,
    )
    catalog.services.register(endpoint, now=time.time())
    return RoutingIndex(catalog=catalog)


def _catalog_provider(index: RoutingIndex):
    def _provider(request: CatalogQueryRequest):
        if request.entry_type != "services":
            return {"items": []}
        query = ServiceQuery(
            namespace=request.namespace,
            name=request.service_name,
            protocol=request.service_protocol,
            port=request.service_port,
            cluster_id=request.cluster_id,
            node_id=request.node_id,
        )
        endpoints = index.find_services(query, now=time.time())
        return {"items": [entry.to_dict() for entry in endpoints]}

    return _provider


def test_dns_resolver_external_names_toggle() -> None:
    index = _build_index()
    provider = _catalog_provider(index)

    resolver = DnsResolver(
        catalog_query=provider,
        config=DnsResolverConfig(zones=("mpreg",), allow_external_names=False),
    )
    result_zone = resolver.resolve("_svc._tcp.tradefeed.market.mpreg", "SRV")
    assert result_zone.rcode == RCODE.NOERROR
    assert any(record.to_rr().rtype == QTYPE.SRV for record in result_zone.records)

    result_no_zone = resolver.resolve("_svc._tcp.tradefeed.market", "SRV")
    assert result_no_zone.rcode == RCODE.NXDOMAIN

    resolver_external = DnsResolver(
        catalog_query=provider,
        config=DnsResolverConfig(zones=("mpreg",), allow_external_names=True),
    )
    result_external = resolver_external.resolve("_svc._tcp.tradefeed.market", "SRV")
    assert result_external.rcode == RCODE.NOERROR
    assert any(record.to_rr().rtype == QTYPE.SRV for record in result_external.records)
