from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import Protocol

from dnslib import RCODE

from mpreg.core.cluster_map import CatalogQueryRequest
from mpreg.core.payloads import PayloadMapping
from mpreg.datastructures.type_aliases import MetadataValue
from mpreg.fabric.catalog import (
    FunctionEndpoint,
    NodeDescriptor,
    QueueEndpoint,
    ServiceEndpoint,
)

from .names import match_zone, parse_node_labels, parse_service_labels
from .records import AAAARecord, ARecord, DnsRecord, SrvRecord, TxtRecord


class CatalogQueryProvider(Protocol):
    def __call__(self, request: CatalogQueryRequest) -> PayloadMapping: ...


@dataclass(frozen=True, slots=True)
class DnsResolverConfig:
    zones: tuple[str, ...]
    min_ttl_seconds: int = 1
    max_ttl_seconds: int = 60
    allow_external_names: bool = False


@dataclass(slots=True)
class DnsResolutionResult:
    records: list[DnsRecord]
    rcode: int


def _ip_version(host: str) -> int | None:
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return None
    return 6 if ip.version == 6 else 4


def _protocol_matches_dns_proto(protocol: str, dns_proto: str) -> bool:
    proto = protocol.lower()
    dns_proto = dns_proto.lower()
    if dns_proto == "tcp":
        return proto in {"tcp", "tcps", "ws", "wss"}
    if dns_proto == "udp":
        return proto == "udp"
    return proto == dns_proto


def _clamp_ttl(ttl_seconds: float, config: DnsResolverConfig) -> int:
    ttl = int(max(config.min_ttl_seconds, min(config.max_ttl_seconds, ttl_seconds)))
    return max(1, ttl)


class DnsResolver:
    def __init__(
        self,
        *,
        catalog_query: CatalogQueryProvider,
        config: DnsResolverConfig,
    ) -> None:
        self._catalog_query = catalog_query
        self._config = config

    def resolve(self, qname: str, qtype: str) -> DnsResolutionResult:
        zone_match = match_zone(qname, self._config.zones)
        if zone_match is None:
            if not self._config.allow_external_names:
                return DnsResolutionResult(records=[], rcode=RCODE.NXDOMAIN)
            labels = tuple(
                label for label in qname.strip().strip(".").lower().split(".") if label
            )
        else:
            labels = zone_match.relative_labels
        if not labels:
            return DnsResolutionResult(records=[], rcode=RCODE.NXDOMAIN)

        service_match = parse_service_labels(labels)
        if service_match:
            service_label, proto_label, name, namespace = service_match
            service_kind = service_label.lower()
            dns_proto = proto_label.lstrip("_").lower()
            if service_kind == "_svc":
                return self._resolve_services(qname, qtype, name, namespace, dns_proto)
            if service_kind == "_mpreg":
                return self._resolve_functions(qname, qtype, name, namespace, dns_proto)
            if service_kind == "_queue":
                return self._resolve_queues(qname, qtype, name, namespace, dns_proto)

        node_id = parse_node_labels(labels)
        if node_id:
            return self._resolve_node(qname, qtype, node_id)

        # Plain name resolves to service endpoints
        name = labels[0]
        namespace = ".".join(labels[1:]) if len(labels) > 1 else ""
        return self._resolve_services(qname, qtype, name, namespace, None)

    def _resolve_services(
        self,
        qname: str,
        qtype: str,
        name: str,
        namespace: str,
        dns_proto: str | None,
    ) -> DnsResolutionResult:
        endpoints = self._fetch_services(name=name, namespace=namespace)
        if not endpoints:
            return DnsResolutionResult(records=[], rcode=RCODE.NXDOMAIN)
        records = []
        for endpoint in endpoints:
            if dns_proto and not _protocol_matches_dns_proto(
                endpoint.protocol, dns_proto
            ):
                continue
            ttl = _clamp_ttl(endpoint.ttl_seconds, self._config)
            if qtype in {"SRV", "ANY"} and dns_proto:
                for target in endpoint.targets:
                    records.append(
                        SrvRecord(
                            name=qname,
                            ttl=ttl,
                            priority=endpoint.priority,
                            weight=endpoint.weight,
                            port=endpoint.port,
                            target=target,
                        )
                    )
            if qtype in {"A", "AAAA", "ANY"}:
                records.extend(
                    self._address_records(
                        qname,
                        ttl,
                        endpoint.targets,
                        include_a=(qtype in {"A", "ANY"}),
                        include_aaaa=(qtype in {"AAAA", "ANY"}),
                    )
                )
            if qtype in {"TXT", "ANY"}:
                records.extend(
                    self._metadata_records(
                        qname,
                        ttl,
                        namespace=endpoint.namespace,
                        tags=endpoint.tags,
                        capabilities=endpoint.capabilities,
                        cluster_id=endpoint.cluster_id,
                        node_id=endpoint.node_id,
                        extra=endpoint.metadata,
                    )
                )
        if not records:
            return DnsResolutionResult(records=[], rcode=RCODE.NOERROR)
        return DnsResolutionResult(records=records, rcode=RCODE.NOERROR)

    def _resolve_functions(
        self,
        qname: str,
        qtype: str,
        name: str,
        namespace: str,
        dns_proto: str,
    ) -> DnsResolutionResult:
        full_name = f"{namespace}.{name}" if namespace else name
        endpoints = self._fetch_functions(full_name, namespace)
        if not endpoints:
            return DnsResolutionResult(records=[], rcode=RCODE.NXDOMAIN)
        records: list[DnsRecord] = []
        node_cache: dict[tuple[str, str], list[NodeDescriptor]] = {}
        for endpoint in endpoints:
            nodes = self._fetch_nodes(endpoint.node_id, endpoint.cluster_id, node_cache)
            ttl = _clamp_ttl(endpoint.ttl_seconds, self._config)
            records.extend(
                self._records_for_nodes(
                    qname,
                    qtype,
                    nodes,
                    ttl=ttl,
                    dns_proto=dns_proto,
                    namespace=endpoint.rpc_summary.namespace
                    if endpoint.rpc_summary
                    else namespace,
                    tags=endpoint.tags,
                    capabilities=tuple(),
                    spec_digest=endpoint.spec_digest
                    or (
                        endpoint.rpc_summary.spec_digest
                        if endpoint.rpc_summary
                        else None
                    ),
                )
            )
        if not records:
            return DnsResolutionResult(records=[], rcode=RCODE.NOERROR)
        return DnsResolutionResult(records=records, rcode=RCODE.NOERROR)

    def _resolve_queues(
        self,
        qname: str,
        qtype: str,
        name: str,
        namespace: str,
        dns_proto: str,
    ) -> DnsResolutionResult:
        full_name = f"{namespace}.{name}" if namespace else name
        endpoints = self._fetch_queues(full_name)
        if not endpoints:
            return DnsResolutionResult(records=[], rcode=RCODE.NXDOMAIN)
        records: list[DnsRecord] = []
        node_cache: dict[tuple[str, str], list[NodeDescriptor]] = {}
        for endpoint in endpoints:
            nodes = self._fetch_nodes(endpoint.node_id, endpoint.cluster_id, node_cache)
            ttl = _clamp_ttl(endpoint.ttl_seconds, self._config)
            records.extend(
                self._records_for_nodes(
                    qname,
                    qtype,
                    nodes,
                    ttl=ttl,
                    dns_proto=dns_proto,
                    namespace=namespace or full_name,
                    tags=endpoint.tags,
                    capabilities=tuple(),
                    spec_digest=None,
                )
            )
        if not records:
            return DnsResolutionResult(records=[], rcode=RCODE.NOERROR)
        return DnsResolutionResult(records=records, rcode=RCODE.NOERROR)

    def _resolve_node(
        self, qname: str, qtype: str, node_id: str
    ) -> DnsResolutionResult:
        nodes = self._fetch_nodes(node_id, None, {})
        if not nodes:
            return DnsResolutionResult(records=[], rcode=RCODE.NXDOMAIN)
        records: list[DnsRecord] = []
        for node in nodes:
            ttl = _clamp_ttl(node.ttl_seconds, self._config)
            records.extend(
                self._records_for_nodes(
                    qname,
                    qtype,
                    [node],
                    ttl=ttl,
                    dns_proto=None,
                    namespace="",
                    tags=node.tags,
                    capabilities=node.capabilities,
                    spec_digest=None,
                )
            )
        if not records:
            return DnsResolutionResult(records=[], rcode=RCODE.NOERROR)
        return DnsResolutionResult(records=records, rcode=RCODE.NOERROR)

    def _address_records(
        self,
        qname: str,
        ttl: int,
        targets: tuple[str, ...],
        *,
        include_a: bool,
        include_aaaa: bool,
    ) -> list[DnsRecord]:
        records: list[DnsRecord] = []
        for target in targets:
            version = _ip_version(target)
            if version == 4 and include_a:
                records.append(ARecord(name=qname, ttl=ttl, address=target))
            elif version == 6 and include_aaaa:
                records.append(AAAARecord(name=qname, ttl=ttl, address=target))
        return records

    def _metadata_records(
        self,
        qname: str,
        ttl: int,
        *,
        namespace: str,
        tags: tuple[str, ...] | frozenset[str],
        capabilities: tuple[str, ...] | frozenset[str],
        cluster_id: str,
        node_id: str,
        extra: dict[str, MetadataValue] | None = None,
        spec_digest: str | None = None,
    ) -> list[DnsRecord]:
        entries: list[str] = []
        if namespace:
            entries.append(f"namespace={namespace}")
        if cluster_id:
            entries.append(f"cluster_id={cluster_id}")
        if node_id:
            entries.append(f"node_id={node_id}")
        if tags:
            entries.append(f"tags={','.join(sorted(tags))}")
        if capabilities:
            entries.append(f"capabilities={','.join(sorted(capabilities))}")
        if spec_digest:
            entries.append(f"spec_digest={spec_digest}")
        if extra:
            for key, value in extra.items():
                if value is None:
                    continue
                entries.append(f"{key}={value}")
        return [TxtRecord(name=qname, ttl=ttl, value=item) for item in entries]

    def _records_for_nodes(
        self,
        qname: str,
        qtype: str,
        nodes: list[NodeDescriptor],
        *,
        ttl: int,
        dns_proto: str | None,
        namespace: str,
        tags: tuple[str, ...] | frozenset[str],
        capabilities: tuple[str, ...] | frozenset[str],
        spec_digest: str | None,
    ) -> list[DnsRecord]:
        records: list[DnsRecord] = []
        for node in nodes:
            for endpoint in node.transport_endpoints:
                if dns_proto and not _protocol_matches_dns_proto(
                    endpoint.protocol, dns_proto
                ):
                    continue
                if qtype in {"SRV", "ANY"} and dns_proto:
                    records.append(
                        SrvRecord(
                            name=qname,
                            ttl=ttl,
                            priority=0,
                            weight=0,
                            port=endpoint.port,
                            target=endpoint.host,
                        )
                    )
                if qtype in {"A", "AAAA", "ANY"}:
                    records.extend(
                        self._address_records(
                            qname,
                            ttl,
                            (endpoint.host,),
                            include_a=(qtype in {"A", "ANY"}),
                            include_aaaa=(qtype in {"AAAA", "ANY"}),
                        )
                    )
            if qtype in {"TXT", "ANY"}:
                records.extend(
                    self._metadata_records(
                        qname,
                        ttl,
                        namespace=namespace,
                        tags=tags,
                        capabilities=capabilities,
                        cluster_id=node.cluster_id,
                        node_id=node.node_id,
                        spec_digest=spec_digest,
                    )
                )
        return records

    def _fetch_services(self, name: str, namespace: str) -> list[ServiceEndpoint]:
        request = CatalogQueryRequest(
            entry_type="services",
            namespace=namespace or None,
            service_name=name,
            limit=None,
            page_token=None,
        )
        payload = self._catalog_query(request)
        items = payload.get("items", []) if isinstance(payload, dict) else []
        return [
            ServiceEndpoint.from_dict(item)  # type: ignore[arg-type]
            for item in items
            if isinstance(item, dict)
        ]

    def _fetch_functions(
        self, function_name: str, namespace: str
    ) -> list[FunctionEndpoint]:
        request = CatalogQueryRequest(
            entry_type="functions",
            namespace=namespace or None,
            function_name=function_name,
            limit=None,
            page_token=None,
        )
        payload = self._catalog_query(request)
        items = payload.get("items", []) if isinstance(payload, dict) else []
        return [
            FunctionEndpoint.from_dict(item)  # type: ignore[arg-type]
            for item in items
            if isinstance(item, dict)
        ]

    def _fetch_queues(self, queue_name: str) -> list[QueueEndpoint]:
        request = CatalogQueryRequest(
            entry_type="queues",
            queue_name=queue_name,
            limit=None,
            page_token=None,
        )
        payload = self._catalog_query(request)
        items = payload.get("items", []) if isinstance(payload, dict) else []
        return [
            QueueEndpoint.from_dict(item)  # type: ignore[arg-type]
            for item in items
            if isinstance(item, dict)
        ]

    def _fetch_nodes(
        self,
        node_id: str,
        cluster_id: str | None,
        cache: dict[tuple[str, str], list[NodeDescriptor]],
    ) -> list[NodeDescriptor]:
        key = (cluster_id or "", node_id)
        if key in cache:
            return cache[key]
        request = CatalogQueryRequest(
            entry_type="nodes",
            node_id=node_id,
            cluster_id=cluster_id,
            limit=None,
            page_token=None,
        )
        payload = self._catalog_query(request)
        items = payload.get("items", []) if isinstance(payload, dict) else []
        nodes = [
            NodeDescriptor.from_dict(item)  # type: ignore[arg-type]
            for item in items
            if isinstance(item, dict)
        ]
        cache[key] = nodes
        return nodes
