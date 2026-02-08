from __future__ import annotations

from dataclasses import dataclass, field

from mpreg.core.payloads import (
    PAYLOAD_FLOAT,
    PAYLOAD_INT,
    PAYLOAD_KEEP_EMPTY,
    PAYLOAD_LIST,
    Payload,
    PayloadMapping,
    payload_from_dataclass,
)
from mpreg.datastructures.service_spec import ServiceSpec
from mpreg.datastructures.type_aliases import (
    EndpointScope,
    MetadataValue,
    NamespaceName,
    PortNumber,
    ServiceName,
    Timestamp,
    TransportProtocolName,
)


def _normalize_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(str(item) for item in value)
    return (str(value),)


@dataclass(frozen=True, slots=True)
class DnsRegisterRequest:
    name: ServiceName
    namespace: NamespaceName
    protocol: TransportProtocolName
    port: PortNumber = field(metadata={PAYLOAD_INT: True})
    targets: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    scope: EndpointScope | None = None
    tags: tuple[str, ...] = field(default_factory=tuple, metadata={PAYLOAD_LIST: True})
    capabilities: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    metadata: dict[str, MetadataValue] = field(
        default_factory=dict, metadata={PAYLOAD_KEEP_EMPTY: True}
    )
    priority: int = field(default=0, metadata={PAYLOAD_INT: True})
    weight: int = field(default=0, metadata={PAYLOAD_INT: True})
    ttl_seconds: float | None = field(default=None, metadata={PAYLOAD_FLOAT: True})

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> DnsRegisterRequest:
        data = payload or {}
        raw_metadata = data.get("metadata", {}) or {}
        metadata: dict[str, MetadataValue] = {}
        if isinstance(raw_metadata, dict):
            for key, value in raw_metadata.items():
                if key is None or value is None:
                    continue
                if isinstance(value, (str, int, float, bool)):
                    metadata[str(key)] = value
                else:
                    metadata[str(key)] = str(value)
        return cls(
            name=str(data.get("name", "")),
            namespace=str(data.get("namespace", "")),
            protocol=str(data.get("protocol", "")),
            port=int(data.get("port", 0) or 0),
            targets=_normalize_tuple(data.get("targets")),
            scope=str(data.get("scope")).lower()
            if data.get("scope") is not None
            else None,
            tags=_normalize_tuple(data.get("tags")),
            capabilities=_normalize_tuple(data.get("capabilities")),
            metadata=metadata,
            priority=int(data.get("priority", 0) or 0),
            weight=int(data.get("weight", 0) or 0),
            ttl_seconds=(
                float(data.get("ttl_seconds"))
                if data.get("ttl_seconds") is not None
                else None
            ),
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class DnsRegisterResponse:
    registration_id: str
    service: ServiceSpec
    registered_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> DnsRegisterResponse:
        service_payload = payload.get("service", {})
        service = (
            ServiceSpec.from_dict(service_payload)
            if isinstance(service_payload, dict)
            else ServiceSpec(name="", namespace="", protocol="", port=0)
        )
        return cls(
            registration_id=str(payload.get("registration_id", "")),
            service=service,
            registered_at=float(payload.get("registered_at", 0.0) or 0.0),
        )


@dataclass(frozen=True, slots=True)
class DnsUnregisterRequest:
    name: ServiceName
    namespace: NamespaceName
    protocol: TransportProtocolName
    port: PortNumber = field(metadata={PAYLOAD_INT: True})

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> DnsUnregisterRequest:
        data = payload or {}
        return cls(
            name=str(data.get("name", "")),
            namespace=str(data.get("namespace", "")),
            protocol=str(data.get("protocol", "")),
            port=int(data.get("port", 0) or 0),
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class DnsUnregisterResponse:
    removed: bool
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> DnsUnregisterResponse:
        return cls(
            removed=bool(payload.get("removed", False)),
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
        )


@dataclass(frozen=True, slots=True)
class DnsListRequest:
    namespace: NamespaceName | None = None
    name: ServiceName | None = None
    protocol: TransportProtocolName | None = None
    port: PortNumber | None = field(default=None, metadata={PAYLOAD_INT: True})
    scope: EndpointScope | None = None
    tags: tuple[str, ...] = field(default_factory=tuple, metadata={PAYLOAD_LIST: True})
    capabilities: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    cluster_id: str | None = None
    node_id: str | None = None
    limit: int | None = field(default=None, metadata={PAYLOAD_INT: True})
    page_token: str | None = None

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> DnsListRequest:
        data = payload or {}
        return cls(
            namespace=str(data.get("namespace"))
            if data.get("namespace") is not None
            else None,
            name=str(data.get("name")) if data.get("name") is not None else None,
            protocol=str(data.get("protocol"))
            if data.get("protocol") is not None
            else None,
            port=int(data.get("port")) if data.get("port") is not None else None,
            scope=str(data.get("scope")).lower()
            if data.get("scope") is not None
            else None,
            tags=_normalize_tuple(data.get("tags")),
            capabilities=_normalize_tuple(data.get("capabilities")),
            cluster_id=str(data.get("cluster_id"))
            if data.get("cluster_id") is not None
            else None,
            node_id=str(data.get("node_id"))
            if data.get("node_id") is not None
            else None,
            limit=int(data.get("limit")) if data.get("limit") is not None else None,
            page_token=str(data.get("page_token")) if data.get("page_token") else None,
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class DnsListResponse:
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    items: tuple[Payload, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    next_page_token: str | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> DnsListResponse:
        items_payload = payload.get("items", []) or []
        items = tuple(item for item in items_payload if isinstance(item, dict))
        return cls(
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            items=items,
            next_page_token=(
                str(payload.get("next_page_token"))
                if payload.get("next_page_token") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class DnsDescribeRequest:
    namespace: NamespaceName | None = None
    name: ServiceName | None = None
    protocol: TransportProtocolName | None = None
    port: PortNumber | None = field(default=None, metadata={PAYLOAD_INT: True})
    scope: EndpointScope | None = None
    tags: tuple[str, ...] = field(default_factory=tuple, metadata={PAYLOAD_LIST: True})
    capabilities: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    cluster_id: str | None = None
    node_id: str | None = None
    limit: int | None = field(default=None, metadata={PAYLOAD_INT: True})
    page_token: str | None = None

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> DnsDescribeRequest:
        data = payload or {}
        return cls(
            namespace=str(data.get("namespace"))
            if data.get("namespace") is not None
            else None,
            name=str(data.get("name")) if data.get("name") is not None else None,
            protocol=str(data.get("protocol"))
            if data.get("protocol") is not None
            else None,
            port=int(data.get("port")) if data.get("port") is not None else None,
            scope=str(data.get("scope")).lower()
            if data.get("scope") is not None
            else None,
            tags=_normalize_tuple(data.get("tags")),
            capabilities=_normalize_tuple(data.get("capabilities")),
            cluster_id=str(data.get("cluster_id"))
            if data.get("cluster_id") is not None
            else None,
            node_id=str(data.get("node_id"))
            if data.get("node_id") is not None
            else None,
            limit=int(data.get("limit")) if data.get("limit") is not None else None,
            page_token=str(data.get("page_token")) if data.get("page_token") else None,
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class DnsDescribeResponse:
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    items: tuple[Payload, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    next_page_token: str | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> DnsDescribeResponse:
        items_payload = payload.get("items", []) or []
        items = tuple(item for item in items_payload if isinstance(item, dict))
        return cls(
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            items=items,
            next_page_token=(
                str(payload.get("next_page_token"))
                if payload.get("next_page_token") is not None
                else None
            ),
        )
