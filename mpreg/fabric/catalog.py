"""Unified routing catalog for fabric discovery and selection."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from mpreg.datastructures.function_identity import FunctionIdentity, FunctionSelector
from mpreg.datastructures.rpc_spec import RpcSpec, RpcSpecSummary
from mpreg.datastructures.trie import TopicTrie
from mpreg.datastructures.type_aliases import (
    ClusterId,
    ConnectionTypeName,
    DurationSeconds,
    EndpointScope,
    FunctionId,
    FunctionName,
    HostAddress,
    JsonDict,
    MetadataKey,
    MetadataValue,
    NodeId,
    PortNumber,
    QueueName,
    RegionName,
    RpcSpecDigest,
    ServiceName,
    SubscriptionId,
    Timestamp,
    TransportProtocolName,
)

from .federation_graph import GeographicCoordinate
from .message import DeliveryGuarantee

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mpreg.core.transport.adapter_registry import ProtocolPortAssignment

DEFAULT_ENDPOINT_SCOPE: EndpointScope = "zone"
VALID_ENDPOINT_SCOPES = {"local", "zone", "region", "global"}
SCOPE_RANKS: dict[str, int] = {"local": 0, "zone": 1, "region": 2, "global": 3}


def normalize_endpoint_scope(value: str | None) -> EndpointScope:
    if value is None:
        return DEFAULT_ENDPOINT_SCOPE
    raw = str(value).strip().lower()
    if not raw:
        return DEFAULT_ENDPOINT_SCOPE
    alias = "zone" if raw == "cluster" else raw
    if alias not in VALID_ENDPOINT_SCOPES:
        return DEFAULT_ENDPOINT_SCOPE
    return alias


def endpoint_scope_rank(scope: str | None) -> int:
    normalized = normalize_endpoint_scope(scope)
    return SCOPE_RANKS.get(normalized, SCOPE_RANKS[DEFAULT_ENDPOINT_SCOPE])


def _normalize_tags(tags: object) -> frozenset[str]:
    if tags is None:
        return frozenset()
    if isinstance(tags, frozenset):
        return frozenset(str(tag) for tag in tags if tag)
    if isinstance(tags, (list, tuple, set)):
        return frozenset(str(tag) for tag in tags if tag)
    return frozenset(str(tags)) if tags else frozenset()


def _normalize_metadata(metadata: object) -> dict[MetadataKey, MetadataValue]:
    if not isinstance(metadata, dict):
        return {}
    normalized: dict[MetadataKey, MetadataValue] = {}
    for key, value in metadata.items():
        if key is None or value is None:
            continue
        key_str = str(key)
        if isinstance(value, (str, int, float, bool)):
            normalized[key_str] = value
        else:
            normalized[key_str] = str(value)
    return normalized


def _is_stale_advertisement_update(
    *,
    existing_advertised_at: Timestamp,
    existing_ttl_seconds: DurationSeconds,
    incoming_advertised_at: Timestamp,
    incoming_ttl_seconds: DurationSeconds,
) -> bool:
    """Reject updates that would regress freshness for the same catalog key."""
    if incoming_advertised_at < existing_advertised_at:
        return True
    if incoming_advertised_at > existing_advertised_at:
        return False
    return incoming_ttl_seconds <= existing_ttl_seconds


@dataclass(frozen=True, slots=True)
class TransportEndpoint:
    """Advertised transport endpoint for a node."""

    connection_type: ConnectionTypeName
    protocol: TransportProtocolName
    host: HostAddress
    port: PortNumber

    @property
    def endpoint(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}"

    def to_dict(self) -> JsonDict:
        return {
            "connection_type": self.connection_type,
            "protocol": self.protocol,
            "host": self.host,
            "port": int(self.port),
            "endpoint": self.endpoint,
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> TransportEndpoint:
        host = str(payload.get("host", ""))
        protocol = str(payload.get("protocol", ""))
        port = int(payload.get("port", 0) or 0)
        endpoint = str(payload.get("endpoint", ""))
        if endpoint and (not host or not protocol or not port):
            from urllib.parse import urlparse

            parsed = urlparse(endpoint)
            protocol = protocol or parsed.scheme
            host = host or parsed.hostname or ""
            port = port or (parsed.port or 0)
        return cls(
            connection_type=str(payload.get("connection_type", "")),
            protocol=protocol,
            host=host,
            port=port,
        )

    @classmethod
    def from_assignment(cls, assignment: ProtocolPortAssignment) -> TransportEndpoint:
        return cls(
            connection_type=assignment.connection_type.value,
            protocol=assignment.protocol.value,
            host=assignment.host,
            port=assignment.port,
        )


@dataclass(frozen=True, slots=True)
class NodeDescriptor:
    node_id: NodeId = ""
    cluster_id: ClusterId = ""
    region: RegionName = ""
    scope: EndpointScope = DEFAULT_ENDPOINT_SCOPE
    tags: frozenset[str] = field(default_factory=frozenset)
    resources: frozenset[str] = field(default_factory=frozenset)
    capabilities: frozenset[str] = field(default_factory=frozenset)
    transport_endpoints: tuple[TransportEndpoint, ...] = field(default_factory=tuple)
    advertised_at: Timestamp = field(default_factory=time.time)
    ttl_seconds: DurationSeconds = 30.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "scope", normalize_endpoint_scope(self.scope))
        object.__setattr__(self, "tags", _normalize_tags(self.tags))
        if self.transport_endpoints:
            ordered = tuple(
                sorted(
                    self.transport_endpoints,
                    key=lambda entry: (
                        entry.connection_type,
                        entry.protocol,
                        entry.host,
                        entry.port,
                    ),
                )
            )
            object.__setattr__(self, "transport_endpoints", ordered)

    def key(self) -> NodeKey:
        return NodeKey(cluster_id=self.cluster_id, node_id=self.node_id)

    def is_expired(self, now: Timestamp | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.advertised_at + self.ttl_seconds)

    def to_dict(self) -> JsonDict:
        payload = {
            "node_id": self.node_id,
            "cluster_id": self.cluster_id,
            "scope": self.scope,
            "resources": sorted(self.resources),
            "capabilities": sorted(self.capabilities),
            "transport_endpoints": [
                entry.to_dict() for entry in self.transport_endpoints
            ],
            "tags": sorted(self.tags),
            "advertised_at": float(self.advertised_at),
            "ttl_seconds": float(self.ttl_seconds),
        }
        if self.region:
            payload["region"] = self.region
        return payload

    @classmethod
    def from_dict(cls, payload: JsonDict) -> NodeDescriptor:
        return cls(
            node_id=str(payload.get("node_id", "")),
            cluster_id=str(payload.get("cluster_id", "")),
            region=str(payload.get("region", "")),
            scope=normalize_endpoint_scope(payload.get("scope")),
            tags=_normalize_tags(payload.get("tags")),
            resources=frozenset(payload.get("resources", [])),
            capabilities=frozenset(payload.get("capabilities", [])),
            transport_endpoints=tuple(
                TransportEndpoint.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("transport_endpoints", [])
            ),
            advertised_at=float(payload.get("advertised_at", time.time())),
            ttl_seconds=float(payload.get("ttl_seconds", 30.0)),
        )


@dataclass(frozen=True, slots=True)
class NodeKey:
    cluster_id: ClusterId
    node_id: NodeId

    def to_dict(self) -> dict[str, str]:
        return {"cluster_id": self.cluster_id, "node_id": self.node_id}

    @classmethod
    def from_dict(cls, payload: JsonDict) -> NodeKey:
        return cls(
            cluster_id=str(payload.get("cluster_id", "")),
            node_id=str(payload.get("node_id", "")),
        )


@dataclass(frozen=True, slots=True)
class FunctionEndpoint:
    identity: FunctionIdentity
    resources: frozenset[str]
    node_id: NodeId
    cluster_id: ClusterId
    scope: EndpointScope = DEFAULT_ENDPOINT_SCOPE
    tags: frozenset[str] = field(default_factory=frozenset)
    rpc_summary: RpcSpecSummary | None = None
    rpc_spec: RpcSpec | None = None
    spec_digest: RpcSpecDigest | None = None
    advertised_at: Timestamp = field(default_factory=time.time)
    ttl_seconds: DurationSeconds = 30.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "scope", normalize_endpoint_scope(self.scope))
        object.__setattr__(self, "tags", _normalize_tags(self.tags))
        if self.rpc_spec is not None and self.rpc_summary is None:
            object.__setattr__(self, "rpc_summary", self.rpc_spec.summary())
        if self.rpc_spec is not None and self.spec_digest is None:
            object.__setattr__(self, "spec_digest", self.rpc_spec.spec_digest)

    def key(self) -> FunctionKey:
        return FunctionKey(
            cluster_id=self.cluster_id,
            node_id=self.node_id,
            identity=self.identity,
            resources=self.resources,
        )

    def is_expired(self, now: Timestamp | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.advertised_at + self.ttl_seconds)

    def to_dict(self) -> JsonDict:
        payload: JsonDict = {
            "identity": self.identity.to_dict(),
            "resources": sorted(self.resources),
            "node_id": self.node_id,
            "cluster_id": self.cluster_id,
            "scope": self.scope,
            "tags": sorted(self.tags),
            "advertised_at": float(self.advertised_at),
            "ttl_seconds": float(self.ttl_seconds),
        }
        if self.rpc_summary is not None:
            payload["rpc_summary"] = self.rpc_summary.to_dict()
        if self.rpc_spec is not None:
            payload["rpc_spec"] = self.rpc_spec.to_dict()
        if self.spec_digest is not None:
            payload["spec_digest"] = self.spec_digest
        return payload

    @classmethod
    def from_dict(cls, payload: JsonDict) -> FunctionEndpoint:
        return cls(
            identity=FunctionIdentity.from_dict(
                payload.get("identity", {})  # type: ignore[arg-type]
            ),
            resources=frozenset(payload.get("resources", [])),
            node_id=str(payload.get("node_id", "")),
            cluster_id=str(payload.get("cluster_id", "")),
            scope=normalize_endpoint_scope(payload.get("scope")),
            tags=_normalize_tags(payload.get("tags")),
            rpc_summary=RpcSpecSummary.from_dict(
                payload.get("rpc_summary", {})  # type: ignore[arg-type]
            )
            if payload.get("rpc_summary") is not None
            else None,
            rpc_spec=RpcSpec.from_dict(
                payload.get("rpc_spec", {})  # type: ignore[arg-type]
            )
            if payload.get("rpc_spec") is not None
            else None,
            spec_digest=str(payload.get("spec_digest"))
            if payload.get("spec_digest") is not None
            else None,
            advertised_at=float(payload.get("advertised_at", time.time())),
            ttl_seconds=float(payload.get("ttl_seconds", 30.0)),
        )


@dataclass(frozen=True, slots=True)
class FunctionKey:
    cluster_id: ClusterId
    node_id: NodeId
    identity: FunctionIdentity
    resources: frozenset[str]

    def to_dict(self) -> JsonDict:
        return {
            "cluster_id": self.cluster_id,
            "node_id": self.node_id,
            "identity": self.identity.to_dict(),
            "resources": sorted(self.resources),
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> FunctionKey:
        return cls(
            cluster_id=str(payload.get("cluster_id", "")),
            node_id=str(payload.get("node_id", "")),
            identity=FunctionIdentity.from_dict(
                payload.get("identity", {})  # type: ignore[arg-type]
            ),
            resources=frozenset(payload.get("resources", [])),
        )


@dataclass(frozen=True, slots=True)
class TopicSubscription:
    subscription_id: SubscriptionId
    node_id: NodeId
    cluster_id: ClusterId
    patterns: tuple[str, ...]
    scope: EndpointScope = DEFAULT_ENDPOINT_SCOPE
    tags: frozenset[str] = field(default_factory=frozenset)
    advertised_at: Timestamp = field(default_factory=time.time)
    ttl_seconds: DurationSeconds = 30.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "scope", normalize_endpoint_scope(self.scope))
        object.__setattr__(self, "tags", _normalize_tags(self.tags))

    def is_expired(self, now: Timestamp | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.advertised_at + self.ttl_seconds)

    def to_dict(self) -> JsonDict:
        return {
            "subscription_id": self.subscription_id,
            "node_id": self.node_id,
            "cluster_id": self.cluster_id,
            "patterns": list(self.patterns),
            "scope": self.scope,
            "tags": sorted(self.tags),
            "advertised_at": float(self.advertised_at),
            "ttl_seconds": float(self.ttl_seconds),
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> TopicSubscription:
        return cls(
            subscription_id=str(payload.get("subscription_id", "")),
            node_id=str(payload.get("node_id", "")),
            cluster_id=str(payload.get("cluster_id", "")),
            patterns=tuple(payload.get("patterns", [])),
            scope=normalize_endpoint_scope(payload.get("scope")),
            tags=_normalize_tags(payload.get("tags")),
            advertised_at=float(payload.get("advertised_at", time.time())),
            ttl_seconds=float(payload.get("ttl_seconds", 30.0)),
        )


class QueueHealth(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class QueueEndpoint:
    queue_name: QueueName
    cluster_id: ClusterId
    node_id: NodeId
    delivery_guarantees: frozenset[DeliveryGuarantee] = field(default_factory=frozenset)
    health: QueueHealth = QueueHealth.HEALTHY
    current_subscribers: int = 0
    metadata: dict[str, str] = field(default_factory=dict)
    scope: EndpointScope = DEFAULT_ENDPOINT_SCOPE
    tags: frozenset[str] = field(default_factory=frozenset)
    advertised_at: Timestamp = field(default_factory=time.time)
    ttl_seconds: DurationSeconds = 30.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "scope", normalize_endpoint_scope(self.scope))
        object.__setattr__(self, "tags", _normalize_tags(self.tags))

    def key(self) -> QueueKey:
        return QueueKey(
            cluster_id=self.cluster_id,
            node_id=self.node_id,
            queue_name=self.queue_name,
        )

    def is_expired(self, now: Timestamp | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.advertised_at + self.ttl_seconds)

    def to_dict(self) -> JsonDict:
        return {
            "queue_name": self.queue_name,
            "cluster_id": self.cluster_id,
            "node_id": self.node_id,
            "delivery_guarantees": [g.value for g in self.delivery_guarantees],
            "health": self.health.value,
            "current_subscribers": int(self.current_subscribers),
            "metadata": dict(self.metadata),
            "scope": self.scope,
            "tags": sorted(self.tags),
            "advertised_at": float(self.advertised_at),
            "ttl_seconds": float(self.ttl_seconds),
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> QueueEndpoint:
        raw_guarantees = payload.get("delivery_guarantees", [])
        return cls(
            queue_name=str(payload.get("queue_name", "")),
            cluster_id=str(payload.get("cluster_id", "")),
            node_id=str(payload.get("node_id", "")),
            delivery_guarantees=frozenset(
                DeliveryGuarantee(value) for value in raw_guarantees
            ),
            health=QueueHealth(str(payload.get("health", QueueHealth.HEALTHY.value))),
            current_subscribers=int(payload.get("current_subscribers", 0)),
            metadata=dict(payload.get("metadata", {})),
            scope=normalize_endpoint_scope(payload.get("scope")),
            tags=_normalize_tags(payload.get("tags")),
            advertised_at=float(payload.get("advertised_at", time.time())),
            ttl_seconds=float(payload.get("ttl_seconds", 30.0)),
        )


@dataclass(frozen=True, slots=True)
class QueueKey:
    cluster_id: ClusterId
    node_id: NodeId
    queue_name: QueueName

    def to_dict(self) -> dict[str, str]:
        return {
            "cluster_id": self.cluster_id,
            "node_id": self.node_id,
            "queue_name": self.queue_name,
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> QueueKey:
        return cls(
            cluster_id=str(payload.get("cluster_id", "")),
            node_id=str(payload.get("node_id", "")),
            queue_name=str(payload.get("queue_name", "")),
        )


@dataclass(frozen=True, slots=True)
class ServiceEndpoint:
    name: ServiceName
    namespace: str
    protocol: TransportProtocolName
    port: PortNumber
    targets: tuple[HostAddress, ...] = field(default_factory=tuple)
    scope: EndpointScope = DEFAULT_ENDPOINT_SCOPE
    tags: frozenset[str] = field(default_factory=frozenset)
    capabilities: frozenset[str] = field(default_factory=frozenset)
    metadata: dict[MetadataKey, MetadataValue] = field(default_factory=dict)
    priority: int = 0
    weight: int = 0
    node_id: NodeId = ""
    cluster_id: ClusterId = ""
    advertised_at: Timestamp = field(default_factory=time.time)
    ttl_seconds: DurationSeconds = 30.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "scope", normalize_endpoint_scope(self.scope))
        object.__setattr__(self, "tags", _normalize_tags(self.tags))
        object.__setattr__(self, "capabilities", _normalize_tags(self.capabilities))
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))
        targets = tuple(sorted({str(target) for target in self.targets if target}))
        object.__setattr__(self, "targets", targets)
        object.__setattr__(self, "priority", int(self.priority))
        object.__setattr__(self, "weight", int(self.weight))
        object.__setattr__(self, "port", int(self.port))

    def key(self) -> ServiceKey:
        return ServiceKey(
            cluster_id=self.cluster_id,
            node_id=self.node_id,
            namespace=self.namespace,
            name=self.name,
            protocol=self.protocol,
            port=self.port,
        )

    def is_expired(self, now: Timestamp | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.advertised_at + self.ttl_seconds)

    def to_dict(self) -> JsonDict:
        payload: JsonDict = {
            "name": self.name,
            "namespace": self.namespace,
            "protocol": self.protocol,
            "port": int(self.port),
            "targets": list(self.targets),
            "scope": self.scope,
            "tags": sorted(self.tags),
            "capabilities": sorted(self.capabilities),
            "metadata": dict(self.metadata),
            "priority": int(self.priority),
            "weight": int(self.weight),
            "node_id": self.node_id,
            "cluster_id": self.cluster_id,
            "advertised_at": float(self.advertised_at),
            "ttl_seconds": float(self.ttl_seconds),
        }
        return payload

    @classmethod
    def from_dict(cls, payload: JsonDict) -> ServiceEndpoint:
        return cls(
            name=str(payload.get("name", "")),
            namespace=str(payload.get("namespace", "")),
            protocol=str(payload.get("protocol", "")),
            port=int(payload.get("port", 0) or 0),
            targets=tuple(payload.get("targets", []) or []),
            scope=normalize_endpoint_scope(payload.get("scope")),
            tags=_normalize_tags(payload.get("tags")),
            capabilities=_normalize_tags(payload.get("capabilities")),
            metadata=_normalize_metadata(payload.get("metadata")),
            priority=int(payload.get("priority", 0) or 0),
            weight=int(payload.get("weight", 0) or 0),
            node_id=str(payload.get("node_id", "")),
            cluster_id=str(payload.get("cluster_id", "")),
            advertised_at=float(payload.get("advertised_at", time.time())),
            ttl_seconds=float(payload.get("ttl_seconds", 30.0)),
        )


@dataclass(frozen=True, slots=True)
class ServiceKey:
    cluster_id: ClusterId
    node_id: NodeId
    namespace: str
    name: ServiceName
    protocol: TransportProtocolName
    port: PortNumber

    def to_dict(self) -> JsonDict:
        return {
            "cluster_id": self.cluster_id,
            "node_id": self.node_id,
            "namespace": self.namespace,
            "name": self.name,
            "protocol": self.protocol,
            "port": int(self.port),
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> ServiceKey:
        return cls(
            cluster_id=str(payload.get("cluster_id", "")),
            node_id=str(payload.get("node_id", "")),
            namespace=str(payload.get("namespace", "")),
            name=str(payload.get("name", "")),
            protocol=str(payload.get("protocol", "")),
            port=int(payload.get("port", 0) or 0),
        )


class CacheRole(Enum):
    COORDINATOR = "coordinator"
    INVALIDATOR = "invalidator"
    REPLICA = "replica"
    SYNC = "sync"
    MONITOR = "monitor"


@dataclass(frozen=True, slots=True)
class CacheRoleEntry:
    role: CacheRole
    node_id: NodeId
    cluster_id: ClusterId
    scope: EndpointScope = DEFAULT_ENDPOINT_SCOPE
    tags: frozenset[str] = field(default_factory=frozenset)
    advertised_at: Timestamp = field(default_factory=time.time)
    ttl_seconds: DurationSeconds = 30.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "scope", normalize_endpoint_scope(self.scope))
        object.__setattr__(self, "tags", _normalize_tags(self.tags))

    def key(self) -> CacheRoleKey:
        return CacheRoleKey(
            cluster_id=self.cluster_id,
            node_id=self.node_id,
            role=self.role,
        )

    def is_expired(self, now: Timestamp | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.advertised_at + self.ttl_seconds)

    def to_dict(self) -> JsonDict:
        return {
            "role": self.role.value,
            "node_id": self.node_id,
            "cluster_id": self.cluster_id,
            "scope": self.scope,
            "tags": sorted(self.tags),
            "advertised_at": float(self.advertised_at),
            "ttl_seconds": float(self.ttl_seconds),
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> CacheRoleEntry:
        return cls(
            role=CacheRole(str(payload.get("role", CacheRole.COORDINATOR.value))),
            node_id=str(payload.get("node_id", "")),
            cluster_id=str(payload.get("cluster_id", "")),
            scope=normalize_endpoint_scope(payload.get("scope")),
            tags=_normalize_tags(payload.get("tags")),
            advertised_at=float(payload.get("advertised_at", time.time())),
            ttl_seconds=float(payload.get("ttl_seconds", 30.0)),
        )


@dataclass(frozen=True, slots=True)
class CacheRoleKey:
    cluster_id: ClusterId
    node_id: NodeId
    role: CacheRole

    def to_dict(self) -> JsonDict:
        return {
            "cluster_id": self.cluster_id,
            "node_id": self.node_id,
            "role": self.role.value,
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> CacheRoleKey:
        return cls(
            cluster_id=str(payload.get("cluster_id", "")),
            node_id=str(payload.get("node_id", "")),
            role=CacheRole(str(payload.get("role", CacheRole.COORDINATOR.value))),
        )


@dataclass(frozen=True, slots=True)
class CacheNodeProfile:
    node_id: NodeId
    cluster_id: ClusterId
    region: RegionName
    coordinates: GeographicCoordinate
    capacity_mb: int
    utilization_percent: float
    avg_latency_ms: float
    reliability_score: float
    scope: EndpointScope = DEFAULT_ENDPOINT_SCOPE
    tags: frozenset[str] = field(default_factory=frozenset)
    advertised_at: Timestamp = field(default_factory=time.time)
    ttl_seconds: DurationSeconds = 30.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "scope", normalize_endpoint_scope(self.scope))
        object.__setattr__(self, "tags", _normalize_tags(self.tags))

    def key(self) -> CacheNodeKey:
        return CacheNodeKey(
            cluster_id=self.cluster_id,
            node_id=self.node_id,
        )

    def is_expired(self, now: Timestamp | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.advertised_at + self.ttl_seconds)

    def to_dict(self) -> JsonDict:
        return {
            "node_id": self.node_id,
            "cluster_id": self.cluster_id,
            "region": self.region,
            "coordinates": {
                "latitude": float(self.coordinates.latitude),
                "longitude": float(self.coordinates.longitude),
            },
            "capacity_mb": int(self.capacity_mb),
            "utilization_percent": float(self.utilization_percent),
            "avg_latency_ms": float(self.avg_latency_ms),
            "reliability_score": float(self.reliability_score),
            "scope": self.scope,
            "tags": sorted(self.tags),
            "advertised_at": float(self.advertised_at),
            "ttl_seconds": float(self.ttl_seconds),
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> CacheNodeProfile:
        coord_payload = payload.get("coordinates")
        if isinstance(coord_payload, dict):
            latitude = float(coord_payload.get("latitude", 0.0))
            longitude = float(coord_payload.get("longitude", 0.0))
        else:
            latitude = 0.0
            longitude = 0.0
        coordinates = GeographicCoordinate(latitude=latitude, longitude=longitude)
        return cls(
            node_id=str(payload.get("node_id", "")),
            cluster_id=str(payload.get("cluster_id", "")),
            region=str(payload.get("region", "")),
            coordinates=coordinates,
            capacity_mb=int(payload.get("capacity_mb", 0)),
            utilization_percent=float(payload.get("utilization_percent", 0.0)),
            avg_latency_ms=float(payload.get("avg_latency_ms", 0.0)),
            reliability_score=float(payload.get("reliability_score", 0.0)),
            scope=normalize_endpoint_scope(payload.get("scope")),
            tags=_normalize_tags(payload.get("tags")),
            advertised_at=float(payload.get("advertised_at", time.time())),
            ttl_seconds=float(payload.get("ttl_seconds", 30.0)),
        )


@dataclass(frozen=True, slots=True)
class CacheNodeKey:
    cluster_id: ClusterId
    node_id: NodeId

    def to_dict(self) -> dict[str, str]:
        return {
            "cluster_id": self.cluster_id,
            "node_id": self.node_id,
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> CacheNodeKey:
        return cls(
            cluster_id=str(payload.get("cluster_id", "")),
            node_id=str(payload.get("node_id", "")),
        )


@dataclass(slots=True)
class FunctionCatalog:
    _entries: dict[FunctionKey, FunctionEndpoint] = field(default_factory=dict)
    _name_to_function_id: dict[FunctionName, FunctionId] = field(default_factory=dict)
    _function_id_to_name: dict[FunctionId, FunctionName] = field(default_factory=dict)

    def register(
        self, endpoint: FunctionEndpoint, *, now: Timestamp | None = None
    ) -> bool:
        if endpoint.is_expired(now):
            return False
        existing = self._entries.get(endpoint.key())
        if existing is not None and _is_stale_advertisement_update(
            existing_advertised_at=existing.advertised_at,
            existing_ttl_seconds=existing.ttl_seconds,
            incoming_advertised_at=endpoint.advertised_at,
            incoming_ttl_seconds=endpoint.ttl_seconds,
        ):
            return False
        identity = endpoint.identity
        self.validate_identity(identity)
        self._name_to_function_id[identity.name] = identity.function_id
        self._function_id_to_name[identity.function_id] = identity.name
        self._entries[endpoint.key()] = endpoint
        return True

    def validate_identity(self, identity: FunctionIdentity) -> None:
        existing_id = self._name_to_function_id.get(identity.name)
        if existing_id and existing_id != identity.function_id:
            raise ValueError(
                "Function name conflict: "
                f"{identity.name} already mapped to {existing_id}"
            )
        existing_name = self._function_id_to_name.get(identity.function_id)
        if existing_name and existing_name != identity.name:
            raise ValueError(
                "Function id conflict: "
                f"{identity.function_id} already mapped to {existing_name}"
            )

    def remove(self, key: FunctionKey) -> bool:
        endpoint = self._entries.pop(key, None)
        if not endpoint:
            return False
        self._cleanup_mappings(endpoint.identity)
        return True

    def _cleanup_mappings(self, identity: FunctionIdentity) -> None:
        if not any(
            entry.identity.name == identity.name for entry in self._entries.values()
        ):
            self._name_to_function_id.pop(identity.name, None)
        if not any(
            entry.identity.function_id == identity.function_id
            for entry in self._entries.values()
        ):
            self._function_id_to_name.pop(identity.function_id, None)

    def prune_expired(self, now: Timestamp | None = None) -> int:
        timestamp = now if now is not None else time.time()
        expired_keys = [
            key for key, entry in self._entries.items() if entry.is_expired(timestamp)
        ]
        for key in expired_keys:
            endpoint = self._entries.pop(key)
            self._cleanup_mappings(endpoint.identity)
        return len(expired_keys)

    def find(
        self,
        selector: FunctionSelector,
        resources: frozenset[str] | None = None,
        *,
        cluster_id: ClusterId | None = None,
        node_id: NodeId | None = None,
        now: Timestamp | None = None,
    ) -> list[FunctionEndpoint]:
        timestamp = now if now is not None else time.time()
        matches = []
        for entry in self._entries.values():
            if entry.is_expired(timestamp):
                continue
            if cluster_id and entry.cluster_id != cluster_id:
                continue
            if node_id and entry.node_id != node_id:
                continue
            if resources and not resources.issubset(entry.resources):
                continue
            if not selector.matches(entry.identity):
                continue
            matches.append(entry)
        return matches

    def entry_count(self) -> int:
        return len(self._entries)

    def entries(self, *, now: Timestamp | None = None) -> tuple[FunctionEndpoint, ...]:
        timestamp = now if now is not None else time.time()
        entries = [
            entry for entry in self._entries.values() if not entry.is_expired(timestamp)
        ]
        return tuple(entries)


@dataclass(slots=True)
class TopicCatalog:
    _subscriptions: dict[SubscriptionId, TopicSubscription] = field(
        default_factory=dict
    )
    _trie: TopicTrie = field(default_factory=TopicTrie)

    def register(
        self, subscription: TopicSubscription, *, now: Timestamp | None = None
    ) -> bool:
        if subscription.is_expired(now):
            return False
        existing = self._subscriptions.get(subscription.subscription_id)
        if existing and _is_stale_advertisement_update(
            existing_advertised_at=existing.advertised_at,
            existing_ttl_seconds=existing.ttl_seconds,
            incoming_advertised_at=subscription.advertised_at,
            incoming_ttl_seconds=subscription.ttl_seconds,
        ):
            return False
        if existing:
            for pattern in existing.patterns:
                self._trie.remove_pattern(pattern, subscription.subscription_id)
        self._subscriptions[subscription.subscription_id] = subscription
        for pattern in subscription.patterns:
            self._trie.add_pattern(pattern, subscription.subscription_id)
        return True

    def remove(self, subscription_id: SubscriptionId) -> bool:
        subscription = self._subscriptions.pop(subscription_id, None)
        if not subscription:
            return False
        for pattern in subscription.patterns:
            self._trie.remove_pattern(pattern, subscription_id)
        return True

    def match(
        self, topic: str, *, now: Timestamp | None = None
    ) -> list[TopicSubscription]:
        timestamp = now if now is not None else time.time()
        subscription_ids = self._trie.match_topic(topic)
        matches = []
        for subscription_id in subscription_ids:
            subscription = self._subscriptions.get(subscription_id)
            if not subscription or subscription.is_expired(timestamp):
                continue
            matches.append(subscription)
        return matches

    def entries(self, *, now: Timestamp | None = None) -> list[TopicSubscription]:
        timestamp = now if now is not None else time.time()
        return [
            subscription
            for subscription in self._subscriptions.values()
            if not subscription.is_expired(timestamp)
        ]

    def prune_expired(self, now: Timestamp | None = None) -> int:
        timestamp = now if now is not None else time.time()
        expired = [
            sub_id
            for sub_id, sub in self._subscriptions.items()
            if sub.is_expired(timestamp)
        ]
        for sub_id in expired:
            self.remove(sub_id)
        return len(expired)

    def entry_count(self) -> int:
        return len(self._subscriptions)


@dataclass(slots=True)
class QueueCatalog:
    _entries: dict[QueueKey, QueueEndpoint] = field(default_factory=dict)

    def register(
        self, endpoint: QueueEndpoint, *, now: Timestamp | None = None
    ) -> bool:
        if endpoint.is_expired(now):
            return False
        key = endpoint.key()
        existing = self._entries.get(key)
        if existing is not None and _is_stale_advertisement_update(
            existing_advertised_at=existing.advertised_at,
            existing_ttl_seconds=existing.ttl_seconds,
            incoming_advertised_at=endpoint.advertised_at,
            incoming_ttl_seconds=endpoint.ttl_seconds,
        ):
            return False
        self._entries[key] = endpoint
        return True

    def remove(self, key: QueueKey) -> bool:
        return self._entries.pop(key, None) is not None

    def find(
        self,
        queue_name: QueueName,
        *,
        cluster_id: ClusterId | None = None,
        now: Timestamp | None = None,
    ) -> list[QueueEndpoint]:
        timestamp = now if now is not None else time.time()
        matches = []
        for entry in self._entries.values():
            if entry.is_expired(timestamp):
                continue
            if entry.queue_name != queue_name:
                continue
            if cluster_id and entry.cluster_id != cluster_id:
                continue
            matches.append(entry)
        return matches

    def prune_expired(self, now: Timestamp | None = None) -> int:
        timestamp = now if now is not None else time.time()
        expired_keys = [
            key for key, entry in self._entries.items() if entry.is_expired(timestamp)
        ]
        for key in expired_keys:
            self._entries.pop(key, None)
        return len(expired_keys)

    def entries(self, *, now: Timestamp | None = None) -> list[QueueEndpoint]:
        timestamp = now if now is not None else time.time()
        return [
            entry for entry in self._entries.values() if not entry.is_expired(timestamp)
        ]

    def entry_count(self) -> int:
        return len(self._entries)


@dataclass(slots=True)
class ServiceCatalog:
    _entries: dict[ServiceKey, ServiceEndpoint] = field(default_factory=dict)

    def register(
        self, endpoint: ServiceEndpoint, *, now: Timestamp | None = None
    ) -> bool:
        if endpoint.is_expired(now):
            return False
        key = endpoint.key()
        existing = self._entries.get(key)
        if existing is not None and _is_stale_advertisement_update(
            existing_advertised_at=existing.advertised_at,
            existing_ttl_seconds=existing.ttl_seconds,
            incoming_advertised_at=endpoint.advertised_at,
            incoming_ttl_seconds=endpoint.ttl_seconds,
        ):
            return False
        self._entries[key] = endpoint
        return True

    def remove(self, key: ServiceKey) -> bool:
        return self._entries.pop(key, None) is not None

    def find(
        self,
        *,
        namespace: str | None = None,
        name: ServiceName | None = None,
        protocol: TransportProtocolName | None = None,
        port: PortNumber | None = None,
        cluster_id: ClusterId | None = None,
        node_id: NodeId | None = None,
        now: Timestamp | None = None,
    ) -> list[ServiceEndpoint]:
        timestamp = now if now is not None else time.time()
        matches = []
        for entry in self._entries.values():
            if entry.is_expired(timestamp):
                continue
            if namespace and entry.namespace != namespace:
                continue
            if name and entry.name != name:
                continue
            if protocol and entry.protocol != protocol:
                continue
            if port is not None and entry.port != port:
                continue
            if cluster_id and entry.cluster_id != cluster_id:
                continue
            if node_id and entry.node_id != node_id:
                continue
            matches.append(entry)
        return matches

    def prune_expired(self, now: Timestamp | None = None) -> int:
        timestamp = now if now is not None else time.time()
        expired_keys = [
            key for key, entry in self._entries.items() if entry.is_expired(timestamp)
        ]
        for key in expired_keys:
            self._entries.pop(key, None)
        return len(expired_keys)

    def entries(self, *, now: Timestamp | None = None) -> list[ServiceEndpoint]:
        timestamp = now if now is not None else time.time()
        return [
            entry for entry in self._entries.values() if not entry.is_expired(timestamp)
        ]

    def entry_count(self) -> int:
        return len(self._entries)


@dataclass(slots=True)
class CacheCatalog:
    _entries: dict[CacheRoleKey, CacheRoleEntry] = field(default_factory=dict)

    def register(self, entry: CacheRoleEntry, *, now: Timestamp | None = None) -> bool:
        if entry.is_expired(now):
            return False
        key = entry.key()
        existing = self._entries.get(key)
        if existing is not None and _is_stale_advertisement_update(
            existing_advertised_at=existing.advertised_at,
            existing_ttl_seconds=existing.ttl_seconds,
            incoming_advertised_at=entry.advertised_at,
            incoming_ttl_seconds=entry.ttl_seconds,
        ):
            return False
        self._entries[key] = entry
        return True

    def remove(self, key: CacheRoleKey) -> bool:
        return self._entries.pop(key, None) is not None

    def find(
        self,
        role: CacheRole,
        *,
        cluster_id: ClusterId | None = None,
        now: Timestamp | None = None,
    ) -> list[CacheRoleEntry]:
        timestamp = now if now is not None else time.time()
        matches = []
        for entry in self._entries.values():
            if entry.is_expired(timestamp):
                continue
            if entry.role != role:
                continue
            if cluster_id and entry.cluster_id != cluster_id:
                continue
            matches.append(entry)
        return matches

    def entries(self, *, now: Timestamp | None = None) -> list[CacheRoleEntry]:
        timestamp = now if now is not None else time.time()
        return [
            entry for entry in self._entries.values() if not entry.is_expired(timestamp)
        ]

    def prune_expired(self, now: Timestamp | None = None) -> int:
        timestamp = now if now is not None else time.time()
        expired_keys = [
            key for key, entry in self._entries.items() if entry.is_expired(timestamp)
        ]
        for key in expired_keys:
            self._entries.pop(key, None)
        return len(expired_keys)

    def entry_count(self) -> int:
        return len(self._entries)


@dataclass(slots=True)
class CacheProfileCatalog:
    _profiles: dict[CacheNodeKey, CacheNodeProfile] = field(default_factory=dict)

    def register(
        self, profile: CacheNodeProfile, *, now: Timestamp | None = None
    ) -> bool:
        if profile.is_expired(now):
            return False
        key = profile.key()
        existing = self._profiles.get(key)
        if existing is not None and _is_stale_advertisement_update(
            existing_advertised_at=existing.advertised_at,
            existing_ttl_seconds=existing.ttl_seconds,
            incoming_advertised_at=profile.advertised_at,
            incoming_ttl_seconds=profile.ttl_seconds,
        ):
            return False
        self._profiles[key] = profile
        return True

    def remove(self, key: CacheNodeKey) -> bool:
        return self._profiles.pop(key, None) is not None

    def find(
        self,
        *,
        cluster_id: ClusterId | None = None,
        node_id: NodeId | None = None,
        now: Timestamp | None = None,
    ) -> list[CacheNodeProfile]:
        timestamp = now if now is not None else time.time()
        matches = []
        for profile in self._profiles.values():
            if profile.is_expired(timestamp):
                continue
            if cluster_id and profile.cluster_id != cluster_id:
                continue
            if node_id and profile.node_id != node_id:
                continue
            matches.append(profile)
        return matches

    def entries(self, *, now: Timestamp | None = None) -> list[CacheNodeProfile]:
        timestamp = now if now is not None else time.time()
        return [
            profile
            for profile in self._profiles.values()
            if not profile.is_expired(timestamp)
        ]

    def prune_expired(self, now: Timestamp | None = None) -> int:
        timestamp = now if now is not None else time.time()
        expired_keys = [
            key
            for key, profile in self._profiles.items()
            if profile.is_expired(timestamp)
        ]
        for key in expired_keys:
            self._profiles.pop(key, None)
        return len(expired_keys)

    def entry_count(self) -> int:
        return len(self._profiles)


@dataclass(slots=True)
class NodeCatalog:
    _nodes: dict[NodeKey, NodeDescriptor] = field(default_factory=dict)

    def register(self, node: NodeDescriptor, *, now: Timestamp | None = None) -> bool:
        if node.is_expired(now):
            return False
        key = node.key()
        existing = self._nodes.get(key)
        if existing is not None and _is_stale_advertisement_update(
            existing_advertised_at=existing.advertised_at,
            existing_ttl_seconds=existing.ttl_seconds,
            incoming_advertised_at=node.advertised_at,
            incoming_ttl_seconds=node.ttl_seconds,
        ):
            return False
        self._nodes[key] = node
        return True

    def remove(self, key: NodeKey) -> bool:
        return self._nodes.pop(key, None) is not None

    def find(
        self,
        *,
        cluster_id: ClusterId | None = None,
        node_id: NodeId | None = None,
        now: Timestamp | None = None,
    ) -> list[NodeDescriptor]:
        timestamp = now if now is not None else time.time()
        matches = []
        for node in self._nodes.values():
            if node.is_expired(timestamp):
                continue
            if cluster_id and node.cluster_id != cluster_id:
                continue
            if node_id and node.node_id != node_id:
                continue
            matches.append(node)
        return matches

    def entries(self, *, now: Timestamp | None = None) -> list[NodeDescriptor]:
        timestamp = now if now is not None else time.time()
        return [node for node in self._nodes.values() if not node.is_expired(timestamp)]

    def prune_expired(self, now: Timestamp | None = None) -> int:
        timestamp = now if now is not None else time.time()
        expired_keys = [
            key for key, entry in self._nodes.items() if entry.is_expired(timestamp)
        ]
        for key in expired_keys:
            self._nodes.pop(key, None)
        return len(expired_keys)

    def entry_count(self) -> int:
        return len(self._nodes)


@dataclass(slots=True)
class RoutingCatalog:
    """Aggregate catalog for routing decisions."""

    functions: FunctionCatalog = field(default_factory=FunctionCatalog)
    topics: TopicCatalog = field(default_factory=TopicCatalog)
    queues: QueueCatalog = field(default_factory=QueueCatalog)
    services: ServiceCatalog = field(default_factory=ServiceCatalog)
    caches: CacheCatalog = field(default_factory=CacheCatalog)
    cache_profiles: CacheProfileCatalog = field(default_factory=CacheProfileCatalog)
    nodes: NodeCatalog = field(default_factory=NodeCatalog)

    def prune_expired(self, now: Timestamp | None = None) -> dict[str, int]:
        return {
            "functions": self.functions.prune_expired(now),
            "topics": self.topics.prune_expired(now),
            "queues": self.queues.prune_expired(now),
            "services": self.services.prune_expired(now),
            "caches": self.caches.prune_expired(now),
            "cache_profiles": self.cache_profiles.prune_expired(now),
            "nodes": self.nodes.prune_expired(now),
        }

    def to_dict(self, *, now: Timestamp | None = None) -> JsonDict:
        timestamp = now if now is not None else time.time()
        return {
            "generated_at": float(timestamp),
            "functions": [
                entry.to_dict() for entry in self.functions.entries(now=timestamp)
            ],
            "topics": [entry.to_dict() for entry in self.topics.entries(now=timestamp)],
            "queues": [entry.to_dict() for entry in self.queues.entries(now=timestamp)],
            "services": [
                entry.to_dict() for entry in self.services.entries(now=timestamp)
            ],
            "caches": [entry.to_dict() for entry in self.caches.entries(now=timestamp)],
            "cache_profiles": [
                entry.to_dict() for entry in self.cache_profiles.entries(now=timestamp)
            ],
            "nodes": [entry.to_dict() for entry in self.nodes.entries(now=timestamp)],
        }

    def load_from_dict(
        self, payload: JsonDict, *, now: Timestamp | None = None
    ) -> dict[str, int]:
        timestamp = now if now is not None else time.time()
        self.functions = FunctionCatalog()
        self.topics = TopicCatalog()
        self.queues = QueueCatalog()
        self.services = ServiceCatalog()
        self.caches = CacheCatalog()
        self.cache_profiles = CacheProfileCatalog()
        self.nodes = NodeCatalog()
        counts = {
            "functions": 0,
            "topics": 0,
            "queues": 0,
            "services": 0,
            "caches": 0,
            "cache_profiles": 0,
            "nodes": 0,
        }
        raw_functions = payload.get("functions", [])
        if isinstance(raw_functions, list):
            for item in raw_functions:
                if not isinstance(item, dict):
                    continue
                endpoint = FunctionEndpoint.from_dict(item)
                if self.functions.register(endpoint, now=timestamp):
                    counts["functions"] += 1
        raw_topics = payload.get("topics", [])
        if isinstance(raw_topics, list):
            for item in raw_topics:
                if not isinstance(item, dict):
                    continue
                subscription = TopicSubscription.from_dict(item)
                if self.topics.register(subscription, now=timestamp):
                    counts["topics"] += 1
        raw_queues = payload.get("queues", [])
        if isinstance(raw_queues, list):
            for item in raw_queues:
                if not isinstance(item, dict):
                    continue
                endpoint = QueueEndpoint.from_dict(item)
                if self.queues.register(endpoint, now=timestamp):
                    counts["queues"] += 1
        raw_services = payload.get("services", [])
        if isinstance(raw_services, list):
            for item in raw_services:
                if not isinstance(item, dict):
                    continue
                endpoint = ServiceEndpoint.from_dict(item)
                if self.services.register(endpoint, now=timestamp):
                    counts["services"] += 1
        raw_caches = payload.get("caches", [])
        if isinstance(raw_caches, list):
            for item in raw_caches:
                if not isinstance(item, dict):
                    continue
                entry = CacheRoleEntry.from_dict(item)
                if self.caches.register(entry, now=timestamp):
                    counts["caches"] += 1
        raw_cache_profiles = payload.get("cache_profiles", [])
        if isinstance(raw_cache_profiles, list):
            for item in raw_cache_profiles:
                if not isinstance(item, dict):
                    continue
                profile = CacheNodeProfile.from_dict(item)
                if self.cache_profiles.register(profile, now=timestamp):
                    counts["cache_profiles"] += 1
        raw_nodes = payload.get("nodes", [])
        if isinstance(raw_nodes, list):
            for item in raw_nodes:
                if not isinstance(item, dict):
                    continue
                node = NodeDescriptor.from_dict(item)
                if self.nodes.register(node, now=timestamp):
                    counts["nodes"] += 1
        return counts

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RoutingCatalog:
        catalog = cls()
        catalog.load_from_dict(payload)
        return catalog
