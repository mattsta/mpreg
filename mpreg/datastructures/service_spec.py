from __future__ import annotations

from dataclasses import dataclass, field

from mpreg.datastructures.type_aliases import (
    DurationSeconds,
    EndpointScope,
    HostAddress,
    JsonDict,
    MetadataKey,
    MetadataValue,
    NamespaceName,
    PortNumber,
    ServiceName,
    TransportProtocolName,
)

DEFAULT_SERVICE_SCOPE: EndpointScope = "zone"


def _normalize_tags(tags: object) -> frozenset[str]:
    if tags is None:
        return frozenset()
    if isinstance(tags, frozenset):
        return frozenset(str(tag) for tag in tags if tag)
    if isinstance(tags, (list, tuple, set)):
        return frozenset(str(tag) for tag in tags if tag)
    return frozenset(str(tags)) if tags else frozenset()


def _normalize_targets(targets: object) -> tuple[HostAddress, ...]:
    if targets is None:
        return tuple()
    if isinstance(targets, (list, tuple, set, frozenset)):
        values = [str(item) for item in targets if item]
    else:
        values = [str(targets)] if targets else []
    if not values:
        return tuple()
    return tuple(sorted({value for value in values if value}))


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


@dataclass(frozen=True, slots=True)
class ServiceSpec:
    name: ServiceName
    namespace: NamespaceName
    protocol: TransportProtocolName
    port: PortNumber
    targets: tuple[HostAddress, ...] = field(default_factory=tuple)
    scope: EndpointScope = DEFAULT_SERVICE_SCOPE
    tags: frozenset[str] = field(default_factory=frozenset)
    capabilities: frozenset[str] = field(default_factory=frozenset)
    metadata: dict[MetadataKey, MetadataValue] = field(default_factory=dict)
    priority: int = 0
    weight: int = 0
    ttl_seconds: DurationSeconds | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", str(self.name))
        object.__setattr__(self, "namespace", str(self.namespace))
        object.__setattr__(self, "protocol", str(self.protocol))
        object.__setattr__(self, "port", int(self.port))
        object.__setattr__(self, "targets", _normalize_targets(self.targets))
        object.__setattr__(self, "tags", _normalize_tags(self.tags))
        object.__setattr__(self, "capabilities", _normalize_tags(self.capabilities))
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))
        object.__setattr__(self, "priority", int(self.priority))
        object.__setattr__(self, "weight", int(self.weight))

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
        }
        if self.ttl_seconds is not None:
            payload["ttl_seconds"] = float(self.ttl_seconds)
        return payload

    @classmethod
    def from_dict(cls, payload: JsonDict) -> ServiceSpec:
        return cls(
            name=str(payload.get("name", "")),
            namespace=str(payload.get("namespace", "")),
            protocol=str(payload.get("protocol", "")),
            port=int(payload.get("port", 0) or 0),
            targets=tuple(payload.get("targets", []) or []),
            scope=str(payload.get("scope", DEFAULT_SERVICE_SCOPE)),
            tags=frozenset(payload.get("tags", []) or []),
            capabilities=frozenset(payload.get("capabilities", []) or []),
            metadata=dict(payload.get("metadata", {}) or {}),
            priority=int(payload.get("priority", 0) or 0),
            weight=int(payload.get("weight", 0) or 0),
            ttl_seconds=(
                float(payload.get("ttl_seconds"))
                if payload.get("ttl_seconds") is not None
                else None
            ),
        )
