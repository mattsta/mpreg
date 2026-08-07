from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field

from mpreg.datastructures.service_spec import ServiceSpec
from mpreg.datastructures.type_aliases import (
    JsonDict,
    NamespaceName,
    PortNumber,
    ServiceName,
    Timestamp,
    TransportProtocolName,
)


@dataclass(frozen=True, slots=True)
class ServiceRegistryKey:
    namespace: NamespaceName
    name: ServiceName
    protocol: TransportProtocolName
    port: PortNumber


@dataclass(frozen=True, slots=True)
class ServiceRegistration:
    spec: ServiceSpec
    registered_at: Timestamp
    registration_id: str

    def to_dict(self) -> JsonDict:
        return {
            "spec": self.spec.to_dict(),
            "registered_at": float(self.registered_at),
            "registration_id": self.registration_id,
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> ServiceRegistration:
        spec_payload = payload.get("spec")
        spec = (
            ServiceSpec.from_dict(spec_payload)  # type: ignore[arg-type]
            if isinstance(spec_payload, dict)
            else ServiceSpec(
                name="",
                namespace="",
                protocol="",
                port=0,
            )
        )
        return cls(
            spec=spec,
            registered_at=float(payload.get("registered_at", 0.0) or 0.0),
            registration_id=str(payload.get("registration_id", "")),
        )


@dataclass(slots=True)
class ServiceRegistry:
    _registrations: dict[ServiceRegistryKey, ServiceRegistration] = field(
        default_factory=dict
    )

    def register(
        self,
        spec: ServiceSpec,
        *,
        now: Timestamp | None = None,
        registration_id: str | None = None,
    ) -> ServiceRegistration:
        timestamp = now if now is not None else time.time()
        key = ServiceRegistryKey(
            namespace=spec.namespace,
            name=spec.name,
            protocol=spec.protocol,
            port=spec.port,
        )
        registration = ServiceRegistration(
            spec=spec,
            registered_at=timestamp,
            registration_id=registration_id or str(uuid.uuid4()),
        )
        self._registrations[key] = registration
        return registration

    def unregister(
        self,
        *,
        namespace: NamespaceName,
        name: ServiceName,
        protocol: TransportProtocolName,
        port: PortNumber,
    ) -> ServiceRegistration | None:
        key = ServiceRegistryKey(
            namespace=namespace,
            name=name,
            protocol=protocol,
            port=port,
        )
        return self._registrations.pop(key, None)

    def registrations(self) -> tuple[ServiceRegistration, ...]:
        return tuple(self._registrations.values())

    def find(
        self,
        *,
        namespace: NamespaceName | None = None,
        name: ServiceName | None = None,
        protocol: TransportProtocolName | None = None,
        port: PortNumber | None = None,
    ) -> tuple[ServiceRegistration, ...]:
        matches = []
        for key, registration in self._registrations.items():
            if namespace and key.namespace != namespace:
                continue
            if name and key.name != name:
                continue
            if protocol and key.protocol != protocol:
                continue
            if port is not None and key.port != port:
                continue
            matches.append(registration)
        return tuple(matches)
