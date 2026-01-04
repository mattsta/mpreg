"""Local function registry for fabric catalog announcements."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from mpreg.datastructures.function_identity import FunctionIdentity, SemanticVersion
from mpreg.datastructures.type_aliases import (
    ClusterId,
    DurationSeconds,
    FunctionId,
    FunctionName,
    NodeId,
    Timestamp,
)

from .catalog import FunctionEndpoint


@dataclass(frozen=True, slots=True)
class LocalFunctionKey:
    name: FunctionName
    function_id: FunctionId


@dataclass(frozen=True, slots=True)
class LocalFunctionRegistration:
    identity: FunctionIdentity
    resources: frozenset[str]
    registered_at: Timestamp = field(default_factory=time.time)

    def to_endpoint(
        self,
        *,
        node_id: NodeId,
        cluster_id: ClusterId,
        ttl_seconds: DurationSeconds,
        advertised_at: Timestamp | None = None,
    ) -> FunctionEndpoint:
        return FunctionEndpoint(
            identity=self.identity,
            resources=self.resources,
            node_id=node_id,
            cluster_id=cluster_id,
            advertised_at=advertised_at if advertised_at is not None else time.time(),
            ttl_seconds=ttl_seconds,
        )


@dataclass(slots=True)
class LocalFunctionRegistry:
    node_id: NodeId
    cluster_id: ClusterId
    ttl_seconds: DurationSeconds = 30.0
    _registrations: dict[
        LocalFunctionKey, dict[SemanticVersion, LocalFunctionRegistration]
    ] = field(default_factory=dict)
    _name_to_function_id: dict[FunctionName, FunctionId] = field(default_factory=dict)
    _function_id_to_name: dict[FunctionId, FunctionName] = field(default_factory=dict)

    def register(
        self,
        identity: FunctionIdentity,
        resources: frozenset[str],
        *,
        now: Timestamp | None = None,
    ) -> LocalFunctionRegistration:
        self._validate_identity(identity)
        key = LocalFunctionKey(name=identity.name, function_id=identity.function_id)
        version_map = self._registrations.setdefault(key, {})
        if identity.version in version_map:
            existing = version_map[identity.version]
            if existing.resources != resources:
                raise ValueError(
                    "Function registration conflict: "
                    f"{identity.name} {identity.version} resources differ."
                )
            return existing
        registration = LocalFunctionRegistration(
            identity=identity,
            resources=resources,
            registered_at=now if now is not None else time.time(),
        )
        version_map[identity.version] = registration
        self._name_to_function_id[identity.name] = identity.function_id
        self._function_id_to_name[identity.function_id] = identity.name
        return registration

    def validate_identity(self, identity: FunctionIdentity) -> None:
        self._validate_identity(identity)

    def registrations(self) -> tuple[LocalFunctionRegistration, ...]:
        entries: list[LocalFunctionRegistration] = []
        for version_map in self._registrations.values():
            entries.extend(version_map.values())
        return tuple(entries)

    def identity_for_name(self, name: FunctionName) -> FunctionIdentity | None:
        function_id = self._name_to_function_id.get(name)
        if not function_id:
            return None
        key = LocalFunctionKey(name=name, function_id=function_id)
        versions = self._registrations.get(key, {})
        if not versions:
            return None
        latest = max(versions.keys())
        return versions[latest].identity

    def _validate_identity(self, identity: FunctionIdentity) -> None:
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
