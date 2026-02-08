from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    FunctionSelector,
    SemanticVersion,
)
from mpreg.datastructures.rpc_spec import (
    RpcDocSpec,
    RpcExampleSpec,
    RpcRegistration,
    RpcSpec,
    RpcSpecSummary,
)
from mpreg.datastructures.type_aliases import (
    EndpointScope,
    FunctionId,
    FunctionName,
    FunctionVersion,
    NamespaceName,
    ResourceName,
    RpcName,
    RpcSpecDigest,
    RpcTag,
    Timestamp,
)


@dataclass(frozen=True, slots=True)
class RpcRegistryKey:
    name: FunctionName
    function_id: FunctionId


@dataclass(slots=True)
class RpcRegistry:
    _registrations: dict[RpcRegistryKey, dict[SemanticVersion, RpcRegistration]] = (
        field(default_factory=dict)
    )
    _name_to_function_id: dict[FunctionName, FunctionId] = field(default_factory=dict)
    _function_id_to_name: dict[FunctionId, FunctionName] = field(default_factory=dict)

    def register(
        self,
        registration: RpcRegistration,
        *,
        now: Timestamp | None = None,
    ) -> RpcRegistration:
        identity = registration.spec.identity
        self._validate_identity(identity)
        key = RpcRegistryKey(name=identity.name, function_id=identity.function_id)
        version_map = self._registrations.setdefault(key, {})
        if identity.version in version_map:
            raise ValueError(
                f"Function '{identity.name}' is already registered "
                f"(version {identity.version})."
            )
        if now is not None:
            registration = RpcRegistration(
                spec=registration.spec,
                handler=registration.handler,
                handler_spec=registration.handler_spec,
                registered_at=now,
                registration_id=registration.registration_id,
            )
        version_map[identity.version] = registration
        self._name_to_function_id[identity.name] = identity.function_id
        self._function_id_to_name[identity.function_id] = identity.name
        return registration

    def register_callable(
        self,
        handler: Callable[..., Any],
        *,
        name: RpcName | None = None,
        function_id: FunctionId | None = None,
        version: FunctionVersion = "1.0.0",
        resources: Iterable[ResourceName] = (),
        tags: Iterable[RpcTag] = (),
        namespace: NamespaceName | None = None,
        scope: EndpointScope | None = None,
        capabilities: Iterable[str] = (),
        doc: RpcDocSpec | None = None,
        examples: Iterable[RpcExampleSpec] = (),
    ) -> RpcRegistration:
        registration = RpcRegistration.from_callable(
            handler,
            name=name,
            function_id=function_id,
            version=version,
            resources=resources,
            tags=tags,
            namespace=namespace,
            scope=scope,
            capabilities=capabilities,
            doc=doc,
            examples=examples,
        )
        return self.register(registration)

    def resolve(self, selector: FunctionSelector) -> RpcRegistration | None:
        name = selector.name
        function_id = selector.function_id
        if not name and function_id:
            name = self._function_id_to_name.get(function_id)
        if name and not function_id:
            function_id = self._name_to_function_id.get(name)
        if not name or not function_id:
            return None
        key = RpcRegistryKey(name=name, function_id=function_id)
        version_map = self._registrations.get(key)
        if not version_map:
            return None
        if selector.version_constraint:
            candidates = [
                version
                for version in version_map
                if selector.version_constraint.matches(version)
            ]
            if not candidates:
                return None
            selected_version = max(candidates)
        else:
            selected_version = max(version_map)
        return version_map[selected_version]

    def registrations(self) -> tuple[RpcRegistration, ...]:
        registrations: list[RpcRegistration] = []
        for version_map in self._registrations.values():
            registrations.extend(version_map.values())
        return tuple(registrations)

    def specs(self) -> tuple[RpcSpec, ...]:
        return tuple(registration.spec for registration in self.registrations())

    def summaries(self) -> tuple[RpcSpecSummary, ...]:
        return tuple(spec.summary() for spec in self.specs())

    def spec_by_digest(self, digest: RpcSpecDigest) -> RpcSpec | None:
        for spec in self.specs():
            if spec.spec_digest == digest:
                return spec
        return None

    def identity_for_name(self, name: FunctionName) -> FunctionIdentity | None:
        function_id = self._name_to_function_id.get(name)
        if not function_id:
            return None
        key = RpcRegistryKey(name=name, function_id=function_id)
        versions = self._registrations.get(key, {})
        if not versions:
            return None
        latest = max(versions.keys())
        return versions[latest].spec.identity

    def _validate_identity(self, identity: FunctionIdentity) -> None:
        existing_id = self._name_to_function_id.get(identity.name)
        if existing_id and existing_id != identity.function_id:
            raise ValueError(
                f"RPC name conflict: {identity.name} already mapped to {existing_id}"
            )
        existing_name = self._function_id_to_name.get(identity.function_id)
        if existing_name and existing_name != identity.name:
            raise ValueError(
                "RPC function id conflict: "
                f"{identity.function_id} already mapped to {existing_name}"
            )
