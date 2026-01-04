"""Adapter to emit local function registry into routing catalog deltas."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field

from mpreg.datastructures.type_aliases import DurationSeconds, Timestamp

from ..catalog import NodeDescriptor, TransportEndpoint
from ..catalog_delta import RoutingCatalogDelta
from ..function_registry import LocalFunctionRegistry


@dataclass(slots=True)
class LocalFunctionCatalogAdapter:
    registry: LocalFunctionRegistry
    node_resources: frozenset[str] = field(default_factory=frozenset)
    node_capabilities: frozenset[str] = field(default_factory=frozenset)
    transport_endpoints: tuple[TransportEndpoint, ...] = field(default_factory=tuple)
    node_ttl_seconds: DurationSeconds | None = None
    function_ttl_seconds: DurationSeconds | None = None

    def build_delta(
        self,
        *,
        now: Timestamp | None = None,
        include_node: bool = True,
        update_id: str | None = None,
    ) -> RoutingCatalogDelta:
        timestamp = now if now is not None else time.time()
        function_ttl = (
            self.function_ttl_seconds
            if self.function_ttl_seconds is not None
            else self.registry.ttl_seconds
        )
        functions = tuple(
            registration.to_endpoint(
                node_id=self.registry.node_id,
                cluster_id=self.registry.cluster_id,
                ttl_seconds=function_ttl,
                advertised_at=timestamp,
            )
            for registration in self.registry.registrations()
        )
        nodes: tuple[NodeDescriptor, ...] = ()
        if include_node:
            node_ttl = (
                self.node_ttl_seconds
                if self.node_ttl_seconds is not None
                else function_ttl
            )
            nodes = (
                NodeDescriptor(
                    node_id=self.registry.node_id,
                    cluster_id=self.registry.cluster_id,
                    resources=self.node_resources,
                    capabilities=self.node_capabilities,
                    transport_endpoints=self.transport_endpoints,
                    advertised_at=timestamp,
                    ttl_seconds=node_ttl,
                ),
            )
        return RoutingCatalogDelta(
            update_id=update_id or str(uuid.uuid4()),
            cluster_id=self.registry.cluster_id,
            sent_at=timestamp,
            functions=functions,
            nodes=nodes,
        )
