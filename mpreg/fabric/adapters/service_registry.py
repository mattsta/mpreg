"""Adapter to emit local service registry into routing catalog deltas."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field

from mpreg.core.service_registry import ServiceRegistry
from mpreg.datastructures.type_aliases import (
    ClusterId,
    DurationSeconds,
    EndpointScope,
    NodeId,
    Timestamp,
)

from ..catalog import (
    DEFAULT_ENDPOINT_SCOPE,
    NodeDescriptor,
    ServiceEndpoint,
    TransportEndpoint,
)
from ..catalog_delta import RoutingCatalogDelta


@dataclass(slots=True)
class LocalServiceCatalogAdapter:
    registry: ServiceRegistry
    node_id: NodeId
    cluster_id: ClusterId
    node_resources: frozenset[str] = field(default_factory=frozenset)
    node_capabilities: frozenset[str] = field(default_factory=frozenset)
    transport_endpoints: tuple[TransportEndpoint, ...] = field(default_factory=tuple)
    node_region: str = ""
    node_scope: EndpointScope = DEFAULT_ENDPOINT_SCOPE
    node_tags: frozenset[str] = field(default_factory=frozenset)
    node_ttl_seconds: DurationSeconds | None = None
    service_ttl_seconds: DurationSeconds | None = None

    def build_delta(
        self,
        *,
        now: Timestamp | None = None,
        include_node: bool = True,
        update_id: str | None = None,
    ) -> RoutingCatalogDelta:
        timestamp = now if now is not None else time.time()
        service_ttl = (
            self.service_ttl_seconds if self.service_ttl_seconds is not None else 30.0
        )
        services = []
        for registration in self.registry.registrations():
            spec = registration.spec
            ttl_seconds = (
                spec.ttl_seconds if spec.ttl_seconds is not None else service_ttl
            )
            services.append(
                ServiceEndpoint(
                    name=spec.name,
                    namespace=spec.namespace,
                    protocol=spec.protocol,
                    port=spec.port,
                    targets=spec.targets,
                    scope=spec.scope,
                    tags=spec.tags,
                    capabilities=spec.capabilities,
                    metadata=spec.metadata,
                    priority=spec.priority,
                    weight=spec.weight,
                    node_id=self.node_id,
                    cluster_id=self.cluster_id,
                    advertised_at=timestamp,
                    ttl_seconds=ttl_seconds,
                )
            )
        services = tuple(services)
        nodes: tuple[NodeDescriptor, ...] = ()
        if include_node:
            node_ttl = (
                self.node_ttl_seconds
                if self.node_ttl_seconds is not None
                else service_ttl
            )
            nodes = (
                NodeDescriptor(
                    node_id=self.node_id,
                    cluster_id=self.cluster_id,
                    region=self.node_region,
                    scope=self.node_scope,
                    tags=self.node_tags,
                    resources=self.node_resources,
                    capabilities=self.node_capabilities,
                    transport_endpoints=self.transport_endpoints,
                    advertised_at=timestamp,
                    ttl_seconds=node_ttl,
                ),
            )
        return RoutingCatalogDelta(
            update_id=update_id or str(uuid.uuid4()),
            cluster_id=self.cluster_id,
            sent_at=timestamp,
            services=services,
            nodes=nodes,
        )
