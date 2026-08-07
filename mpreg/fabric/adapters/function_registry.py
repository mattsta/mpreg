"""Adapter to emit local function registry into routing catalog deltas."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field

from mpreg.core.rpc_spec_sharing import RpcSpecSharePolicy
from mpreg.datastructures.type_aliases import (
    ClusterId,
    DurationSeconds,
    EndpointScope,
    NodeId,
    Timestamp,
)

from ...core.rpc_registry import RpcRegistry
from ..catalog import (
    DEFAULT_ENDPOINT_SCOPE,
    FunctionEndpoint,
    NodeDescriptor,
    TransportEndpoint,
    normalize_endpoint_scope,
)
from ..catalog_delta import RoutingCatalogDelta


@dataclass(slots=True)
class LocalFunctionCatalogAdapter:
    registry: RpcRegistry
    node_id: NodeId
    cluster_id: ClusterId
    node_resources: frozenset[str] = field(default_factory=frozenset)
    node_capabilities: frozenset[str] = field(default_factory=frozenset)
    transport_endpoints: tuple[TransportEndpoint, ...] = field(default_factory=tuple)
    node_region: str = ""
    node_scope: EndpointScope = DEFAULT_ENDPOINT_SCOPE
    node_tags: frozenset[str] = field(default_factory=frozenset)
    node_ttl_seconds: DurationSeconds | None = None
    function_ttl_seconds: DurationSeconds | None = None
    spec_share_policy: RpcSpecSharePolicy | None = None

    def build_delta(
        self,
        *,
        now: Timestamp | None = None,
        include_node: bool = True,
        update_id: str | None = None,
    ) -> RoutingCatalogDelta:
        timestamp = now if now is not None else time.time()
        function_ttl = (
            self.function_ttl_seconds if self.function_ttl_seconds is not None else 30.0
        )
        functions = []
        for spec in self.registry.specs():
            include_spec = (
                self.spec_share_policy.include_spec(spec)
                if self.spec_share_policy
                else False
            )
            functions.append(
                FunctionEndpoint(
                    identity=spec.identity,
                    resources=spec.resources,
                    node_id=self.node_id,
                    cluster_id=self.cluster_id,
                    scope=normalize_endpoint_scope(spec.scope),
                    tags=spec.tags,
                    rpc_summary=spec.summary(),
                    rpc_spec=spec if include_spec else None,
                    spec_digest=spec.spec_digest,
                    advertised_at=timestamp,
                    ttl_seconds=function_ttl,
                )
            )
        functions = tuple(functions)
        nodes: tuple[NodeDescriptor, ...] = ()
        if include_node:
            node_ttl = (
                self.node_ttl_seconds
                if self.node_ttl_seconds is not None
                else function_ttl
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
            functions=functions,
            nodes=nodes,
        )
