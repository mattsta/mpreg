from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TypeVar

from mpreg.core.model import RPCServerStatus
from mpreg.core.payloads import (
    PAYLOAD_FLOAT,
    PAYLOAD_INT,
    PAYLOAD_KEEP_EMPTY,
    PAYLOAD_LIST,
    Payload,
    PayloadMapping,
    payload_from_dataclass,
)
from mpreg.core.rpc_discovery import (
    RpcDescribeRequest,
    RpcListRequest,
    RpcReportRequest,
)
from mpreg.datastructures.type_aliases import (
    ClusterId,
    EndpointScope,
    NodeId,
    TenantId,
    Timestamp,
)
from mpreg.fabric.catalog import TransportEndpoint

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class NodeLoadMetrics:
    """Snapshot of node load metrics for client-side selection."""

    active_clients: int
    peer_count: int
    status: str
    status_timestamp: Timestamp | None
    messages_processed: int
    rpc_responses_skipped: int
    server_messages: int
    other_messages: int
    load_score: float

    @classmethod
    def from_status(
        cls,
        status: RPCServerStatus,
        *,
        now: Timestamp,
        stale_after_seconds: float,
    ) -> NodeLoadMetrics:
        metrics = status.metrics or {}
        active_clients = int(status.active_clients or 0)
        peer_count = int(status.peer_count or 0)
        messages_processed = int(metrics.get("messages_processed", 0) or 0)
        rpc_responses_skipped = int(metrics.get("rpc_responses_skipped", 0) or 0)
        server_messages = int(metrics.get("server_messages", 0) or 0)
        other_messages = int(metrics.get("other_messages", 0) or 0)
        status_timestamp = status.timestamp or None
        load_score = float(active_clients)
        if status.status != "ok":
            load_score += 1000.0
        if status_timestamp is None or (now - status_timestamp) > stale_after_seconds:
            load_score += 500.0
        return cls(
            active_clients=active_clients,
            peer_count=peer_count,
            status=status.status,
            status_timestamp=status_timestamp,
            messages_processed=messages_processed,
            rpc_responses_skipped=rpc_responses_skipped,
            server_messages=server_messages,
            other_messages=other_messages,
            load_score=load_score,
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> NodeLoadMetrics:
        return cls(
            active_clients=int(payload.get("active_clients", 0) or 0),
            peer_count=int(payload.get("peer_count", 0) or 0),
            status=str(payload.get("status", "unknown")),
            status_timestamp=payload.get("status_timestamp"),
            messages_processed=int(payload.get("messages_processed", 0) or 0),
            rpc_responses_skipped=int(payload.get("rpc_responses_skipped", 0) or 0),
            server_messages=int(payload.get("server_messages", 0) or 0),
            other_messages=int(payload.get("other_messages", 0) or 0),
            load_score=float(payload.get("load_score", 0.0) or 0.0),
        )


@dataclass(frozen=True, slots=True)
class ClusterNodeSnapshot:
    """Cluster node snapshot for clients."""

    node_id: NodeId
    cluster_id: ClusterId
    resources: tuple[str, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    capabilities: tuple[str, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    region: str | None = None
    scope: EndpointScope | None = None
    tags: tuple[str, ...] = field(default_factory=tuple, metadata={PAYLOAD_LIST: True})
    transport_endpoints: tuple[TransportEndpoint, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    advertised_urls: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    advertised_at: Timestamp | None = field(
        default=None,
        metadata={PAYLOAD_FLOAT: True, PAYLOAD_KEEP_EMPTY: True},
    )
    load: NodeLoadMetrics | None = field(
        default=None, metadata={PAYLOAD_KEEP_EMPTY: True}
    )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> ClusterNodeSnapshot:
        load_payload = payload.get("load")
        load = (
            NodeLoadMetrics.from_dict(load_payload)
            if isinstance(load_payload, dict)
            else None
        )
        return cls(
            node_id=str(payload.get("node_id", "")),
            cluster_id=str(payload.get("cluster_id", "")),
            region=str(payload.get("region")) if payload.get("region") else None,
            scope=str(payload.get("scope")) if payload.get("scope") else None,
            resources=tuple(payload.get("resources", []) or []),
            capabilities=tuple(payload.get("capabilities", []) or []),
            tags=tuple(payload.get("tags", []) or []),
            transport_endpoints=tuple(
                TransportEndpoint.from_dict(entry)
                for entry in payload.get("transport_endpoints", []) or []
            ),
            advertised_urls=tuple(payload.get("advertised_urls", []) or []),
            advertised_at=payload.get("advertised_at"),
            load=load,
        )


@dataclass(frozen=True, slots=True)
class ClusterMapSnapshot:
    """Cluster map snapshot for discovery-aware clients."""

    cluster_id: ClusterId
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    nodes: tuple[ClusterNodeSnapshot, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> ClusterMapSnapshot:
        return cls(
            cluster_id=str(payload.get("cluster_id", "")),
            generated_at=float(payload.get("generated_at", 0.0)),
            nodes=tuple(
                ClusterNodeSnapshot.from_dict(node_payload)
                for node_payload in payload.get("nodes", []) or []
            ),
        )


@dataclass(frozen=True, slots=True)
class ClusterMapRequest:
    """Request payload for cluster_map_v2."""

    scope: EndpointScope | None = None
    namespace_filter: str | None = None
    viewer_tenant_id: TenantId | None = None
    capabilities: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    resources: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    cluster_id: ClusterId | None = None
    limit: int | None = field(default=None, metadata={PAYLOAD_INT: True})
    page_token: str | None = None

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> ClusterMapRequest:
        data = payload or {}
        return cls(
            scope=str(data.get("scope")).lower()
            if data.get("scope") is not None
            else None,
            namespace_filter=(
                str(data.get("namespace_filter"))
                if data.get("namespace_filter") is not None
                else None
            ),
            viewer_tenant_id=(
                str(data.get("viewer_tenant_id"))
                if data.get("viewer_tenant_id") is not None
                else None
            ),
            capabilities=tuple(data.get("capabilities", []) or []),
            resources=tuple(data.get("resources", []) or []),
            cluster_id=(
                str(data.get("cluster_id"))
                if data.get("cluster_id") is not None
                else None
            ),
            limit=int(data.get("limit")) if data.get("limit") is not None else None,
            page_token=(
                str(data.get("page_token")) if data.get("page_token") else None
            ),
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class ClusterMapResponse:
    """Response payload for cluster_map_v2."""

    cluster_id: ClusterId
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    nodes: tuple[ClusterNodeSnapshot, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    next_page_token: str | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> ClusterMapResponse:
        nodes_payload = payload.get("nodes", []) or []
        nodes = tuple(
            ClusterNodeSnapshot.from_dict(node_payload)
            for node_payload in nodes_payload
            if isinstance(node_payload, dict)
        )
        return cls(
            cluster_id=str(payload.get("cluster_id", "")),
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            nodes=nodes,
            next_page_token=(
                str(payload.get("next_page_token"))
                if payload.get("next_page_token") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class CatalogQueryRequest:
    """Request payload for catalog_query."""

    entry_type: str
    namespace: str | None = None
    scope: EndpointScope | None = None
    viewer_cluster_id: ClusterId | None = None
    viewer_tenant_id: TenantId | None = None
    capabilities: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    resources: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    tags: tuple[str, ...] = field(default_factory=tuple, metadata={PAYLOAD_LIST: True})
    cluster_id: ClusterId | None = None
    node_id: NodeId | None = None
    function_name: str | None = None
    function_id: str | None = None
    version_constraint: str | None = None
    queue_name: str | None = None
    service_name: str | None = None
    service_protocol: str | None = None
    service_port: int | None = field(default=None, metadata={PAYLOAD_INT: True})
    topic: str | None = None
    limit: int | None = field(default=None, metadata={PAYLOAD_INT: True})
    page_token: str | None = None

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> CatalogQueryRequest:
        data = payload or {}
        raw_tags = data.get("tags", []) or []
        tags = (raw_tags,) if isinstance(raw_tags, str) else tuple(raw_tags)
        return cls(
            entry_type=str(data.get("entry_type", "")).lower(),
            namespace=(
                str(data.get("namespace"))
                if data.get("namespace") is not None
                else None
            ),
            scope=str(data.get("scope")).lower()
            if data.get("scope") is not None
            else None,
            viewer_cluster_id=(
                str(data.get("viewer_cluster_id"))
                if data.get("viewer_cluster_id") is not None
                else None
            ),
            viewer_tenant_id=(
                str(data.get("viewer_tenant_id"))
                if data.get("viewer_tenant_id") is not None
                else None
            ),
            capabilities=tuple(data.get("capabilities", []) or []),
            resources=tuple(data.get("resources", []) or []),
            tags=tags,
            cluster_id=(
                str(data.get("cluster_id"))
                if data.get("cluster_id") is not None
                else None
            ),
            node_id=str(data.get("node_id"))
            if data.get("node_id") is not None
            else None,
            function_name=(
                str(data.get("function_name")) if data.get("function_name") else None
            ),
            function_id=(
                str(data.get("function_id")) if data.get("function_id") else None
            ),
            version_constraint=(
                str(data.get("version_constraint"))
                if data.get("version_constraint")
                else None
            ),
            queue_name=str(data.get("queue_name")) if data.get("queue_name") else None,
            service_name=(
                str(data.get("service_name")) if data.get("service_name") else None
            ),
            service_protocol=(
                str(data.get("service_protocol"))
                if data.get("service_protocol")
                else None
            ),
            service_port=(
                int(data.get("service_port"))
                if data.get("service_port") is not None
                else None
            ),
            topic=str(data.get("topic")) if data.get("topic") else None,
            limit=int(data.get("limit")) if data.get("limit") is not None else None,
            page_token=(
                str(data.get("page_token")) if data.get("page_token") else None
            ),
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_rpc_list(
        cls, request: RpcListRequest, *, include_pagination: bool = True
    ) -> CatalogQueryRequest:
        return cls(
            entry_type="functions",
            namespace=request.namespace,
            scope=request.scope,
            viewer_cluster_id=request.viewer_cluster_id,
            viewer_tenant_id=request.viewer_tenant_id,
            capabilities=request.capabilities,
            resources=request.resources,
            tags=request.tags,
            cluster_id=request.cluster_id,
            node_id=request.node_id,
            function_name=request.function_name,
            function_id=request.function_id,
            version_constraint=request.version_constraint,
            limit=request.limit if include_pagination else None,
            page_token=request.page_token if include_pagination else None,
        )

    @classmethod
    def from_rpc_describe(
        cls, request: RpcDescribeRequest, *, include_pagination: bool = True
    ) -> CatalogQueryRequest:
        return cls(
            entry_type="functions",
            namespace=request.namespace,
            scope=request.scope,
            viewer_cluster_id=request.viewer_cluster_id,
            viewer_tenant_id=request.viewer_tenant_id,
            capabilities=request.capabilities,
            resources=request.resources,
            tags=request.tags,
            cluster_id=request.cluster_id,
            node_id=request.node_id,
            function_name=request.function_name,
            function_id=request.function_id,
            version_constraint=request.version_constraint,
            limit=request.limit if include_pagination else None,
            page_token=request.page_token if include_pagination else None,
        )

    @classmethod
    def from_rpc_report(cls, request: RpcReportRequest) -> CatalogQueryRequest:
        return cls(
            entry_type="functions",
            namespace=request.namespace,
            scope=request.scope,
            viewer_cluster_id=request.viewer_cluster_id,
            viewer_tenant_id=request.viewer_tenant_id,
        )


@dataclass(frozen=True, slots=True)
class CatalogQueryResponse:
    """Response payload for catalog_query."""

    entry_type: str
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    items: tuple[Payload, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    next_page_token: str | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> CatalogQueryResponse:
        items_payload = payload.get("items", []) or []
        items = tuple(item for item in items_payload if isinstance(item, dict))
        return cls(
            entry_type=str(payload.get("entry_type", "")),
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            items=items,
            next_page_token=(
                str(payload.get("next_page_token"))
                if payload.get("next_page_token") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class CatalogWatchRequest:
    """Request payload for catalog_watch."""

    namespace: str | None = None
    scope: EndpointScope | None = None
    cluster_id: ClusterId | None = None
    viewer_tenant_id: TenantId | None = None

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> CatalogWatchRequest:
        data = payload or {}
        return cls(
            namespace=(
                str(data.get("namespace"))
                if data.get("namespace") is not None
                else None
            ),
            scope=str(data.get("scope")).lower()
            if data.get("scope") is not None
            else None,
            cluster_id=(
                str(data.get("cluster_id"))
                if data.get("cluster_id") is not None
                else None
            ),
            viewer_tenant_id=(
                str(data.get("viewer_tenant_id"))
                if data.get("viewer_tenant_id") is not None
                else None
            ),
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class CatalogWatchResponse:
    """Response payload for catalog_watch."""

    topic: str
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    scope: EndpointScope | None = None
    namespace: str | None = None
    cluster_id: str | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> CatalogWatchResponse:
        return cls(
            topic=str(payload.get("topic", "")),
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            scope=str(payload.get("scope")).lower()
            if payload.get("scope") is not None
            else None,
            namespace=(
                str(payload.get("namespace"))
                if payload.get("namespace") is not None
                else None
            ),
            cluster_id=(
                str(payload.get("cluster_id"))
                if payload.get("cluster_id") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class ListPeersRequest:
    """Request payload for list_peers."""

    scope: EndpointScope | None = None
    cluster_id: str | None = None

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> ListPeersRequest:
        data = payload or {}
        return cls(
            scope=str(data.get("scope")).lower()
            if data.get("scope") is not None
            else None,
            cluster_id=(
                str(data.get("cluster_id"))
                if data.get("cluster_id") is not None
                else None
            ),
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class PageWindow:
    """Pagination window for discovery responses."""

    offset: int
    limit: int

    @classmethod
    def from_token(
        cls, token: str | None, *, default_limit: int = 200, max_limit: int = 2000
    ) -> PageWindow:
        if not token:
            return cls(offset=0, limit=default_limit)
        try:
            raw_offset, raw_limit = token.split(":", 1)
            offset = max(0, int(raw_offset))
            limit = max(1, int(raw_limit))
        except ValueError, TypeError:
            return cls(offset=0, limit=default_limit)
        if limit > max_limit:
            limit = max_limit
        return cls(offset=offset, limit=limit)

    def next_token(self, total: int) -> str | None:
        next_offset = self.offset + self.limit
        if next_offset >= total:
            return None
        return f"{next_offset}:{self.limit}"


def paginate_items[T](
    items: Iterable[T],
    *,
    limit: int | None = None,
    page_token: str | None = None,
    default_limit: int = 200,
    max_limit: int = 2000,
) -> tuple[tuple[T, ...], str | None]:
    item_list = list(items)
    window = PageWindow.from_token(
        page_token, default_limit=default_limit, max_limit=max_limit
    )
    if limit is not None:
        window = PageWindow(offset=window.offset, limit=min(limit, max_limit))
    sliced = item_list[window.offset : window.offset + window.limit]
    return tuple(sliced), window.next_token(len(item_list))


@dataclass(frozen=True, slots=True)
class PeerSnapshot:
    """Peer snapshot for list_peers responses."""

    url: str
    cluster_id: str
    funs: tuple[str, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    locs: tuple[str, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    last_seen: Timestamp | None = field(
        metadata={PAYLOAD_FLOAT: True, PAYLOAD_KEEP_EMPTY: True}
    )
    status: str
    status_timestamp: Timestamp | None = field(
        metadata={PAYLOAD_FLOAT: True, PAYLOAD_KEEP_EMPTY: True}
    )
    scope: EndpointScope | None = None
    region: str | None = None
    advertised_urls: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    transport_endpoints: tuple[TransportEndpoint, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    load: NodeLoadMetrics | None = field(
        default=None, metadata={PAYLOAD_KEEP_EMPTY: True}
    )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> PeerSnapshot:
        load_payload = payload.get("load")
        load = (
            NodeLoadMetrics.from_dict(load_payload)
            if isinstance(load_payload, dict)
            else None
        )
        return cls(
            url=str(payload.get("url", "")),
            cluster_id=str(payload.get("cluster_id", "")),
            funs=tuple(payload.get("funs", []) or []),
            locs=tuple(payload.get("locs", []) or []),
            last_seen=payload.get("last_seen"),
            status=str(payload.get("status", "")),
            status_timestamp=payload.get("status_timestamp"),
            scope=str(payload.get("scope")).lower()
            if payload.get("scope") is not None
            else None,
            region=str(payload.get("region")) if payload.get("region") else None,
            advertised_urls=tuple(payload.get("advertised_urls", []) or []),
            transport_endpoints=tuple(
                TransportEndpoint.from_dict(entry)
                for entry in payload.get("transport_endpoints", []) or []
            ),
            load=load,
        )
