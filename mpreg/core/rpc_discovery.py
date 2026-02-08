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
from mpreg.datastructures.function_identity import FunctionIdentity
from mpreg.datastructures.rpc_spec import RpcSpec, RpcSpecSummary
from mpreg.datastructures.type_aliases import (
    ClusterId,
    EndpointScope,
    FunctionId,
    FunctionName,
    NamespaceName,
    NodeId,
    RpcNamespace,
    RpcSpecDigest,
    RpcTag,
    TenantId,
    Timestamp,
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
class RpcListRequest:
    namespace: NamespaceName | None = None
    scope: EndpointScope | None = None
    viewer_cluster_id: ClusterId | None = None
    viewer_tenant_id: TenantId | None = None
    capabilities: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    resources: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    tags: tuple[RpcTag, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    cluster_id: ClusterId | None = None
    node_id: NodeId | None = None
    function_name: FunctionName | None = None
    function_id: FunctionId | None = None
    version_constraint: str | None = None
    query: str | None = None
    limit: int | None = field(default=None, metadata={PAYLOAD_INT: True})
    page_token: str | None = None

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> RpcListRequest:
        data = payload or {}
        return cls(
            namespace=str(data.get("namespace"))
            if data.get("namespace") is not None
            else None,
            scope=str(data.get("scope")).lower()
            if data.get("scope") is not None
            else None,
            viewer_cluster_id=str(data.get("viewer_cluster_id"))
            if data.get("viewer_cluster_id") is not None
            else None,
            viewer_tenant_id=str(data.get("viewer_tenant_id"))
            if data.get("viewer_tenant_id") is not None
            else None,
            capabilities=_normalize_tuple(data.get("capabilities")),
            resources=_normalize_tuple(data.get("resources")),
            tags=_normalize_tuple(data.get("tags")),
            cluster_id=str(data.get("cluster_id"))
            if data.get("cluster_id") is not None
            else None,
            node_id=str(data.get("node_id")) if data.get("node_id") else None,
            function_name=str(data.get("function_name"))
            if data.get("function_name")
            else None,
            function_id=str(data.get("function_id"))
            if data.get("function_id")
            else None,
            version_constraint=str(data.get("version_constraint"))
            if data.get("version_constraint")
            else None,
            query=str(data.get("query")) if data.get("query") else None,
            limit=int(data.get("limit")) if data.get("limit") is not None else None,
            page_token=str(data.get("page_token"))
            if data.get("page_token") is not None
            else None,
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class RpcListItem:
    identity: FunctionIdentity
    namespace: RpcNamespace
    node_id: NodeId
    cluster_id: ClusterId
    resources: tuple[str, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    tags: tuple[RpcTag, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    scope: EndpointScope
    summary: RpcSpecSummary | None = None
    spec_digest: RpcSpecDigest | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> RpcListItem:
        return cls(
            identity=FunctionIdentity.from_dict(
                payload.get("identity", {})  # type: ignore[arg-type]
            ),
            namespace=str(payload.get("namespace", "")),
            node_id=str(payload.get("node_id", "")),
            cluster_id=str(payload.get("cluster_id", "")),
            resources=tuple(payload.get("resources", []) or []),
            tags=tuple(payload.get("tags", []) or []),
            scope=str(payload.get("scope", "")),
            summary=RpcSpecSummary.from_dict(
                payload.get("summary", {})  # type: ignore[arg-type]
            )
            if payload.get("summary") is not None
            else None,
            spec_digest=str(payload.get("spec_digest"))
            if payload.get("spec_digest") is not None
            else None,
        )


@dataclass(frozen=True, slots=True)
class RpcListResponse:
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    items: tuple[RpcListItem, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    next_page_token: str | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> RpcListResponse:
        return cls(
            generated_at=float(payload.get("generated_at", 0.0)),
            items=tuple(
                RpcListItem.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("items", [])
            ),
            next_page_token=str(payload.get("next_page_token"))
            if payload.get("next_page_token") is not None
            else None,
        )


@dataclass(frozen=True, slots=True)
class RpcDescribeRequest:
    mode: str = "auto"
    detail_level: str = "full"
    namespace: NamespaceName | None = None
    scope: EndpointScope | None = None
    viewer_cluster_id: ClusterId | None = None
    viewer_tenant_id: TenantId | None = None
    capabilities: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    resources: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    tags: tuple[RpcTag, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    cluster_id: ClusterId | None = None
    node_id: NodeId | None = None
    function_name: FunctionName | None = None
    function_id: FunctionId | None = None
    version_constraint: str | None = None
    query: str | None = None
    spec_digests: tuple[RpcSpecDigest, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    function_names: tuple[FunctionName, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    function_ids: tuple[FunctionId, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    max_nodes: int | None = field(default=None, metadata={PAYLOAD_INT: True})
    timeout_seconds: float | None = field(default=None, metadata={PAYLOAD_FLOAT: True})
    limit: int | None = field(default=None, metadata={PAYLOAD_INT: True})
    page_token: str | None = None

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> RpcDescribeRequest:
        data = payload or {}
        return cls(
            mode=str(data.get("mode", "auto")),
            detail_level=str(data.get("detail_level", "full")),
            namespace=str(data.get("namespace"))
            if data.get("namespace") is not None
            else None,
            scope=str(data.get("scope")).lower()
            if data.get("scope") is not None
            else None,
            viewer_cluster_id=str(data.get("viewer_cluster_id"))
            if data.get("viewer_cluster_id") is not None
            else None,
            viewer_tenant_id=str(data.get("viewer_tenant_id"))
            if data.get("viewer_tenant_id") is not None
            else None,
            capabilities=_normalize_tuple(data.get("capabilities")),
            resources=_normalize_tuple(data.get("resources")),
            tags=_normalize_tuple(data.get("tags")),
            cluster_id=str(data.get("cluster_id"))
            if data.get("cluster_id") is not None
            else None,
            node_id=str(data.get("node_id")) if data.get("node_id") else None,
            function_name=str(data.get("function_name"))
            if data.get("function_name")
            else None,
            function_id=str(data.get("function_id"))
            if data.get("function_id")
            else None,
            version_constraint=str(data.get("version_constraint"))
            if data.get("version_constraint")
            else None,
            query=str(data.get("query")) if data.get("query") else None,
            spec_digests=_normalize_tuple(data.get("spec_digests")),
            function_names=_normalize_tuple(data.get("function_names")),
            function_ids=_normalize_tuple(data.get("function_ids")),
            max_nodes=int(data.get("max_nodes"))
            if data.get("max_nodes") is not None
            else None,
            timeout_seconds=float(data.get("timeout_seconds"))
            if data.get("timeout_seconds") is not None
            else None,
            limit=int(data.get("limit")) if data.get("limit") is not None else None,
            page_token=str(data.get("page_token"))
            if data.get("page_token") is not None
            else None,
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class RpcDescribeItem:
    identity: FunctionIdentity
    namespace: RpcNamespace
    node_id: NodeId
    cluster_id: ClusterId
    resources: tuple[str, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    tags: tuple[RpcTag, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    scope: EndpointScope
    summary: RpcSpecSummary | None = None
    spec: RpcSpec | None = None
    spec_digest: RpcSpecDigest | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> RpcDescribeItem:
        return cls(
            identity=FunctionIdentity.from_dict(
                payload.get("identity", {})  # type: ignore[arg-type]
            ),
            namespace=str(payload.get("namespace", "")),
            node_id=str(payload.get("node_id", "")),
            cluster_id=str(payload.get("cluster_id", "")),
            resources=tuple(payload.get("resources", []) or []),
            tags=tuple(payload.get("tags", []) or []),
            scope=str(payload.get("scope", "")),
            summary=RpcSpecSummary.from_dict(
                payload.get("summary", {})  # type: ignore[arg-type]
            )
            if payload.get("summary") is not None
            else None,
            spec=RpcSpec.from_dict(payload.get("spec", {}) or {})  # type: ignore[arg-type]
            if payload.get("spec") is not None
            else None,
            spec_digest=str(payload.get("spec_digest"))
            if payload.get("spec_digest") is not None
            else None,
        )


@dataclass(frozen=True, slots=True)
class RpcDescribeError:
    node_id: NodeId
    cluster_id: ClusterId
    error: str

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class RpcDescribeResponse:
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    items: tuple[RpcDescribeItem, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    errors: tuple[RpcDescribeError, ...] = field(
        default_factory=tuple,
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True},
    )
    next_page_token: str | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> RpcDescribeResponse:
        return cls(
            generated_at=float(payload.get("generated_at", 0.0)),
            items=tuple(
                RpcDescribeItem.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("items", [])
            ),
            errors=tuple(
                RpcDescribeError(
                    node_id=str(item.get("node_id", "")),
                    cluster_id=str(item.get("cluster_id", "")),
                    error=str(item.get("error", "")),
                )
                for item in payload.get("errors", [])
            ),
            next_page_token=str(payload.get("next_page_token"))
            if payload.get("next_page_token") is not None
            else None,
        )


@dataclass(frozen=True, slots=True)
class RpcReportRequest:
    namespace: NamespaceName | None = None
    scope: EndpointScope | None = None
    viewer_cluster_id: ClusterId | None = None
    viewer_tenant_id: TenantId | None = None

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> RpcReportRequest:
        data = payload or {}
        return cls(
            namespace=str(data.get("namespace"))
            if data.get("namespace") is not None
            else None,
            scope=str(data.get("scope")).lower()
            if data.get("scope") is not None
            else None,
            viewer_cluster_id=str(data.get("viewer_cluster_id"))
            if data.get("viewer_cluster_id") is not None
            else None,
            viewer_tenant_id=str(data.get("viewer_tenant_id"))
            if data.get("viewer_tenant_id") is not None
            else None,
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class RpcReportCount:
    key: str
    count: int = field(metadata={PAYLOAD_INT: True})

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> RpcReportCount:
        return cls(
            key=str(payload.get("key", "")),
            count=int(payload.get("count", 0) or 0),
        )


@dataclass(frozen=True, slots=True)
class RpcReportResponse:
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    total_functions: int = field(metadata={PAYLOAD_INT: True})
    namespace_counts: tuple[RpcReportCount, ...] = field(
        default_factory=tuple,
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True},
    )
    cluster_counts: tuple[RpcReportCount, ...] = field(
        default_factory=tuple,
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True},
    )
    tag_counts: tuple[RpcReportCount, ...] = field(
        default_factory=tuple,
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True},
    )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> RpcReportResponse:
        namespace_payload = payload.get("namespace_counts", []) or []
        cluster_payload = payload.get("cluster_counts", []) or []
        tag_payload = payload.get("tag_counts", []) or []
        namespace_counts = tuple(
            RpcReportCount.from_dict(item)  # type: ignore[arg-type]
            for item in namespace_payload
            if isinstance(item, dict)
        )
        cluster_counts = tuple(
            RpcReportCount.from_dict(item)  # type: ignore[arg-type]
            for item in cluster_payload
            if isinstance(item, dict)
        )
        tag_counts = tuple(
            RpcReportCount.from_dict(item)  # type: ignore[arg-type]
            for item in tag_payload
            if isinstance(item, dict)
        )
        return cls(
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            total_functions=int(payload.get("total_functions", 0) or 0),
            namespace_counts=namespace_counts,
            cluster_counts=cluster_counts,
            tag_counts=tag_counts,
        )
