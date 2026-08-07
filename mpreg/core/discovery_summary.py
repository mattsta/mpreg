from __future__ import annotations

import hashlib
from collections import deque
from dataclasses import dataclass, field

from mpreg.core.payloads import (
    PAYLOAD_CONVERTER,
    PAYLOAD_FLOAT,
    PAYLOAD_INT,
    PAYLOAD_KEEP_EMPTY,
    PAYLOAD_LIST,
    Payload,
    PayloadMapping,
    payload_from_dataclass,
    payload_list,
    payload_mapping,
)
from mpreg.datastructures.type_aliases import (
    ClusterId,
    EndpointScope,
    NamespaceName,
    NodeId,
    TenantId,
    Timestamp,
)
from mpreg.fabric.catalog import FunctionEndpoint, RoutingCatalog

from .model import PubSubMessage
from .namespace_policy import NamespacePolicyEngine

DISCOVERY_SUMMARY_TOPIC = "mpreg.discovery.summary"


def _ingress_payload(value: object) -> dict[object, object]:
    return payload_mapping(value, key_converter=str, value_converter=payload_list)


@dataclass(frozen=True, slots=True)
class ServiceSummary:
    namespace: NamespaceName
    service_id: str
    regions: tuple[str, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    endpoint_count: int = field(metadata={PAYLOAD_INT: True})
    health_band: str
    latency_band_ms: tuple[int, int] = field(metadata={PAYLOAD_LIST: True})
    ttl_seconds: float = field(metadata={PAYLOAD_FLOAT: True})
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    scope: EndpointScope | None = None
    policy_version: str | None = None
    source_cluster: ClusterId | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> ServiceSummary:
        latency_band = payload.get("latency_band_ms", (0, 0))
        if isinstance(latency_band, (list, tuple)) and len(latency_band) >= 2:
            latency_values = (
                int(latency_band[0] or 0),
                int(latency_band[1] or 0),
            )
        else:
            latency_values = (0, 0)
        return cls(
            namespace=str(payload.get("namespace", "")),
            service_id=str(payload.get("service_id", "")),
            scope=(
                str(payload.get("scope")).lower()
                if payload.get("scope") is not None
                else None
            ),
            regions=tuple(payload.get("regions", []) or []),
            endpoint_count=int(payload.get("endpoint_count", 0) or 0),
            health_band=str(payload.get("health_band", "")),
            latency_band_ms=latency_values,
            ttl_seconds=float(payload.get("ttl_seconds", 0.0) or 0.0),
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            policy_version=(
                str(payload.get("policy_version"))
                if payload.get("policy_version") is not None
                else None
            ),
            source_cluster=(
                str(payload.get("source_cluster"))
                if payload.get("source_cluster") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class SummaryQueryRequest:
    scope: EndpointScope | None = None
    namespace: NamespaceName | None = None
    service_id: str | None = None
    viewer_cluster_id: ClusterId | None = None
    viewer_tenant_id: TenantId | None = None
    include_ingress: bool = False
    ingress_limit: int | None = field(default=None, metadata={PAYLOAD_INT: True})
    ingress_scope: EndpointScope | None = None
    ingress_capabilities: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    ingress_tags: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    limit: int | None = field(default=None, metadata={PAYLOAD_INT: True})
    page_token: str | None = None

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> SummaryQueryRequest:
        data = payload or {}
        return cls(
            scope=str(data.get("scope")).lower()
            if data.get("scope") is not None
            else None,
            namespace=(
                str(data.get("namespace"))
                if data.get("namespace") is not None
                else None
            ),
            service_id=(
                str(data.get("service_id"))
                if data.get("service_id") is not None
                else None
            ),
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
            include_ingress=bool(data.get("include_ingress", False)),
            ingress_limit=(
                int(data.get("ingress_limit"))
                if data.get("ingress_limit") is not None
                else None
            ),
            ingress_scope=(
                str(data.get("ingress_scope")).lower()
                if data.get("ingress_scope") is not None
                else None
            ),
            ingress_capabilities=tuple(data.get("ingress_capabilities", []) or []),
            ingress_tags=tuple(data.get("ingress_tags", []) or []),
            limit=int(data.get("limit")) if data.get("limit") is not None else None,
            page_token=str(data.get("page_token")) if data.get("page_token") else None,
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class SummaryQueryResponse:
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    items: tuple[ServiceSummary, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    ingress: dict[ClusterId, tuple[NodeId, ...]] | None = field(
        default=None,
        metadata={
            PAYLOAD_CONVERTER: _ingress_payload,
        },
    )
    next_page_token: str | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> SummaryQueryResponse:
        ingress_payload = payload.get("ingress", {}) or {}
        ingress: dict[ClusterId, tuple[NodeId, ...]] = {}
        if isinstance(ingress_payload, dict):
            for cluster_id, urls in ingress_payload.items():
                if urls is None:
                    continue
                if isinstance(urls, (list, tuple)):
                    ingress[str(cluster_id)] = tuple(str(item) for item in urls)
        return cls(
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            items=tuple(
                ServiceSummary.from_dict(item)
                for item in payload.get("items", []) or []
                if isinstance(item, dict)
            ),
            ingress=ingress or None,
            next_page_token=(
                str(payload.get("next_page_token"))
                if payload.get("next_page_token") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class SummaryWatchRequest:
    scope: EndpointScope | None = None
    namespace: NamespaceName | None = None
    cluster_id: ClusterId | None = None
    viewer_tenant_id: TenantId | None = None

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> SummaryWatchRequest:
        data = payload or {}
        return cls(
            scope=str(data.get("scope")).lower()
            if data.get("scope") is not None
            else None,
            namespace=(
                str(data.get("namespace"))
                if data.get("namespace") is not None
                else None
            ),
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
class SummaryWatchResponse:
    topic: str
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    scope: EndpointScope | None = None
    namespace: NamespaceName | None = None
    cluster_id: ClusterId | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> SummaryWatchResponse:
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
class DiscoverySummaryMessage:
    summaries: tuple[ServiceSummary, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    namespaces: tuple[NamespaceName, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    source_node: NodeId
    source_cluster: ClusterId
    published_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    scope: EndpointScope | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> DiscoverySummaryMessage:
        summaries_payload = payload.get("summaries", []) or []
        summaries = tuple(
            ServiceSummary.from_dict(entry)
            for entry in summaries_payload
            if isinstance(entry, dict)
        )
        raw_namespaces = payload.get("namespaces", []) or []
        namespaces = tuple(str(item) for item in raw_namespaces if item is not None)
        return cls(
            summaries=summaries,
            namespaces=namespaces,
            source_node=str(payload.get("source_node", "")),
            source_cluster=str(payload.get("source_cluster", "")),
            published_at=float(payload.get("published_at", 0.0) or 0.0),
            scope=(
                str(payload.get("scope")).lower()
                if payload.get("scope") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class SummaryExportTemplate:
    namespace: NamespaceName
    service_id: str
    regions: tuple[str, ...]
    endpoint_count: int
    health_band: str
    latency_band_ms: tuple[int, int]
    scope: EndpointScope | None = None
    policy_version: str | None = None
    source_cluster: ClusterId | None = None

    @classmethod
    def from_summary(cls, summary: ServiceSummary) -> SummaryExportTemplate:
        return cls(
            namespace=summary.namespace,
            service_id=summary.service_id,
            scope=summary.scope,
            regions=summary.regions,
            endpoint_count=summary.endpoint_count,
            health_band=summary.health_band,
            latency_band_ms=summary.latency_band_ms,
            policy_version=summary.policy_version,
            source_cluster=summary.source_cluster,
        )

    def to_summary(
        self, *, ttl_seconds: float, generated_at: Timestamp
    ) -> ServiceSummary:
        return ServiceSummary(
            namespace=self.namespace,
            service_id=self.service_id,
            scope=self.scope,
            regions=self.regions,
            endpoint_count=self.endpoint_count,
            health_band=self.health_band,
            latency_band_ms=self.latency_band_ms,
            ttl_seconds=ttl_seconds,
            generated_at=generated_at,
            policy_version=self.policy_version,
            source_cluster=self.source_cluster,
        )


@dataclass(slots=True)
class SummaryExportNamespaceState:
    templates: tuple[SummaryExportTemplate, ...]
    fingerprint: str
    last_change_at: Timestamp


@dataclass(frozen=True, slots=True)
class SummaryStoreForwardEntry:
    message: PubSubMessage
    stored_at: Timestamp


@dataclass(slots=True)
class NamespaceSummaryExportStats:
    namespace: NamespaceName
    exports_total: int = field(default=0, metadata={PAYLOAD_INT: True})
    summaries_exported: int = field(default=0, metadata={PAYLOAD_INT: True})
    last_export_at: Timestamp | None = field(
        default=None, metadata={PAYLOAD_FLOAT: True}
    )
    last_export_count: int = field(default=0, metadata={PAYLOAD_INT: True})

    def record(self, count: int, timestamp: Timestamp) -> None:
        export_count = int(count)
        self.exports_total += 1
        self.summaries_exported += export_count
        self.last_export_at = timestamp
        self.last_export_count = export_count

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(slots=True)
class SummaryExportState:
    exports_total: int = 0
    summaries_exported: int = 0
    last_export_at: Timestamp | None = None
    last_export_count: int = 0
    last_export_namespaces: tuple[NamespaceName, ...] = ()
    hold_downs_total: int = 0
    per_namespace: dict[NamespaceName, NamespaceSummaryExportStats] = field(
        default_factory=dict
    )
    _namespace_state: dict[NamespaceName, SummaryExportNamespaceState] = field(
        default_factory=dict
    )
    _store_forward: deque[SummaryStoreForwardEntry] = field(default_factory=deque)

    def _fingerprint_templates(
        self, templates: tuple[SummaryExportTemplate, ...]
    ) -> str:
        parts: list[str] = []
        for template in templates:
            parts.append(
                "|".join(
                    (
                        template.service_id,
                        template.scope or "",
                        str(template.endpoint_count),
                        ",".join(template.regions),
                        template.health_band,
                        f"{template.latency_band_ms[0]}-{template.latency_band_ms[1]}",
                        template.policy_version or "",
                        template.source_cluster or "",
                    )
                )
            )
        payload = "\n".join(parts).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def prepare_export(
        self,
        summaries: list[ServiceSummary],
        *,
        timestamp: Timestamp,
        ttl_seconds: float,
        hold_down_seconds: float,
    ) -> list[ServiceSummary]:
        grouped: dict[NamespaceName, list[ServiceSummary]] = {}
        for summary in summaries:
            grouped.setdefault(summary.namespace, []).append(summary)

        export_summaries: list[ServiceSummary] = []
        for namespace, items in grouped.items():
            templates = tuple(
                SummaryExportTemplate.from_summary(summary)
                for summary in sorted(items, key=lambda entry: entry.service_id)
            )
            fingerprint = self._fingerprint_templates(templates)
            state = self._namespace_state.get(namespace)

            if state is None:
                self._namespace_state[namespace] = SummaryExportNamespaceState(
                    templates=templates,
                    fingerprint=fingerprint,
                    last_change_at=timestamp,
                )
                selected_templates = templates
            else:
                if fingerprint == state.fingerprint:
                    selected_templates = state.templates
                else:
                    if (
                        hold_down_seconds > 0
                        and (timestamp - state.last_change_at) < hold_down_seconds
                    ):
                        selected_templates = state.templates
                        self.hold_downs_total += 1
                    else:
                        selected_templates = templates
                        state.templates = templates
                        state.fingerprint = fingerprint
                        state.last_change_at = timestamp

            export_summaries.extend(
                template.to_summary(
                    ttl_seconds=ttl_seconds,
                    generated_at=timestamp,
                )
                for template in selected_templates
            )

        return export_summaries

    def record_store_forward(
        self,
        message: PubSubMessage,
        *,
        now: Timestamp,
        max_age_seconds: float,
        max_messages: int,
    ) -> None:
        if max_age_seconds <= 0 or max_messages <= 0:
            return
        self._store_forward.append(
            SummaryStoreForwardEntry(message=message, stored_at=now)
        )
        while len(self._store_forward) > max_messages:
            self._store_forward.popleft()
        self._prune_store_forward(now=now, max_age_seconds=max_age_seconds)

    def store_forward_backlog(
        self, *, now: Timestamp, max_age_seconds: float
    ) -> tuple[PubSubMessage, ...]:
        if max_age_seconds <= 0:
            return ()
        cutoff = now - max_age_seconds
        backlog = tuple(
            entry.message for entry in self._store_forward if entry.stored_at >= cutoff
        )
        self._prune_store_forward(now=now, max_age_seconds=max_age_seconds)
        return backlog

    def _prune_store_forward(self, *, now: Timestamp, max_age_seconds: float) -> None:
        cutoff = now - max_age_seconds
        while self._store_forward and self._store_forward[0].stored_at < cutoff:
            self._store_forward.popleft()

    def store_forward_size(self) -> int:
        return len(self._store_forward)

    def record_export(
        self, summaries: list[ServiceSummary], timestamp: Timestamp
    ) -> None:
        export_count = len(summaries)
        self.exports_total += 1
        self.summaries_exported += export_count
        self.last_export_at = timestamp
        self.last_export_count = export_count
        namespace_counts: dict[NamespaceName, int] = {}
        for summary in summaries:
            namespace = summary.namespace
            if not namespace:
                continue
            namespace_counts[namespace] = namespace_counts.get(namespace, 0) + 1
        self.last_export_namespaces = tuple(sorted(namespace_counts))
        for namespace, count in namespace_counts.items():
            stats = self.per_namespace.get(namespace)
            if stats is None:
                stats = NamespaceSummaryExportStats(namespace=namespace)
                self.per_namespace[namespace] = stats
            stats.record(count, timestamp)

    def summary_for_namespace(
        self, namespace: NamespaceName
    ) -> tuple[int | None, Timestamp | None]:
        if not namespace:
            return None, None
        prefix = f"{namespace}."
        matches = [
            stats
            for name, stats in self.per_namespace.items()
            if name == namespace or name.startswith(prefix)
        ]
        if not matches:
            return None, None
        summaries_exported = sum(stats.summaries_exported for stats in matches)
        last_export_at = max((stats.last_export_at or 0.0) for stats in matches)
        return (
            summaries_exported,
            last_export_at if last_export_at > 0.0 else None,
        )

    def snapshot(
        self,
        *,
        enabled: bool,
        interval_seconds: float,
        export_scope: EndpointScope | None,
        hold_down_seconds: float,
        store_forward_seconds: float,
        store_forward_max_messages: int,
        configured_namespaces: tuple[NamespaceName, ...],
        generated_at: Timestamp,
    ) -> SummaryExportSnapshot:
        return SummaryExportSnapshot(
            enabled=enabled,
            generated_at=generated_at,
            interval_seconds=float(interval_seconds),
            export_scope=export_scope,
            hold_down_seconds=float(hold_down_seconds),
            store_forward_seconds=float(store_forward_seconds),
            store_forward_max_messages=int(store_forward_max_messages),
            configured_namespaces=configured_namespaces,
            exports_total=int(self.exports_total),
            summaries_exported=int(self.summaries_exported),
            last_export_at=self.last_export_at,
            last_export_count=int(self.last_export_count),
            last_export_namespaces=self.last_export_namespaces,
            hold_downs_total=int(self.hold_downs_total),
            store_forward_queued=self.store_forward_size(),
            per_namespace=tuple(
                sorted(self.per_namespace.values(), key=lambda stats: stats.namespace)
            ),
        )


@dataclass(frozen=True, slots=True)
class SummaryExportSnapshot:
    enabled: bool
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    interval_seconds: float = field(metadata={PAYLOAD_FLOAT: True})
    export_scope: EndpointScope | None = field(metadata={PAYLOAD_KEEP_EMPTY: True})
    hold_down_seconds: float = field(metadata={PAYLOAD_FLOAT: True})
    store_forward_seconds: float = field(metadata={PAYLOAD_FLOAT: True})
    store_forward_max_messages: int = field(metadata={PAYLOAD_INT: True})
    configured_namespaces: tuple[NamespaceName, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    exports_total: int = field(metadata={PAYLOAD_INT: True})
    summaries_exported: int = field(metadata={PAYLOAD_INT: True})
    last_export_count: int = field(metadata={PAYLOAD_INT: True})
    last_export_namespaces: tuple[NamespaceName, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    hold_downs_total: int = field(metadata={PAYLOAD_INT: True})
    store_forward_queued: int = field(metadata={PAYLOAD_INT: True})
    per_namespace: tuple[NamespaceSummaryExportStats, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    last_export_at: Timestamp | None = field(
        default=None, metadata={PAYLOAD_FLOAT: True}
    )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


def summarize_functions(
    catalog: RoutingCatalog,
    *,
    now: Timestamp,
    region: str,
    ttl_seconds: float,
    policy_engine: NamespacePolicyEngine | None,
    viewer_cluster_id: ClusterId,
    viewer_tenant_id: TenantId | None = None,
    enforce_summary_exports: bool = False,
    source_cluster_id: ClusterId | None = None,
    export_scope: EndpointScope | None = None,
    summary_scope: EndpointScope | None = None,
) -> list[ServiceSummary]:
    summaries: list[ServiceSummary] = []
    grouped: dict[str, list[FunctionEndpoint]] = {}
    for endpoint in catalog.functions.entries(now=now):
        service_id = endpoint.identity.name
        grouped.setdefault(service_id, []).append(endpoint)

    for service_id, endpoints in grouped.items():
        namespace = _namespace_from_service_id(service_id)
        policy_version = None
        if policy_engine and policy_engine.enabled:
            rule = policy_engine.match_rule(namespace)
            if enforce_summary_exports:
                if source_cluster_id is None:
                    continue
                decision = policy_engine.allows_summary_export(
                    namespace,
                    source_cluster_id,
                    export_scope=export_scope,
                    now=now,
                )
                if not decision.allowed:
                    continue
            else:
                decision = policy_engine.allows_viewer(
                    namespace,
                    viewer_cluster_id,
                    viewer_tenant_id=viewer_tenant_id,
                )
                if not decision.allowed:
                    continue
            if rule is not None:
                policy_version = rule.policy_version
        summaries.append(
            ServiceSummary(
                namespace=namespace,
                service_id=service_id,
                regions=(region,) if region else (),
                endpoint_count=len(endpoints),
                health_band="healthy",
                latency_band_ms=(0, 0),
                ttl_seconds=ttl_seconds,
                generated_at=now,
                policy_version=policy_version,
                source_cluster=source_cluster_id,
                scope=summary_scope,
            )
        )
    return summaries


def _namespace_from_service_id(service_id: str) -> NamespaceName:
    if "." not in service_id:
        return service_id
    return service_id.rsplit(".", 1)[0]
