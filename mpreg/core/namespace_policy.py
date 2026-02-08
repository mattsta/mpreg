from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from threading import RLock

from mpreg.core.payloads import (
    PAYLOAD_FLOAT,
    PAYLOAD_INT,
    PAYLOAD_KEEP_EMPTY,
    PAYLOAD_LIST,
    Payload,
    PayloadMapping,
    payload_from_dataclass,
)
from mpreg.datastructures.type_aliases import (
    ClusterId,
    NamespaceName,
    TenantId,
    Timestamp,
)
from mpreg.fabric.catalog import (
    CacheNodeProfile,
    CacheRoleEntry,
    FunctionEndpoint,
    NodeDescriptor,
    QueueEndpoint,
    ServiceEndpoint,
    TopicSubscription,
)
from mpreg.fabric.catalog_policy import CatalogFilterPolicy

_DISCOVERY_TOPIC_PREFIX = "mpreg.discovery."


@dataclass(frozen=True, slots=True)
class CutoverWindow:
    starts_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    ends_at: Timestamp | None = field(default=None, metadata={PAYLOAD_FLOAT: True})
    export_scopes: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    allow_summaries: bool | None = None

    def is_active(self, *, now: Timestamp) -> bool:
        if now < self.starts_at:
            return False
        return not (self.ends_at is not None and now > self.ends_at)

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> CutoverWindow:
        raw_scopes = payload.get("export_scopes", ())
        allow_summaries = payload.get("allow_summaries")
        return cls(
            starts_at=float(payload.get("starts_at", 0.0) or 0.0),
            ends_at=(
                float(payload.get("ends_at"))
                if payload.get("ends_at") is not None
                else None
            ),
            export_scopes=tuple(str(item) for item in raw_scopes or ()),
            allow_summaries=(
                bool(allow_summaries) if allow_summaries is not None else None
            ),
        )


@dataclass(frozen=True, slots=True)
class NamespacePolicyRule:
    namespace: NamespaceName
    owners: tuple[ClusterId, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    visibility: tuple[ClusterId, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    visibility_tenants: tuple[TenantId, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    export_scopes: tuple[str, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )
    allow_summaries: bool = False
    cutover_windows: tuple[CutoverWindow, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True}
    )
    policy_version: str | None = None

    def matches(self, namespace: NamespaceName) -> bool:
        if namespace == self.namespace:
            return True
        prefix = f"{self.namespace}."
        return namespace.startswith(prefix)

    def active_cutover_window(self, *, now: Timestamp) -> CutoverWindow | None:
        if not self.cutover_windows:
            return None
        active = [
            window for window in self.cutover_windows if window.is_active(now=now)
        ]
        if not active:
            return None
        return max(active, key=lambda window: window.starts_at)

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> NamespacePolicyRule:
        owners = payload.get("owners", ())
        visibility = payload.get("visibility", ())
        visibility_tenants = payload.get("visibility_tenants", ())
        export_scopes = payload.get("export_scopes", ())
        raw_windows = payload.get("cutover_windows", ())
        if isinstance(raw_windows, dict):
            raw_windows = [raw_windows]
        windows: list[CutoverWindow] = []
        for item in raw_windows or ():
            if isinstance(item, CutoverWindow):
                windows.append(item)
            elif isinstance(item, dict):
                windows.append(CutoverWindow.from_dict(item))
        return cls(
            namespace=str(payload.get("namespace", "")),
            owners=tuple(str(item) for item in owners or ()),
            visibility=tuple(str(item) for item in visibility or ()),
            visibility_tenants=tuple(str(item) for item in visibility_tenants or ()),
            export_scopes=tuple(str(item) for item in export_scopes or ()),
            allow_summaries=bool(payload.get("allow_summaries", False)),
            cutover_windows=tuple(windows),
            policy_version=(
                str(payload.get("policy_version"))
                if payload.get("policy_version") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class NamespacePolicyDecision:
    allowed: bool
    reason: str
    rule: NamespacePolicyRule | None = None

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(slots=True)
class NamespacePolicyEngine:
    enabled: bool
    default_allow: bool
    rules: tuple[NamespacePolicyRule, ...] = field(
        default_factory=tuple, metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )

    def _best_match(self, namespace: NamespaceName) -> NamespacePolicyRule | None:
        matches = [rule for rule in self.rules if rule.matches(namespace)]
        if not matches:
            return None
        return max(matches, key=lambda rule: len(rule.namespace))

    def match_rule(self, namespace: NamespaceName) -> NamespacePolicyRule | None:
        return self._best_match(namespace)

    def allows_source(
        self, namespace: NamespaceName, source_cluster: ClusterId
    ) -> NamespacePolicyDecision:
        if not self.enabled:
            return NamespacePolicyDecision(True, "policy_disabled")
        rule = self._best_match(namespace)
        if rule is None:
            return NamespacePolicyDecision(self.default_allow, "no_policy")
        if not rule.owners:
            return NamespacePolicyDecision(True, "owners_unrestricted", rule=rule)
        if source_cluster in rule.owners:
            return NamespacePolicyDecision(True, "owner_allowed", rule=rule)
        return NamespacePolicyDecision(False, "owner_denied", rule=rule)

    def allows_viewer(
        self,
        namespace: NamespaceName,
        viewer_cluster: ClusterId,
        *,
        viewer_tenant_id: TenantId | None = None,
    ) -> NamespacePolicyDecision:
        if not self.enabled:
            return NamespacePolicyDecision(True, "policy_disabled")
        rule = self._best_match(namespace)
        if rule is None:
            return NamespacePolicyDecision(self.default_allow, "no_policy")
        if not rule.visibility and not rule.visibility_tenants:
            return NamespacePolicyDecision(True, "visibility_unrestricted", rule=rule)
        if rule.visibility_tenants and viewer_tenant_id:
            if viewer_tenant_id in rule.visibility_tenants:
                return NamespacePolicyDecision(True, "tenant_allowed", rule=rule)
        if rule.visibility and viewer_cluster in rule.visibility:
            return NamespacePolicyDecision(True, "viewer_allowed", rule=rule)
        return NamespacePolicyDecision(False, "viewer_denied", rule=rule)

    def allows_summary_export(
        self,
        namespace: NamespaceName,
        source_cluster: ClusterId,
        *,
        export_scope: str | None,
        now: Timestamp,
    ) -> NamespacePolicyDecision:
        if not self.enabled:
            return NamespacePolicyDecision(True, "policy_disabled")
        rule = self._best_match(namespace)
        if rule is None:
            return NamespacePolicyDecision(self.default_allow, "no_policy")
        if rule.owners and source_cluster not in rule.owners:
            return NamespacePolicyDecision(False, "owner_denied", rule=rule)
        allow_summaries = rule.allow_summaries
        if rule.cutover_windows:
            window = rule.active_cutover_window(now=now)
            if window is None:
                return NamespacePolicyDecision(False, "cutover_closed", rule=rule)
            if window.allow_summaries is not None:
                allow_summaries = window.allow_summaries
            if window.export_scopes:
                if export_scope is None or export_scope not in window.export_scopes:
                    return NamespacePolicyDecision(
                        False, "cutover_scope_denied", rule=rule
                    )
        if not allow_summaries:
            return NamespacePolicyDecision(False, "summaries_disabled", rule=rule)
        if rule.export_scopes:
            if export_scope is None or export_scope not in rule.export_scopes:
                return NamespacePolicyDecision(False, "scope_denied", rule=rule)
        return NamespacePolicyDecision(True, "summary_allowed", rule=rule)

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class NamespaceCatalogFilterPolicy:
    base_policy: CatalogFilterPolicy
    namespace_policy: NamespacePolicyEngine

    def allows_cluster(self, cluster_id: ClusterId) -> bool:
        return self.base_policy.allows_cluster(cluster_id)

    def _namespace_allowed(
        self, namespace: NamespaceName, cluster_id: ClusterId
    ) -> bool:
        if cluster_id == self.base_policy.local_cluster:
            return True
        decision = self.namespace_policy.allows_source(namespace, cluster_id)
        return decision.allowed

    def allows_function(self, endpoint: FunctionEndpoint) -> bool:
        if not self.base_policy.allows_function(endpoint):
            return False
        return self._namespace_allowed(endpoint.identity.name, endpoint.cluster_id)

    def allows_node(self, node: NodeDescriptor) -> bool:
        return self.base_policy.allows_node(node)

    def allows_queue(self, endpoint: QueueEndpoint) -> bool:
        if not self.base_policy.allows_queue(endpoint):
            return False
        return self._namespace_allowed(endpoint.queue_name, endpoint.cluster_id)

    def allows_service(self, endpoint: ServiceEndpoint) -> bool:
        if not self.base_policy.allows_service(endpoint):
            return False
        return self._namespace_allowed(endpoint.namespace, endpoint.cluster_id)

    def allows_cache(self, entry: CacheRoleEntry) -> bool:
        return self.base_policy.allows_cache(entry)

    def allows_cache_profile(self, profile: CacheNodeProfile) -> bool:
        return self.base_policy.allows_cache_profile(profile)

    def allows_topic(self, subscription: TopicSubscription) -> bool:
        if not self.base_policy.allows_topic(subscription):
            return False
        if subscription.cluster_id == self.base_policy.local_cluster:
            return True
        prefixes = _topic_prefixes(subscription.patterns)
        for prefix in prefixes:
            if prefix.startswith(_DISCOVERY_TOPIC_PREFIX):
                continue
            if prefix and not self._namespace_allowed(prefix, subscription.cluster_id):
                return False
        return True


@dataclass(frozen=True, slots=True)
class NamespaceStatusRequest:
    namespace: NamespaceName
    viewer_cluster_id: ClusterId | None = None
    viewer_tenant_id: TenantId | None = None

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> NamespaceStatusRequest:
        data = payload or {}
        return cls(
            namespace=str(data.get("namespace", "")),
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
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class NamespaceStatusResponse:
    namespace: NamespaceName
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    viewer_cluster_id: ClusterId | None = field(metadata={PAYLOAD_KEEP_EMPTY: True})
    viewer_tenant_id: TenantId | None = field(metadata={PAYLOAD_KEEP_EMPTY: True})
    allowed: bool
    reason: str
    rule: NamespacePolicyRule | None = None
    summaries_exported: int | None = field(default=None, metadata={PAYLOAD_INT: True})
    last_export_at: Timestamp | None = field(
        default=None, metadata={PAYLOAD_FLOAT: True}
    )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> NamespaceStatusResponse:
        rule_payload = payload.get("rule")
        return cls(
            namespace=str(payload.get("namespace", "")),
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            viewer_cluster_id=(
                str(payload.get("viewer_cluster_id"))
                if payload.get("viewer_cluster_id") is not None
                else None
            ),
            viewer_tenant_id=(
                str(payload.get("viewer_tenant_id"))
                if payload.get("viewer_tenant_id") is not None
                else None
            ),
            allowed=bool(payload.get("allowed", False)),
            reason=str(payload.get("reason", "")),
            rule=(
                NamespacePolicyRule.from_dict(rule_payload)
                if isinstance(rule_payload, dict)
                else None
            ),
            summaries_exported=(
                int(payload.get("summaries_exported"))
                if payload.get("summaries_exported") is not None
                else None
            ),
            last_export_at=(
                float(payload.get("last_export_at"))
                if payload.get("last_export_at") is not None
                else None
            ),
        )


def _topic_prefixes(patterns: tuple[str, ...]) -> tuple[str, ...]:
    prefixes: list[str] = []
    for pattern in patterns:
        prefix = pattern
        if "*" in prefix:
            prefix = prefix.split("*", 1)[0]
        if "#" in prefix:
            prefix = prefix.split("#", 1)[0]
        prefix = prefix.rstrip(".")
        prefixes.append(prefix)
    return tuple(prefixes)


@dataclass(frozen=True, slots=True)
class NamespacePolicyValidationResponse:
    valid: bool
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    rule_count: int = field(metadata={PAYLOAD_INT: True})
    errors: tuple[str, ...] = field(
        default=(), metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> NamespacePolicyValidationResponse:
        return cls(
            valid=bool(payload.get("valid", False)),
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            rule_count=int(payload.get("rule_count", 0) or 0),
            errors=tuple(payload.get("errors", []) or []),
        )


@dataclass(frozen=True, slots=True)
class NamespacePolicyApplyResponse:
    applied: bool
    valid: bool
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    rule_count: int = field(metadata={PAYLOAD_INT: True})
    errors: tuple[str, ...] = field(
        default=(), metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> NamespacePolicyApplyResponse:
        return cls(
            applied=bool(payload.get("applied", False)),
            valid=bool(payload.get("valid", False)),
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            rule_count=int(payload.get("rule_count", 0) or 0),
            errors=tuple(payload.get("errors", []) or []),
        )


@dataclass(frozen=True, slots=True)
class NamespacePolicyExportResponse:
    enabled: bool
    default_allow: bool
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    rule_count: int = field(metadata={PAYLOAD_INT: True})
    rules: tuple[NamespacePolicyRule, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> NamespacePolicyExportResponse:
        rules_payload = payload.get("rules", []) or []
        rules = tuple(
            NamespacePolicyRule.from_dict(rule)
            for rule in rules_payload
            if isinstance(rule, dict)
        )
        return cls(
            enabled=bool(payload.get("enabled", False)),
            default_allow=bool(payload.get("default_allow", False)),
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            rule_count=int(payload.get("rule_count", 0) or 0),
            rules=rules,
        )


@dataclass(frozen=True, slots=True)
class NamespacePolicyApplyRequest:
    rules: tuple[NamespacePolicyRule, ...] | None = field(
        default=None,
        metadata={PAYLOAD_LIST: True},
    )
    enabled: bool | None = None
    default_allow: bool | None = None
    actor: str | None = None

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> NamespacePolicyApplyRequest:
        data = payload or {}
        rules: tuple[NamespacePolicyRule, ...] | None = None
        if "rules" in data:
            raw_rules = data.get("rules", [])
            parsed_rules: list[NamespacePolicyRule] = []
            if isinstance(raw_rules, dict):
                raw_rules = [raw_rules]
            for item in raw_rules or []:
                if isinstance(item, NamespacePolicyRule):
                    parsed_rules.append(item)
                elif isinstance(item, dict):
                    parsed_rules.append(NamespacePolicyRule.from_dict(item))
            rules = tuple(parsed_rules)
        enabled = data.get("enabled")
        default_allow = data.get("default_allow")
        actor = data.get("actor")
        return cls(
            rules=rules,
            enabled=bool(enabled) if enabled is not None else None,
            default_allow=bool(default_allow) if default_allow is not None else None,
            actor=str(actor) if actor is not None else None,
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class NamespacePolicyAuditRequest:
    limit: int | None = field(default=None, metadata={PAYLOAD_INT: True})

    @classmethod
    def from_dict(cls, payload: PayloadMapping | None) -> NamespacePolicyAuditRequest:
        data = payload or {}
        return cls(
            limit=int(data.get("limit")) if data.get("limit") is not None else None
        )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)


@dataclass(frozen=True, slots=True)
class NamespacePolicyAuditEntry:
    event: str
    timestamp: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    actor: str | None
    valid: bool
    rule_count: int = field(metadata={PAYLOAD_INT: True})
    errors: tuple[str, ...] = field(default=(), metadata={PAYLOAD_LIST: True})

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> NamespacePolicyAuditEntry:
        return cls(
            event=str(payload.get("event", "")),
            timestamp=float(payload.get("timestamp", 0.0) or 0.0),
            actor=(
                str(payload.get("actor")) if payload.get("actor") is not None else None
            ),
            valid=bool(payload.get("valid", False)),
            rule_count=int(payload.get("rule_count", 0) or 0),
            errors=tuple(payload.get("errors", []) or []),
        )


@dataclass(frozen=True, slots=True)
class NamespacePolicyAuditResponse:
    generated_at: Timestamp = field(metadata={PAYLOAD_FLOAT: True})
    entries: tuple[NamespacePolicyAuditEntry, ...] = field(
        metadata={PAYLOAD_LIST: True, PAYLOAD_KEEP_EMPTY: True}
    )

    def to_dict(self) -> Payload:
        return payload_from_dataclass(self)

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> NamespacePolicyAuditResponse:
        entries_payload = payload.get("entries", []) or []
        entries = tuple(
            NamespacePolicyAuditEntry.from_dict(entry)
            for entry in entries_payload
            if isinstance(entry, dict)
        )
        return cls(
            generated_at=float(payload.get("generated_at", 0.0) or 0.0),
            entries=entries,
        )


@dataclass(slots=True)
class NamespacePolicyAuditLog:
    max_entries: int = 500
    _entries: deque[NamespacePolicyAuditEntry] = field(default_factory=deque)
    _lock: RLock = field(default_factory=RLock)

    def record(self, entry: NamespacePolicyAuditEntry) -> None:
        with self._lock:
            self._entries.append(entry)
            while len(self._entries) > self.max_entries:
                self._entries.popleft()

    def snapshot(
        self, *, limit: int | None = None
    ) -> tuple[NamespacePolicyAuditEntry, ...]:
        with self._lock:
            entries = list(self._entries)
        if limit is not None and limit >= 0:
            entries = entries[-limit:]
        return tuple(entries)


def validate_namespace_policy_rules(
    rules: tuple[NamespacePolicyRule, ...],
) -> tuple[str, ...]:
    errors: list[str] = []
    seen: set[str] = set()
    for idx, rule in enumerate(rules):
        namespace = rule.namespace.strip()
        if not namespace:
            errors.append(f"rules[{idx}].namespace is empty")
            continue
        if namespace in seen:
            errors.append(f"rules[{idx}].namespace duplicate: {namespace}")
        seen.add(namespace)
        for widx, window in enumerate(rule.cutover_windows):
            if window.starts_at <= 0:
                errors.append(f"rules[{idx}].cutover_windows[{widx}].starts_at invalid")
            if window.ends_at is not None and window.ends_at <= window.starts_at:
                errors.append(
                    f"rules[{idx}].cutover_windows[{widx}].ends_at before starts_at"
                )
    return tuple(errors)
