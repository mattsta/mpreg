"""Discovery monitoring metric builders extracted from MPREGServer."""

from __future__ import annotations

import time
from typing import Any

def build_discovery_summary_metrics(server: Any) -> dict[str, Any]:
    timestamp = time.time()
    snapshot = server._summary_export_state.snapshot(
        enabled=server.settings.discovery_summary_export_enabled,
        interval_seconds=server.settings.discovery_summary_export_interval_seconds,
        export_scope=server.settings.discovery_summary_export_scope,
        hold_down_seconds=float(
            server.settings.discovery_summary_export_hold_down_seconds
        ),
        store_forward_seconds=float(
            server.settings.discovery_summary_export_store_forward_seconds
        ),
        store_forward_max_messages=int(
            server.settings.discovery_summary_export_store_forward_max_messages
        ),
        configured_namespaces=tuple(
            server.settings.discovery_summary_export_namespaces
        ),
        generated_at=timestamp,
    )
    return snapshot.to_dict()

def build_discovery_cache_metrics(server: Any) -> dict[str, Any]:
    from mpreg.core.discovery_resolver import (
        CatalogEntryCounts,
        DiscoveryResolverCacheStatsResponse,
    )
    from mpreg.core.discovery_summary_resolver import (
        DiscoverySummaryCacheStatsResponse,
        SummaryCacheEntryCounts,
    )

    timestamp = time.time()
    namespaces = tuple(server.settings.discovery_resolver_namespaces)
    resolver = (
        server._discovery_resolver if server._discovery_resolver_enabled() else None
    )
    if resolver is None:
        resolver_response = DiscoveryResolverCacheStatsResponse(
            enabled=False,
            generated_at=timestamp,
            namespaces=namespaces,
            entry_counts=CatalogEntryCounts(),
            stats=None,
            query_cache=None,
        )
    else:
        counts = resolver.entry_counts()
        stats = resolver.stats_snapshot()
        query_cache = resolver.query_cache_snapshot()
        resolver_response = DiscoveryResolverCacheStatsResponse(
            enabled=True,
            generated_at=timestamp,
            namespaces=namespaces,
            entry_counts=counts,
            stats=stats,
            query_cache=query_cache,
        )

    summary_namespaces = tuple(server.settings.discovery_summary_resolver_namespaces)
    summary_resolver = (
        server._discovery_summary_resolver
        if server._discovery_summary_resolver_enabled()
        else None
    )
    if summary_resolver is None:
        summary_response = DiscoverySummaryCacheStatsResponse(
            enabled=False,
            generated_at=timestamp,
            namespaces=summary_namespaces,
            entry_counts=SummaryCacheEntryCounts(),
            stats=None,
        )
    else:
        summary_counts = summary_resolver.entry_counts()
        summary_stats = summary_resolver.stats_snapshot()
        summary_response = DiscoverySummaryCacheStatsResponse(
            enabled=True,
            generated_at=timestamp,
            namespaces=summary_namespaces,
            entry_counts=summary_counts,
            stats=summary_stats,
        )

    return {
        "resolver_cache": resolver_response.to_dict(),
        "summary_cache": summary_response.to_dict(),
    }

def build_discovery_policy_metrics(server: Any) -> dict[str, Any]:
    from mpreg.core.discovery_monitoring import DiscoveryPolicyStatus

    timestamp = time.time()
    engine = server._namespace_policy_engine
    audit_log = server._namespace_policy_audit_log
    access_log = server._discovery_access_audit_log
    rules = engine.rules if engine else ()
    entries = audit_log.snapshot() if audit_log else ()
    recent_entries = entries[-20:] if entries else ()
    if access_log:
        access_entries_all = access_log.snapshot()
        access_total = len(access_entries_all)
        access_entries = access_entries_all[-20:]
    else:
        access_entries = ()
        access_total = 0
    status = DiscoveryPolicyStatus(
        enabled=bool(engine and engine.enabled),
        generated_at=timestamp,
        default_allow=engine.default_allow if engine else True,
        rule_count=len(rules),
        rules=rules,
        audit_entries=recent_entries,
        audit_total=len(entries),
        access_entries=access_entries,
        access_total=access_total,
    )
    return status.to_dict()

def build_discovery_lag_metrics(server: Any) -> dict[str, Any]:
    from mpreg.core.discovery_monitoring import DiscoveryLagStatus

    timestamp = time.time()
    resolver = (
        server._discovery_resolver if server._discovery_resolver_enabled() else None
    )
    last_delta_at = resolver.stats.last_delta_at if resolver else None
    delta_lag_seconds = (
        timestamp - last_delta_at if last_delta_at is not None else None
    )
    last_seed_at = resolver.stats.last_seed_at if resolver else None
    last_summary_export_at = server._summary_export_state.last_export_at
    summary_export_lag_seconds = (
        timestamp - last_summary_export_at
        if last_summary_export_at is not None
        else None
    )
    status = DiscoveryLagStatus(
        generated_at=timestamp,
        resolver_enabled=bool(resolver),
        last_delta_at=last_delta_at,
        delta_lag_seconds=delta_lag_seconds,
        last_seed_at=last_seed_at,
        summary_export_enabled=server.settings.discovery_summary_export_enabled,
        last_summary_export_at=last_summary_export_at,
        summary_export_lag_seconds=summary_export_lag_seconds,
    )
    return status.to_dict()
