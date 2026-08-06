"""Monitoring metric payload builders extracted from MPREGServer."""

from __future__ import annotations

from typing import Any

def build_persistence_snapshot_metrics(server: Any) -> dict[str, Any]:
    from mpreg.core.persistence.config import PersistenceMode

    config = server.settings.persistence_config
    if config is None:
        return {"enabled": False}

    payload: dict[str, Any] = {
        "enabled": True,
        "mode": config.mode.value,
        "data_dir": str(config.data_dir),
    }
    if config.mode is PersistenceMode.SQLITE:
        payload["sqlite_path"] = str(config.sqlite_path())

    registry = getattr(server, "_persistence_registry", None)
    if registry is not None:
        payload["registry_open"] = getattr(registry, "_opened", False)

    catalog_counts: dict[str, int] = {}
    route_key_info: dict[str, Any] = {}
    control = getattr(server, "_fabric_control_plane", None)
    if control is not None:
        catalog = control.catalog
        catalog_counts = {
            "functions": catalog.functions.entry_count(),
            "topics": catalog.topics.entry_count(),
            "queues": catalog.queues.entry_count(),
            "services": catalog.services.entry_count(),
            "caches": catalog.caches.entry_count(),
            "cache_profiles": catalog.cache_profiles.entry_count(),
            "nodes": catalog.nodes.entry_count(),
        }
        if control.route_key_registry is not None:
            rkr = control.route_key_registry
            rkr.purge_expired()
            route_key_info = {
                "clusters": len(rkr.key_sets),
                "active_keys": sum(len(ks.keys) for ks in rkr.key_sets.values()),
            }

    payload["fabric"] = {
        "catalog_entries": catalog_counts,
        "route_keys": route_key_info,
        "snapshot_last_saved_at": getattr(
            server, "_fabric_snapshot_last_saved_at", None
        ),
        "snapshot_last_restored_at": getattr(
            server, "_fabric_snapshot_last_restored_at", None
        ),
        "snapshot_saved_counts": getattr(
            server, "_fabric_snapshot_last_saved_counts", None
        ),
        "snapshot_restored_counts": getattr(
            server, "_fabric_snapshot_last_restored_counts", None
        ),
        "snapshot_saved_route_keys": getattr(
            server, "_fabric_snapshot_last_route_keys_saved", None
        ),
        "snapshot_restored_route_keys": getattr(
            server, "_fabric_snapshot_last_route_keys_restored", None
        ),
    }
    return payload

def build_dns_metrics(server: Any) -> dict[str, Any]:
    if not server.settings.dns_gateway_enabled:
        return {"enabled": False}
    gateway = getattr(server, "_dns_gateway", None)
    if gateway is None:
        return {"enabled": True, "status": "starting"}
    return {
        "enabled": True,
        "status": "running",
        "udp_port": gateway.bound_udp_port,
        "tcp_port": gateway.bound_tcp_port,
        "zones": list(server.settings.dns_zones or ()),
        "metrics": gateway.metrics_snapshot(),
    }
