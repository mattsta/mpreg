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

def build_strong_metrics(server: Any) -> dict[str, Any]:
    """Process-local STRONG put metrics for operators (not WAN SLA)."""
    enabled_flag = bool(getattr(server.settings, "cache_strong_enabled", False))
    cm = getattr(server, "_cache_manager", None)
    be = getattr(server, "_strong_local_backend", None)
    base: dict[str, Any] = {
        "enabled_flag": enabled_flag,
        "coordinator_bound": False,
        "backend_present": be is not None,
        "pending_count": 0,
        "purge_task": getattr(server, "_strong_pending_purge_task", None) is not None,
        "settings": {
            "replica_factor": getattr(
                server.settings, "cache_strong_replica_factor", None
            ),
            "min_replicas": getattr(server.settings, "cache_strong_min_replicas", None),
            "prepare_timeout_s": getattr(
                server.settings, "cache_strong_prepare_timeout_s", None
            ),
            "commit_timeout_s": getattr(
                server.settings, "cache_strong_commit_timeout_s", None
            ),
            "pending_ttl_s": getattr(
                server.settings, "cache_strong_pending_ttl_s", None
            ),
        },
    }
    if cm is not None and hasattr(cm, "strong_metrics_snapshot"):
        snap = cm.strong_metrics_snapshot()
        base["coordinator_bound"] = bool(snap.get("enabled"))
        base["pending_count"] = int(snap.get("pending_count") or 0)
        base["counters"] = dict(snap.get("counters") or {})
        base["latency_ms"] = dict(snap.get("latency_ms") or {})
        base["coordinator"] = dict(snap.get("coordinator") or {})
        if hasattr(cm, "strong_status"):
            try:
                st = cm.strong_status()
                if isinstance(st, dict) and "capabilities" in st:
                    base["capabilities"] = dict(st["capabilities"] or {})
            except Exception:  # noqa: BLE001
                pass
    elif be is not None and hasattr(be, "pending_count"):
        try:
            base["pending_count"] = int(be.pending_count())
        except Exception:  # noqa: BLE001
            pass
        base["counters"] = {}
        base["latency_ms"] = {}
    else:
        base["counters"] = {}
        base["latency_ms"] = {}
    # Honest capability flags (v1 put-only MVP) when GCM status unavailable
    if "capabilities" not in base:
        bound = bool(base.get("coordinator_bound"))
        base["capabilities"] = {
            "put_majority_commit": bound,
            "get_quorum": False,  # v1.1
            "delete_quorum": False,  # v1.1
            "local_ryw_after_put": True,
        }
    # Simple health hint for doctor
    if not enabled_flag:
        base["health"] = "disabled"
    elif not base["coordinator_bound"]:
        base["health"] = "misconfigured"
    elif int(base["pending_count"]) > 64:
        base["health"] = "degraded_pending"
    else:
        base["health"] = "ok"
    return base

def build_shared_audit_metrics(server: Any) -> dict[str, Any]:
    """Shared-audit epidemic metrics + health for operators."""
    from mpreg.server_pkg.shared_audit.metrics import get_shared_audit_metrics

    enabled = bool(getattr(server.settings, "mgmt_audit_shared_enabled", False))
    store = getattr(server, "_shared_audit_store", None)
    rep = getattr(server, "_shared_audit_replicator", None)
    counters = get_shared_audit_metrics().snapshot()
    health_dict: dict[str, Any] | None = None
    store_size = 0
    if store is not None and hasattr(store, "size"):
        try:
            store_size = int(store.size())
        except Exception:  # noqa: BLE001
            store_size = 0
    if rep is not None and hasattr(rep, "health"):
        try:
            h = rep.health()
            health_dict = h.to_dict() if hasattr(h, "to_dict") else dict(h)
        except Exception:  # noqa: BLE001
            health_dict = None
    peers = 0
    if health_dict and "peers_known" in health_dict:
        peers = int(health_dict["peers_known"] or 0)
    status = "disabled"
    if enabled and store is not None:
        drops = int(counters.get("publish_dropped", 0) or 0)
        if drops > 0:
            status = "degraded_drops"
        elif peers < 1 and enabled:
            status = "ok_no_peers"  # single-node or not yet meshed
        else:
            status = "ok"
    elif enabled and store is None:
        status = "misconfigured"
    # Honest capability flags (v1 G-Set epidemic — not SIEM/BFT/infinite retention)
    capabilities = {
        "gset_epidemic": bool(enabled and store is not None),
        "siem": False,
        "bft": False,
        "infinite_retention": False,
        "linearizable_cluster_ops": False,
        "multi_tenant_beyond_cluster_id": False,
    }
    return {
        "enabled_flag": enabled,
        "store_present": store is not None,
        "replicator_present": rep is not None,
        "store_size": store_size,
        "counters": counters,
        "health": health_dict,
        "status": status,
        "capabilities": capabilities,
        "settings": {
            "reconcile_interval_s": getattr(
                server.settings, "mgmt_audit_shared_reconcile_interval_s", None
            ),
            "gossip_targets": getattr(
                server.settings, "mgmt_audit_shared_gossip_targets", None
            ),
        },
    }

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
