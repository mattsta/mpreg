"""Build normalized /mgmt/v1 read models from a live MPREGServer."""

from __future__ import annotations

import time
from typing import Any

def build_mgmt_v1_summary(server: Any) -> dict[str, Any]:
    """Return cluster/nodes/routes/catalog/health summaries for management APIs."""
    settings = server.settings
    cluster_id = str(settings.cluster_id)
    node_name = str(settings.name)
    now = time.time()

    nodes: list[dict[str, Any]] = [
        {
            "node_id": getattr(server, "url", None)
            or f"{settings.host}:{settings.port}",
            "name": node_name,
            "cluster_id": cluster_id,
            "status": "local",
            "resources": sorted(settings.resources or []),
            "local": True,
        }
    ]

    # Peer directory / connections when available
    peers = []
    cluster = getattr(server, "cluster", None)
    if cluster is not None:
        peer_connections = getattr(cluster, "peer_connections", {}) or {}
        for url in peer_connections:
            peers.append(str(url))
            nodes.append(
                {
                    "node_id": str(url),
                    "name": str(url),
                    "cluster_id": cluster_id,
                    "status": "connected",
                    "resources": [],
                    "local": False,
                }
            )

    routes: list[dict[str, Any]] = []
    control = getattr(server, "_fabric_control_plane", None) or getattr(
        server, "fabric_control_plane", None
    )
    catalog_summary = {
        "functions": 0,
        "queues": 0,
        "topics": 0,
        "caches": 0,
        "nodes": len(nodes),
    }
    if control is not None:
        catalog = getattr(control, "catalog", None)
        if catalog is not None:
            functions = getattr(catalog, "functions", None)
            queues = getattr(catalog, "queues", None)
            topics = getattr(catalog, "topics", None)
            caches = getattr(catalog, "caches", None) or getattr(
                catalog, "cache_profiles", None
            )
            try:
                catalog_summary["functions"] = (
                    len(list(functions.entries())) if functions else 0
                )
            except Exception:
                catalog_summary["functions"] = 0
            try:
                catalog_summary["queues"] = len(list(queues.entries())) if queues else 0
            except Exception:
                catalog_summary["queues"] = 0
            try:
                catalog_summary["topics"] = len(list(topics.entries())) if topics else 0
            except Exception:
                catalog_summary["topics"] = 0
            try:
                if caches is not None and hasattr(caches, "entries"):
                    catalog_summary["caches"] = len(list(caches.entries()))
            except Exception:
                catalog_summary["caches"] = 0

        route_table = getattr(control, "route_table", None) or getattr(
            control, "routes", None
        )
        entries = []
        if route_table is not None:
            raw = getattr(route_table, "entries", None)
            if isinstance(raw, dict):
                entries = list(raw.values())
            elif raw is not None and hasattr(raw, "values"):
                try:
                    entries = list(raw.values())
                except Exception:
                    entries = []
        for entry in entries[:200]:
            routes.append(
                {
                    "destination": str(
                        getattr(entry, "destination", None)
                        or getattr(entry, "destination_cluster", None)
                        or getattr(entry, "prefix", "")
                    ),
                    "next_hop": str(
                        getattr(entry, "next_hop", None)
                        or getattr(entry, "next_hop_cluster", None)
                        or ""
                    ),
                    "hop_count": getattr(entry, "hop_count", None)
                    or getattr(entry, "as_path_len", None),
                    "source": "path_vector",
                }
            )

    health = {
        "status": "ok",
        "peer_connections": len(peers),
        "catalog": catalog_summary,
        "checked_at": now,
    }

    return {
        "cluster": {
            "cluster_id": cluster_id,
            "node_count": len(nodes),
            "local_node": node_name,
            "gossip_interval": getattr(settings, "gossip_interval", None),
            "fabric_routing_enabled": getattr(settings, "fabric_routing_enabled", True),
            "last_update_at": now,
        },
        "nodes": nodes,
        "routes": routes,
        "catalog": catalog_summary,
        "health": health,
    }
