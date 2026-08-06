"""Minimal OpenAPI document for monitoring and management HTTP surfaces.

This is the canonical machine-readable contract for ops tools and future UI.
Handlers remain the source of truth for behavior; this document lists paths
and methods so clients share one invalidated interface.
"""

from __future__ import annotations

from typing import Any

def build_monitoring_openapi() -> dict[str, Any]:
    """Return an OpenAPI 3.0 document for the monitoring HTTP server."""
    # When monitoring_auth_token is configured, mutations and metrics require bearer.
    _bearer = [{"bearerAuth": []}]
    paths: dict[str, Any] = {
        "/health/clusters/{cluster_id}": {
            "get": {"summary": "Per-cluster health", "tags": ["health"]}
        },
        "/performance/bottlenecks": {
            "get": {"summary": "Performance bottlenecks", "tags": ["performance"]}
        },
        "/performance/clusters/{cluster_id}": {
            "get": {"summary": "Per-cluster performance", "tags": ["performance"]}
        },
        "/live": {
            "get": {
                "summary": "Process liveness (always 200 if HTTP is up)",
                "description": "Liveness probe responses — process up only.",
                "tags": ["health"],
                "responses": {"200": {"description": "Process alive"}},
            }
        },
        "/ready": {
            "get": {
                "summary": "Readiness / traffic admission (503 when draining or unhealthy)",
                "description": (
                    "HTTP 200 means the node admits traffic under current health score "
                    "and is not draining. HTTP 503 when draining or below ready threshold. "
                    "Not a full-health certificate (OBS-07)."
                ),
                "tags": ["health"],
                "responses": {
                    "200": {
                        "description": "Ready to admit traffic",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "ready": {"type": "boolean"},
                                        "draining": {"type": "boolean"},
                                        "health_score": {"type": "number"},
                                    },
                                }
                            }
                        },
                    },
                    "503": {"description": "Not ready (draining or degraded)"},
                },
            }
        },
        "/health": {
            "get": {
                "summary": "Federation health snapshot (liveness-shaped HTTP 200)",
                "tags": ["health"],
            }
        },
        "/health/summary": {"get": {"summary": "Health summary", "tags": ["health"]}},
        "/health/clusters": {
            "get": {"summary": "Per-cluster health", "tags": ["health"]}
        },
        "/metrics": {"get": {"summary": "JSON metrics root", "tags": ["metrics"]}},
        "/metrics/rpc": {"get": {"summary": "RPC metrics", "tags": ["metrics"]}},
        "/metrics/pubsub": {"get": {"summary": "Pubsub metrics", "tags": ["metrics"]}},
        "/metrics/queue": {"get": {"summary": "Queue metrics", "tags": ["metrics"]}},
        "/metrics/cache": {"get": {"summary": "Cache metrics", "tags": ["metrics"]}},
        "/metrics/transport": {
            "get": {"summary": "Transport metrics", "tags": ["metrics"]}
        },
        "/metrics/persistence": {
            "get": {"summary": "Persistence metrics", "tags": ["metrics"]}
        },
        "/metrics/strong": {
            "get": {
                "summary": "STRONG majority-commit put metrics (process-local)",
                "description": "Not a WAN SLA. Lab/process latency ring + put counters.",
                "tags": ["metrics"],
            }
        },
        "/metrics/shared-audit": {
            "get": {
                "summary": "Shared audit G-Set epidemic metrics",
                "tags": ["metrics"],
            }
        },
        "/mgmt/v1/strong": {
            "get": {
                "summary": "STRONG cache put readiness snapshot",
                "tags": ["mgmt"],
            }
        },
        "/discovery/summary": {
            "get": {"summary": "Discovery summary export", "tags": ["discovery"]}
        },
        "/discovery/lag": {"get": {"summary": "Discovery lag", "tags": ["discovery"]}},
        "/dns/metrics": {"get": {"summary": "DNS gateway metrics", "tags": ["dns"]}},
        "/topology": {"get": {"summary": "Federation topology", "tags": ["topology"]}},
        "/topology/graph": {"get": {"summary": "Topology graph", "tags": ["topology"]}},
        "/topology/paths": {"get": {"summary": "Topology paths", "tags": ["topology"]}},
        "/routing/link-state": {
            "get": {"summary": "Link-state snapshot", "tags": ["routing"]}
        },
        "/alerts": {"get": {"summary": "Active alerts", "tags": ["alerts"]}},
        "/config": {"get": {"summary": "Runtime config snapshot", "tags": ["config"]}},
        "/metrics/prometheus": {
            "get": {
                "summary": "Prometheus text exposition",
                "tags": ["metrics"],
                "security": _bearer,
            }
        },
        "/metrics/unified": {
            "get": {"summary": "Unified JSON metrics", "tags": ["metrics"]}
        },
        "/routing/decisions": {
            "get": {
                "summary": "Recent fabric route decisions",
                "tags": ["routing"],
                "parameters": [
                    {"name": "limit", "in": "query", "schema": {"type": "integer"}},
                    {"name": "message_id", "in": "query", "schema": {"type": "string"}},
                    {
                        "name": "correlation_id",
                        "in": "query",
                        "schema": {"type": "string"},
                    },
                    {
                        "name": "traceparent",
                        "in": "query",
                        "schema": {"type": "string"},
                        "description": "Filter by W3C traceparent",
                    },
                ],
            }
        },
        "/routing/trace": {
            "get": {"summary": "Route selection trace", "tags": ["routing"]}
        },
        "/mgmt/v1/cluster": {"get": {"summary": "Cluster summary", "tags": ["mgmt"]}},
        "/mgmt/v1/nodes": {"get": {"summary": "Nodes", "tags": ["mgmt"]}},
        "/mgmt/v1/routes": {"get": {"summary": "Routes", "tags": ["mgmt"]}},
        "/mgmt/v1/catalog": {"get": {"summary": "Catalog counts", "tags": ["mgmt"]}},
        "/mgmt/v1/health": {"get": {"summary": "Mgmt health", "tags": ["mgmt"]}},
        "/mgmt/v1/raft": {
            "get": {"summary": "Raft consensus status", "tags": ["mgmt"]}
        },
        "/mgmt/v1/policy/dry-run": {
            "post": {
                "summary": "Policy dry-run",
                "tags": ["mgmt"],
                "security": _bearer,
            }
        },
        "/mgmt/v1/nodes/drain": {
            "post": {
                "summary": "Enter or clear node drain (affects /ready)",
                "tags": ["mgmt"],
                "security": _bearer,
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "draining": {"type": "boolean"},
                                    "actor": {"type": "string"},
                                    "reason": {"type": "string"},
                                },
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Drain state applied"},
                    "400": {"description": "Invalid body"},
                    "401": {"description": "Auth required when token configured"},
                    "503": {"description": "Provider unbound"},
                },
            }
        },
        "/mgmt/v1/peers/detach": {
            "post": {
                "summary": "Detach a peer connection from this node",
                "tags": ["mgmt"],
                "security": _bearer,
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["peer_url"],
                                "properties": {
                                    "peer_url": {"type": "string"},
                                    "actor": {"type": "string"},
                                    "reason": {"type": "string"},
                                },
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Peer detached"},
                    "400": {"description": "Invalid request"},
                    "401": {"description": "Auth required when token configured"},
                    "503": {"description": "Provider unbound"},
                },
            }
        },
        "/mgmt/v1/policy/apply": {
            "post": {
                "summary": "Apply namespace policy rules",
                "tags": ["mgmt"],
                "security": _bearer,
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "rules": {"type": "array"},
                                    "enabled": {"type": "boolean"},
                                    "default_allow": {"type": "boolean"},
                                    "actor": {"type": "string"},
                                },
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Policy applied or validation result"},
                    "400": {"description": "Validation failed"},
                    "401": {"description": "Auth required when token configured"},
                    "503": {"description": "Provider unbound"},
                },
            }
        },
        "/mgmt/v1/audit": {
            "get": {
                "summary": "Admin mutation audit trail",
                "tags": ["mgmt"],
                "security": _bearer,
                "parameters": [
                    {"name": "limit", "in": "query", "schema": {"type": "integer"}}
                ],
            }
        },
        "/mgmt/v1/schema": {
            "get": {"summary": "This OpenAPI document", "tags": ["mgmt"]}
        },
        "/": {"get": {"summary": "Monitoring landing", "tags": ["meta"]}},
        "/alerts/history": {"get": {"summary": "Alert history", "tags": ["alerts"]}},
        "/alerts/acknowledge": {
            "post": {
                "summary": "Acknowledge alert",
                "tags": ["alerts"],
                "security": [{"MonitoringBearer": []}],
            }
        },
        "/config/policies": {"get": {"summary": "Config policies", "tags": ["config"]}},
        "/config/validation": {
            "get": {"summary": "Config validation report", "tags": ["config"]}
        },
        "/metrics/connections": {
            "get": {"summary": "Connection metrics", "tags": ["metrics"]}
        },
        "/metrics/performance": {
            "get": {"summary": "Performance metrics", "tags": ["metrics"]}
        },
        "/metrics/timeseries": {
            "get": {"summary": "Timeseries metrics", "tags": ["metrics"]}
        },
        "/performance": {"get": {"summary": "Performance overview", "tags": ["ops"]}},
        "/performance/trends": {
            "get": {"summary": "Performance trends", "tags": ["ops"]}
        },
        "/topology/analysis": {
            "get": {"summary": "Topology analysis", "tags": ["topology"]}
        },
        "/transport/endpoints": {
            "get": {"summary": "Transport endpoints", "tags": ["transport"]}
        },
        "/openapi.json": {
            "get": {"summary": "This OpenAPI document", "tags": ["meta"]}
        },
        "/endpoints": {"get": {"summary": "Endpoint directory", "tags": ["meta"]}},
        "/discovery/cache": {
            "get": {"summary": "Discovery cache stats", "tags": ["discovery"]}
        },
        "/discovery/policy": {
            "get": {"summary": "Discovery policy", "tags": ["discovery"]}
        },
    }
    return {
        "openapi": "3.0.3",
        "info": {
            "title": "MPREG Monitoring and Management API",
            "version": "1.0.0",
            "description": (
                "HTTP surface for health, metrics, routing diagnostics, and "
                "management read/write models (drain, detach, policy apply). "
                "Bearer auth applies to mutations and metrics when "
                "monitoring_auth_token is set."
            ),
        },
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                }
            }
        },
        "paths": paths,
    }

def monitoring_route_table() -> list[tuple[str, str]]:
    """ERG-T14-01: canonical (method, path) pairs matching FederationMonitoringSystem.

    Kept next to the OpenAPI document so drift tests have one import surface.
    When adding a route on the aiohttp app, add it here and to build_monitoring_openapi.
    """
    gets = [
        "/",
        "/live",
        "/ready",
        "/health",
        "/health/summary",
        "/health/clusters",
        "/health/clusters/{cluster_id}",
        "/metrics",
        "/metrics/performance",
        "/metrics/connections",
        "/metrics/timeseries",
        "/metrics/unified",
        "/metrics/rpc",
        "/metrics/pubsub",
        "/metrics/queue",
        "/metrics/cache",
        "/metrics/transport",
        "/metrics/persistence",
        "/metrics/strong",
        "/metrics/shared-audit",
        "/metrics/prometheus",
        "/transport/endpoints",
        "/mgmt/v1/cluster",
        "/mgmt/v1/nodes",
        "/mgmt/v1/routes",
        "/mgmt/v1/catalog",
        "/mgmt/v1/health",
        "/mgmt/v1/raft",
        "/mgmt/v1/strong",
        "/mgmt/v1/audit",
        "/mgmt/v1/schema",
        "/discovery/summary",
        "/discovery/cache",
        "/discovery/policy",
        "/discovery/lag",
        "/dns/metrics",
        "/topology",
        "/topology/graph",
        "/topology/paths",
        "/topology/analysis",
        "/performance",
        "/performance/bottlenecks",
        "/performance/trends",
        "/performance/clusters/{cluster_id}",
        "/alerts",
        "/alerts/history",
        "/config",
        "/config/policies",
        "/config/validation",
        "/routing/trace",
        "/routing/decisions",
        "/routing/link-state",
        "/endpoints",
        "/openapi.json",
    ]
    posts = [
        "/alerts/acknowledge",
        "/mgmt/v1/policy/dry-run",
        "/mgmt/v1/nodes/drain",
        "/mgmt/v1/peers/detach",
        "/mgmt/v1/policy/apply",
    ]
    return [("GET", p) for p in gets] + [("POST", p) for p in posts]

def openapi_path_set() -> set[str]:
    """Paths declared in the OpenAPI document."""
    doc = build_monitoring_openapi()
    return set((doc.get("paths") or {}).keys())

def route_table_path_set() -> set[str]:
    return {p for _, p in monitoring_route_table()}
