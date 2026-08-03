"""Minimal OpenAPI document for monitoring and management HTTP surfaces.

This is the canonical machine-readable contract for ops tools and future UI.
Handlers remain the source of truth for behavior; this document lists paths
and methods so clients share one invalidated interface.
"""

from __future__ import annotations

from typing import Any

def build_monitoring_openapi() -> dict[str, Any]:
    """Return an OpenAPI 3.0 document for the monitoring HTTP server."""
    paths: dict[str, Any] = {
        "/live": {
            "get": {
                "summary": "Process liveness (always 200 if HTTP is up)",
                "tags": ["health"],
            }
        },
        "/ready": {
            "get": {
                "summary": "Readiness / traffic admission (503 when not ready)",
                "tags": ["health"],
            }
        },
        "/health": {
            "get": {
                "summary": "Federation health snapshot (liveness-shaped HTTP 200)",
                "tags": ["health"],
            }
        },
        "/health/summary": {"get": {"summary": "Health summary", "tags": ["health"]}},
        "/metrics/prometheus": {
            "get": {
                "summary": "Prometheus text exposition",
                "tags": ["metrics"],
                "security": [{"bearerAuth": []}],
            }
        },
        "/metrics/unified": {"get": {"summary": "Unified JSON metrics", "tags": ["metrics"]}},
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
        "/routing/trace": {"get": {"summary": "Route selection trace", "tags": ["routing"]}},
        "/mgmt/v1/cluster": {"get": {"summary": "Cluster summary", "tags": ["mgmt"]}},
        "/mgmt/v1/nodes": {"get": {"summary": "Nodes", "tags": ["mgmt"]}},
        "/mgmt/v1/routes": {"get": {"summary": "Routes", "tags": ["mgmt"]}},
        "/mgmt/v1/catalog": {"get": {"summary": "Catalog counts", "tags": ["mgmt"]}},
        "/mgmt/v1/health": {"get": {"summary": "Mgmt health", "tags": ["mgmt"]}},
        "/mgmt/v1/raft": {"get": {"summary": "Raft consensus status", "tags": ["mgmt"]}},
        "/mgmt/v1/policy/dry-run": {
            "post": {"summary": "Policy dry-run", "tags": ["mgmt"]}
        },
        "/mgmt/v1/nodes/drain": {
            "post": {
                "summary": "Drain node (reserved)",
                "tags": ["mgmt"],
                "responses": {"501": {"description": "Not implemented"}},
            }
        },
        "/mgmt/v1/peers/detach": {
            "post": {
                "summary": "Detach peer (reserved)",
                "tags": ["mgmt"],
                "responses": {"501": {"description": "Not implemented"}},
            }
        },
        "/mgmt/v1/policy/apply": {
            "post": {
                "summary": "Apply policy (reserved)",
                "tags": ["mgmt"],
                "responses": {"501": {"description": "Not implemented"}},
            }
        },
        "/mgmt/v1/audit": {"get": {"summary": "Admin audit (read-path)", "tags": ["mgmt"]}},
        "/mgmt/v1/schema": {"get": {"summary": "This OpenAPI document", "tags": ["mgmt"]}},
        "/openapi.json": {"get": {"summary": "This OpenAPI document", "tags": ["meta"]}},
        "/endpoints": {"get": {"summary": "Endpoint directory", "tags": ["meta"]}},
        "/discovery/cache": {"get": {"summary": "Discovery cache stats", "tags": ["discovery"]}},
        "/discovery/policy": {"get": {"summary": "Discovery policy", "tags": ["discovery"]}},
    }
    return {
        "openapi": "3.0.3",
        "info": {
            "title": "MPREG Monitoring and Management API",
            "version": "1.0.0",
            "description": (
                "HTTP surface for health, metrics, routing diagnostics, and "
                "management read models. Bearer auth when monitoring_auth_token is set."
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
