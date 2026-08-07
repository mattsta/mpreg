"""Minimal OpenAPI document for monitoring and management HTTP surfaces.

This is the canonical machine-readable contract for ops tools and future UI.
Handlers remain the source of truth for behavior; this document lists paths
and methods so clients share one invalidated interface.
"""

from __future__ import annotations

from typing import Any

def _strong_metrics_schema() -> dict[str, Any]:
    """JSON Schema for /metrics/strong and /mgmt/v1/strong payloads (honesty-first)."""
    return {
        "type": "object",
        "description": (
            "Process-local STRONG put metrics. Not a WAN SLA. "
            "v1 is put-only: get_quorum and delete_quorum are always false."
        ),
        "properties": {
            "status": {"type": "string", "example": "ok"},
            "timestamp": {"type": "number"},
            "strong": {
                "type": "object",
                "properties": {
                    "enabled_flag": {"type": "boolean"},
                    "coordinator_bound": {"type": "boolean"},
                    "backend_present": {"type": "boolean"},
                    "pending_count": {"type": "integer", "minimum": 0},
                    "health": {
                        "type": "string",
                        "enum": [
                            "disabled",
                            "misconfigured",
                            "degraded_pending",
                            "ok",
                            "unwired",
                        ],
                    },
                    "counters": {
                        "type": "object",
                        "additionalProperties": {"type": "integer"},
                        "description": (
                            "Includes puts_ok, puts_fail, refused_disabled, "
                            "gets_refused, deletes_refused (1012 refuse paths), "
                            "aborts_peer_ok, aborts_peer_fail (CFT best-effort ABORT)."
                        ),
                    },
                    "latency_ms": {
                        "type": "object",
                        "description": "Process-local put latency ring — not WAN SLO.",
                        "properties": {
                            "sample_count": {"type": "integer"},
                            "p50_ms": {"type": "number"},
                            "p99_ms": {"type": "number"},
                            "max_ms": {"type": "number"},
                            "avg_ms": {"type": "number"},
                        },
                    },
                    "capabilities": {
                        "type": "object",
                        "description": (
                            "Honest v1 product surface. get_quorum/delete_quorum "
                            "must be false (quorum get/delete are v1.1 non-goals). "
                            "cft_only and abort_best_effort are always true."
                        ),
                        "properties": {
                            "put_majority_commit": {"type": "boolean"},
                            "get_quorum": {
                                "type": "boolean",
                                "enum": [False],
                                "description": "Always false in v1 (refuse 1012).",
                            },
                            "delete_quorum": {
                                "type": "boolean",
                                "enum": [False],
                                "description": "Always false in v1 (refuse 1012).",
                            },
                            "local_ryw_after_put": {"type": "boolean"},
                            "cft_only": {
                                "type": "boolean",
                                "enum": [True],
                                "description": "Always true — not BFT.",
                            },
                            "abort_best_effort": {
                                "type": "boolean",
                                "enum": [True],
                                "description": (
                                    "Always true — lost ABORT may leave peer L1 "
                                    "until delivered ABORT or later LWW success "
                                    "put (CFT limit; not pending TTL)."
                                ),
                            },
                            "pending_ttl_clears_residual_l1": {
                                "type": "boolean",
                                "enum": [False],
                                "description": (
                                    "Always false — purge_expired_pending does "
                                    "not uncommit residual L1 after COMMIT apply."
                                ),
                            },
                            "retry_abort_ops_driven": {
                                "type": "boolean",
                                "enum": [True],
                                "description": (
                                    "Always true — strong_retry_abort is "
                                    "ops-driven CFT best-effort, not automatic "
                                    "background heal."
                                ),
                            },
                        },
                        "required": ["get_quorum", "delete_quorum"],
                    },
                    "pending_count": {"type": "integer", "minimum": 0},
                    "visible_count": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Local visible L1 strong entries (may include CFT residuals).",
                    },
                    "backups_count": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Pre-commit backups for live visible/pending ops.",
                    },
                    "last_abort_fail_peers": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Peers that exhausted ABORT retries on the last "
                            "failed put (CFT residual candidates; ops only — "
                            "not residual-free proof, not auto-heal)."
                        ),
                    },
                    "abort_fail_peer_count": {
                        "type": "integer",
                        "minimum": 0,
                        "description": (
                            "len(last_abort_fail_peers) — same value as Prometheus "
                            "mpreg_strong_abort_fail_peers. CFT residual candidate "
                            "count; process-local; not residual-free proof; not auto-heal."
                        ),
                    },
                    "last_abort_fail_op_id": {
                        "type": "string",
                        "description": "op_id associated with last_abort_fail_peers.",
                    },
                    "residual_ops_hint": {
                        "type": "string",
                        "description": (
                            "Operator remediation when abort_fail peers are "
                            "non-empty (points at cache-strong-retry-abort). "
                            "May fill --namespace/--key from recent_abort_fails "
                            "key field when available (process-local). "
                            "Empty string when no residual candidates. "
                            "CFT best-effort guidance — not automatic heal, "
                            "not residual-free proof, not SIEM/BFT."
                        ),
                        # T61: populated example (empty string is the clean-put case)
                        "example": (
                            "hint: after network recovery, ops re-ABORT "
                            "(not auto-heal): uv run mpreg client "
                            "cache-strong-retry-abort --url <ws> "
                            "--op-id op-abc123 --namespace orders "
                            "--key cart-42 --peer ws://127.0.0.1:9001 "
                            "(CFT best-effort; still fails while ABORT dropped)"
                        ),
                    },
                    "recent_abort_fails": {
                        "type": "array",
                        "description": (
                            "Bounded ring of recent abort-fail events "
                            "({op_id, peers, ts, key}); process-local."
                        ),
                        "items": {
                            "type": "object",
                            "properties": {
                                "op_id": {"type": "string"},
                                "peers": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                                "ts": {"type": "number"},
                                "key": {
                                    "type": "string",
                                    "description": "namespace/identifier",
                                    "example": "orders/cart-42",
                                },
                                "retry": {"type": "boolean"},
                            },
                        },
                        "example": [
                            {
                                "op_id": "op-abc123",
                                "peers": ["ws://127.0.0.1:9001"],
                                "ts": 1720000000.0,
                                "key": "orders/cart-42",
                            }
                        ],
                    },
                    "retry_abort_calls": {
                        "type": "integer",
                        "minimum": 0,
                        "description": (
                            "Ops-driven strong_retry_abort invocations "
                            "(not automatic background heal)."
                        ),
                    },
                    "retry_abort_cleared": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "retry_abort runs that cleared all targeted peers.",
                    },
                    "retry_abort_still_fail": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "retry_abort runs that still had fail peers (CFT).",
                    },
                    "backups_pruned_total": {
                        "type": "integer",
                        "minimum": 0,
                        "description": (
                            "Cumulative orphan pre-commit backups dropped "
                            "(not residual L1 clear)."
                        ),
                    },
                    "coordinator": {"type": "object"},
                    "settings": {"type": "object"},
                },
            },
        },
    }

def _shared_audit_metrics_schema() -> dict[str, Any]:
    """JSON Schema for /metrics/shared-audit (not SIEM / not BFT)."""
    return {
        "type": "object",
        "description": (
            "Shared audit G-Set epidemic metrics. Bounded watermark window — "
            "not SIEM, not BFT, not infinite retention. capabilities.siem / "
            "bft / infinite_retention / linearizable_cluster_ops / "
            "multi_tenant_beyond_cluster_id are always false in v1."
        ),
        "properties": {
            "status": {"type": "string", "example": "ok"},
            "timestamp": {"type": "number"},
            "shared_audit": {
                "type": "object",
                "properties": {
                    "enabled_flag": {"type": "boolean"},
                    "store_present": {"type": "boolean"},
                    "replicator_present": {"type": "boolean"},
                    "store_size": {"type": "integer", "minimum": 0},
                    "status": {
                        "type": "string",
                        "enum": [
                            "disabled",
                            "misconfigured",
                            "degraded_drops",
                            "ok_no_peers",
                            "ok",
                            "unwired",
                        ],
                    },
                    "counters": {
                        "type": "object",
                        "additionalProperties": {"type": "integer"},
                        "description": (
                            "Epidemic counters: deltas_sent/recv, digests_sent, "
                            "pulls_*, publish_dropped, rejected_cross_cluster, …"
                        ),
                    },
                    "health": {"type": "object", "nullable": True},
                    "capabilities": {
                        "type": "object",
                        "description": (
                            "Honest v1 capability advertisement. gset_epidemic is "
                            "true when flag on and store present; siem/bft/"
                            "infinite_retention/linearizable_cluster_ops/"
                            "multi_tenant_beyond_cluster_id must be false."
                        ),
                        "properties": {
                            "gset_epidemic": {"type": "boolean"},
                            "siem": {
                                "type": "boolean",
                                "enum": [False],
                                "description": "Always false — not a SIEM.",
                            },
                            "bft": {
                                "type": "boolean",
                                "enum": [False],
                                "description": "Always false — CFT gossip only.",
                            },
                            "infinite_retention": {
                                "type": "boolean",
                                "enum": [False],
                                "description": "Always false — bounded watermark.",
                            },
                            "linearizable_cluster_ops": {
                                "type": "boolean",
                                "enum": [False],
                                "description": (
                                    "Always false — audit visibility is not "
                                    "linearizable cluster mutation."
                                ),
                            },
                            "multi_tenant_beyond_cluster_id": {
                                "type": "boolean",
                                "enum": [False],
                                "description": (
                                    "Always false — isolation is cluster_id reject only."
                                ),
                            },
                        },
                        "required": [
                            "siem",
                            "bft",
                            "infinite_retention",
                            "linearizable_cluster_ops",
                            "multi_tenant_beyond_cluster_id",
                        ],
                    },
                    "settings": {"type": "object"},
                },
                "required": ["capabilities"],
            },
        },
    }

def _platform_cache_rpc_catalog() -> dict[str, Any]:
    """Document platform cache RPC FQNs (wire names, not HTTP paths).

    These are WebSocket/RPC plane commands registered under resource ``cache``.
    Not an HTTP path table — operators discover them via ``mpreg.rpc.list`` /
    client façades. Honesty-first descriptions only.
    """
    from mpreg.core.rpc_naming import PlatformRpc

    return {
        "type": "object",
        "description": (
            "Platform cache RPC command catalog (FQN wire names). "
            "Invoked over the MPREG RPC plane, not HTTP. "
            "STRONG retry_abort is ops-driven CFT best-effort — not automatic "
            "heal, not BFT, not residual-free while ABORT is lost."
        ),
        "properties": {
            "namespace": {
                "type": "string",
                "enum": ["mpreg.cache"],
                "description": "Reserved platform cache namespace.",
            },
            "resource": {
                "type": "string",
                "enum": ["cache"],
                "description": "Registration resource tag for routing.",
            },
            "commands": {
                "type": "object",
                "properties": {
                    "get": {
                        "type": "object",
                        "properties": {
                            "fqn": {
                                "type": "string",
                                "enum": [PlatformRpc.CACHE_GET],
                            },
                            "summary": {
                                "type": "string",
                                "description": "Fetch cache entry (default EVENTUAL).",
                            },
                        },
                        "required": ["fqn"],
                    },
                    "put": {
                        "type": "object",
                        "properties": {
                            "fqn": {
                                "type": "string",
                                "enum": [PlatformRpc.CACHE_PUT],
                            },
                            "summary": {
                                "type": "string",
                                "description": (
                                    "Store entry; optional consistency_level=strong "
                                    "when cache_strong_enabled (put-only MVP)."
                                ),
                            },
                        },
                        "required": ["fqn"],
                    },
                    "invalidate": {
                        "type": "object",
                        "properties": {
                            "fqn": {
                                "type": "string",
                                "enum": [PlatformRpc.CACHE_INVALIDATE],
                            },
                            "summary": {
                                "type": "string",
                                "description": "Invalidate by pattern.",
                            },
                        },
                        "required": ["fqn"],
                    },
                    "strong_retry_abort": {
                        "type": "object",
                        "properties": {
                            "fqn": {
                                "type": "string",
                                "enum": [PlatformRpc.CACHE_STRONG_RETRY_ABORT],
                            },
                            "summary": {
                                "type": "string",
                                "description": (
                                    "Ops-driven CFT re-ABORT for residual "
                                    "candidates (not automatic heal, not BFT)."
                                ),
                            },
                            "body": {
                                "type": "object",
                                "properties": {
                                    "namespace": {"type": "string"},
                                    "identifier": {"type": "string"},
                                    "op_id": {"type": "string"},
                                    "version": {"type": "string"},
                                    "peers": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "description": (
                                            "Optional residual peer ids; default "
                                            "last_abort_fail_peers on handler node."
                                        ),
                                    },
                                },
                                "required": ["namespace", "identifier", "op_id"],
                            },
                            "result_honesty": {
                                "type": "object",
                                "properties": {
                                    "ops_driven": {
                                        "type": "boolean",
                                        "enum": [True],
                                    },
                                    "automatic_heal": {
                                        "type": "boolean",
                                        "enum": [False],
                                    },
                                    "cft_best_effort": {
                                        "type": "boolean",
                                        "enum": [True],
                                    },
                                },
                            },
                        },
                        "required": ["fqn"],
                    },
                },
                "required": ["get", "put", "invalidate", "strong_retry_abort"],
            },
            "client_facade": {
                "type": "string",
                "description": (
                    "MPREGClient.cache_get/put/invalidate/"
                    "cache_strong_retry_abort; CLI: mpreg client cache-*"
                ),
            },
        },
        "required": ["namespace", "commands"],
    }

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
                "description": (
                    "Not a WAN SLA. Process-local put counters + latency ring. "
                    "v1 put-only MVP: STRONG get/delete always refuse 1012; "
                    "capabilities.get_quorum and capabilities.delete_quorum are false. "
                    "Prometheus: mpreg_strong_* including gets_refused/deletes_refused, "
                    "mpreg_strong_abort_fail_peers (CFT residual candidate count; not "
                    "auto-heal), and JSON residual_ops_hint (ops guidance string)."
                ),
                "tags": ["metrics", "strong"],
                "responses": {
                    "200": {
                        "description": "STRONG metrics envelope",
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/StrongMetricsResponse"}
                            }
                        },
                    }
                },
            }
        },
        "/metrics/shared-audit": {
            "get": {
                "summary": "Shared audit G-Set epidemic metrics",
                "description": (
                    "Bounded G-Set epidemic — not SIEM, not BFT, not infinite retention. "
                    "capabilities.siem/bft/infinite_retention/linearizable_cluster_ops/"
                    "multi_tenant_beyond_cluster_id are always false. "
                    "Prometheus: mpreg_shared_audit_*."
                ),
                "tags": ["metrics", "audit"],
                "responses": {
                    "200": {
                        "description": "Shared audit metrics envelope",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": "#/components/schemas/SharedAuditMetricsResponse"
                                }
                            }
                        },
                    }
                },
            }
        },
        "/mgmt/v1/strong": {
            "get": {
                "summary": "STRONG cache put readiness snapshot",
                "description": (
                    "Same payload shape as /metrics/strong (mgmt alias). "
                    "Not WAN SLA; get/delete quorum are v1.1 non-goals."
                ),
                "tags": ["mgmt", "strong"],
                "responses": {
                    "200": {
                        "description": "STRONG readiness / metrics",
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/StrongMetricsResponse"}
                            }
                        },
                    }
                },
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
            },
            "schemas": {
                "StrongMetricsResponse": _strong_metrics_schema(),
                "SharedAuditMetricsResponse": _shared_audit_metrics_schema(),
                "PlatformCacheRpcCatalog": _platform_cache_rpc_catalog(),
            },
            "x-mpreg-platform-rpc": {
                "cache": {
                    "$ref": "#/components/schemas/PlatformCacheRpcCatalog"
                },
            },
        },
        "paths": paths,
        "tags": [
            {
                "name": "strong",
                "description": (
                    "ConsistencyLevel.STRONG put majority-commit (flag-gated). "
                    "Not WAN SLA, not BFT, not fsync. Get/delete quorum v1.1. "
                    "Ops re-ABORT: mpreg.cache.strong_retry_abort (CFT; not auto-heal)."
                ),
            },
            {
                "name": "audit",
                "description": (
                    "Shared mgmt audit G-Set epidemic. Not SIEM, not BFT, "
                    "not infinite retention, not linearizable cluster ops."
                ),
            },
            {
                "name": "platform-rpc",
                "description": (
                    "Platform RPC FQN catalog (see components.schemas."
                    "PlatformCacheRpcCatalog). Wire names over WS/RPC plane, "
                    "not HTTP paths."
                ),
            },
            {"name": "metrics", "description": "Process metrics endpoints"},
            {"name": "mgmt", "description": "Management read/write models"},
            {"name": "health", "description": "Liveness and readiness"},
        ],
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
