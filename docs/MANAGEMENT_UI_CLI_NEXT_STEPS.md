# Management UI + CLI End-of-Action Report (Full Spec + Project Plan)

Purpose: provide a complete specification and project plan for a unified
management plane that can visualize, audit, and control MPREG fabric clusters
at small, medium, and large scale. This document is a forward plan only.

## 1) Executive Summary

We will deliver a management plane that provides:

- Real-time visualization of topology, routing, catalog, and health.
- Safe operational controls (drain, detach, policy changes, route control).
- Audited, role-scoped actions and deterministic CLI output.
- A single API contract shared by the UI and CLI.

This plane will be hosted by the existing monitoring HTTP service and backed
by the fabric control plane so the platform manages itself.

## 2) Current Foundation (What Exists)

- Fabric monitoring endpoints (HTTP) and metrics, including routing + transport views.
- Persistence snapshot observability (`/metrics/persistence`) for fabric catalog + route keys.
- Unified fabric control plane: catalog, gossip, route control, link-state.
- Transport endpoint registry and catalog advertisement.
- CLI monitoring commands (health/metrics + route-trace/link-state/transport-endpoints).
- Admin shortcuts: monitor endpoints listing, persistence status, and compact status summary.

## 2.1) Recent Implementation Notes (Rolled Into Foundation)

- Added CLI commands: `monitor health`, `metrics`, `persistence`, `persistence-watch`,
  `metrics-watch --system`, `endpoints`, and `status`.
- Added persistence snapshot metrics in monitoring endpoints and exposed them in docs.
- Added a compact admin status view (health + persistence + transport + unified metrics).

## 2.2) Monitoring/Admin Surface Audit (What Exists vs. What’s Missing)

### Current Monitoring Endpoints (HTTP)

- Health: `/health`, `/health/summary`, `/health/clusters`, `/health/clusters/{cluster_id}`
- Metrics: `/metrics`, `/metrics/unified`, `/metrics/rpc`, `/metrics/pubsub`, `/metrics/queue`,
  `/metrics/cache`, `/metrics/transport`, `/metrics/persistence`
- Routing: `/routing/trace`, `/routing/link-state`
- Topology: `/topology`, `/topology/graph`, `/topology/paths`, `/topology/analysis`
- Transport: `/transport/endpoints`
- Endpoint listing: `/endpoints`

### Current CLI Admin Coverage (Derived from Monitoring)

- One-shot: `monitor health`, `monitor metrics`, `monitor persistence`, `monitor endpoints`, `monitor status`
- Watches: `monitor health-watch`, `monitor metrics-watch`, `monitor persistence-watch`
- Routing helpers: `monitor route-trace`, `monitor link-state`, `monitor transport-endpoints`

### Gaps vs. Management Goals

- No management API contract yet (`/mgmt/v1/*`) despite endpoints existing.
- CLI output formatting is mixed (JSON-only for most monitor commands).
- No REPL/IOS-like CLI mode yet (exec/config/diag).
- UI not implemented; no shared schema contract for CLI + UI.
- Missing audit trail for admin actions (read-only only today).
- No consistency between monitoring payloads and a normalized management data model.

## 3) Goals and Non-Goals

### Goals

- Single management surface for cluster and node operations.
- Full visibility into discovery, routing, health, and consensus.
- Operator-grade CLI with IOS-like modes and automation-ready output.
- UI that scales to 1k+ nodes with aggregation and drill-down.
- Audited, authenticated control operations.

### Non-Goals (initial phase)

- Multi-tenant org partitioning beyond simple RBAC.
- External ticketing integrations (PagerDuty, Slack, etc).
- Cross-cloud UI HA deployments.

## 4) Design Principles

- One API contract for CLI and UI.
- No direct mutation without authentication and audit.
- Read-only endpoints must be safe, low latency, and cacheable.
- Mutations must be reversible or clearly documented if not reversible.
- No fixed ports in examples; use `<port>` placeholders.

## 5) Management Plane Architecture

### Components

1. Management API Server
   - Runs inside the monitoring HTTP service.
   - Exposes `/mgmt/v1/*` endpoints.
2. Management Data Model
   - Normalized schema across catalog, routes, link-state, health.
3. Control Adapters
   - Thin layer mapping API operations to fabric control-plane APIs.
4. Audit + Event Stream
   - Append-only audit log and event feed (SSE).
5. CLI Console
   - Interactive terminal UX, scriptable outputs.
6. UI Dashboard
   - Graph, metrics, route explorer, and action workflows.

### Data Flow

- Fabric control plane produces catalog, routes, link-state, metrics.
- Management API aggregates and returns normalized views.
- Mutating commands apply to control plane and produce audit records.
- Event stream emits state changes and audit events.

## 6) Normalized Management Data Model

### ClusterSummary

Fields: cluster_id, node_count, area_count, gossip_status, route_status,
link_state_status, consensus_status, last_update_at.

### NodeSummary

Fields: node_id, cluster_id, status, capabilities, resources,
transport_endpoints, uptime_seconds, last_seen, health, warnings.

### EdgeSummary

Fields: source_cluster, target_cluster, latency_ms, bandwidth_mbps,
reliability_score, path_vector_present, link_state_present.

### RouteSummary

Fields: destination_cluster, next_hop_cluster, path, hop_count, metrics,
source (path_vector|link_state|graph_fallback), ttl_seconds.

### CatalogSummary

Functions, queues, topics, caches, nodes. Each entry includes:
identity, node_id, cluster_id, resources/capabilities, TTL, advertised_at.

### HealthSummary

Queue backlog, cache utilization, RPC error rate, transport saturation,
consensus status, and alert flags.

### AuditRecord

Fields: audit_id, actor, action, target, timestamp, outcome, details.

## 7) Management API Specification

All endpoints are served by the monitoring HTTP service.
Base path: `/mgmt/v1`.

### 7.1 Common Conventions

- Responses include `observed_at` and `request_id`.
- Pagination: `?limit=<n>&cursor=<id>`.
- Filtering: `?cluster_id=...&node_id=...&type=...`.
- Error format: `{ "error": { "code": "...", "message": "...", "details": ... } }`.

### 7.2 Read-Only Endpoints (Phase 1)

- `GET /mgmt/v1/topology`
- `GET /mgmt/v1/catalog`
- `GET /mgmt/v1/routes`
- `GET /mgmt/v1/link-state`
- `GET /mgmt/v1/health`
- `GET /mgmt/v1/consensus`
- `GET /mgmt/v1/transport/endpoints`

Example response (topology):

```json
{
  "request_id": "req-123",
  "observed_at": 1700000000.0,
  "clusters": [
    { "cluster_id": "cluster-a", "node_count": 3, "areas": ["area-1"] }
  ],
  "nodes": [
    {
      "node_id": "ws://127.0.0.1:<port>",
      "cluster_id": "cluster-a",
      "status": "healthy",
      "transport_endpoints": [
        {
          "connection_type": "internal",
          "protocol": "ws",
          "host": "127.0.0.1",
          "port": 12000,
          "endpoint": "ws://127.0.0.1:<port>"
        }
      ]
    }
  ],
  "edges": [
    {
      "source_cluster": "cluster-a",
      "target_cluster": "cluster-b",
      "latency_ms": 8.2
    }
  ]
}
```

### 7.3 Mutating Endpoints (Phase 2)

- `POST /mgmt/v1/node/{node_id}/drain` { "enabled": true|false }
- `POST /mgmt/v1/node/{node_id}/detach`
- `POST /mgmt/v1/node/{node_id}/attach`
- `POST /mgmt/v1/catalog/refresh`
- `POST /mgmt/v1/routes/recompute`
- `POST /mgmt/v1/routes/withdraw` { "destination_cluster": "cluster-x" }
- `POST /mgmt/v1/link-state/enable` { "mode": "prefer|only", "area_policy": {...} }
- `POST /mgmt/v1/link-state/disable`
- `POST /mgmt/v1/policy/route` { "profile": "gold-tier" }

Responses always include `status`, `audit_id`, and `details`.

### 7.4 Event Streaming (Phase 3)

- `GET /mgmt/v1/events` (SSE)
  - Filters: `type`, `cluster_id`, `node_id`.
  - Resume: `last_event_id`.

Event types:

- `catalog_update`
- `route_announcement`
- `route_withdrawal`
- `link_state_update`
- `peer_connection_established`
- `peer_connection_lost`
- `audit`

## 8) IOS-like CLI Specification

### Modes

- `exec`: read-only
- `config`: mutation allowed
- `diag`: deep debugging and traces

### Grammar

- `show <subject> [filters]`
- `config <subject> <action> [args]`
- `audit tail [filters]`

### Examples

```
mpreg> show topology
mpreg> show routes cluster cluster-b
mpreg> show catalog functions
mpreg> show link-state area area-1
mpreg> config node ws://127.0.0.1:<port> drain on
mpreg> config route-policy apply gold-tier
mpreg> audit tail
```

### Output Formats

- `--format table` (default)
- `--format json` (machine)
- `--format compact` (low bandwidth)

## 9) Graphical UI Specification

### Primary Views

1. Topology Graph
   - Nodes and edges with overlays (routes, link-state, health).
   - Drill-down into node detail and actions.
2. Route Explorer
   - Select function/queue/topic and show computed path + hops.
3. Health Dashboard
   - Per-node status, alerts, and subsystem metrics.
4. Metrics Timeline
   - Throughput, latency, backlog, route churn, gossip volume.

### Large-Cluster UX (1k+ nodes)

- Aggregation by cluster/region/area.
- Progressive disclosure for route tables.
- Heatmaps for saturation and health.
- Search + filter by node_id, function_id, cluster_id.

## 10) Security and Auth

- Management endpoints must be authenticated (mTLS or signed API token).
- Roles: read-only, operator, admin.
- Every mutating command creates an audit record.
- Rate limiting per role and endpoint.

## 11) Testing Requirements

- Unit: schema serialization, permission checks, audit formatting.
- Integration: management API + control adapters.
- End-to-end: CLI and UI against live fabric nodes.
- Scale: 100, 1k, and 2k node model tests.

## 12) Documentation Deliverables

- Management API reference.
- CLI operator guide with examples.
- UI operator guide with workflows and screenshots.
- Troubleshooting appendix (failure states + recommended actions).

## 12.1) Related References

- `docs/ARCHITECTURE.md`
- `docs/MPREG_PROTOCOL_SPECIFICATION.md`
- `docs/OBSERVABILITY_TROUBLESHOOTING.md`
- `docs/PRODUCTION_DEPLOYMENT.md`

## 13) Project Plan (Phased Delivery)

Phase 1: Data Model + Read-Only API

- Define normalized schema.
- Implement `/mgmt/v1/topology`, `/catalog`, `/routes`, `/health`.
- Add unit tests for schema stability.

Phase 2: CLI (Exec Mode)

- Implement read-only CLI commands.
- Support json/table/compact output.
- Add CLI integration tests.
  - Status: read-only monitor commands exist; add table/compact formatting
    for `monitor metrics` and `monitor metrics-watch`.
  - Add `monitor status-watch` for periodic summary output (table + json).
  - Add endpoint-derived health summaries with severity coloring.
  - Normalize CLI output fields to match the future `/mgmt/v1` schema.

Phase 3: Mutations + Audit

- Add node drain/detach/attach commands.
- Add route recompute/withdraw.
- Add audit store and event streaming.

Phase 4: UI Dashboard

- Build topology + health + route explorer UI.
- Add event stream integration.
- Add load testing for 1k nodes.

Phase 5: Hardening

- Auth, RBAC, rate limiting.
- Rollback paths for mutations.
- Full scale tests and docs.

## 14) Exit Criteria

- Operators can answer "what is unhealthy and why?" in <5 commands.
- Every mutating command is authenticated, audited, and reversible.
- UI renders 1k+ nodes with aggregation and responsive drill-down.
- CLI and UI share the same API contract and behavior.
