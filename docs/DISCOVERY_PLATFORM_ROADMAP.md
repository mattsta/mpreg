# MPREG Global-Scale Discovery Platform Plan

This document captures the long-term, end-to-end implementation plan for
scalable discovery, delegation, and routing for MPREG without degrading
existing performance. It is the authoritative project roadmap and should be
kept in sync with implementation changes.

## Goals

- Enable near-global-scale service discovery using MPREG primitives.
- Maintain existing RPC/pubsub/queue/cache performance and behavior.
- Support scoped visibility with explicit delegation and safe cutovers.
- Provide self-managing resolver caches with bounded load and latency.
- Deliver complete observability, validation, and operator runbooks.
- Add optional DNS interoperability that exposes MPREG namespaces through
  standard DNS client tooling without changing the data plane.

## Implementation Status (Current)

- Phase 1 APIs (`cluster_map_v2`, `catalog_query`, scoped `list_peers`) are
  implemented in server/client with integration coverage in
  `tests/integration/test_discovery_api.py`.
- Catalog endpoints include explicit scope/tags metadata; `catalog_query`
  supports tag-based filtering and scope enforcement.
- Phase 2 delta stream baseline is implemented via `catalog_watch` and
  `mpreg.discovery.delta` (publishes routing catalog deltas with namespace tags).
- catalog_watch supports namespaced topics (`mpreg.discovery.delta.<namespace>`);
  summary_watch supports namespaced topics (`mpreg.discovery.summary.<namespace>`).
- Resolver cache mode is implemented with delta-driven ingestion and resolver-mode
  query paths; integration coverage in `tests/integration/test_discovery_api.py`.
- Discovery resolver query cache supports stale-while-revalidate and negative
  caching with configurable TTLs.
- Delta gap reconciliation and snapshot resync tooling are implemented via
  resolver cache stats/resync RPCs and optional periodic resync.
- Phase 3 baseline namespace policy engine is implemented (config rules,
  query filtering, namespace_status, policy apply/validate/audit RPCs);
  CLI helpers for policy apply/validate/audit are in place; export
  enforcement and auth remain pending.
- Namespace policy export RPC/CLI are implemented for exporting current rules.
- Phase 4 baseline summary query and summary export stream are implemented for
  local catalog summaries; hold-down, export scope enforcement, and
  store-and-forward buffering are now in place, while global tiering remains pending.
- Summary resolver cache mode is implemented; `summary_query` supports
  `scope=region`/`scope=global` to read from the summary cache when enabled.
- Summary exports are forwarded across the fabric, and store-forward backlog
  retains the global summary message for new subscribers.
- Summary exports now publish scope-aware topics (`mpreg.discovery.summary.<scope>`)
  with scope metadata; summary resolvers can subscribe by scope and filter cache
  entries by requested scope.
- Summary payloads include source cluster hints to guide cross-region redirect.
- Summary queries can include ingress hints mapping source clusters to known
  entry URLs (opt-in).
- Summary ingress hints accept optional scope/capability/tag filters for
  selecting ingress endpoints.
- MPREGClusterClient can consume summary ingress hints via `call_with_summary`
  to target source clusters explicitly.
- MPREGClusterClient `call` supports opt-in ingress hint usage when
  `target_cluster` is provided.
- MPREGClusterClient supports opt-in summary redirects on missing commands
  (`auto_summary_redirect`) to prefer authoritative clusters.
- Namespace policy cutover windows are enforced for summary export gating.
- Discovery policy viewer identity is derived from connection context; request
  overrides are ignored on authenticated connections.
- Discovery resolver subscriptions are announced/refreshed in the fabric, and
  `mpreg.discovery.*` topics bypass namespace policy filtering to keep delta and
  summary propagation healthy under strict policies.
- Production deployment guidance now documents the discovery policy exemption
  for `mpreg.discovery.*` control topics.
- list_peers supports scoped filtering and reports per-peer scope/region classification.
- cluster_map/cluster_map_v2 include node region metadata for scoped routing decisions.
- Discovery monitoring endpoints (/discovery/summary, /discovery/cache,
  /discovery/policy, /discovery/lag) are implemented in the monitoring plane.
- CLI discovery helpers are implemented (discovery query/summary/watch/status,
  resolver cache stats/resync).
- Discovery backpressure rate limiting is implemented for catalog/summary
  queries, watches, cluster_map_v2, and namespace_status (optional via settings).
- Tenant-aware discovery visibility is implemented via `visibility_tenants`,
  `viewer_tenant_id`, and access-audit logging (RPC/CLI) with rate limiting
  keyed to tenant identity when enabled. Tenant identity can be bound to
  authenticated connections via bearer/api-key credentials or a trusted header.
- Discovery load and chaos harnesses are available under `tools/debug/` for
  real-world query and restart validation.
- Discovery operational runbooks are documented in `docs/DISCOVERY_RUNBOOKS.md`.
- Reporting commands are implemented (mpreg report namespace-health/export-lag).
- Load-aware client selection now accounts for latency/error history (MPREGClusterClient).
- MPREGClusterClient supports optional region preference when selecting ingress endpoints.
- End-to-end integration coverage includes multi-region delegation and cutover
  propagation scenarios for summary exports.
- Cluster client HA integration coverage validated in
  `tests/integration/test_cluster_client_ha.py`.
- Phases 4+ (tiering, global export controls) remain pending.
- DNS interoperability workstream is implemented (dns*\* RPCs, service catalog
  integration, DNS gateway, and runbooks in `docs/DNS*\*`).
- DNS record encoding now emits correct RR types (SRV/TXT/AAAA/etc) for
  interoperability correctness.

## Audit Findings (Current)

- Global tiering/redirect mechanics are available via opt-in client settings;
  default call paths still require explicit summary usage.
- Scope-aware summary routing (scoped topics + cache filtering) is implemented,
  but automatic client redirection from global summaries still requires opt-in.
- Tenant-aware discovery visibility now supports authenticated binding; request
  overrides remain configurable for legacy clients.
- Optional signature verification for summary exports is not implemented.
- Load/chaos harnesses are manual and not yet wired into CI gates.

## Executive Summary

- Extend MPREG with an optional discovery plane that scales to global namespaces
  while keeping the existing fabric hot path unchanged.
- Introduce tiered exposure scopes (local/zone/region/global) and summarized
  exports to support safe delegation and cutovers.
- Add resolver nodes, scoped query APIs, and delta streams to enable cache-first
  discovery at scale.
- Preserve SOLID boundaries, SPOR elimination, and self-managing behaviors
  across RPC/pubsub/queue/cache.
- Provide comprehensive tests, monitoring, and documentation for users, admins,
  and operators.

## Guiding Principles

- No performance regression: existing routing and RPC flow are untouched unless
  new features are explicitly enabled.
- SOLID boundaries: discovery APIs, caching, export policy, and summarization are
  modular and independently testable.
- SPOR elimination: no single resolver, hub, or global registry becomes a hard
  dependency.
- Self-managing: TTLs, anti-entropy, hold-downs, and graceful degradation are
  default behaviors.
- Scoped visibility: namespace policy controls exposure across tiers and tenants.
- DNS interoperability is opt-in and never required for core MPREG usage.

## System Model

- Data plane: RPC/pubsub/queue/cache traffic uses the existing fabric router and
  unified envelope.
- Discovery plane: catalog queries, delta streams, and resolver caches provide
  scoped discovery.
- Interop plane: optional DNS gateway resolves MPREG names to catalog entries
  without becoming a new source of truth.
- Tiered scope: local (node), zone (cluster), region (regional mesh), global
  (summaries only).
- Address strategy: local endpoints remain authoritative; global exports are
  summaries or region pointers.
- Delegation boundary: explicit namespace rules define who can see or export
  what at each tier.

## Non-Goals

- Replace DNS or run a global recursive resolver.
- Provide authoritative public-root delegation or internet-wide DNS takeover.
- Change existing data plane semantics or client APIs by default.

## Glossary

- Scope: Visibility boundary for discovery entries.
  - LOCAL: node-local, never exported.
  - ZONE: cluster/segment-local.
  - REGION: regional mesh scope.
  - GLOBAL: summary-only export across regions.
- Namespace: Logical service identity for policy ownership and visibility.
  - Example: org.team.service or svc.prod.us-east.\*
- Resolver: Read-optimized node serving discovery queries from cache.
- Summary: Aggregated discovery record for cross-region propagation.

## Architecture Overview

### Tiered Addressing Model (IP-Like)

Multi-tier exposure mirrors IP address scopes and enables safe cutovers:

- LOCAL: node-only endpoints, never exported.
- ZONE: cluster-local endpoints for same-segment discovery.
- REGION: regional mesh with summary exports to global.
- GLOBAL: summaries only, used to redirect to authoritative regions.

Safe cutover points happen at namespace and region boundaries. Exports are only
allowed when policy explicitly permits them, and summary TTLs provide controlled
rollout windows.

### Planes

- Data Plane: Existing fabric routing for RPC/pubsub/queue/cache.
- Discovery Plane: Optional, scoped discovery APIs and delta streams.

### Core Components

- Scoped Discovery APIs (catalog_query, cluster_map_v2, catalog_watch).
- Resolver Mode (cache-first, delta-driven).
- Namespace Policy Engine (visibility + export controls).
- Summary Exporter (regional/global aggregation).
- Load/Health Scoring (optional routing preference).

### Design Principles

- SPOR avoidance: no single authoritative dependency.
- SOLID boundaries: discovery components are isolated, modular, and testable.
- Self-managing: TTLs, anti-entropy, hold-downs, and backpressure are default.
- Backward compatible: existing APIs remain unchanged.

### Resolver Query Flow

1. Client sends catalog_query or cluster_map_v2 to a resolver.
2. Resolver answers from cache (TTL + stale-while-revalidate).
3. If cache is stale, resolver refreshes via deltas or snapshot reconciliation.
4. Client receives scoped endpoints or summary pointers for regional redirect.

### Discovery Flow by System

- RPC: FunctionSelector + namespace + scope -> endpoints -> load-aware target.
- Pubsub: Topic subscriptions summarized across regions, full fan-out in-region.
- Queue: Queue endpoints summarized for region discovery, full delivery local.
- Cache: Cache profiles summarized globally, full profiles region-local.

### Scale Risks and Guardrails

- Catalog explosion: enforce paged queries and summary-only global exports.
- Snapshot overload: prefer deltas and scoped queries over full maps.
- Flapping exports: hold-down timers for summary propagation.
- Resolver overload: rate limits, query caps, and cache prewarming.

## Data Model Extensions

### EndpointScope

```
LOCAL | ZONE | REGION | GLOBAL
```

### NamespacePolicy

Fields:

- namespace: string
- owners: tuple[str, ...] (regions/clusters)
- visibility: tuple[str, ...] (tenants/clusters)
- visibility_tenants: tuple[str, ...]
- export_scopes: tuple[EndpointScope, ...]
- allow_summaries: bool
- cutover_windows: tuple[CutoverWindow, ...]
- policy_version: string

### ServiceSummary

Fields:

- namespace: string
- service_id: string
- regions: tuple[str, ...]
- endpoint_count: int
- health_band: string (healthy/degraded/critical)
- latency_band_ms: tuple[int, int]
- ttl_seconds: float
- generated_at: float
- policy_version: string
- source_cluster: string | None

### DiscoveryDelta

Fields:

- delta_id: string
- namespace: string
- scope: EndpointScope
- action: create|update|remove
- payload: entry or summary
- sent_at: float

### ResolverCacheEntry

Fields:

- key: string
- payload: entry or summary
- ttl_seconds: float
- stale_until: float
- last_refresh: float
- negative: bool

## Discovery APIs

### catalog_query (new)

Purpose: Filtered discovery queries for large catalogs.

Request fields:

- namespace: string or prefix
- scope: EndpointScope
- capabilities: tuple[str, ...]
- tags: tuple[str, ...]
- cluster_id: string | None
- limit: int
- page_token: string | None

Response fields:

- items: list[entry]
- next_page_token: string | None
- generated_at: float

### cluster_map_v2 (new)

Purpose: Scoped snapshot for clients or resolvers.

Request fields:

- scope: EndpointScope
- namespace_filter: string | None
- capabilities: tuple[str, ...] | None
- limit: int
- page_token: string | None

Response fields:

- cluster_id: string
- nodes: list[node snapshot]
- next_page_token: string | None
- generated_at: float

### catalog_watch (new)

Purpose: Delta stream for resolvers.

Topic format:

- mpreg.discovery.delta.<namespace>

Payload:

- DiscoveryDelta

### namespace_status (new)

Purpose: Admin visibility into policy and export status.

Response fields:

- namespace
- policy_version
- owners
- export_scopes
- summaries_exported
- last_export_at

## Resolver Mode

### Behavior

- Discovery-only server profile; no function registration required.
- Subscribe to catalog deltas for assigned namespaces.
- Cache-first query responses with TTL and stale-while-revalidate.
- Negative caching for missing namespaces.
- Backpressure: hard limit on response size and query rate.

### Cache Policy Defaults

- TTLs scale by scope (local < zone < region < global).
- Stale-while-revalidate window: 2x TTL.
- Negative cache TTL: short (e.g., 10-30s).

### Failure Handling

- Delta gap: request snapshot reconciliation.
- Stale cache: serve if within stale window, refresh in background.
- Resolver outage: client fallback to seed ingress nodes.

## Namespace Delegation and Export Policy

### Delegation Rules

- Each namespace has one or more owners (regions or clusters).
- Only owners can export summaries across region boundaries.
- Non-owners can serve local endpoints but not global summaries.

### Cutover Windows

- Time-bound export rules to enable safe migrations.
- Explicit fallback rules (deny by default on expiry).

### Enforcement Points

- Catalog ingestion: reject entries violating policy.
- Export pipeline: enforce summary-only outside region.
- Query pipeline: filter by viewer visibility.

## Summary Export Pipeline

### Regional Aggregation

- Aggregate endpoints into ServiceSummary records.
- Apply hold-down to prevent flapping exports.
- Export to global discovery topics.

### Global Propagation

- Only ServiceSummary records cross regions.
- Resolvers redirect clients to authoritative regions.

### Store-and-Forward (optional)

- Batch exports for intermittent or low-bandwidth links.
- Priority by namespace criticality.
- Global summary snapshot retained in store-forward backlog for new subscribers.

## Routing Integration

### Load/Health Scoring

- Use active_clients, error rate, and latency when available.
- Stale metrics are down-weighted, not hard rejected.
- If no metrics exist, routing remains deterministic.

### Cross-System Consistency

- RPC uses FunctionSelector and load scoring.
- Pubsub/queue/cache remain multi-target; selection order prefers healthier nodes.

## Security and Isolation

- Namespace ACLs for visibility and export.
- Optional route signing for summary exports.
- Query authentication for tenant-scoped data.
- Rate limits per tenant and per namespace.
- Audit log for policy changes and export events.

## Observability and Reporting

### Metrics

- discovery.cache.hit_rate
- discovery.cache.stale_serves
- discovery.delta.lag_ms
- discovery.delta.dropped
- discovery.query.latency_ms
- discovery.summary.exports
- discovery.summary.hold_downs

### Monitoring Endpoints

- /discovery/summary
- /discovery/cache
- /discovery/policy
- /discovery/lag

### Reports

- namespace health report
- export lag report
- resolver cache status

## Admin & Ops UX

- CLI: mpreg discovery query/summary/watch/status (implemented).
- Policy tools: mpreg namespace policy apply/validate/export.
- Resolver ops: mpreg resolver cache-stats, mpreg resolver resync (implemented).
- Reporting: mpreg report namespace-health, mpreg report export-lag (implemented).
- Runbooks: documented incident actions for stale caches, export outages, and
  policy rollback.

## Performance Guardrails

- Hot path isolation: discovery computations run off the RPC execution path.
- Bounded responses: always paginated or summarized for large scopes.
- Adaptive TTLs: TTLs scale with scope; global summaries have longer TTLs.
- Backpressure policies: query rate limits and delta throttling prevent overload.
- Store-and-forward: optional batch export windows for intermittent links.

## Testing Strategy

### Unit Tests

- Policy parsing and enforcement (allow/deny).
- Scope filtering for catalog_query and cluster_map_v2.
- Summary aggregation correctness.
- Cache TTL and stale-while-revalidate.
- Negative cache behavior.

### Integration Tests

- Resolver ingesting deltas and serving queries.
- Namespace export and visibility boundaries.
- Cross-region summaries with hold-down.
- Client discovery + routing across regions.

### Property Tests

- Delta idempotency under replay.
- Policy monotonicity (deny remains deny).
- Summary stability under churn.

### Load/Soak Tests

- 100k+ endpoints per region with bounded query latency.
- Resolver cache hit rate > 99% on steady workloads.
- Delta lag bounded under high churn.

### Chaos Tests

- Partition between region and global summary.
- Resolver crash and warm restart.
- Policy rollback during active traffic.
- High churn + concurrent queries.

## Documentation Goals

### User Docs

- Discovery client usage (cluster_map_v2, catalog_query).
- Resolver-aware clients and fallback behavior.
- Namespace patterns and best practices.

### Admin Docs

- Namespace policy configuration and cutovers.
- Resolver deployment and capacity planning.
- Export policy and summary interpretation.

### Ops Docs

- Monitoring dashboards and alert thresholds.
- Incident response playbooks for stale caches and export lag.
- Performance tuning guidance.

## Migration & Cutover

- Stage 1: keep existing discovery, add scoped APIs in parallel.
- Stage 2: deploy resolvers per region and migrate client traffic gradually.
- Stage 3: enable namespace policies for select namespaces only.
- Stage 4: enable summary exports to global tier; keep local catalogs untouched.
- Stage 5: switch large clients to catalog_query + resolver discovery.

## Risks & Mitigations

- Catalog explosion: enforce paged queries and summary exports at global tier.
- Policy errors: versioned policies + rollback with audit logs and dry-run.
- Delta gaps: replay buffer + periodic snapshot reconciliation.
- Resolver overload: rate limits + cache prewarming + shard by namespace.
- Latency drift: stale-while-revalidate and regional fallback pointers.

## Definition of Done

- Correctness: all discovery responses honor policy and scope filters.
- Performance: discovery latency stays within SLO while RPC path unchanged.
- Reliability: resolvers tolerate partitions and recover via deltas + snapshots.
- Security: namespace exports and queries are fully authorized and audited.
- Documentation: complete user/admin/ops/monitoring references with examples.

## Rollout Plan

### Phase 0: Spec Lock

- Finalize models, APIs, and policy semantics.
- Confirm non-regression constraints.

### Phase 1: Scoped Discovery APIs

- Implement catalog_query and cluster_map_v2.
- Add pagination and server-side filtering.

### Phase 2: Delta Streams + Resolver Cache

- Implement catalog_watch and delta topics.
- Add resolver mode with cache and stale policies.
- Status: catalog_watch + delta topic live; resolver cache + resync tooling implemented.

### Phase 3: Namespace Policy Engine

- Policy parsing, enforcement, and audit logging.
- Admin tooling for validation and rollback.
- Status: policy rule parsing + query filtering + namespace_status + policy
  apply/validate/audit RPCs implemented, with CLI helpers for policy
  validate/apply/audit.

### Phase 4: Summary Export and Tiering

- Regional summarization and global export.
- Hold-down and store-and-forward options.
- Status: summary_watch/export pipeline + hold-down + export scope + store-and-forward implemented; summary cache/resolver mode implemented; global tiering pending.

### Phase 5: Load-Aware Selection Enhancements

- Extend scoring with latency/error rates.
- Ensure deterministic fallback behavior.
- Status: MPREGClusterClient uses latency/error-aware scoring with stale-aware penalties.

### Phase 6: Global-Scale Validation

- Full-scale load + chaos validation.
- SLOs and acceptance criteria sign-off.

### Phase 7: DNS Interoperability Gateway

- Add optional DNS protocol support that maps MPREG catalog entries to DNS
  records (A/AAAA/CNAME/SRV/TXT).
- Ensure namespace policy and catalog scopes are enforced for DNS responses.
- Keep DNS data derived from the catalog (no separate authority).

## Acceptance Criteria

- Existing RPC/pubsub/queue/cache performance unchanged by default.
- Scoped discovery returns correct results per policy and scope.
- Resolver cache hit rate >= 99% under steady load.
- Summary exports stable under churn with bounded update rate.
- Full documentation and runbooks delivered and verified.
- DNS gateway responses are correct, policy-compliant, and bounded by TTLs.
- DNS gateway failure does not affect core MPREG data-plane operations.

## Implementation Pointers (Initial)

Suggested locations:

- mpreg/core/cluster_map.py (extend to v2 fields and scope)
- mpreg/fabric/catalog.py (add scope + namespace metadata)
- mpreg/fabric/index.py (add scoped query helpers)
- mpreg/server.py (new RPC handlers for catalog_query, cluster_map_v2)
- mpreg/fabric/gossip.py (discovery delta topics)
- mpreg/fabric/monitoring_endpoints.py (discovery endpoints)
- mpreg/cli (discovery query and policy tooling)
- tests/integration (resolver + summary export/summary watch coverage)
- mpreg/dns/\* (DNS gateway, resolver, name encoding, record models)
- mpreg/fabric/catalog.py (new service/DNS entries)
- mpreg/fabric/catalog_delta.py (delta propagation for services)
- mpreg/fabric/index.py (service lookup helpers)
- mpreg/fabric/announcers.py (service announcer)
- mpreg/server.py (dns\_\* RPCs and DNS server wiring)

## End-to-End Validation Scenarios

1. Multi-region with namespace delegation:
   - Region A owns svc.prod.us-east.\*
   - Region B owns svc.prod.eu-west.\*
   - Global summaries only, no cross-region endpoints
   - Clients resolve via resolver to region owner.

2. Controlled cutover:
   - Export policy toggled on during window
   - Summary appears globally within TTL
   - Rollback removes export within hold-down limits.

3. Resolver outage:
   - Clients fail over to ingress seeds
   - Discovery remains available with bounded latency.

4. High churn:
   - Endpoint joins/leaves at 10% per minute
   - Summary updates are stable and limited.

5. Tenant isolation:
   - Tenant A cannot query Tenant B namespaces
   - Audit logs show rejected queries.

6. DNS interoperability:
   - Register service + RPC endpoints in namespace svc.prod.us-east.\*
   - DNS SRV returns only authorized endpoints for a tenant-restricted viewer
   - DNS TTL honors catalog TTL bounds
   - Gateway down does not affect MPREG client discovery

## DNS Interoperability Plan (New Workstream)

### Objectives

- Expose MPREG namespaces via standard DNS clients without introducing a new
  authoritative registry.
- Use RoutingCatalog as the single source of truth.
- Enforce namespace policy, scopes, and export controls.

### Data Model (Catalog + Specs)

1. Add a first-class service registry entry:
   - New dataclass `ServiceEndpoint` (name, namespace, protocol, port, targets,
     scope, tags, capabilities, metadata, ttl_seconds, cluster_id, node_id).
   - Optional aliases for mapping multiple names to the same endpoint.
2. Add `ServiceKey` for removals and uniqueness.
3. Extend `RoutingCatalog`, `RoutingIndex`, and `RoutingCatalogDelta` with
   service entries and removals.
4. Add summary/ingress derivation rules for service endpoints when exporting.

### Catalog Integration & Gossip

1. Extend `CatalogBroadcaster` and add `FabricServiceAnnouncer`.
2. Extend catalog policy to filter service entries by namespace policy.
3. Add storage in resolver cache (service entries + TTL tracking).
4. Update delta pruning, cache seeding, and snapshot formats.

### DNS Gateway (Protocol Adapter)

1. New `mpreg/dns/` module:
   - `server.py`: UDP+TCP DNS server (async, non-blocking).
   - `resolver.py`: catalog -> DNS record resolution.
   - `records.py`: typed record dataclasses (A/AAAA/CNAME/SRV/TXT).
   - `names.py`: namespace-to-DNS label encoding/decoding.
2. Mapping rules:
   - `_mpreg._tcp.<function>.<namespace>.mpreg` => SRV + A/AAAA
   - `_queue._tcp.<queue>.<namespace>.mpreg` => SRV + A/AAAA
   - `_svc._tcp.<service>.<namespace>.mpreg` => SRV + A/AAAA
   - `<node_label>.node.<zone>` => A/AAAA (DNS-safe label or base32)
3. TXT record metadata includes namespace, scope, tags, cluster_id, node_id,
   spec_digest, and capabilities.
4. Enforce max response size and TCP fallback when needed.

### RPC/CLI Surface

1. RPC endpoints:
   - `dns_register`, `dns_unregister`
   - `dns_list`, `dns_describe`
2. CLI:
   - `mpreg client dns-register`
   - `mpreg client dns-list`
   - `mpreg client dns-resolve`
   - `mpreg client dns-node-encode` / `dns-node-decode`
3. All endpoints enforce namespace policy + rate limits.

### Config & Policy

1. New settings in `MPREGSettings`:
   - `dns_gateway_enabled`, `dns_listen_host`, `dns_udp_port`,
     `dns_tcp_port`, `dns_zones`, `dns_max_ttl_seconds`,
     `dns_min_ttl_seconds`, `dns_allow_external_names`.
2. DNS responses obey `NamespacePolicyEngine` and export rules.
3. DNS gateway must be optional and fail-closed without affecting data plane.

### Observability & Monitoring

1. Add DNS metrics to monitoring endpoints:
   - Query counts, latency, NXDOMAIN rates, policy denials.
2. Include DNS health in unified system monitoring.

### Testing Plan

1. Unit tests:
   - Name encoding/decoding
   - Record mapping rules
   - TTL normalization and bounds
   - Policy enforcement on DNS queries
2. Integration tests:
   - DNS gateway resolves catalog entries from live cluster
   - Service endpoint registration -> gossip -> DNS resolution
   - Tenant-aware policy gating with positive/negative cases
3. End-to-end tests:
   - Multi-cluster topology with resolver mode
   - DNS gateway behind a resolver node
   - Failure of DNS gateway does not break RPC discovery
4. Performance/chaos:
   - Burst DNS queries during catalog churn
   - Ensure bounded CPU/memory usage

### Documentation Plan

1. New docs:
   - `docs/DNS_INTEROP_GUIDE.md` (overview + examples)
   - `docs/DNS_RUNBOOKS.md` (operations + troubleshooting)
2. Update existing docs:
   - `README.md` (DNS interoperability overview)
   - `docs/DISCOVERY_RUNBOOKS.md` (DNS gateway operations)
   - `docs/FEDERATION_ARCHITECTURE.md` (interop plane overview)
3. Example usage:
   - `dig`/`nslookup` examples for RPC, queue, and service records.

### Rollout & Validation

1. Phase 7a: Catalog + gossip support for service endpoints.
2. Phase 7b: DNS gateway read-only mode (RPC/queue records only).
3. Phase 7c: Enable `dns_register` for custom services.
4. Phase 7d: Enable monitoring + runbooks + scale tests.

Original Parts and Reasons and Vision to Remember:
Below is a coherent, long‑term, end‑to‑end implementation spec that integrates multi‑tier discovery, namespace delegation, resolver caching, and global summaries into MPREG without degrading existing performance. This
is organized as a nested project plan with phases, deliverables, tests, docs, and acceptance criteria.

Executive Summary

- Extend MPREG with an optional discovery plane that scales to global namespaces while keeping the existing fabric hot path unchanged.
- Introduce tiered exposure scopes (local/zone/region/global) and summarized exports to support safe delegation and cutovers.
- Add resolver nodes, scoped query APIs, and delta streams to enable cache‑first discovery at scale.
- Preserve SOLID boundaries, SPOR elimination, and self‑managing behaviors across RPC/pubsub/queue/cache.
- Provide comprehensive tests, monitoring, and documentation for users, admins, and operators.

Guiding Principles

- No performance regression: existing fabric routing and RPC flow are untouched unless new features are explicitly enabled.
- SOLID boundaries: discovery APIs, caching, export policy, and summarization are modular and independently testable.
- SPOR elimination: no single resolver, hub, or global registry becomes a hard dependency.
- Self‑managing: TTLs, anti‑entropy, hold‑downs, and graceful degradation are default behaviors.
- Scoped visibility: namespace policy controls exposure across tiers and tenants.

System Model

- Data plane: RPC/pubsub/queue/cache traffic uses the existing fabric router and unified envelope.
- Discovery plane: catalog queries, delta streams, and resolver caches provide scoped discovery.
- Tiered scope: local (node), zone (cluster), region (regional mesh), global (summaries only).
- Address strategy: local endpoints remain authoritative; global exports are summaries or region pointers.
- Delegation boundary: explicit namespace rules define who can see or export what at each tier.

Core Components

- Discovery APIs: catalog_query, catalog_watch, cluster_map_v2, and scoped list_peers.
- Resolver mode: discovery‑only servers that cache deltas and serve queries with TTL/stale logic.
- Namespace policy engine: enforcement of visibility, export rules, and delegation ownership.
- Summary exporter: generates ServiceSummary objects for cross‑region/global propagation.
- Load/health scoring: optional routing preference using latency/error/load metrics.

Data Model Additions

- EndpointScope: LOCAL, ZONE, REGION, GLOBAL with explicit export and visibility rules.
- NamespacePolicy: owner, allowed consumers, export scopes, and cutover windows.
- ServiceSummary: service/namespace, regions, endpoint counts, health band, latency band, TTL.
- DiscoveryDelta: create/update/remove events with scope + namespace metadata.
- ResolverCacheEntry: TTL, stale‑until, source region, negative cache markers.

Discovery APIs (New)

- catalog_query: filtered query by namespace, scope, capabilities, tags, region, and pagination.
- catalog_watch: subscribe to delta topics mpreg.discovery.delta.<namespace>.
- cluster_map_v2: scoped snapshot with server‑side filtering and pagination.
- list_peers enriched: includes scope, advertised URLs, transport endpoints, and load.
- namespace_status: visibility/export rules + current summary export stats.

Resolver Mode

- Read‑optimized server profile: no function registration, only discovery cache and query APIs.
- Cache‑first responses: TTL + stale‑while‑revalidate + negative caching for missing services.
- Delta ingestion: subscribe to discovery topics for assigned namespaces and scopes.
- Backpressure: rate limit queries and enforce pagination to prevent large responses.
- Self‑healing: cache eviction and resync on sequence gaps or stale windows.

Namespace Delegation & Export Policy

- Ownership: namespace is owned by one or more regions with explicit export gates.
- Visibility: policies define which tenants or clusters can see a namespace.
- Cutover windows: time‑bounded export rules for staged migration or incident response.
- Scope rules: local endpoints never export globally unless explicitly authorized.
- Auditability: all exports include policy id + signature (optional).

Summary Export & Tiering

- Regional aggregation: region hubs summarize local endpoints into ServiceSummary.
- Global propagation: only summaries move across regional boundaries; full endpoints stay local.
- Store‑and‑forward: optional batch windows for intermittent or low‑bandwidth regions.
- Hold‑down timers: suppress flapping summaries to avoid global churn.
- Fallback pointers: summaries include region references for resolver redirection.

Routing & Load Selection

- Default behavior unchanged: deterministic selection remains if no load metrics are present.
- Optional load preference: favor lower load/latency/error rate when metrics exist.
- Staleness penalty: stale load metrics are down‑weighted rather than hard‑rejected.
- Multi‑target fan‑out: unchanged for pubsub and queue, enhanced scoring for routing order.
- Health bands: routing avoids DEGRADED or CRITICAL if alternatives exist.

Security & Isolation

- Namespace ACLs: per‑tenant visibility and export restrictions.
- Route signing: optional signature verification for cross‑region summaries.
- Query authz: discovery queries require tenant or cluster credentials where applicable.
- Rate limits: per‑tenant query limits and per‑namespace quotas.
- Policy versioning: allow staged rollout with rollback to last known good.

Observability & Reporting

- Discovery metrics: cache hit rate, delta lag, stale serve count, negative cache count.
- Namespace health: endpoints per namespace, summary lag, export failures.
- Routing metrics: load‑aware selection ratio, latency/error‑rate histograms.
- Audit logs: namespace export events, policy changes, and resolver cache replays.
- Monitoring endpoints: /discovery/summary, /discovery/cache, /discovery/policy.

Admin & Ops UX

- CLI: mpreg discovery query, mpreg discovery watch, mpreg discovery status.
- Policy tools: mpreg namespace policy apply/validate/export.
- Resolver ops: mpreg resolver cache-stats, mpreg resolver resync.
- Reporting: mpreg report namespace-health, mpreg report export-lag.
- Runbooks: documented incident actions for stale caches, export outages, and policy rollback.

Performance Guardrails

- Hot path isolation: discovery computations run off the RPC execution path.
- Bounded responses: always paginated or summarized for large scopes.
- Adaptive TTLs: TTLs scale with scope; global summaries have longer TTLs.
- Backpressure policies: query rate limits and delta throttling prevent overload.
- Zero‑copy deltas: avoid large allocations during watch ingestion.

———

Phase 0: Architecture & Spec Lock

- Scope: formalize data model changes and API contracts for discovery plane and summaries.
- Deliverables: design doc + schema definitions + API spec + rollout strategy.
- Tests: none; spec validation via reviews and threat modeling.
- Docs: architecture and protocol updates with “draft” flags.
- Exit criteria: design sign‑off with non‑regression guarantee for existing fabric.

Phase 1: Scoped Discovery API

- Scope: implement catalog_query, cluster_map_v2, and scoped list_peers.
- Deliverables: RPC handlers, server filters, pagination, and scope annotations.
- Tests: unit tests for filters and pagination; integration for large catalog slices.
- Docs: client guide + protocol spec + admin query examples.
- Exit criteria: queries serve filtered views without impacting existing routes.

Phase 2: Delta Streams & Resolver Cache

- Scope: catalog_watch topics and a resolver cache engine.
- Deliverables: delta event schema, watch subscriptions, cache TTL/stale logic.
- Tests: delta integrity, replay, stale‑while‑revalidate, negative caching.
- Docs: resolver mode user guide and operational tuning guidance.
- Exit criteria: resolvers serve from cache with 99% hit rate in test workloads.

Phase 3: Namespace Policy & Delegation

- Scope: implement namespace policy config and enforcement at export/query points.
- Deliverables: policy parser, ACL enforcement, audit logging, policy versioning.
- Tests: cross‑tenant isolation, deny‑by‑default, rollback semantics.
- Docs: admin policy reference + examples + cutover playbook.
- Exit criteria: policy‑bounded exports with zero cross‑tenant leaks.

Phase 4: Summary Export & Global Tiering

- Scope: generate and propagate ServiceSummary objects across regions.
- Deliverables: summarizer, export scheduler, hold‑down, store‑and‑forward.
- Tests: summary accuracy, churn suppression, region failover.
- Docs: global topology deployment guide and summary metrics reporting.
- Exit criteria: summaries stable under churn with bounded global update rates.

Phase 5: Load‑Aware Selection Enhancements

- Scope: integrate latency/error‑rate metrics into load scoring (optional).
- Deliverables: metrics aggregation, scoring adapters, routing preference hooks.
- Tests: selection bias tests, fallback correctness, stale metric handling.
- Docs: routing tuning guide and selection priority explanation.
- Exit criteria: better distribution under load without regressions.

Phase 6: Global‑Scale Validation

- Scope: scale tests, chaos drills, and long‑running reliability validation.
- Deliverables: load harness, topology simulator, fault injection scripts.
- Tests: large catalog, cross‑region partition, resolver cache failover, churn.
- Docs: operational runbooks, incident procedures, and SLOs.
- Exit criteria: passes global‑scale test matrix and reliability goals.

———

Testing Strategy

- Unit: policy parsing, filtering, summary generation, cache TTL behavior.
- Integration: resolver + hub + region graphs with filtered queries and deltas.
- Property‑based: delta idempotency, policy monotonicity, summary stability.
- Load/soak: 100k+ endpoints simulated with bounded response latency.
- Chaos: partitions, delayed deltas, misconfigured policies, resolver crashes.

Documentation Plan

- User docs: client discovery guide, HA patterns, scoped queries.
- Admin docs: namespace policy lifecycle, export rules, cutover playbooks.
- Monitoring docs: discovery metrics, cache status, export lag dashboards.
- Protocol docs: discovery RPCs, delta schemas, summary formats.
- Examples: enterprise topology, regional resolvers, safe cutover scenario.

Migration & Cutover

- Stage 1: keep existing discovery, add scoped APIs in parallel.
- Stage 2: deploy resolvers per region and migrate client traffic gradually.
- Stage 3: enable namespace policies for select namespaces only.
- Stage 4: enable summary exports to global tier; keep local catalogs untouched.
- Stage 5: switch large clients to catalog_query + resolver discovery.

Risks & Mitigations

- Catalog explosion: enforce paged queries and summary exports at global tier.
- Policy errors: versioned policies + rollback with audit logs and dry‑run.
- Delta gaps: replay buffer + periodic snapshot reconciliation.
- Resolver overload: rate limits + cache prewarming + shard by namespace.
- Latency drift: stale‑while‑revalidate and regional fallback pointers.

Definition of Done

- Correctness: all discovery responses honor policy and scope filters.
- Performance: discovery latency stays within SLO while RPC path unchanged.
- Reliability: resolvers tolerate partitions and recover via deltas + snapshots.
- Security: namespace exports and queries are fully authorized and audited.
- Documentation: complete user/admin/ops/monitoring references with examples.
