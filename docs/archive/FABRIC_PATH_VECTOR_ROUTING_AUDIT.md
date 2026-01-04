# Fabric Path-Vector Routing Audit and Plan

## Scope

Audit the current path-vector routing control plane and evaluate whether
OSPF-like or BGP-like features should be added. Provide a decision framework
and a concrete project plan that balances reliability, security, complexity,
and test surface.

## Current Architecture Snapshot

### Core components

- mpreg/fabric/route_control.py
  - RouteDestination, RoutePath, RouteMetrics, RoutePolicy, RouteTable
- mpreg/fabric/route_announcer.py
  - RouteAnnouncementPublisher, RouteAnnouncementProcessor, RouteAnnouncer
- mpreg/fabric/gossip.py
  - ROUTE_ADVERTISEMENT message handling
- mpreg/fabric/control_plane.py
  - Control plane wiring (route table, announcer, processor)
- mpreg/fabric/federation_planner.py
  - Uses RouteTable first, then graph router fallback
- mpreg/fabric/federation_graph.py
  - GraphBasedFederationRouter (membership graph fallback)

### Current behavior

- Path-vector announcements are sent via gossip with TTL and epoch.
- RouteTable accepts announcements if:
  - non-expired, non-looping, valid path origin
  - policy allows (max_hops + tag filters)
- Route selection is score-based on metrics (latency, hops, cost, reliability).
- RouteTable caps routes per destination and purges expired routes.
- Signed announcements + withdrawals are supported (configurable).
- Hold-down + flap suppression stabilize churn when enabled.
- Deterministic tie-breakers stabilize equal-score selection.
- Route key registry enables rotation with overlap windows.
- Neighbor-specific import policy overrides are supported via policy directory.
- Route selection trace explains why a next hop was chosen.
- Planner uses RouteTable first, then graph router, then fallback neighbor.
- Optional link-state mode provides shortest-path routing when enabled.

### Strengths

- Simple, predictable path-vector core with loop checks and TTL.
- Typed metrics and policy scoring.
- Works with existing fabric gossip and planner.
- Graph fallback covers missing route announcements.

### Gaps vs OSPF/BGP capabilities

BGP-like (inter-cluster)

- Withdrawals + hold-down + flap suppression implemented (optional).
- Route origin validation + signed announcements implemented (optional).
- Import policy hooks + route tags implemented; global export policy supported.
- Per-neighbor export targeting via gossip target filtering implemented.
- Deterministic tie-breakers implemented.
- Policy hot reload + key distribution automation implemented.

OSPF-like (intra-cluster)

- Optional link-state database + SPF routing implemented.
- Link-state ECMP selection + area scoping implemented (optional).

## Decision Framework

### When to add BGP-like features

Add if any of the following are true:

- Cross-cluster links are unstable or high churn.
- Route leaks or mis-advertisements are a realistic threat.
- You need predictable policies (cost, locality, allowed routes).
- You need faster convergence than TTL-based expiry.

### When to add OSPF-like features

Add if any of the following are true:

- Intra-cluster topology is large and dynamic.
- You need deterministic shortest paths and ECMP within a cluster.
- You need fast convergence on link failures without gossip propagation delay.

### Complexity vs benefit notes

- BGP-like policy and safety controls improve correctness and security at
  moderate complexity and low runtime cost.
- Full OSPF-like link-state adds more moving parts and tests; it is only worth
  it if intra-cluster routing needs deterministic SPF and fast convergence.
- Path-vector plus graph fallback already covers many cases; do not add
  complexity without concrete failure modes or scale pressure.

## Recommendation

### Adopt now (low risk, high value)

1. Route origin validation and signed announcements. (DONE)
2. Explicit withdraws with hold-down timers for fast removal. (DONE)
3. Import policy hooks (per neighbor and per route tags). (DONE)
4. Deterministic tie-breakers for stable selection. (DONE)

### Adopt later (conditional)

1. Flap dampening (if route churn is frequent). (DONE)
2. Soft reconfiguration and policy reload (if policy changes are frequent).

### Do not adopt yet

1. Full OSPF link-state module unless intra-cluster routing becomes a
   bottleneck or requires strict SPF/ECMP guarantees.
   (UPDATED: optional link-state is available, still no ECMP/area scoping.)

## Status Summary (2026-01-xx)

- Path-vector + withdrawals + hold-down + dampening: DONE.
- Signed announcements + origin validation: DONE.
- Deterministic selection + policy hooks: DONE.
- Optional link-state mode: DONE.
- Link-state ECMP + area scoping: DONE.
- Route security registry + rotation overlap: DONE.
- Route selection trace: DONE.
- Scale/churn integration tests: DONE (small mesh).
- Route key refresh provider automation: DONE.
- Route policy reload integration: DONE.
- Route trace monitoring endpoint + CLI: DONE.
- Multi-area link-state announcements + area mismatch counters: DONE.
- ABR-style summary export filters + link-state CLI: DONE.

## Next Steps (Recommended)

1. Optional: evaluate multi-level area hierarchy or summary compression if
   intra-area scaling demands it beyond explicit summary filters.

## Proposed Project Plan

### Phase 0: Requirements and success metrics

- Define target convergence time and churn tolerance for route control.
- Define threat model for route advertisements (spoofing, leaks).
- Define allowed policy dimensions (cluster tags, cost classes, locality).

### Phase 1: Announcement security and validation

- Add announcement signatures with cluster identity keys.
- Add origin authorization checks (allowed origins list in policy).
- Add unit tests for signature verification and origin policy.

### Phase 2: Withdraws and hold-down

- Add RouteWithdrawal message type and handling in route processor.
- Add hold-down timers to prevent thrash on rapid churn.
- Add unit tests for withdraw handling, hold-down, and expiry interplay.
- Add integration test for fast failover after peer loss.

### Phase 3: Policy extensions

- Extend RoutePolicy with import/export filters and route tags.
- Allow per-neighbor policy hooks in RouteAnnouncementProcessor.
- Add tests for policy enforcement and deterministic tie-breakers.

### Phase 4: Optional stability controls

- Add route flap dampening (optional, off by default).
- Add metrics for flap counts and suppression duration.
- Add tests for dampening thresholds and recovery.

### Phase 5: Optional link-state (if required)

- Design a minimal link-state module for intra-cluster routing.
- Add SPF + ECMP selection (optional, off by default).
- Integration tests comparing convergence vs path-vector.

## Test Plan

- Unit: route signature validation, origin policy, withdraws, hold-downs.
- Integration: route announcement + withdrawal propagation across 3+ nodes.
- E2E: failover under churn with hop budgets and loop avoidance.
- Regression: ensure planner uses RouteTable first, graph fallback second.

## Open Questions

- Do we have cluster identity key material available at control-plane startup?
- Should route policy be per cluster, per neighbor, or both?
- Is gossip transport reliable enough for keepalive semantics, or do we need
  an explicit route session layer?
