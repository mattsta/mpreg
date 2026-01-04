# Fabric Path-Vector Routing Project Plan

## Vision

Strengthen the fabric route control plane with BGP-like safety, policy, and
stability features while preserving the current path-vector simplicity. The
outcome should improve predictability, availability, performance, reliability,
security, and monitoring without introducing unnecessary complexity.

## Scope

Applies to route control plane and its integration with gossip, control plane,
and federation planner. This is an additive, optional capability set with
sane defaults that keep the current behavior unless explicitly enabled.

## Success Criteria

- Predictable path selection across churny clusters.
- Fast route withdrawal and recovery under link loss.
- Authenticated route advertisements with advertiser validation.
- Deterministic selection under equal-score paths.
- Clear observability into route changes and convergence.
- All changes covered by unit + integration + E2E tests.

## Audit Snapshot (2026-01-xx)

- Path-vector (BGP-like) control plane: implemented with signatures, policy filters,
  withdrawals, hold-downs, flap suppression, deterministic tie-breakers, and
  observability metrics/logs.
- Link-state (OSPF-like) optional mode: implemented and covered by unit +
  integration tests; planner supports prefer/only modes.
- Protocol + docs: route signatures/withdrawals and link-state updates documented;
  observability + troubleshooting updated with route metrics.
- Key distribution: gossip-based route key announcements plus refresh providers are
  implemented; registries stay synchronized across clusters.
- Remaining gaps: long-running churn/soak validation comparing link-state vs
  path-vector under failure injection.

## Capability Improvements (What This Adds)

### Predictability

- Deterministic tie-breakers for route selection.
- Explicit policy ordering and consistent import/export rules.

### Availability

- Withdrawals + hold-down reduce stale routes after failures.
- Faster re-route on peer loss than TTL expiration alone.

### Performance

- Lower churn from flap dampening and hold-down timers.
- Reduced unnecessary route relays with policy filters.

### Reliability

- Better loop prevention via origin validation and path checks.
- Policy-enforced acceptance of routes reduces bad paths.

### Security

- Signed announcements and origin authorization prevent spoofing.
- Policy-based route filtering reduces route leaks.

### Monitoring

- Route lifecycle events are logged with consistent scope.
- Metrics for convergence time, churn, withdrawals, and suppression.

## Baseline Constraints and Assumptions

- Fabric gossip is the transport for control-plane announcements.
- Path-vector remains the default control plane; new features are opt-in.
- Cluster identity and membership are trusted via external mTLS/TLS + access key.
- Route updates fit within existing gossip envelopes and hop limits.
- No fixed ports in tests; port allocator is mandatory.

## Design Outline (Expanded Pre-Planning)

### Data Model Extensions (Planned)

- RouteAnnouncement additions:
  - signature: bytes (optional)
  - public_key: bytes (optional)
  - signature_algorithm: str (default \"ed25519\")
  - route_tags: tuple[str, ...] (policy tagging)
- New message type:
  - RouteWithdrawal (destination + path + advertiser + epoch)
- RouteHoldDown:
  - destination + next_hop + until_timestamp
- RoutePolicy extensions:
  - allowed_advertisers: set[ClusterId] | None
  - allowed_destinations: set[ClusterId] | None
  - allowed_tags: set[str] | None
  - deny_tags: set[str] | None
  - deterministic_tiebreakers: tuple[str, ...]
- RouteStabilityPolicy:
  - hold_down_seconds
  - flap_threshold
  - suppression_window_seconds

### Configuration Surface

- FabricControlPlane.create():
  - route_security_config (require_signatures, allowed_advertisers, allow_unauth)
  - route_stability_config (hold_down, flap_dampening)
  - route_policy (extended with tags and deterministic ordering)
- Defaults: match current behavior (no signatures enforced, no hold-down, no dampening).

### Control-Plane Pipeline (Revised)

1. Receive route announcement via gossip.
2. Validate schema + signature (if required).
3. Apply policy filters (advertiser + tags + destination).
4. Apply hold-down and suppression rules.
5. Update route table and optionally re-advertise.

### Deterministic Route Selection

- Score-based ordering remains primary.
- Tie-breakers applied in order:
  1. lowest hop_count
  2. lowest latency_ms
  3. highest reliability_score
  4. stable advertiser ordering

### Security Model (Planned)

- Signature verifies advertiser identity, not origin.
- Origin authorization is policy-driven (allowed advertisers and tag filters).
- Future upgrade path: chained signatures per hop if needed.

### Observability Model (Planned)

- Log events: announce/accept/reject/withdraw/hold-down/suppress/purge.
- Metrics:
  - routes_active_total
  - routes_withdrawn_total
  - routes_suppressed_total
  - convergence_seconds (per destination)
  - announcement_rejects_total (by reason)

### Rollout Strategy

- Feature flags default off.
- Enable signatures in single-cluster testbed, then multi-cluster.
- Enable withdrawals next, then hold-down/dampening.
- Keep graph fallback in planner as safety net.

### Failure Scenarios (Planned)

- Single peer loss: withdrawal propagated within one gossip cycle.
- Route churn: hold-down prevents immediate re-acceptance.
- Malicious advertiser: signature + allowed_advertisers rejects spoofed updates.
- Policy change: new policy invalidates routes deterministically on next refresh.

### Monitoring and SLO Targets (Planned)

- Convergence time: < 2 gossip intervals for single-hop withdrawal.
- Route churn rate: < 5% of routes suppressed by hold-down in steady state.
- Security violations: 0 unsigned announcements accepted when enforced.

### API and Config Checklist

- RouteAnnouncement: signature metadata + tags.
- RouteWithdrawal: destination + path + advertiser + epoch + signature metadata.
- RoutePolicy: advertiser allow/deny + tag allow/deny + deterministic ordering.
- RouteStabilityPolicy: hold-down seconds (dampening later).
- ControlPlane: signer + security config + public-key resolver.

### Test Matrix (Planned)

- Unit: announcement signature verification, tag filtering, tie-breakers.
- Unit: withdrawal removes routes and sets hold-down.
- Unit: hold-down rejects flapping updates.
- Integration: withdrawal propagates across 3 nodes.
- E2E: multi-hop routing continues after withdrawal via alternate path.

### Risk Notes

- Signature verification adds CPU cost; use caching where possible.
- Withdrawals can increase gossip chatter; keep TTL and hop limits sane.
- Policy complexity can cause false drops; provide dry-run mode for audits.

## Integration Touchpoints (Expanded Pre-Planning)

### Server Initialization

- FabricControlPlane creates RouteWithdrawalCoordinator when route control is enabled.
- MPREGServer subscribes the coordinator to the ConnectionEventBus.

### Connection Event Flow

1. Connection lost event published.
2. RouteWithdrawalCoordinator resolves cluster_id from node_url.
3. RouteTable removes routes learned from the lost neighbor.
4. Withdrawals are published to gossip for downstream removal.

### Planner Interaction

- FabricFederationPlanner continues to use RouteTable first, graph fallback second.
- Withdrawals accelerate route table cleanup before TTL expiry.

### Event-Driven Withdrawal Integration (Planned)

- ConnectionEventBus publishes `LOST` events on transport failures.
- RouteWithdrawalCoordinator converts `node_url -> cluster_id` and withdraws
  learned routes from that neighbor cluster.
- Withdrawals are broadcast immediately to speed convergence.
- Control plane keeps graph fallback for safety if route table is empty.

### Metrics and Monitoring (Planned)

- Expose RouteTable stats via a lightweight snapshot method.
- Track:
  - withdrawals_received / withdrawals_applied
  - hold_down_rejects
  - announcements_rejected + rejection_reasons
- Use these counters in troubleshooting runbooks and alerts.

### Neighbor Policy (Planned)

- Per-neighbor import filters (allowed tags/advertisers/destinations).
- Default: fall back to global RoutePolicy when no neighbor policy exists.
- Enforcement lives in RouteAnnouncementProcessor to keep RouteTable generic.

### Export Policy (Planned)

- Optional global export filter applied before gossip publication.
- Intended for controlling which local announcements are broadcast.
- Withdrawals bypass export policy to prevent stale routes.

### Flap Dampening (Planned)

- Track flap counts per (destination, advertiser) key.
- Suppress announcements when flap_threshold reached within window.
- Suppression window doubles as cooldown period.

## Configuration Examples (Planned)

### Enforce Signatures + Hold-Down

```python
route_security_config = RouteSecurityConfig(
    require_signatures=True,
    allow_unsigned=False,
)
route_stability_policy = RouteStabilityPolicy(hold_down_seconds=10.0)
FabricControlPlane.create(
    ...,
    route_security_config=route_security_config,
    route_stability_policy=route_stability_policy,
)
```

### Export Policy Filter

```python
route_export_policy = RoutePolicy(allowed_tags={"gold"})
FabricControlPlane.create(
    ...,
    route_export_policy=route_export_policy,
)
```

### Allow Unsigned (Default)

```python
FabricControlPlane.create(...)
```

## Operational Runbook (Planned)

- Verify public key resolver returns expected keys for cluster_id.
- Use `RouteTable.stats.to_dict()` for churn/withdrawal diagnostics.
- Confirm withdrawals propagate via gossip before lowering TTLs.

## Project Phases

### Phase 0: Requirements and Metrics

- Define convergence targets and failure scenarios (e.g., 2-hop loss).
- Define trust model for cluster identity keys.
- Define policy dimensions (local-pref, tag filters, cost classes).
- Update docs to reflect requirements and constraints.

Deliverables

- Route control requirements doc section.
- Convergence/availability targets logged in this plan.
- Test matrix outline for new features.

### Phase 1: Signed Announcements + Advertiser Authorization

- Add signature fields to route announcements (optional, gated by config).
- Add advertiser authorization in RoutePolicy (allowed advertisers list or tags).
- Reject unauthenticated or unauthorized announcements.

Tests

- Unit: valid signatures accepted, invalid signatures rejected.
- Unit: advertiser authorization filters work for allow/deny cases.
- Integration: signed announcements propagate and update tables.
  Status
- DONE (signature fields, signer + verifier, advertiser authorization, and tests implemented).

### Phase 2: Withdrawals + Hold-Down Timers

- Add RouteWithdrawal message type (payload includes destination + path).
- Add hold-down window to prevent immediate re-acceptance of flapping paths.
- Ensure withdraws supersede TTL where applicable.

Tests

- Unit: withdraw removes route entries immediately.
- Unit: hold-down prevents immediate re-acceptance.
- Integration: peer loss triggers withdraw and re-route.
  Status
- DONE (withdrawals + hold-down implemented; gossip propagation + coordinator tests added; peer-loss hook + integration test complete).

### Phase 3: Policy Extensions and Deterministic Selection

- Extend RoutePolicy with import/export hooks and route tags.
- Add deterministic tie-breakers (e.g., lowest hop count then latency then
  stable advertiser ordering).
- Add per-neighbor policy enforcement in RouteAnnouncementProcessor.

Tests

- Unit: policy filters block/allow expected routes.
- Unit: deterministic tie-breaking produces consistent selection.
- Integration: mixed-policy cluster selects expected next hops.
  Status
- DONE (neighbor policy filter + export policy hook + deterministic tie-breakers covered; example walkthrough added).

### Phase 4: Stability Controls (Optional)

- Add flap dampening (configurable thresholds and suppression windows).
- Add suppression metrics and logs.

Tests

- Unit: flap counters and suppression windows enforced.
- Integration: repeated route churn leads to suppression and recovery.
  Status
- DONE (suppression tracking + unit/integration coverage complete; suppression logs added).

### Phase 5: Observability and Monitoring

- Add route lifecycle logs (announce/withdraw/accept/reject/purge).
- Add metrics: convergence time, active routes per destination, suppression.
- Update observability docs and troubleshooting flow.

Tests

- Unit: metrics counters updated on key events.
- Integration: route churn updates metrics as expected.
  Status
- DONE (route_metrics surfaced via server status; docs updated; churn integration test complete; lifecycle logs + convergence/active-route metrics added).

### Phase 6: Optional Link-State Evaluation

- Re-evaluate need for intra-cluster link-state (OSPF-like) module.
- Prototype SPF/ECMP for intra-cluster if justified by scale needs.

Deliverables

- Decision memo: `docs/FABRIC_LINK_STATE_ROUTING.md` (path-vector default, link-state optional).
  Status
- DONE (link-state mode implemented as optional fabric feature; path-vector remains
  default with link-state as prefer/only mode).

## Next Steps (Post-Audit)

1. Key management lifecycle: DONE (RouteKeyRegistry + rotation overlap tests +
   docs for key distribution/rotation flow).
2. Neighbor policy surfaces: DONE (RoutePolicyDirectory + settings wiring +
   tests; per-neighbor import policies documented).
3. Scale validation: DONE (integration churn/soak tests for path-vector +
   link-state convergence under updates).
4. Route introspection: DONE (RouteSelectionTrace + explain_selection helper +
   tests and troubleshooting guidance).

## Next Cycle (Planned)

1. Key distribution automation: DONE
   - RouteKeyProvider refresh loop + tests added.
2. Policy reloads and safe transitions: DONE
   - Live route policy reload implemented + integration test added.
3. Export targeting options: DONE (neighbor export policies + gossip target filter
   - integration test coverage).
4. Link-state enhancements: DONE
   - Optional ECMP selection and area scoping implemented.
   - Added mesh ECMP validation alongside path-vector scale coverage.
5. Route trace surface: DONE
   - Monitoring endpoint + CLI `monitor route-trace` added.

## Future Work (Optional)

### Multi-level area hierarchy (beyond parent/child mapping)

- Goals: support large multi-area fabrics with explicit ABR boundaries and
  predictable export domains without flooding all areas.
- Data model: introduce an explicit area graph (parent/child or mesh) and
  allow ABR nodes to participate in multiple areas with defined export rules.
- Control plane: add summary export rules that can target one or more parent
  areas, optionally scoped by neighbor allow/deny lists and area-local filters.
- Configuration surface:
  - `fabric_link_state_area_policy.area_hierarchy` (already present)
  - `fabric_link_state_area_policy.summary_filters` (already present)
  - Optional: `fabric_link_state_area_policy.area_graph` for multi-parent area
    graphs and ABR role declarations.
- Tests:
  - Unit: verify multi-parent summary export selection and filter enforcement.
  - Integration: multi-area mesh with two ABRs; confirm summaries only flow
    into permitted areas and do not leak into disallowed areas.
- Observability: extend `/routing/link-state` to expose summary exports by
  source/target area and filtered counts.

### Summary compression policies

- Goals: reduce control-plane payload size as area membership grows.
- Policy knobs:
  - Max summarized neighbors per area (cap list length deterministically).
  - Prefer high-reliability or low-latency neighbors when summarizing.
  - Stable ordering to keep summaries deterministic across nodes.
- Implementation sketch:
  - Add a summary selection helper used by `LinkStateAreaPolicy.summarize_neighbors`.
  - Enforce caps with deterministic ordering (latency/reliability + stable tie-breakers).
- Tests:
  - Unit: summary caps and deterministic ordering.
  - Integration: large area mesh with cap; verify summaries stay within bounds.
- Docs: add operational guidance for when to enable summary compression and the
  tradeoffs (lower control-plane cost vs less granular topology detail).

## Implementation Notes

- Keep defaults aligned with current behavior (features gated by config).
- Avoid heavy control-plane churn; favor incremental state changes.
- Maintain typed data structures (no tuple[str, str] keys).

## Documentation Updates (Complete)

- DONE: protocol spec includes signed announcements + withdrawals.
- DONE: routing architecture docs updated with policy + stability features.
- DONE: example config for policy + security enforcement.
- DONE: optional link-state mode documented with use cases + config knobs.
- DONE: route security + policy docs and route-trace troubleshooting guidance.
- DONE: minimal live demo for route security + neighbor policy filtering.

## Test Strategy (Summary)

- Unit: route control data structures + policy validation.
- Integration: gossip propagation + route selection across live nodes.
- E2E: multi-hop routing under failure and recovery conditions.

## Open Questions

- Key distribution is via registry today; integrate external PKI/rotation
  workflows when needed (mTLS, HSM, or deployment automation).
- Per-neighbor policies are implemented; do we need per-cluster overlays too?
- Is gossip transport sufficient for route control reliability, or do we
  need a lightweight session layer?

## Progress Log

- 2026-01-xx: Plan created, pending requirements definitions.
- 2026-01-xx: Expanded pre-planning (data model, config, rollout, risks).
- 2026-01-xx: Phase 1 started (signed announcements + policy tags).
- 2026-01-xx: Added RouteWithdrawal + hold-down policy + route stats counters.
- 2026-01-xx: Added withdrawal coordinator + gossip withdrawal propagation.
- 2026-01-xx: Added connection-loss integration test for withdrawals.
- 2026-01-xx: Added export policy hook and flap dampening suppression logic.
- 2026-01-xx: Surfaced route_metrics in server status + observability docs.
- 2026-01-xx: Added route churn suppression integration coverage.
- 2026-01-xx: Added route lifecycle logs for announce/withdraw/hold-down/suppress events.
- 2026-01-xx: Added deterministic tie-breaker tests + convergence/active-route metrics.
- 2026-01-xx: Ran demo smoke + demo suite for end-to-end validation.
- 2026-01-xx: Added optional link-state routing mode + gossip integration tests.
- 2026-01-xx: Added route key registry + rotation overlap tests and resolver flow.
- 2026-01-xx: Added neighbor policy directory + route trace introspection helpers.
- 2026-01-xx: Added path-vector/link-state churn validation integration tests.
- 2026-01-xx: Added `mpreg/examples/fabric_route_security_demo.py` to show
  key rotation + neighbor policy filtering live.
- 2026-01-xx: Added route key provider refresh loop + tests + config wiring.
- 2026-01-xx: Added live route policy reload support + integration coverage.
- 2026-01-xx: Added monitoring `/routing/trace` endpoint + CLI `monitor route-trace`.
- 2026-01-xx: Added link-state area scoping + ECMP selection, updated docs, and
  added mesh ECMP validation in scale tests.
- 2026-01-xx: Added multi-area link-state announcements with area maps, neighbor
  filters, and monitoring surface for area mismatch counters.
- 2026-01-xx: Added ABR-style summary export filters with area hierarchy support,
  plus `mpreg monitor link-state` CLI integration.
