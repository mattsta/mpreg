# Discovery Runbooks

Operational guidance for MPREG discovery, summary export, and resolver behavior.

## Summary Export Lag

Symptoms

- `mpreg discovery lag` shows `summary_export_lag_seconds` increasing.
- `summary_query` for `scope=global` returns empty results.

Checks

- `mpreg discovery lag --url ws://node:<port>`
- `mpreg discovery summary --url ws://node:<port> --namespace svc.market --scope global`
- `mpreg discovery policy status --url ws://node:<port>`

Actions

- Ensure `discovery_summary_export_enabled=True` on source nodes.
- Verify `allow_summaries` and `export_scopes` in namespace policy.
- Confirm `discovery_summary_export_scope` is set (local/zone/region/global).
- Reduce `discovery_summary_export_interval_seconds` for faster propagation.

## Resolver Cache Stale Or Empty

Symptoms

- `catalog_query` returns stale or empty entries from a resolver.
- `discovery cache-stats` shows low entry counts after expected propagation.

Checks

- `mpreg discovery cache-stats --url ws://resolver:<port>`
- `mpreg discovery resync --url ws://resolver:<port>`
- Validate delta stream health via `catalog_watch`.

Actions

- Enable `discovery_resolver_mode=True` and `discovery_resolver_seed_on_start=True`.
- Trigger `discovery_resolver_resync` when deltas fall behind.
- Confirm `discovery_resolver_namespaces` include the target namespace.

## Summary Resolver Scope Mismatch

Symptoms

- Summary cache exists, but `summary_query` returns no entries for a scope.

Checks

- `mpreg discovery summary --url ws://resolver:<port> --scope region`
- `mpreg discovery summary --url ws://resolver:<port> --scope global`
- Verify configured scopes in `discovery_summary_resolver_scopes`.

Actions

- Align export scope and resolver scopes (e.g. global to global).
- For scoped routing, set `discovery_summary_export_include_unscoped=False`.

## Policy Or Tenant Visibility Denials

Symptoms

- `catalog_query` or `summary_query` returns empty for known services.
- CLI shows `discovery_access_denied` on watches.

Checks

- `mpreg discovery policy status --url ws://node:<port>`
- `mpreg discovery access-audit --url ws://node:<port>`

Actions

- Review `visibility` and `visibility_tenants` rules for the namespace.
- If tenant mode is enabled, include `viewer_tenant_id` in requests.
- Ensure authenticated connections map to the intended cluster/tenant identity.

## Discovery Rate Limiting

Symptoms

- RPCs fail with 429 and `discovery_rate_limited`.

Checks

- Inspect `discovery_rate_limit_requests_per_minute` and window settings.
- Review access audit logs for `rate_limited` reasons.

Actions

- Increase `discovery_rate_limit_requests_per_minute` or reduce client QPS.
- Use resolver nodes to offload high-frequency queries.

## Cluster Map Missing Nodes

Symptoms

- `cluster_map_v2` returns fewer nodes than expected.

Checks

- `mpreg discovery cluster-map --url ws://node:<port> --scope zone`
- Validate `list_peers` output and fabric gossip convergence.

Actions

- Confirm nodes advertise the expected `scope` and `region`.
- Increase gossip stability windows if churn is high.

## Resolver Or Hub Outage

Symptoms

- Clients lose discovery while core services remain up.

Checks

- Confirm multiple resolver nodes are active and reachable.
- Use `cluster_map_v2` to locate alternate ingress URLs.

Actions

- Deploy at least two resolvers per region.
- Configure clients to rotate between ingress URLs from `summary_query`.
- Use `tools/debug/discovery_chaos_harness.py` to validate restart behavior.

## DNS Interoperability

Notes

- The DNS gateway is optional and derives records from the routing catalog.
- Namespace policy and scope rules still apply to DNS responses.
- Enable `dns_allow_external_names` if clients omit the zone suffix.

References

- `docs/DNS_INTEROP_GUIDE.md`
- `docs/DNS_RUNBOOKS.md`

## Large Cluster Auto-Discovery Convergence

Symptoms

- `tests/test_comprehensive_auto_discovery.py::TestAutoDiscoveryLargeClusters::test_30_node_multi_hub_auto_discovery`
  fails intermittently under `-n 3`.
- Failure message: `Auto-discovery did not converge within the timeout`.

Checks

- Run repeat evidence:
  `uv run python tools/debug/pytest_evidence_harness.py --manifest tools/debug/manifests/auto30_multi_hub_diag_n3_single_repeat5.json`
- Summarize failure signatures:
  `uv run python tools/debug/pytest_log_digest.py --log <failed_run_log_path>`
- Quantify dial saturation:
  `uv run python tools/debug/peer_dial_saturation_report.py --log <failed_run_log_path> --min-targets 20`

Interpretation

- If `saturated_entries` is high and most entries show `due>0` with `selected=0`,
  peer reconciliation is idling because it hit `desired_connected` before
  discovery fully converged.
- For 30-node clusters, peer target counts are typically `29`; use adaptive
  discovery-aware dialing instead of static timeout increases.

Actions

- Verify adaptive peer dialing is active in `mpreg/server.py`:
  `discovery_ratio` and `exploration_slots` in `[DIAG_PEER_DIAL]` loop logs.
- Re-run post-fix stability check:
  `uv run python tools/debug/pytest_evidence_harness.py --manifest tools/debug/manifests/auto30_multi_hub_n3_single_repeat3_postfix.json`
- Keep `50-node` investigation separate; do not overfit 30-node behavior to 50-node time budgets.

## Fabric Churn Recovery (Peer Snapshot Convergence)

Symptoms

- `tests/test_fabric_soak_churn.py::test_fabric_churn_recovery` intermittently
  reports:
  `Expected 11 peers without departed URLs; got 12`.

Checks

- Capture churn state transitions:
  `uv run python tools/debug/fabric_churn_trace.py --output-json artifacts/evidence/fabric_churn_trace.json`
- Run repeat proof with xdist:
  `uv run python tools/debug/pytest_evidence_harness.py --test tests/test_fabric_soak_churn.py::test_fabric_churn_recovery --repeat 5 --run-mode single --timeout 180 --output-dir artifacts/evidence/churn_probe --pytest-arg=-q --pytest-arg=-n --pytest-arg=3`
- Enable peer snapshot diagnostics for focused replay:
  `MPREG_DEBUG_PEER_SNAPSHOT=1`

Interpretation

- If `departed_in_peer_list` remains non-zero, departed peers are still leaking
  into `list_peers` responses.
- If peer count oscillates above expected while GOODBYE coverage is partial, peer
  status may be stale and not being demoted to disconnected quickly enough.

Actions

- Verify peer snapshot gating in `mpreg/server.py`:
  `_should_exclude_disconnected_peer_snapshot`.
- Verify dial-failure demotion path is active for small-cluster `peer_reconcile`
  failures.
- Keep fixes adaptive (gossip/peer-target aware) and avoid global static timeout
  inflation.
