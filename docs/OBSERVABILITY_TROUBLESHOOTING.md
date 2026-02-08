# Observability and Troubleshooting

This guide covers logging module filters, monitoring endpoints, and a practical
troubleshooting workflow for the unified fabric control plane.

## Logging

MPREG uses loguru with module-based debug filtering. Configure it with
`MPREGSettings.log_level` and `MPREGSettings.log_debug_scopes` or through the
CLI.

If `log_level=DEBUG`, all debug logs are emitted regardless of filters.
When debug scopes are set, any log whose module name starts with one of the
prefixes will be emitted.

Example (server settings):

```python
from mpreg.core.config import MPREGSettings

settings = MPREGSettings(
    log_level="INFO",
    log_debug_scopes=("server", "fabric.gossip", "fabric.router"),
)
```

CLI example:

```bash
uv run mpreg --verbose server start --port <port> --cluster-id demo
```

### Known Module Prefixes

- **Fabric control plane**: `fabric.gossip`, `fabric.peer_directory`, `fabric.membership`, `fabric.router`, `fabric.link_state`, `fabric.connection`, `fabric.consensus`, `fabric.resilience`, `fabric.alerting`, `fabric.monitoring`, `fabric.auto_discovery`, `fabric.hubs`, `fabric.hub_registry`, `fabric.hub_hierarchy`, `fabric.queue`, `fabric.queue_federation`, `fabric.cache`, `fabric.federation`, `fabric.blockchain`, `fabric.metrics`
- **Core runtime**: `core.connection`, `core.rpc_registry`, `core.monitoring`, `core.transport.factory`, `core.transport.adapter`, `core.transport.circuit_breaker`, `core.cache_ops`, `core.cache_pubsub`, `core.location_consistency`, `core.timer`, `core.topic_dependency`
- **Client**: `client`, `client.api`, `client.pubsub`
- **Queue/Cache**: `queue`, `queue.manager`, `cache`, `cache.store`
- **Consensus/Raft**: `raft.impl`, `raft.rpc`, `raft.storage`, `raft.tasks`
- **Other**: `server`, `cluster`, `connection.events`, `port_allocator`, `goodbye`, `tasks`, `mpreg`

If a component lacks a module prefix in logs, it likely isn't emitting from
MPREG code paths.

## Monitoring Endpoints

Monitoring endpoints are controlled by `monitoring_enabled`, `monitoring_host`,
and `monitoring_port`. When `monitoring_port` is omitted, an available port is
auto-allocated to avoid collisions (from the `monitoring` port category).

Example:

```bash
uv run mpreg server start --port <port> --monitoring-port <port>
```

Useful transport endpoints:

- `/transport/endpoints` (or `mpreg monitor transport-endpoints`) shows the
  adapter-assigned endpoints when `base_port=0` is used.
- `/dns/metrics` (or `mpreg monitor dns`) reports DNS gateway status, ports, and query statistics.
- `mpreg monitor dns-watch` streams DNS metrics on a polling interval.
  Useful persistence endpoint:
- `/metrics/persistence` surfaces snapshot status (enabled/mode/last save+restore).
  CLI shortcut: `uv run mpreg monitor persistence --url http://127.0.0.1:<port>`
  Useful admin utilities:
- `uv run mpreg monitor endpoints --url http://127.0.0.1:<port>` to list
  all monitoring endpoints.
- `uv run mpreg monitor metrics --system unified --url http://127.0.0.1:<port>`
  for one-shot metrics pulls.
- `uv run mpreg monitor persistence-watch --url http://127.0.0.1:<port>`
  for periodic snapshot status.
- `uv run mpreg monitor health --summary --url http://127.0.0.1:<port>` for
  a quick health snapshot.
- `uv run mpreg monitor metrics-watch --system transport --url http://127.0.0.1:<port>`
  to watch specific subsystem metrics.
- `uv run mpreg monitor status --url http://127.0.0.1:<port>` for a compact
  admin summary (health + persistence + transport + unified metrics).

## Troubleshooting Flow

### 1) Peer Discovery Issues

Symptoms: missing peers, incomplete catalog, partial discovery.

Actions:

- `mpreg client list-peers --url ws://host:<port> --scope region` (optional
  `--cluster-id` to filter; `--target-cluster` for federated routing)
- `mpreg client call cluster_map --url ws://host:<port>` to inspect advertised URLs
  and load metrics seen by the ingress node.
- `http://host:<monitoring-port>/discovery/cache` (resolver + summary cache),
  `/discovery/summary` (summary export), `/discovery/policy`, `/discovery/lag`
  for discovery health.
- `mpreg discovery status --url http://host:<monitoring-port>` for a consolidated
  discovery status snapshot.
- `mpreg report namespace-health --url http://host:<monitoring-port>` for
  per-namespace export health.
- `mpreg report export-lag --url http://host:<monitoring-port>` for summary lag.
- Verify gossip interval and catalog TTL values
- Enable `fabric.router` and `mpreg` module prefixes to see catalog updates

### 2) RPC Routing Failures

Symptoms: `CommandNotFoundException`, version mismatch, unexpected routing.

Actions:

- Confirm `function_id` and `version_constraint` on registration and calls
- Verify resource filters (`locs`) are a subset of advertised resources
- Enable `fabric.router` to inspect routing decisions and targets

### 3) GOODBYE / Reconnect Storms

Symptoms: peers reappear after departure, repeated reconnections.

Actions:

- Enable `goodbye` module prefix
- Enable peer snapshot diagnostics:
  `MPREG_DEBUG_PEER_SNAPSHOT=1`
- Check `goodbye_reconnect_grace_seconds`
- Verify catalog deltas are filtered by policy before peer updates
- Capture churn trace evidence:
  `uv run python tools/debug/fabric_churn_trace.py --output-json artifacts/evidence/fabric_churn_trace.json`
- Inspect whether `departed_in_peer_list` and `departed_in_directory` converge
  to zero in the generated JSON report.

### 4) Cross-Cluster Routing Errors

Symptoms: hop limit exceeded, loops, or missing routes.

Actions:

- Inspect `routing_path` and `federation_path` headers
- Verify `hop_budget` and route TTLs
- Enable `fabric.router` and `fabric.route_control` to review path-vector updates
- If link-state mode is enabled, check `fabric.link_state` for adjacency updates
- If route signing is enabled, verify advertiser public keys and signature policy
- Use `RouteTable.explain_selection()` to capture a route trace (scores, tiebreakers,
  and filtered candidates).
- Or query the monitoring endpoint: `/routing/trace?destination=<cluster_id>` or
  use `mpreg monitor route-trace --destination <cluster_id>`.
- Use `/routing/link-state` (or `mpreg monitor link-state`) to inspect
  `area_mismatch_rejects` and `allowed_areas` when link-state areas are configured.
- Check `RouteTable.stats` for hold-down rejections and withdrawals
- Review suppression counters if routes are flapping
- Use server status `route_metrics` to confirm suppression/withdrawals, active routes,
  and convergence timing

## Proof-First Debug Workflow

Use this loop for distributed-system flakes instead of random retries:

1. Capture repeat evidence with fixed manifests.
   `uv run python tools/debug/pytest_evidence_harness.py --manifest <manifest.json>`
2. Extract deterministic failure signatures.
   `uv run python tools/debug/pytest_log_digest.py --log <run.log>`
3. Add scoped diagnostics and rerun.
   `MPREG_DEBUG_PEER_DIAL=1`, `MPREG_DEBUG_PEER_DIAL_POLICY=1`,
   `MPREG_DEBUG_GOSSIP_SCHED=1`
4. Quantify control-loop behavior.
   `uv run python tools/debug/analyze_peer_dial_diag.py --log <run.log>`
   `uv run python tools/debug/peer_dial_saturation_report.py --log <run.log>`
5. Implement adaptive fixes in platform logic, not global timeout tuning.
6. Re-validate with repeat manifests, then run broader subset/full-suite checkpoints.

Evidence artifacts:

- `tools/debug/pytest_evidence_harness.py` writes a timestamped run directory
  with per-run logs plus `evidence_report.json` and `evidence_report.txt`.
- Keep artifact roots under `artifacts/evidence/` and retain the exact manifest
  used for reproducibility.

## Audit/Test/Debug Suite Catalog

Primary orchestration:

- `tools/debug/pytest_evidence_harness.py`
  Repeatable pytest orchestration (single or batch mode) with timeouts and
  structured reports.
- `tools/debug/pytest_stall_watchdog.py`
  Watches long-running suites and records stalls for dead-run detection.
- `tools/debug/pytest_log_digest.py`
  Extracts deterministic failure signatures from noisy logs.
- `tools/debug/pytest_collection_order_audit.py`
  Captures collection order to audit xdist/sharding side effects.

Membership/churn deep dives:

- `tools/debug/fabric_churn_trace.py`
  Traces peer list, peer directory, and departed-url convergence under churn.
- `tools/debug/goodbye_reentry_debug.py`
  Focused GOODBYE/re-entry lifecycle diagnostics.

Discovery/convergence:

- `tools/debug/auto_discovery_multi_hub_debug.py`
- `tools/debug/auto_discovery_topology_probe.py`
- `tools/debug/auto_discovery_topology_summary.py`
- `tools/debug/analyze_auto_discovery_diag_log.py`

Dial scheduler analysis:

- `tools/debug/analyze_peer_dial_diag.py`
- `tools/debug/peer_dial_saturation_report.py`

Raft/consensus/load:

- `tools/debug/live_raft_cluster_size_probe.py`
- `tools/debug/live_raft_high_load_probe.py`
- `tools/debug/scalability_boundary_probe.py`

Manifest library:

- Versioned manifests live in `tools/debug/manifests/`.
- Prefer manifests over ad-hoc command lines for repeatability and low-fragility
  debugging.

## Operational Tips

- Keep default log level at INFO for throughput tests.
- Low-throughput alerts are adaptive; they only trigger after a baseline is established
  and the cluster sustains a meaningful drop relative to that baseline.
- Permissive bridging logs cross-federation connections at INFO by default. Set
  `FederationConfig.log_cross_federation_warnings=True` if you want warnings.
- Use narrow module prefixes to avoid log floods in large clusters.
- When debugging, reduce gossip intervals temporarily and revert after triage.
