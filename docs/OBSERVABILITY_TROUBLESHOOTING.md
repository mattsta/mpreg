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
- **Core runtime**: `core.connection`, `core.registry`, `core.monitoring`, `core.transport.factory`, `core.transport.adapter`, `core.transport.circuit_breaker`, `core.cache_ops`, `core.cache_pubsub`, `core.location_consistency`, `core.timer`, `core.topic_dependency`
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

- `mpreg client list-peers --url ws://host:<port>`
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
- Check `goodbye_reconnect_grace_seconds`
- Verify catalog deltas are filtered by policy before peer updates

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

## Operational Tips

- Keep default log level at INFO for throughput tests.
- Low-throughput alerts are adaptive; they only trigger after a baseline is established
  and the cluster sustains a meaningful drop relative to that baseline.
- Permissive bridging logs cross-federation connections at INFO by default. Set
  `FederationConfig.log_cross_federation_warnings=True` if you want warnings.
- Use narrow module prefixes to avoid log floods in large clusters.
- When debugging, reduce gossip intervals temporarily and revert after triage.
