# MPREG Production Deployment Guide (Unified Fabric)

This guide describes how to deploy MPREG with the unified fabric control plane
for production use. MPREG uses a single routing fabric for RPC, pub/sub, queues,
and cache. All cross-node routing is catalog-driven and gossip-propagated.

## Architecture Summary

- **Control plane**: RoutingCatalog + gossip + route announcements.
- **Data plane**: UnifiedMessage envelopes over WebSocket/TCP/TLS.
- **Routing**: Path-vector route table with hop budgets and loop prevention.
- **Membership**: Gossip + server lifecycle (STATUS/GOODBYE) for diagnostics.

## Recommended Topologies

MPREG supports multiple deployment shapes; all rely on the same fabric:

1. **Mesh** (small clusters, fastest convergence)
   - Every node peers with a few neighbors.
2. **Hub-and-spoke** (regional scaling)
   - Regional hubs interconnect spokes and share route announcements.
3. **Hybrid** (global scale)
   - Regional hubs plus mesh inside regions.

### Ingress Nodes and Client Discovery

Ingress nodes can run with no local functions and still participate fully in
the fabric routing mesh. Clients can connect to any ingress node and discover
other endpoints via the `cluster_map` RPC, then fail over or load-balance across
advertised URLs.

For HA client access, prefer the cluster-aware client:

```python
from mpreg.client.cluster_client import MPREGClusterClient

client = MPREGClusterClient(
    seed_urls=("ws://ingress-a:<port>", "ws://ingress-b:<port>")
)
await client.connect()
result = await client.call("echo", "ping")
await client.disconnect()
```

The cluster-aware client prefers lower load endpoints and applies latency/error
penalties using recent call history to stabilize selection.

### Resolver Nodes (Discovery Cache)

Resolver nodes serve discovery queries from a cache populated by catalog deltas.
They still participate in the fabric data plane; only discovery queries are
cache-backed.

```python
from mpreg.core.config import MPREGSettings

settings = MPREGSettings(
    name="resolver-1",
    connect="ws://hub:<port>",
    discovery_resolver_mode=True,
    discovery_resolver_seed_on_start=True,
    discovery_resolver_namespaces=("svc.market",),
    discovery_resolver_prune_interval_seconds=30.0,
    discovery_resolver_resync_interval_seconds=300.0,
)
```

### DNS Gateway (Optional)

DNS interoperability can be enabled on ingress or resolver nodes to expose
service discovery through standard DNS clients without changing the data plane.

```python
from mpreg.core.config import MPREGSettings

settings = MPREGSettings(
    name="dns-gateway",
    dns_gateway_enabled=True,
    dns_zones=("mpreg",),
    dns_allow_external_names=False,
    dns_min_ttl_seconds=1.0,
    dns_max_ttl_seconds=60.0,
)
```

If legacy clients omit the zone suffix, set `dns_allow_external_names=True` and
document the names you serve to avoid becoming an unintended open resolver.

### Namespace Policy (Discovery Visibility)

Namespace policies gate discovery visibility by namespace prefix. Discovery
control topics under `mpreg.discovery.*` are exempt from policy filtering so
summary/delta propagation continues even when discovery policies default-deny.
Tenant-aware policies can be enabled with `discovery_tenant_mode=True`, using
`visibility_tenants` in rules and `viewer_tenant_id` in discovery requests.
To bind tenant identity to authenticated connections, configure token
credentials and disable request overrides.

```python
from mpreg.core.discovery_tenant import DiscoveryTenantCredential
from mpreg.core.namespace_policy import NamespacePolicyRule

settings = MPREGSettings(
    discovery_policy_enabled=True,
    discovery_policy_default_allow=True,
    discovery_policy_rules=(
        NamespacePolicyRule(
            namespace="svc.secret",
            owners=("cluster-a",),
            visibility=("secure-cluster",),
            visibility_tenants=("tenant-secure",),
            policy_version="v1",
        ),
    ),
    discovery_tenant_mode=True,
    discovery_tenant_allow_request_override=False,
    discovery_tenant_credentials=(
        DiscoveryTenantCredential(
            tenant_id="tenant-secure",
            token="tenant-secure-token",
            scheme="bearer",
        ),
    ),
)
```

If tenant identity is injected by a trusted gateway, set
`discovery_tenant_header="x-mpreg-tenant"` to read the tenant from headers.

CLI helpers:

```bash
mpreg client namespace-policy export --url ws://node:<port>
mpreg client namespace-policy validate --url ws://node:<port> --rules-file policy.json
mpreg client namespace-policy apply --url ws://node:<port> --rules-file policy.json --enabled
mpreg client namespace-policy audit --url ws://node:<port> --limit 20
mpreg discovery access-audit --url ws://node:<port> --limit 20
```

### Summary Export (Discovery Summaries)

Summary exports publish periodic service summaries for higher-tier aggregators.

```python
settings = MPREGSettings(
    discovery_summary_export_enabled=True,
    discovery_summary_export_interval_seconds=30.0,
    discovery_summary_export_namespaces=("svc.market", "svc.risk"),
    discovery_summary_export_scope="global",
    discovery_summary_export_include_unscoped=True,
    discovery_summary_export_hold_down_seconds=5.0,
    discovery_summary_export_store_forward_seconds=300.0,
    discovery_summary_export_store_forward_max_messages=200,
)
```

Summary resolvers cache summary exports for `summary_query` with `scope=region`
or `scope=global`:

```python
settings = MPREGSettings(
    discovery_summary_resolver_mode=True,
    discovery_summary_resolver_namespaces=("svc.market",),
    discovery_summary_resolver_prune_interval_seconds=30.0,
    discovery_summary_resolver_scopes=("global",),
)
```

### Discovery Backpressure (Rate Limits)

Discovery RPCs can be rate limited per viewer cluster to protect resolvers.
Set the per-minute limit to `0` to disable. When tenant mode is enabled,
rate limits are keyed by tenant identity.

```python
settings = MPREGSettings(
    discovery_rate_limit_requests_per_minute=120,
    discovery_rate_limit_window_seconds=60.0,
    discovery_rate_limit_max_keys=2000,
)
```

### Runbooks

Operational guidance for discovery lag, resolver cache health, policy denials,
and rate-limit tuning lives in `docs/DISCOVERY_RUNBOOKS.md`.

## Core Configuration

Use `MPREGSettings` to configure nodes. The fabric is enabled by default.
If `port` is omitted (or set to `0`), MPREG auto-allocates a free port and
invokes `on_port_assigned` with the chosen value.

```python
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer

def notify_port(port: int) -> None:
    print(f"MPREG_URL=ws://0.0.0.0:{port}")

from mpreg.fabric.link_state import LinkStateMode
from mpreg.fabric.link_state import LinkStateAreaPolicy

peer_url = "ws://10.0.0.2:<peer-port>"

settings = MPREGSettings(
    host="0.0.0.0",
    port=None,
    on_port_assigned=notify_port,
    name="node-a",
    cluster_id="cluster-a",
    peers=[peer_url],
    resources={"cpu", "gpu"},
    fabric_routing_enabled=True,
    fabric_routing_max_hops=5,
    fabric_catalog_ttl_seconds=120.0,
    fabric_route_ttl_seconds=30.0,
    fabric_route_announce_interval_seconds=10.0,
    fabric_raft_request_timeout_seconds=1.0,
    # Optional link-state routing (disabled by default)
    fabric_link_state_mode=LinkStateMode.DISABLED,
    fabric_link_state_ttl_seconds=30.0,
    fabric_link_state_announce_interval_seconds=10.0,
    fabric_link_state_ecmp_paths=1,
    fabric_link_state_area=None,
    fabric_link_state_area_policy=None,
)

server = MPREGServer(settings)
```

Use `LinkStateMode.PREFER` when the cluster topology is stable and you want
deterministic shortest-path routing. Keep `DISABLED` for high-churn clusters.

### Optional Route Security and Policy Controls

```python
from mpreg.fabric.route_keys import RouteKeyRegistry
from mpreg.fabric.route_security import RouteAnnouncementSigner, RouteSecurityConfig
from mpreg.fabric.route_control import RoutePolicy
from mpreg.fabric.route_policy_directory import RouteNeighborPolicy, RoutePolicyDirectory

signer = RouteAnnouncementSigner.create()
registry = RouteKeyRegistry()
registry.register_key(cluster_id="cluster-a", public_key=signer.public_key)

directory = RoutePolicyDirectory(default_policy=RoutePolicy(max_hops=5))
directory.register(
    RouteNeighborPolicy(
        cluster_id="cluster-b",
        policy=RoutePolicy(allowed_tags={"gold"}),
    )
)
export_directory = RoutePolicyDirectory(
    default_policy=RoutePolicy(allowed_destinations=set())
)
export_directory.register(
    RouteNeighborPolicy(
        cluster_id="cluster-b",
        policy=RoutePolicy(allowed_tags={"gold"}),
    )
)

settings = MPREGSettings(
    fabric_route_security_config=RouteSecurityConfig(
        require_signatures=True,
        allow_unsigned=False,
    ),
    fabric_route_signer=signer,
    fabric_route_key_registry=registry,
    fabric_route_neighbor_policies=directory,
    fabric_route_export_neighbor_policies=export_directory,
)
```

### Federation Trust Policy

Cross-cluster routing is governed by `federation_config`:

```python
from mpreg.fabric.federation_config import create_permissive_bridging_config

settings = MPREGSettings(
    port=None,
    cluster_id="cluster-a",
    federation_config=create_permissive_bridging_config("cluster-a"),
)
```

Use strict isolation for production unless clusters are authenticated via
mTLS or a trusted network overlay.

## Fabric Cache and Queue Defaults

Enable built-in cache or queue subsystems when desired:

```python
settings = MPREGSettings(
    enable_default_cache=True,
    enable_default_queue=True,
    enable_cache_federation=True,
    cache_region="us-west",
    cache_latitude=37.7749,
    cache_longitude=-122.4194,
    cache_capacity_mb=2048,
)
```

## Persistence Configuration

Enable the unified persistence layer to restore queues and cache (L2) on
restart; the fabric catalog + route key registry can also snapshot metadata for
faster discovery recovery. SQLite is the default durable backend:

```python
from mpreg.core.persistence.config import PersistenceConfig, PersistenceMode

settings = MPREGSettings(
    enable_default_cache=True,
    enable_default_queue=True,
    persistence_config=PersistenceConfig(
        mode=PersistenceMode.SQLITE,
    ),
)
```

You can also supply a TOML/JSON settings file:

```bash
uv run mpreg server start-config /etc/mpreg/server.toml
```

Or pass persistence flags directly:

```bash
uv run mpreg server start --persistence-mode sqlite --persistence-dir /var/lib/mpreg
```

## Observability

Logging uses loguru with module-filtered debug output:

```python
settings = MPREGSettings(
    log_level="INFO",
    log_debug_scopes=("fabric.router", "goodbye"),
)
```

- Keep INFO for production throughput.
- Enable targeted module prefixes only during investigations.

Metrics are generated by `mpreg/core/statistics.py` and can be exported via
your existing monitoring pipeline (Prometheus/OTel integration is external).

Persistence snapshots can be inspected at `/metrics/persistence` on the
monitoring endpoint.

See `docs/OBSERVABILITY_TROUBLESHOOTING.md` for detailed workflows.

Route tracing is available at the monitoring endpoint:
`/routing/trace?destination=<cluster_id>`.

## Performance + Scale Validation

Baseline soak/churn validation lives in `tests/test_fabric_soak_churn.py`.

- Latest validated run (2026-02-08):
  - Full suite under parallelism: `uv run pytest -n 3` -> `1960 passed, 1 xfailed`.
  - Churn recovery stability probe:
    `uv run python tools/debug/pytest_evidence_harness.py --test tests/test_fabric_soak_churn.py::test_fabric_churn_recovery --repeat 5 --run-mode single --timeout 180 --output-dir artifacts/evidence/churn_probe --pytest-arg=-q --pytest-arg=-n --pytest-arg=3`
    -> `5/5` pass with evidence report in
    `artifacts/evidence/churn_probe/<timestamp>/evidence_report.txt`.
- Standard soak/churn run:
  `uv run pytest tests/test_fabric_soak_churn.py -n 0 -vs`

## Operational Checklist

- Ensure unique `cluster_id` per cluster.
- Use the port allocator in tests; avoid fixed ports.
- Keep catalog TTL > gossip interval for convergence.
- Set hop budgets to bound cross-cluster fan-out.
- Validate routing convergence with the fabric integration suites.

## Example: Two Federated Clusters

```python
from mpreg.core.config import MPREGSettings
from mpreg.fabric.federation_config import create_permissive_bridging_config
from mpreg.server import MPREGServer

from mpreg.core.port_allocator import port_range_context

with port_range_context(2, "servers") as ports:
    cluster_a_url = f"ws://127.0.0.1:{ports[0]}"
    cluster_b_url = f"ws://127.0.0.1:{ports[1]}"
    cluster_a = MPREGServer(
        MPREGSettings(
            port=ports[0],
            name="cluster-a-node",
            cluster_id="cluster-a",
            peers=[cluster_b_url],
            federation_config=create_permissive_bridging_config("cluster-a"),
        )
    )

    cluster_b = MPREGServer(
        MPREGSettings(
            port=ports[1],
            name="cluster-b-node",
            cluster_id="cluster-b",
            peers=[cluster_a_url],
            federation_config=create_permissive_bridging_config("cluster-b"),
        )
    )
```

This produces a shared fabric catalog with cross-cluster routing and path-vector
route announcements. RPC calls with `target_cluster` are routed across the fabric
without legacy federation modules.
