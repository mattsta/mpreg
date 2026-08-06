# MPREG Settings Groups

`MPREGSettings` is large by design (one process can enable every subsystem).
Treat knobs as **groups** and start from a profile.

## Profiles

```bash
mpreg profile list
mpreg server start-config $(mpreg profile path dev)
mpreg config-check mpreg/profiles/cluster.toml
```

| Profile              | Intent                                               |
| -------------------- | ---------------------------------------------------- |
| `dev`                | Local single node, minimal systems                   |
| `single-node`        | One process with cache + queue                       |
| `cluster`            | Multi-node same `cluster_id` (set `connect`/`peers`) |
| `federated`          | Cross-cluster path-vector emphasis                   |
| `discovery-resolver` | Dedicated discovery cache/export node                |

## Groups

### Identity

`name`, `cluster_id`, `host`, `port`, `advertised_urls`, `resources`

### Monitoring

`monitoring_enabled`, `monitoring_port`, `monitoring_host`,
`monitoring_enable_cors` (default **false**), `monitoring_auth_token`

### Fabric routing

`fabric_routing_enabled`, TTLs, announce intervals, link-state mode,
route policies, route security / key registry

### Discovery

resolver modes, summary export, **`discovery_summary_signing_secret`**,
namespace policy, tenant mode, rate limits

### Data systems

`enable_default_cache`, `enable_default_queue`, `enable_cache_federation`,
cache geo/capacity

### Persistence

`persistence_config` (`off` via absence, or `memory` / `sqlite`)

### DNS interop

`dns_gateway_enabled`, ports, zones, viewer identity

## Production checklist

1. `monitoring_auth_token` set; CORS off
2. Summary export signed if enabled
3. Persistence mode explicit if queues/cache must survive restart
4. `mpreg config-check` exits 0
5. `mpreg doctor` against live monitoring URL

## Doctor checks

`mpreg doctor` probes (when `MPREG_MONITORING_URL` is set):

- `/health` or `/health/summary`
- `/metrics/prometheus`
- `/routing/decisions?limit=5`
- `/mgmt/v1/cluster` and `/mgmt/v1/catalog`

Exit non-zero if any probe fails. Optional `--data-plane` smokes RPC via `MPREG_URL` / `--rpc-url` (USE-T10-02). Optional `--deep` probes raft/link-state. Pass token via `MPREG_MONITORING_TOKEN` or
`--token` when auth is enabled.

## Config check

```bash
mpreg config-check path/to/settings.toml
mpreg config-check $(mpreg profile path federated)
```

Validates loadable settings and common footguns (CORS on with public bind,
missing monitoring token when monitoring enabled in production profiles).
