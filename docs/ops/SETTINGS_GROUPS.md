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

### Cache STRONG (majority-commit put)

Default **off**. When enabled, `GlobalCacheManager` STRONG put uses a
majority-commit barrier (see `docs/CACHING_SYSTEM.md`).

| Setting | Default | Notes |
| ------- | ------- | ----- |
| `cache_strong_enabled` | `false` | Master switch; off → error `1012` |
| `cache_strong_replica_factor` | `3` | Target replica set size |
| `cache_strong_min_replicas` | `3` | Fail `1015` if live eligible below this |
| `cache_strong_lab_single_node` | `false` | Lab-only single-node path |
| `cache_strong_prepare_timeout_s` | `2.0` | Prepare barrier |
| `cache_strong_commit_timeout_s` | `2.0` | Commit barrier |
| `cache_strong_pending_ttl_s` | `30.0` | Pending TTL backstop |

### Management audit

| Setting | Default | Notes |
| ------- | ------- | ----- |
| `mgmt_audit_path` | unset | Optional local JSONL durability |
| `mgmt_audit_shared_enabled` | `false` | Cluster G-Set shared audit store |
| `mgmt_audit_shared_max_entries` | `2000` | Per-store retention bound |
| `mgmt_audit_shared_gossip_targets` | `3` | Epidemic fan-out |
| `mgmt_audit_shared_reconcile_interval_s` | `2.0` | Digest/PULL anti-entropy interval |

`GET /mgmt/v1/audit?scope=cluster` requires shared enabled; otherwise use
`scope=local` (default). Design: `docs/SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md`.

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
