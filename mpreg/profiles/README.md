# MPREG Settings Profiles

Starter TOML files for `mpreg server start-config <path>` or
`MPREGSettings.from_path(...)`.

| Profile | Use when |
|---------|----------|
| `dev.toml` | Local hacking, minimal systems |
| `single-node.toml` | One process with cache + queue |
| `cluster.toml` | Multi-node same cluster (set `connect`/`peers`) |
| `federated.toml` | Cross-cluster fabric member (path-vector; **requires route signatures + summary HMAC**) |
| `soft-rt.toml` | Soft real-time: tighter hop/TTL, LS PREFER, fail-closed client deadlines |
| `discovery-resolver.toml` | Dedicated discovery resolver |

## Client modality defaults (pair with server profile)

| Profile | Recommended `RpcExecutionMode` | Notes |
|---------|----------------------------------|-------|
| `dev.toml` / `cluster.toml` | `M1_ASYNC` via `default_ha_policy()` | Throughput retries OK |
| `federated.toml` | `M1_ASYNC` (HA) or explicit policy | Multi-hop; set deadlines for long DAGs |
| `soft-rt.toml` | `M2_SOFT_RT` | Shared wall deadline; no retry past remaining |

Server profiles encode fabric/routing knobs. Client deadlines live on
`ClientCallPolicy` / `MPREGClient(..., call_policy=...)` so the same binary
can mix modalities per call.

Copy a profile, set `name`, `cluster_id`, ports, and secrets
(`monitoring_auth_token`) before production use.

## Security note (ERG-T10-13)

`soft-rt.toml` and `cluster.toml` optimize latency defaults and may omit federated control-plane hardening (route signatures / gossip HMAC / discovery policy). Treat them as single trust-domain profiles. For multi-cluster untrusted links use `federated.toml` (and rotate `change-me` secrets; enable `discovery_policy_enabled`).
