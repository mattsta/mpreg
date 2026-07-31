# MPREG Settings Profiles

Starter TOML files for `mpreg server start-config <path>` or
`MPREGSettings.from_path(...)`.

| Profile | Use when |
|---------|----------|
| `dev.toml` | Local hacking, minimal systems |
| `single-node.toml` | One process with cache + queue |
| `cluster.toml` | Multi-node same cluster (set `connect`/`peers`) |
| `federated.toml` | Cross-cluster fabric member |
| `discovery-resolver.toml` | Dedicated discovery resolver |

Copy a profile, set `name`, `cluster_id`, ports, and secrets
(`monitoring_auth_token`) before production use.
