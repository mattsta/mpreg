# App Catalog

Status: `shipped` | `partial` | `planned`

Levels:

- **L0** `00_getting_started`
- **L1** `01_simple`
- **L2** `02_moderate`
- **L3** `03_complex`
- **L4** `04_world`

Kinds: `product` | `plane` | `integration` | `legacy`

**Unified runner (entrypoints only):**

```bash
uv run mpreg-example list
uv run mpreg-example run <id>
uv run mpreg-example smoke
uv run mpreg-example suite
uv run mpreg-example demo tier1|tier2|tier3|quick|all_planes|product_vertical
uv run mpreg examples …   # same runner
uv run mpreg demo tier1   # delegates to mpreg-example
```

**Never** `python -m` / `uv run python`.

## Matrix (40 shipped)

| ID | Level | Kind | Primary lesson | Systems |
|----|-------|------|----------------|---------|
| `hello_rpc` | L0 | product | Register + RPC chain | rpc |
| `hello_cluster` | L0 | product | Multi-node resources + DAG | rpc, cluster |
| `hello_trace` | L0 | product | Correlation timeline | monitoring |
| `hello_pubsub` | L0 | product | Topic wildcards + fan-out | pubsub |
| `hello_cache` | L0 | product | Cache put/get | cache |
| `hello_ports` | L0 | product | Dynamic port allocation | rpc, ports |
| `ha_client_failover` | L1 | product | Multi-seed HA client | rpc, ha-client |
| `job_queue_worker` | L1 | product | At-least-once + quorum | queue |
| `url_shortener_rpc` | L1 | product | CRUD-ish RPC + cache | rpc, cache |
| `sensor_ingest_pubsub` | L1 | product | Multi-pattern sensor bus | pubsub |
| `session_cache` | L1 | product | Session TTL put/rotate | cache |
| `auto_port_bootstrap` | L1 | legacy | OS-assigned ports + join | rpc, cluster, ports |
| `plane_rpc` | L1 | plane | Full RPC plane tour | rpc |
| `plane_pubsub` | L1 | plane | Full pubsub plane tour | pubsub |
| `plane_queue` | L1 | plane | Full queue plane tour | queue |
| `plane_cache` | L1 | plane | Full cache plane tour | cache |
| `plane_fabric` | L1 | plane | Full fabric plane tour | fabric |
| `plane_monitoring` | L1 | plane | Full monitoring plane tour | monitoring |
| `cache_atomic_ops` | L1 | plane | CAS / incr / structures / ns bulk | cache |
| `namespace_policy_gate` | L1 | plane | Validate/apply/status/export/audit | namespace |
| `plane_dns` | L1 | plane | DNS register/list/describe/resolve | dns, discovery |
| `unified_client_tour` | L1 | product | Four-plane MPREGClient façade | rpc, cache, queue |
| `pubsub_request_reply` | L1 | product | publish_with_reply round-trip | pubsub |
| `order_intake` | L2 | product | RPC+cache+pubsub+queue | multi-plane |
| `media_pipeline` | L2 | product | Multi-stage ETL RPC | rpc, cluster |
| `feature_flag_mesh` | L2 | product | Federated L4 flags | cache, fabric |
| `webhook_dispatcher` | L2 | product | Events → durable egress | pubsub, queue |
| `config_reload_live` | L2 | legacy | Restart durability | cache, queue, persistence |
| `rpc_plus_cache` | L2 | integration | RPC output cached | rpc, cache |
| `pubsub_plus_queue` | L2 | integration | Fan-out → queue | pubsub, queue |
| `cache_plus_federation` | L2 | integration | L4 cache federation | cache, fabric |
| `ml_inference_mesh` | L2 | product | Router + vision/NLP | rpc, cluster |
| `multi_region_shop` | L3 | product | Two-cluster fabric RPC | fabric, multi-cluster |
| `signed_route_border` | L3 | legacy | Signed routes + policy + rotation | fabric, security |
| `partition_safe_counter` | L3 | product | Majority vs minority quorum | consensus, chaos |
| `discovery_join` | L3 | product | Third node join | discovery, cluster |
| `chaos_checkout` | L3 | product | Deadlines + fail-closed partition | chaos, rpc |
| `fabric_snapshot_restart` | L3 | legacy | Fabric snapshot across restart | fabric, persistence |
| `tier3_expansion` | L3 | legacy | Full multi-system expansion | multi-plane |
| `global_edge_control_plane` | L4 | product | Hub + US/EU edges + timeline | fabric, monitoring |

## Legacy → unified mapping

| Legacy path / CLI | Canonical app id |
|-------------------|------------------|
| `tier1_single_system_full --system rpc` | `plane_rpc` |
| `… pubsub/queue/cache/fabric/monitoring` | `plane_*` |
| `tier2_integrations` | `rpc_plus_cache` + `pubsub_plus_queue` + `cache_plus_federation` |
| `tier3_full_system_expansion` | `tier3_expansion` |
| `quick_demo` / `simple_working_demo` | `demo quick` / `plane_rpc` |
| `auto_port_cluster_bootstrap` | `auto_port_bootstrap` |
| `persistence_restart_demo` | `config_reload_live` |
| `fabric_route_security_demo` | `signed_route_border` |
| `fabric_snapshot_restart_demo` | `fabric_snapshot_restart` |
| `mpreg demo tier1\|tier2\|tier3\|all` | `mpreg-example demo …` |

Legacy `.py` files remain as implementation backends for plane/integration wrappers; **user-facing execution is only via entrypoints**.

## Bundles

| Bundle | Command | Contents |
|--------|---------|----------|
| **smoke** | `mpreg-example smoke` | L0 hellos + ha_client + job_queue (8) |
| **suite** | `mpreg-example suite` | All 40 shipped apps |
| **tier1** | `mpreg-example demo tier1` | Core `plane_*` (rpc…monitoring) |
| **tier2** | `mpreg-example demo tier2` | Three integration apps |
| **tier3** | `mpreg-example demo tier3` | `tier3_expansion` |
| **quick** | `mpreg-example demo quick` | `hello_rpc` + `plane_rpc` |
| **product_vertical** | `mpreg-example demo product_vertical` | Learning path slice |
| **all_planes** | `mpreg-example demo all_planes` | Core planes + dns + atomic + ns policy |

## Pytest

```bash
uv run pytest tests/examples_apps -m example_smoke
uv run pytest tests/examples_apps -m example_suite
uv run pytest tests/examples_apps
```

Markers: `example_apps`, `example_smoke`, `example_suite`.

## Path on disk

```text
mpreg/examples/apps/
  _shared/           # runtime, registry, runner
  00_getting_started/
  01_simple/
  02_moderate/
  03_complex/
  04_world/
```
