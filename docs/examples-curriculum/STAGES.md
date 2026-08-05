# Growth Cycle Stages (Phases A–D)

Each phase has **scope**, **POC apps**, **runner/docs deliverables**, and
**exit criteria**. Status is tracked in [TRACKER.md](TRACKER.md).

---

## Phase A — Foundation + L0/L1 (scaffold & smoke)

### Scope

- Curriculum docs (this tree).
- `mpreg/examples/apps/` layout + `_shared` runtime.
- Central runner: list / describe / run / smoke / suite.
- L0 apps: `hello_rpc`, `hello_cluster`, `hello_trace`.
- L1 apps: `ha_client_failover`, `job_queue_worker`.
- Wire smoke into scripts + CLI.
- Operate guide (configure/start/run/manage).

### POC apps (must ship)

| ID | Lesson | Asserts |
|----|--------|---------|
| `hello_rpc` | Register + call + dependency-style chain | Numeric/score invariant |
| `hello_cluster` | 2 peers, resource `locs` | Cross-resource DAG result |
| `hello_trace` | Correlation id / monitoring timeline | Timeline length ≥ N |
| `ha_client_failover` | Multi-seed cluster client | Call succeeds with 2 seeds |
| `job_queue_worker` | At-least-once queue worker | Expected deliveries |

### Runner / docs

- `mpreg-example list|run|smoke|suite|describe|path` (also `mpreg examples …`)
- `tests/examples_apps/` pytest live mains
- `docs/examples-curriculum/*`
- Updates: BOOK, EXAMPLES, `mpreg/examples/README.md`
- `scripts/run_example_apps_smoke.sh` / `suite.sh` via entrypoint

### Exit criteria

- [x] `uv run mpreg-example smoke` exit 0  
- [x] Each Phase A app has README + `run.py` entry  
- [x] Dynamic ports only  
- [x] OPERATE.md covers local lifecycle  
- [x] Pytest markers `example_smoke` / `example_suite`  

---

## Phase B — Moderate composition (L2)

### Scope

- L2 product apps composing 2–4 planes.
- Extract reusable “intake” patterns from `real_world_examples.py` / tier3.
- Optional settings.toml samples for long-lived local runs.

### POC apps

| ID | Story | Planes |
|----|--------|--------|
| `order_intake` | Create order → cache idempotency key → notify → queue fulfill | RPC + cache + pubsub + queue |
| `media_pipeline` | Multi-stage ETL-style RPC across resources | RPC multi-node |
| `feature_flag_mesh` | Flags in cache + fabric gossip | Cache federation |
| `webhook_dispatcher` | Event in, durable out | Pubsub + queue + monitoring |
| `config_reload_live` | Restart retains catalog/routes | Persistence + fabric snapshot |

### Exit criteria

- [x] `order_intake` in suite  
- [x] Suite documents which planes each app touches  
- [x] All Phase B POC apps shipped  
- [x] Production exit ramps reference profiles + HA client  

---

## Phase C — Complex mesh operators (L3)

### Scope

- Multi-cluster fabric, route policy/security hooks, chaos modes.
- Teaching apps for partitions and discovery — **honest limits**.

### POC apps

| ID | Story | Operators |
|----|--------|-----------|
| `multi_region_shop` | Two clusters, federated RPC | Fabric bridging, cluster_id, locs |
| `signed_route_border` | Key rotation under traffic | Route security |
| `partition_safe_counter` | Minority cannot commit | Raft teaching |
| `discovery_join` | Node joins via discovery | Membership |
| `chaos_checkout` | Fault inject + deadlines | FaultInjector + call policy |

### Exit criteria

- [x] `multi_region_shop` green in suite  
- [x] Chaos apps fail closed with clear messages  
- [x] All Phase C POC apps shipped  
- [x] Cross-links to FABRIC_* and DISCOVERY runbooks  

---

## Phase D — World reference (L4)

### Scope

- One flagship: `global_edge_control_plane` (name may finalize in tracker).
- Edge POPs + regional hubs + fabric + monitoring + documented failure domains.
- Operator tour: doctor, decisions, prometheus, profiles.

### Exit criteria

- [x] Flagship README + runnable `global_edge_control_plane`  
- [x] Included in full suite (35 apps)  
- [x] Explicit non-claims section  

---

## Phase E — Feature-catalog gap fill + depth (active)

See [PROJECT_PLAN.md](./PROJECT_PLAN.md) for waves E0–E16.

### Shipped in E1–E14 (+ F verticals start)

| ID | Lesson |
|----|--------|
| `cache_atomic_ops` | CAS / incr / structures / namespace bulk |
| `namespace_policy_gate` | ns validate/apply/status/export/audit |
| `plane_dns` | DNS register + UDP/TCP resolve |
| `unified_client_tour` | MPREGClient four-plane façade |
| `pubsub_request_reply` | publish_with_reply |
| `discovery_watch_summary` | catalog_watch + summary query/watch |
| `fabric_graph_resilience` | graph paths + circuit breaker |
| `cache_event_bus` | cache ops → topic notifications |
| `job_queue_dlq` | poison retries → DLQ |
| `rpc_versioned_topic` | function_id + version_constraint |
| `client_auth_token` | monitoring bearer + client auth_token |
| `chaos_transport` | FaultInjector skew/dup/reorder/drop |
| `ops_cli_tour` | mpreg CLI + usability friction |
| `hello_queue` / `hello_dns` | L0 hellos |
| `billing_ledger` / `notification_fanout` / `inventory_reserve` | product verticals |
| `rpc_deadline_budget` / `topic_queue_bridge` / `multi_region_dns_policy` | composition |

**Registry total: 56 apps.** API friction log: [API_FRICTION.md](./API_FRICTION.md).

### Exit criteria (Phase E)

- [x] ≥50 shipped apps (**56**)  
- [x] Depth contract on every app  
- [x] Prioritized FEATURE_CATALOG gaps closed or non-claimed  
- [x] E1–E14 gap apps green via `mpreg-example run`  
- [ ] E15 full suite + coverage report  
- [>] E16 catalog doc sync  

---

## Continuous practices (all phases)

1. **Demo-as-test** — smoke on PR; suite on main/nightly as capacity allows.  
2. **No fixed ports** in apps.  
3. **Tracker updates** when apps land.  
4. **Prefer extending apps/** over growing `real_world_examples.py`.  
5. **CLI is the user interface** — `uv run mpreg-example` / `mpreg examples` / `mpreg demo` only (never `python -m`).

## Dependency graph (implementation)

```text
Phase A ──► Phase B ──► Phase C ──► Phase D
   │            │            │
   └── shared runtime + runner (required by all)
```
