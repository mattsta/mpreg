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

## Phase E — Feature-catalog gap fill + depth (**COMPLETE**)

See [PROJECT_PLAN.md](./PROJECT_PLAN.md) for waves E0–E16. All exit criteria met
(E15 suite + E16 catalog sync closed in later waves).

**Registry after E:** 56 apps → continued in F/G/H/I.

---

## Phase F — Breadth to 70 (**COMPLETE**)

Verticals, hellos, oracles, second L4 (`multi_pop_edge_mesh`), CI/BOOK/OPERATE
sync. **70 apps.**

---

## Phase G — Platform DX + observability (**COMPLETE**)

High/Med friction F1/F9/F18/F20/F21; `ExampleProbe`; ≥8 apps latency/throughput.

---

## Phase H — FQN + Med friction + universal obs (**COMPLETE**)

FQN namespace deny (`mpreg.*`); F2–F8/F17; default-on probe for every app.

---

## Phase I — Residual polish + FQN teach + docs (**COMPLETE**)

| ID | Lesson |
|----|--------|
| `rpc_fqn_namespace` | Bare→FQN, `mpreg.*` deny, `bound_rpc_namespace` |
| F13 | `route_not_found` fabric-bridge messaging |
| F15 | Deadline client fail-closed ≠ handler preemption (docs) |
| F16 | `list_port_categories` + unknown-category errors |
| F19 | RaftOracle dual-leader fail-fast at `observe_role` |

**Registry total: 71 apps.** Living plan: [PROJECT_PLAN.md](./PROJECT_PLAN.md).

### Exit criteria (Phase I)

- [x] FQN curriculum proof app green  
- [x] Residual Info friction closed or honest non-claim  
- [x] FEATURE_CATALOG / TRACKER / APP_CATALOG current  
- [x] unit + full suite green at 71  

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

## Phase J — Productize F10–F12 + catalog residual teach (COMPLETE)

Linear continuation after Phase I. Productized residual Info friction:

1. **F11** — optional WS handshake auth (`rpc_auth_token`)
2. **F12** — turnkey dev TLS PEMs + server `wss://` listener
3. **F10** — live admission control via mgmt drain/detach (not only lab injector)
4. Catalog residual teach apps (disco resolver/audit, queue federation, transport
   health/TCP/multi-protocol, blockchain message types)

Exit: **78** suite apps; FEATURE_CATALOG residual gaps closed or honest depth non-claims.

## Phase K — Residual depth + catalog partials (COMPLETE)

Linear continuation after Phase J. Closed the depth non-claims and high-value
FEATURE_CATALOG partials:

1. `disco.signatures` + gossip HMAC
2. rpc.describe / rpc.report inventory
3. client.trace + mon.trace_bind
4. tx.correlation + chaos.no_loop
5. CERT_REQUIRED mTLS mesh
6. Packet-loss plane model composed with live drain
7. Blockchain hub settlement path

Exit: **85** suite apps; program idle pending new charter.

