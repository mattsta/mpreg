# Curriculum Examples — Living Project Plan

**Last updated:** 2026-08-05 (Phase N — Low DX F22/F23 + planning scrub → 96 apps; 0 open friction)  
**Owner drive:** sequential iterative completion of curriculum **and** long-term
platform correctness: API unification, operator ergonomics, latency/throughput
observability in every example path — **not** batch-and-stop.

Legend: `[x]` done · `[~]` partial · `[>]` in progress · `[ ]` not started

---

## 1. North-star goals

| # | Goal | Success measure | Status |
|---|------|-----------------|--------|
| G1 | **Full-power curriculum** — every honest platform surface taught at the lowest level that can prove it | FEATURE_CATALOG rows `shipped` (or explicit non-claim if product gap) | [x] F10–F12 productized in Phase J; remaining internal-only surfaces taught or honest non-claim |
| G2 | **Depth contract on every app** | ≥2 scenarios; L0 ≥3 ensures; L1+ ≥5 ensures; `app_run` summary | [x] 0 thin; new apps meet contract on land |
| G3 | **Entrypoint-only UX** | `uv run mpreg-example` / `mpreg examples` / `mpreg demo` only | [x] |
| G4 | **Demo-as-test** | live `main()` via pytest + smoke/suite green | [x] unit + full suite green |
| G5 | **Feature join integrity** | every app tagged; pytest `test_every_app_has_feature_tags` | [x] |
| G6 | **50–70 apps when catalog requires** | expand matrix beyond thin 35; no bulk-empty shells | [x] **70** shipped (band complete) |
| G7 | **Living plan** | this file + TRACKER + friction log updated every commit slice | [x] |
| G8 | **Commit-as-you-go** | clean what/why/how messages per layer | [x] practice; continue |
| G9 | **API discovery / usability** | curriculum apps surface edge cases + improvement candidates | [x] F1–F21 logged; **Phase G fixes platform** |
| G10 | **Platform DX unification** | High/Med friction fixed in code (not only mitigated in apps) | [x] F1/F9/F18/F20/F21 |
| G11 | **Observability-proven examples** | Apps emit + assert latency/throughput surfaces; annotated | [x] ≥8 apps + READMEs |
| G12 | **Long-term interface consistency** | Coerce/sync surprising APIs; hard caps; nested-async CLI | [x] Phase G |
| G13 | **Universal example observability** | Every curriculum app emits latency/throughput via probe (default-on) | [x] Phase H |
| G14 | **Close remaining Med friction** | F2–F8, F17 fixed in platform (or honest non-claim) | [x] Phase H (F4→FQN ns-deny) |
| G15 | **Productize F10–F12 + catalog residuals** | Live drain/detach, WS rpc_auth, dev TLS helper + teach apps | [x] Phase J |
| G16 | **Close FEATURE_CATALOG residual gaps** | disco resolver/audit, queue_fed, mon.transport, tx.tcp/multi, blockchain | [x] Phase J |
| G17 | **Close Phase J depth non-claims** | CERT_REQUIRED mTLS, packet-loss teach, hub settlement, disco.signatures | [x] Phase K |
| G18 | **Promote high-value FEATURE partials** | rpc.describe/report, client.trace, tx.correlation, chaos.no_loop, mon.trace_bind | [x] Phase K |
| G19 | **Promote remaining FEATURE partials** | queue.ack/receive, client.pubsub/backlog, cache geo/L2, fabric modes, rpc concurrency, mon.logging, chaos.crash, tx.CB, ns.engine | [x] Phase L |
| G20 | **Close last FEATURE partials** | ops.cli_planes/ns/discovery + pubsub.fabric_forward | [x] Phase M |
| G21 | **Close residual Low DX + planning truth** | F22/F23 platform fix; scrub stale POC/VISION residuals | [x] Phase N |

---

## 2. Current baseline

| Metric | Value |
|--------|------:|
| Shipped apps | **96** |
| Smoke apps | 8 |
| Suite apps | 96 (all registry `suite=True`) |
| Feature IDs in `features.py` constants | ~140+ |
| FEATURE_CATALOG prioritized gaps 1–8 | **closed** (honest non-claims where needed) |
| Thin apps (scen<2 or L1+ ens<5) | **0** |
| Branch vs origin | main ahead local only (no push unless asked) |
| Last new-app validation | Phase M apps green; Phase N deepen F22/F23 paths |
| Last unit | `pytest tests/examples_apps -m unit` → **111 passed** |
| Last full suite | **96/96 passed** (~103s) |
| Phase G | **COMPLETE** — DX fixes + ExampleProbe + 8 apps obs-proven |
| Phase H | **COMPLETE** — FQN ns-deny + Med friction + universal probe |
| Phase I | **COMPLETE** — residual Info polish + FQN curriculum + catalog sync |
| Phase J | **COMPLETE** — F10/F11/F12 productized + catalog residual teach |
| Phase K | **COMPLETE** — residual depth non-claims + catalog partials productized |
| Phase L | **COMPLETE** — FEATURE partial promotion batch (10 apps) |
| Phase M | **COMPLETE** — residual CLI ops + fabric_forward |
| Phase N | **COMPLETE** — F22/F23 Low DX + planning scrub |

### Thin backlog

**Empty** — E6 closed all nine thin apps.

---

## 3. Phases

### Phase A–D — Foundation through L4 flagship

| Phase | Scope | Status | Exit |
|-------|-------|--------|------|
| A | Runtime, runner, L0/L1, smoke | [x] | smoke green |
| B | L2 product + integrations | [x] | suite includes L2 |
| C | L3 mesh / chaos / fabric security | [x] | suite includes L3 |
| D | L4 `global_edge_control_plane` | [x] | suite includes L4 |

### Phase E — Feature-catalog gap fill + depth (**COMPLETE**)

| Wave | Deliverables | Apps (new or deepen) | Status |
|------|--------------|----------------------|--------|
| **E0** | Living PROJECT_PLAN + tracker refresh | docs | [x] |
| **E1** | Cache atomic / structures / ns ops | `cache_atomic_ops` (L1 plane) | [x] |
| **E2** | Namespace policy wire path | `namespace_policy_gate` (L1 plane) | [x] |
| **E3** | DNS plane register/list/describe/resolve | `plane_dns` (L1 plane) | [x] |
| **E4** | Unified client four-plane façade | `unified_client_tour` (L1 product) | [x] |
| **E5** | Pubsub request/reply | `pubsub_request_reply` (L1 product) | [x] |
| **E6** | Deepen 9 thin apps to depth contract | listed historically | [x] |
| **E7** | Discovery watches + summary query | `discovery_watch_summary` (L3) | [x] |
| **E8** | Fabric graph / resilience drills | `fabric_graph_resilience` (L3) | [x] |
| **E9** | Chaos extras (clock skew, dup, reorder, drop) | `chaos_transport` (L3) | [x] |
| **E10** | Ops CLI teaching apps (call/dns/doctor) | `ops_cli_tour` (L2) | [x] |
| **E11** | Cache pubsub events integration | `cache_event_bus` (L2) | [x] |
| **E12** | Queue DLQ path | `job_queue_dlq` (L1) | [x] |
| **E13** | Topic-aware / versioned RPC | `rpc_versioned_topic` (L1) | [x] |
| **E14** | TLS / auth_token path | `client_auth_token` (L1; mTLS non-claim) | [x] |
| **E15** | Full suite automation + coverage report | suite green + docs | [x] |
| **E16** | APP_CATALOG / STAGES / FEATURE_CATALOG sync | docs | [x] |

**Phase E exit criteria** — all met.

### Phase F — Breadth to 60–70 + operator polish (**COMPLETE**)

| Item | Status |
|------|--------|
| Additional product verticals (billing, notifications, inventory) | [x] `billing_ledger`, `notification_fanout`, `inventory_reserve` |
| Multi-region + DNS + policy composition (L3/L4) | [x] `multi_region_dns_policy` |
| L0 hellos for queue/dns | [x] `hello_queue`, `hello_dns` |
| Deadline budget teaching | [x] `rpc_deadline_budget` + `deadline_hop_budget` |
| Topic→queue bridge integration | [x] `topic_queue_bridge` |
| Taxonomy / persistence / profiles / oracles / intermediate | [x] +6 → 62 |
| Discovery rate limit + observability SLO/trace | [x] `discovery_rate_limit`, `observability_slo_trace` |
| Topic router + dependency resolver labs | [x] `topic_queue_router_lab`, `topic_dependency_lab` |
| Shipping vertical | [x] `shipping_fulfillment` |
| Fabric hub hierarchy + leader election | [x] `fabric_hub_hierarchy`, `leader_election_lab` |
| Second L4 world | [x] `multi_pop_edge_mesh` → **70** |
| Nightly suite in CI docs | [x] OPERATE.md + BOOK.md + ci.yml documented |
| Curriculum BOOK / EXAMPLES chapter sync | [x] counts + entrypoint-only paths |
| Friction log F17–F21 | [x] |

**Phase F exit criteria**

- [x] ≥70 shipped apps in registry  
- [x] Second L4 world tour  
- [x] CI/nightly suite documented  
- [x] BOOK + EXAMPLES entrypoint-only sync  
- [x] Full suite green at 70  

### Phase G — Platform DX + observability-proven curriculum (**COMPLETE**)

We are the sole consumers of this platform. Phase G **fixes and unifies**
interfaces for long-term correctness, operator ergonomics, and measurable
latency/throughput — not just curriculum workarounds.

#### G goals (detailed)

| ID | Goal | Success measure | Status |
|----|------|-----------------|--------|
| PG1 | Fix High/Med friction in platform code | F1, F9, F18, F20, F21 closed in source; apps drop workarounds | [x] |
| PG2 | Nested-async-safe CLI | `run_coro` helper; no bare `asyncio.run` in hot CLI paths | [x] |
| PG3 | Shared example observability probe | `ExampleProbe` records ops, latency histogram, throughput; snapshot + print | [x] |
| PG4 | Runtime integration | `app_run` can attach probe; `obs_ok` / scenario timing annotations | [x] |
| PG5 | Curriculum apps prove metrics | ≥8 representative apps emit + `ensure` on p50/p95/count/rps | [x] |
| PG6 | Metrics teaching app | Dedicated L1 `plane_metrics_probe` (or deepen observability app) | [x] |
| PG7 | ServerMetricsTracker snapshot | In-process snapshot API for curriculum without HTTP scrape | [x] |
| PG8 | Docs / friction log / plan living | F# marked fixed; TRACKER; completion % | [x] |
| PG9 | Full validation | unit + suite green after platform changes | [x] |
| PG10 | Optional: CLI aliases / doctor URL | F2/F3 closed in Phase H (aliases + doctor URL reject) | [x] |

#### Phase G waves (sequential)

| Wave | Deliverables | Status |
|------|--------------|--------|
| **G0** | Living plan Phase G section + dashboard reset | [x] |
| **G1** | Platform fixes: CircuitBreaker F9, SQLite Path F18, rate-limit F20, router stats F21 | [x] |
| **G2** | CLI `run_coro` nested-loop safe (F1) | [x] |
| **G3** | `ExampleProbe` + `ServerMetricsTracker.snapshot` + runtime hooks | [x] |
| **G4** | Wire probe into hello_rpc, plane_rpc, order_intake, shipping, multi_pop, observability, discovery_rate_limit, topic_queue_router | [x] |
| **G5** | Metrics teaching scenarios + README annotations | [x] |
| **G6** | Update friction log (fixed rows); suite + unit proof | [x] |
| **G7** | Commit-as-you-go; plan → Phase G complete when exit met | [x] |

#### Phase G exit criteria

- [x] F9, F18, F20, F21 fixed in platform (not only taught)  
- [x] F1 nested CLI safe via shared helper on client/dns/doctor paths  
- [x] `ExampleProbe` in `_shared` used by ≥8 apps with latency/throughput ensures  
- [x] READMEs annotate where metrics appear in run output  
- [x] `uv run pytest tests/examples_apps -m unit` green  
- [x] `uv run mpreg-example suite` green  
- [x] PROJECT_PLAN dashboard reflects Phase G completion %  

### Phase H — Remaining Med friction + universal observability (**COMPLETE**)

Sole-consumer platform ownership: remaining **Med** friction closed in source
(or superseded by better design), and **every** curriculum app proves
latency/throughput via default-on probe.

#### H goals (detailed)

| ID | Goal | Success measure | Status |
|----|------|-----------------|--------|
| PH1 | Universal ExampleProbe | `app_run` defaults `probe=True`; scenario auto-timing | [x] |
| PH2 | CLI ergonomics F2/F3/F14 | Top-level `mpreg call` / `mpreg dns` aliases; doctor WS URL detect; `--targets` alias | [x] |
| PH3 | RPC FQN + namespace deny (supersedes F4 short-name denylist) | Every wire name is dotted FQN; bare → active ns; `mpreg.*` user-deny; optional hierarchical `bound_rpc_namespace` | [x] |
| PH4 | Cache event listeners F7 | `add_event_listener` callbacks fire on `notify_cache_event` | [x] |
| PH5 | Cache invalidate kwargs F8 | Keyword-only `invalidate` + helpful TypeError on bad kwargs | [x] |
| PH6 | TopicPattern F17 | `{param}` templates match as single-segment wildcards in `matches_topic` | [x] |
| PH7 | Versioned RPC F5/F6 | Multi-version same-node; loud collision; VERSION_MISMATCH (1002) proven | [x] |
| PH8 | Friction log + apps | FIXED rows; affected apps drop workarounds / prove fixes | [x] |
| PH9 | README universal note | Shared convention: `◆ obs` appears on every app run | [x] |
| PH10 | Full validation | unit + suite green; plan 100% | [x] |

#### Phase H waves (sequential)

| Wave | Deliverables | Status |
|------|--------------|--------|
| **H0** | Living plan Phase H section + dashboard | [x] |
| **H1** | Runtime: default probe + scenario auto-record | [x] |
| **H2** | CLI F2/F3/F14 aliases + doctor URL detect | [x] |
| **H3** | FQN namespace deny (not short-name list); F7 listeners; F8 invalidate; F17 template match | [x] |
| **H4** | F5/F6 version registry clarity + app proof | [x] |
| **H5** | Update friction/TRACKER/READMEs; suite + unit | [x] |
| **H6** | Commit; plan → Phase H complete | [x] |

#### Phase H exit criteria

- [x] `app_run` probe default-on; suite apps emit `◆ obs`  
- [x] F2, F3, F7, F8, F17 fixed in platform  
- [x] F4 superseded by FQN + **namespace deny** (`mpreg.*`); short-name denylist removed  
- [x] F5/F6: multi-version same-node + VERSION_MISMATCH path proven (`rpc_versioned_topic`)  
- [x] Friction log FIXED section updated  
- [x] `uv run pytest tests/examples_apps -m unit` green (85)  
- [x] `uv run mpreg-example suite` green (**70/70**)  
- [x] PROJECT_PLAN Phase H dashboard 100%  

#### RPC FQN naming (Phase H design — north star)

**Explicit > implicit.** Every RPC name on the wire is a dotted FQN
(`namespace...leaf`). Bare names (no `.`) are the only exception: they
auto-qualify by prepending the **active** namespace
(`bound_rpc_namespace` if set, else `default_rpc_namespace`, default `app`).

| Rule | Behavior |
|------|----------|
| Wire identity | Always FQN (`app.add`, `orders.create`, `mpreg.system.echo`) |
| Bare register/call | `add` → `{active_ns}.add` |
| Explicit FQN | Pass-through unchanged |
| **Namespace deny** | Users **cannot** register under `mpreg` / `mpreg.*` — full flexibility everywhere else |
| Platform builtins | `PlatformRpc.*` under `mpreg.system` / `.disco` / `.dns` / `.rpc` / `.policy` / `.queue` / `.cache`; `allow_platform=True` only |
| Hierarchical bound | `bound_rpc_namespace` locks register/call to a prefix (operator↔client conformance); bare names qualify under the bound; platform calls still allowed on call path |
| Same short leaf | `app.echo` ≠ `mpreg.system.echo` — no context-dependent short-name magic |

Module: `mpreg/core/rpc_naming.py`. Settings:
`MPREGSettings.default_rpc_namespace`, `bound_rpc_namespace`. Clients:
`MPREGClientAPI` / `MPREGClient` / `MPREGClusterClient` mirror those fields.

Curriculum proof: **`rpc_fqn_namespace`** (Phase I).

---

### Phase I — Residual polish + FQN curriculum + living-doc sync (**COMPLETE**)

Phase H closed Med friction and FQN platform rules, but satellite docs lagged
and residual Info/Low items needed honest close-or-document. Phase I finishes
the program wave without inventing bulk-thin apps.

#### I goals

| ID | Goal | Success measure | Status |
|----|------|-----------------|--------|
| PI1 | Living plan Phase I + recover true open work | This section + dashboard | [x] |
| PI2 | Sync TRACKER / STAGES / API_FRICTION / APP counts | Docs match 71 apps + FIXED rows | [x] |
| PI3 | Residual friction F13/F15/F16/F19 | Platform or curriculum proof | [x] |
| PI4 | Honest residual F10/F11/F12 | Non-claims (not fake fixes) | [x] |
| PI5 | FQN curriculum proof app | `rpc_fqn_namespace` green in suite | [x] |
| PI6 | FEATURE_CATALOG gap rows = reality | False `gap` → shipped; residual honest | [x] |
| PI7 | Full validation | unit + suite green; plan 100% | [x] |

#### I waves

| Wave | Deliverables | Status |
|------|--------------|--------|
| **I0** | Phase I section + open-work audit | [x] |
| **I1** | Doc sync (TRACKER/STAGES/friction/APP) | [x] |
| **I2** | F13 route errors; F15 deadline docs; F16 `list_port_categories`; F19 RaftOracle docs | [x] |
| **I3** | `rpc_fqn_namespace` L1 app | [x] |
| **I4** | FEATURE_CATALOG shipped-vs-gap truth | [x] |
| **I5** | unit + suite + commit | [x] |

#### Phase I exit criteria

- [x] `rpc_fqn_namespace` teaches bare qualify, `mpreg.*` deny, bound ns  
- [x] F13: `route_not_found` names fabric bridge; proven in `multi_region_dns_policy`  
- [x] F15: deadline preemption honesty in `rpc_deadline_budget`  
- [x] F16: `list_port_categories()` + unknown-category error taught in `hello_ports`  
- [x] F19: RaftOracle dual-leader fail-fast documented + `routing_oracle_lab`  
- [x] F10/F11/F12 remain honest Info non-claims  
- [x] FEATURE_CATALOG / TRACKER / STAGES / API_FRICTION / APP_CATALOG current  
- [x] unit + suite green at **71** apps  

---

### Phase J — Productize F10–F12 + catalog residual teach (**COMPLETE**)

Phase I left F10/F11/F12 as honest Info non-claims and several FEATURE_CATALOG
rows as platform-only gaps. Phase J **productizes** those surfaces and lands
curriculum apps that prove them — then closes residual catalog teach paths.

#### J goals

| ID | Goal | Success measure | Status |
|----|------|-----------------|--------|
| PJ1 | Living plan Phase J charter + linear task serialization | This section + TRACKER | [x] |
| PJ2 | F11: optional WS `rpc_auth_token` enforcement | Platform + `client_auth_token` | [x] |
| PJ3 | F12: turnkey dev TLS helper + `wss://` path | `dev_certs` + `tls_dev_handshake` | [x] |
| PJ4 | F10: live admission chaos (drain/detach + `/ready`) | `live_partition_chaos` | [x] |
| PJ5 | Catalog residuals: disco resolver/audit, queue_fed | dedicated L1/L2 apps | [x] |
| PJ6 | Catalog residuals: mon.transport, tx.tcp/multi, blockchain | dedicated teach apps | [x] |
| PJ7 | FEATURE/APP/TRACKER/friction sync + suite | docs + unit + suite green | [x] |

#### J waves

| Wave | Deliverables | Status |
|------|--------------|--------|
| **J0** | Phase J section + open-work linearization | [x] |
| **J1** | F11: `rpc_auth_token` on settings/server + deepen `client_auth_token` | [x] |
| **J2** | F12: `generate_dev_tls_material` + server TLS PEMs + `tls_dev_handshake` | [x] |
| **J3** | F10: `live_partition_chaos` drain/detach/`/ready` | [x] |
| **J4** | Residual apps: discovery_resolver_audit, queue_federation_lab, transport_*, blockchain_message_lab | [x] |
| **J5** | Docs sync + unit + suite + commit | [x] |

#### Phase J exit criteria

- [x] F11: unauthenticated WS rejected when `rpc_auth_token` set; matching token unlocks RPC  
- [x] F12: `generate_dev_tls_material` + `wss://` RPC; plain `ws://` fail-closed  
- [x] F10: live `/mgmt/v1/nodes/drain` → `/ready` 503; detach applied; lab injector contrast  
- [x] Catalog residuals taught: disco audit/resolver, queue_fed, mon.transport, tx.tcp/multi, blockchain  
- [x] FEATURE_CATALOG / APP_CATALOG / TRACKER / API_FRICTION current at **78** apps  
- [x] unit + suite green  

---

### Phase K — Residual depth + catalog partials (**COMPLETE**)

Phase J left honest **depth** non-claims (CERT_REQUIRED mTLS mesh, raw packet-loss
teach, on-chain hub settlement, `disco.signatures`) and many FEATURE_CATALOG
`partial`/`gap` rows. Phase K **serializes those into one linear charter** and
productizes the teachable platform surfaces — no idle gap.

#### K goals

| ID | Goal | Success measure | Status |
|----|------|-----------------|--------|
| PK1 | Charter Phase K into all living plans | PROJECT_PLAN + TRACKER + STAGES | [x] |
| PK2 | `disco.signatures` + gossip HMAC | `discovery_signatures_lab` | [x] |
| PK3 | `rpc.describe` / `rpc.report` inventory | `rpc_inventory_tour` | [x] |
| PK4 | `client.trace` + `mon.trace_bind` | `client_trace_bind` | [x] |
| PK5 | `tx.correlation` + `chaos.no_loop` | `correlation_routing_lab` | [x] |
| PK6 | CERT_REQUIRED mTLS mesh | `mtls_mesh_handshake` | [x] |
| PK7 | Packet-loss plane model + live drain compose | `packet_loss_chaos` | [x] |
| PK8 | Hub settlement beyond bare types | `blockchain_hub_settlement` | [x] |
| PK9 | FEATURE/APP/friction sync + suite | **85** apps green | [x] |

#### K waves (linear)

| Wave | Deliverables | Status |
|------|--------------|--------|
| **K0** | Serialize residuals into this charter | [x] |
| **K1** | Stale API_FRICTION F10–F12 → FIXED | [x] |
| **K2** | `discovery_signatures_lab` | [x] |
| **K3** | `rpc_inventory_tour` | [x] |
| **K4** | `client_trace_bind` | [x] |
| **K5** | `correlation_routing_lab` | [x] |
| **K6** | `mtls_mesh_handshake` (CERT_REQUIRED) | [x] |
| **K7** | `packet_loss_chaos` | [x] |
| **K8** | `blockchain_hub_settlement` | [x] |
| **K9** | Catalog partial→shipped + docs | [x] |
| **K10** | unit + suite + commit | [x] |

#### Phase K exit criteria

- [x] All Phase J depth non-claims have dedicated curriculum proof apps  
- [x] `disco.signatures` gap → shipped  
- [x] rpc.describe/report, client.trace, mon.trace_bind, tx.correlation, chaos.no_loop taught  
- [x] FEATURE_CATALOG / APP_CATALOG / TRACKER / API_FRICTION current at **85**  
- [x] unit + suite green  

---

### Phase L — FEATURE partial promotion batch (**COMPLETE**)

Phase K left ~35 FEATURE_CATALOG `partial` rows with thin plane coverage and
operator-topology residuals. Phase L **integrates the partial-promotion audit
into the living charter** and ships dedicated depth apps (or retags) for every
teachable library surface — serialized with residual honesty for CLI-only ops.

#### L goals

| ID | Goal | Success measure | Status |
|----|------|-----------------|--------|
| PL1 | Charter Phase L into all living plans | PROJECT_PLAN + TRACKER + STAGES | [x] |
| PL2 | queue.ack / receive / broadcast / fnf depth | `queue_ack_receive_lab` | [x] |
| PL3 | client.pubsub + pubsub.backlog | `pubsub_client_backlog` | [x] |
| PL4 | cache geo/repl/L2/invalidate + pers.mode | `cache_replication_geo` | [x] |
| PL5 | fabric.strict/explicit/catalog/link_state | `fabric_policy_modes` | [x] |
| PL6 | rpc.concurrency + routing_topic + M3 | `rpc_concurrency_lab` | [x] |
| PL7 | cluster_map + catalog_query | `cluster_map_catalog` | [x] |
| PL8 | mon.logging + correlation/health | `mon_logging_json` | [x] |
| PL9 | chaos.crash + tx.CB + ns.engine | 3 dedicated labs | [x] |
| PL10 | FEATURE/APP sync + suite | **95** apps green | [x] |

#### L waves (linear)

| Wave | Deliverables | Status |
|------|--------------|--------|
| **L0** | Serialize partial audit into this charter | [x] |
| **L1** | `queue_ack_receive_lab` | [x] |
| **L2** | `pubsub_client_backlog` | [x] |
| **L3** | `cache_replication_geo` | [x] |
| **L4** | `fabric_policy_modes` | [x] |
| **L5** | `rpc_concurrency_lab` + `cluster_map_catalog` | [x] |
| **L6** | `mon_logging_json` + `chaos_crash_recover` | [x] |
| **L7** | `tx_circuit_breaker_lab` + `ns_engine_direct` | [x] |
| **L8** | Catalog partial→shipped + unit + suite + commit | [x] |

#### Phase L exit criteria

- [x] Teachable FEATURE `partial` rows promoted to `shipped` with dedicated apps  
- [x] Remaining partials honest: CLI ops docs + `pubsub.fabric_forward` thin  
- [x] FEATURE_CATALOG / APP_CATALOG / TRACKER / STAGES / API_FRICTION at **95**  
- [x] unit + suite green  

---

### Phase M — Residual CLI ops + fabric_forward (**COMPLETE**)

Phase L left 4 FEATURE `partial` rows (CLI ops + fabric_forward). Phase M
**integrates those into the living charter** and closes them.

#### M goals

| ID | Goal | Success measure | Status |
|----|------|-----------------|--------|
| PM1 | Charter Phase M | living plans | [x] |
| PM2 | ops.cli_planes/ns/discovery | `ops_cli_tour` deepen | [x] |
| PM3 | pubsub.fabric_forward | `pubsub_fabric_forward_lab` | [x] |
| PM4 | FEATURE partials = 0 teachable | catalog sync | [x] |
| PM5 | unit + suite | **96** green | [x] |

#### Phase M exit criteria

- [x] Last teachable FEATURE partials shipped  
- [x] **96** apps; unit + suite green  

---

### Phase N — Residual Low DX + planning truth (**COMPLETE**)

Phase M left open Low friction (F22/F23) and stale POC follow-ups while
FEATURE teachable partials were already 0. Phase N **serializes those into one
linear charter** and closes them without inventing topology claims.

#### N goals

| ID | Goal | Success measure | Status |
|----|------|-----------------|--------|
| PN1 | Charter Phase N into living plans | G21 + waves | [x] |
| PN2 | F22 — `publish` accepts Mapping\|MessageHeaders | `MessageHeaders.coerce` | [x] |
| PN3 | F23 — empty `entry_type` defaults + clearer error | server + CatalogQueryRequest | [x] |
| PN4 | Teach apps prove F22/F23 | pubsub_client_backlog + cluster_map_catalog | [x] |
| PN5 | Scrub stale POC/VISION residuals | media_pipeline + CI smoke already real | [x] |
| PN6 | unit + suite green | **96** apps; friction open = 0 | [x] |

#### Phase N waves (sequential)

| Wave | Deliverable | Status |
|------|-------------|--------|
| **N0** | Serialize residuals into this charter | [x] |
| **N1** | F22 MessageHeaders Mapping coerce | [x] |
| **N2** | F23 catalog_query entry_type default/error | [x] |
| **N3** | Unit tests + curriculum deepen | [x] |
| **N4** | POC_NOTES / API_FRICTION / catalogs scrub | [x] |
| **N5** | unit + suite + commit | [x] |

#### Phase N exit criteria

- [x] F22/F23 fixed in platform and taught in apps  
- [x] API_FRICTION open High/Med/Low curriculum rows = **0**  
- [x] Stale POC follow-ups closed or honest residual only  
- [x] unit + suite green at **96**  

---

## 4. Target app matrix growth

| Band | Now | Target | Notes |
|------|----:|-------:|-------|
| L0 hellos | 8 | 6–8 | + hello_queue, hello_dns |
| L1 product/planes | ~25 | 18+ | + `rpc_fqn_namespace` |
| L2 product/integ | ~21 | 14+ | + shipping, router, dependency |
| L3 complex | ~16 | 12+ | + hubs, leader election |
| L4 world | 2 | 2–3 | global_edge + multi_pop |
| **Total** | **96** | **50–70+** | band complete; Phase M +1; 0 teachable FEATURE partials |

---

## 5. Automation & validation gates (every slice)

```bash
# Single app
uv run mpreg-example run <id>

# Registry unit (fast)
uv run pytest tests/examples_apps -m unit -q

# Smoke
uv run mpreg-example smoke

# Full suite (after batches)
uv run mpreg-example suite

# Live pytest
uv run pytest tests/examples_apps -q
```

**Per-slice checklist**

1. Implement / deepen app with `app_run` + `scenario(*feature_ids)` + `ensure`  
2. README with story / lesson / run / proves / architecture / non-claims / exit ramp  
3. `features.py` APP_FEATURES + constants if new IDs  
4. `registry.py` entry + timeouts in `test_curriculum_apps.py`  
5. `uv run mpreg-example run <id>` green  
6. **Log API friction** in §9 when an edge case or usability issue appears  
7. Update FEATURE_CATALOG depth + APP_CATALOG row  
8. Update this plan accomplishments + %  
9. **Commit** with what/why/how (no push unless asked)

---

## 6. Accomplishments log

| Date | Slice | Result |
|------|-------|--------|
| 2026-08-05 | Phases A–D + feature depth pass | 35 apps; suite 35/35; feature tags + join tests |
| 2026-08-05 | PROJECT_PLAN.md created | Phase E waves E0–E16 defined; thin list; gates |
| 2026-08-05 | E1–E5 gap apps | +5 apps → **40**; atomic/ns/dns/unified/reply all green |
| 2026-08-05 | E6 deepen thin apps | 9 thin → 0; tier3 inlined multi-scenario; signed_route/snapshot multi-scen |
| 2026-08-05 | E7–E12 | +4 → **44**: discovery_watch_summary, fabric_graph_resilience, cache_event_bus, job_queue_dlq |
| 2026-08-05 | E9–E14 + F verticals | +12 → **56**: versioned RPC, auth token, chaos_transport, ops_cli, hellos, billing/notify/inventory, deadline, bridge, multi-region DNS; unit 71; all 12 runs green |
| 2026-08-05 | E15 full suite | **56/56 passed** (~75s) via `mpreg-example suite` |
| 2026-08-05 | E16 APP_CATALOG sync | matrix lists all 56 shipped ids |
| 2026-08-05 | F breadth wave | +6 → **62**: taxonomy, persistence_kv, profiles, intermediate RPC, routing oracle, deadline hop; F17–F19 friction |
| 2026-08-05 | Full suite re-run | **62/62 passed** (~76s) |
| 2026-08-05 | F final wave → 70 | +8: discovery_rate_limit, observability_slo_trace, topic_queue_router_lab, topic_dependency_lab, shipping_fulfillment, fabric_hub_hierarchy, leader_election_lab, multi_pop_edge_mesh; F20–F21; CI/BOOK/OPERATE/EXAMPLES sync |
| 2026-08-05 | Phase G complete | F1/F9/F18/F20/F21; ExampleProbe; ≥8 obs apps |
| 2026-08-05 | Phase H complete | FQN ns-deny; F2–F8/F17; universal probe; 70/70 suite |
| 2026-08-05 | Phase I complete | `rpc_fqn_namespace`; F13/F15/F16/F19; catalog/doc sync → **71** |
| 2026-08-05 | Phase J complete | F10 drain/detach; F11 rpc_auth; F12 dev_certs/wss; +7 apps (tls/disco/queue_fed/live_chaos/transport×2/blockchain) → **78** |
| 2026-08-05 | Phase K complete | signatures, rpc inventory, trace bind, correlation/no-loop, CERT_REQUIRED mTLS, packet loss, hub settlement → **85** |
| 2026-08-05 | Phase L complete | queue ack/receive, pubsub client/backlog, cache geo/L2, fabric modes, rpc concurrency, cluster map/catalog, mon json, chaos crash, tx CB, ns engine → **95** |
| 2026-08-05 | Phase M complete | ops CLI planes/ns/discovery deepen + pubsub_fabric_forward_lab → **96**; 0 teachable FEATURE partials |
| 2026-08-05 | Phase N complete | F22 Mapping headers coerce; F23 empty entry_type default; POC scrub; **96** apps; 0 open friction |

---

## 7. Completion dashboard

### Curriculum A–F (closed)

| Area | Weight | Done | Notes |
|------|-------:|-----:|-------|
| Runner + foundation | 10% | 10 | [x] |
| L0–L4 baseline matrix | 20% | 20 | [x] 35+ apps |
| Feature-catalog tagging | 10% | 10 | [x] |
| Gap apps (E1–E14) | 30% | 30 | [x] |
| Depth contract all apps | 10% | 10 | [x] |
| Phase F breadth 50–70 | 10% | 10 | [x] **70** apps |
| Docs/plan living sync | 5% | 5 | [x] |
| Automation/report (E15) | 5% | 5 | [x] |
| **Curriculum A–F** | **100%** | **100%** | closed |

### Phase G (platform + obs) — **COMPLETE 100%**

| Area | Weight | Done | Notes |
|------|-------:|-----:|-------|
| G0 living plan | 5% | 5 | [x] |
| G1 platform friction fixes | 25% | 25 | [x] F9/F18/F20/F21 |
| G2 nested-async CLI | 10% | 10 | [x] F1 `run_coro` |
| G3 ExampleProbe + tracker snapshot | 20% | 20 | [x] obs.py + snapshot() |
| G4 wire ≥8 apps | 20% | 20 | [x] 8 apps probe+ensures |
| G5 README annotations | 5% | 5 | [x] Observability sections |
| G6 friction log + suite proof | 10% | 10 | [x] FIXED rows + green |
| G7 commit + plan close | 5% | 5 | [x] |
| **Phase G overall** | **100%** | **100%** | complete |

### Phase H (Med friction + universal obs) — complete

| Area | Weight | Done | Notes |
|------|-------:|-----:|-------|
| H0 living plan | 5% | 5 | [x] drafted |
| H1 universal probe default | 20% | 20 | [x] app_run probe=True |
| H2 CLI F2/F3/F14 | 15% | 15 | [x] aliases + doctor WS reject + --targets |
| H3 FQN/F7/F8/F17 platform | 30% | 30 | [x] namespace deny + F7/F8/F17 |
| H4 F5/F6 version DX | 10% | 10 | [x] multi-version + VERSION_MISMATCH proven |
| H5 docs + suite proof | 15% | 15 | [x] friction/READMEs; 85 unit; 70/70 suite |
| H6 commit + plan close | 5% | 5 | [x] |
| **Phase H overall** | **100%** | **100%** | complete |

### Phase I (residual polish + FQN teach + docs) — complete

| Area | Weight | Done | Notes |
|------|-------:|-----:|-------|
| I0 living plan / open-work audit | 10% | 10 | [x] |
| I1 TRACKER/STAGES/friction/APP sync | 20% | 20 | [x] |
| I2 residual friction F13/F15/F16/F19 | 25% | 25 | [x] |
| I3 `rpc_fqn_namespace` | 20% | 20 | [x] |
| I4 FEATURE_CATALOG truth | 15% | 15 | [x] |
| I5 unit + suite + commit | 10% | 10 | [x] |
| **Phase I overall** | **100%** | **100%** | complete |

### Phase J (F10–F12 + catalog residuals) — complete

| Slice | Weight | Done | Status |
|-------|-------:|-----:|--------|
| J0 charter | 10% | 10 | [x] |
| J1 F11 rpc_auth | 15% | 15 | [x] |
| J2 F12 dev TLS | 15% | 15 | [x] |
| J3 F10 live drain | 15% | 15 | [x] |
| J4 residual apps | 25% | 25 | [x] |
| J5 docs+suite | 20% | 20 | [x] |
| **Phase J overall** | **100%** | **100%** | complete |

### Phase K (residual depth + catalog partials) — complete

| Slice | Weight | Done | Status |
|-------|-------:|-----:|--------|
| K0–K1 charter + friction | 10% | 10 | [x] |
| K2–K5 catalog partial apps | 35% | 35 | [x] |
| K6–K8 depth non-claim apps | 35% | 35 | [x] |
| K9–K10 docs + suite | 20% | 20 | [x] |
| **Phase K overall** | **100%** | **100%** | complete |

---

### Phase L (FEATURE partial promotion) — complete

| Slice | Weight | Done | Status |
|-------|-------:|-----:|--------|
| L0 charter | 10% | 10 | [x] |
| L1–L7 depth apps (10) | 70% | 70 | [x] |
| L8 docs + suite | 20% | 20 | [x] |
| **Phase L overall** | **100%** | **100%** | complete |

---

### Phase M (residual CLI + fabric_forward) — complete

| Slice | Weight | Done | Status |
|-------|-------:|-----:|--------|
| M0–M1 charter + ops deepen | 40% | 40 | [x] |
| M2 fabric_forward lab | 30% | 30 | [x] |
| M3–M5 catalog + suite | 30% | 30 | [x] |
| **Phase M overall** | **100%** | **100%** | complete |

---

### Phase N (Low DX F22/F23 + planning scrub) — complete

| Slice | Weight | Done | Status |
|-------|-------:|-----:|--------|
| N0 charter | 10% | 10 | [x] |
| N1–N2 F22/F23 platform | 50% | 50 | [x] |
| N3–N5 teach + docs + suite | 40% | 40 | [x] |
| **Phase N overall** | **100%** | **100%** | complete |

---

## 8. Working rules (non-negotiable)

1. **Do not stop and claim done** after a small batch — update plan and continue next wave.  
2. **No bulk-thin apps** — every new id meets depth contract on first land.  
3. **Honesty** — non-claims for unproven surfaces; never invent APIs.  
4. **Entrypoints only** — never document `python -m`.  
5. **Commit as you go** — each wave or app slice is commit-ready.  
6. **Validate every step** — run the app before the next one.  
7. **Curriculum = API discovery** — every friction is logged in §9 for platform UX work.

---

## 9. API friction / usability discovery log

Curriculum apps are a **forced walk of public APIs**. Findings below are
candidates for platform DX improvements (not claims that apps are broken).

| ID | Surface | Finding | Severity | Suggested improvement | Found in |
|----|---------|---------|----------|----------------------|----------|
| F1 | CLI nesting | `mpreg` click handlers call `asyncio.run` → **cannot** be invoked from an already-running event loop | High | Offer async entrypoints or thread offload | `ops_cli_tour`  **FIXED Phase G** (run_coro) |
| F2 | CLI IA | Operators guess `mpreg dns …` / `mpreg call …` | Med | Top-level aliases | `ops_cli_tour` **FIXED H** |
| F3 | `mpreg doctor` | `--url` is monitoring HTTP; WS URL fails | Med | Dual URL + clear reject | `ops_cli_tour` **FIXED H** |
| F4 | RPC registry | Built-in `echo` collision | Med | **Superseded:** FQN + `mpreg.*` ns-deny | `ops_cli_tour` **FIXED H** |
| F5 | Versioned RPC | Multi-version same-node | Med | Registry multi-version + app proof | `rpc_versioned_topic` **FIXED H** |
| F6 | Version miss | Bad constraint → generic not-found | Med | VERSION_MISMATCH 1002 | `rpc_versioned_topic` **FIXED H** |
| F7 | Cache events | `add_event_listener` registration-only | Med | Fire on notify | `cache_event_bus` **FIXED H** |
| F8 | Cache invalidation | Wrong kwargs | Med | Keyword-only + TypeError | `cache_event_bus` **FIXED H** |
| F9 | CircuitBreaker | `timeout_seconds` ≠ `current_timeout` | Med | Sync on init | **FIXED G** |
| F10 | Chaos model | `FaultInjector` is lab-only | Info | Live drain/detach + `/ready` | `live_partition_chaos` **FIXED J** |
| F11 | Auth | Client `auth_token` not enforced on local WS RPC | Info | `rpc_auth_token` handshake gate | `client_auth_token` **FIXED J** |
| F12 | mTLS | No turnkey local-cert path | Info | `generate_dev_tls_material` + tls_* settings | `tls_dev_handshake` **FIXED J** |
| F13 | Cross-cluster | Peers alone ≠ fabric bridge | Info | Clearer route errors | `multi_region_dns_policy` **FIXED I** (`route_not_found` fabric hint) |
| F14 | DNS CLI | `--target` not `--targets` | Low | Alias | `ops_cli_tour` **FIXED H** |
| F15 | M2 deadline | Handler runs after client fail-closed | Info | Docs / cooperative cancel | `rpc_deadline_budget` **DOCUMENTED I** |
| F16 | Port categories | Fixed enum | Low | Discoverable list API | `hello_ports` **FIXED I** (`list_port_categories`) |
| F17 | TopicPattern | `{param}` templates → False | Med | `{x}`→`*` in matcher | `topic_taxonomy_tour` **FIXED H** |
| F18 | SQLite backend | `db_path` must be `Path` | Med | Coerce str→Path | **FIXED G** |
| F19 | RaftOracle | Dual-leader raises on `observe_role` | Info | Document fail-fast | `routing_oracle_lab` **DOCUMENTED I** |
| F20 | DiscoveryRateLimiter | soft max_keys | Med | Hard prune | **FIXED G** |
| F21 | TopicQueueRouter | route success stats | Med | Bump on pure route | **FIXED G** |
| F22 | PubSub headers | bare `dict` rejected by type | Low | Accept Mapping via coerce | **FIXED N** |
| F23 | catalog_query | empty entry_type ValueError | Low | Default `functions` + clearer error | **FIXED N** |

**Process:** when a new app hits friction, append a row here **and** a `step("friction: …")` in the app so operators see it live.

See also: [API_FRICTION.md](./API_FRICTION.md).

---

## 10. Program status

**Curriculum program (Phases A–F): COMPLETE at 100%.**

**Phase G (platform DX + observability proof): COMPLETE at 100%.**  
F1/F9/F18/F20/F21 fixed; ExampleProbe + ServerMetricsTracker.snapshot; 8 apps.

**Phase H (FQN ns-deny + Med friction + universal obs): COMPLETE 100%.**  
Close F2–F8/F17 in platform; default-on probe; CLI aliases; doctor URL clarity.

**Phase I (residual polish + FQN curriculum + doc/catalog sync): COMPLETE 100%.**  
`rpc_fqn_namespace`; F13/F15/F16/F19 closed or documented; FEATURE_CATALOG truth.

**Phase J (productize F10–F12 + catalog residual teach): COMPLETE 100%.**  
F11/F12/F10 + residual teach apps. **78** apps at J exit.

**Phase K (residual depth + catalog partials): COMPLETE 100%.**  
`discovery_signatures_lab`; `rpc_inventory_tour`; `client_trace_bind`;
`correlation_routing_lab`; `mtls_mesh_handshake` (CERT_REQUIRED);
`packet_loss_chaos`; `blockchain_hub_settlement`. **85** apps at K exit.

**Phase L (FEATURE partial promotion batch): COMPLETE 100%.**  
10 depth labs. **95** apps at L exit.

**Phase M (residual CLI + fabric_forward): COMPLETE 100%.**  
`ops_cli_tour` deepened; `pubsub_fabric_forward_lab`. **96** apps.
FEATURE_CATALOG teachable `partial` rows: **0**.

**Phase N (Low DX F22/F23 + planning scrub): COMPLETE 100%.**  
`MessageHeaders.coerce` accepts Mapping; catalog `entry_type` defaults to
`functions` with clearer errors; POC_NOTES/VISION residuals scrubbed.
API_FRICTION open curriculum rows: **0**. **96** apps.

Honest remaining (operator topology / kernel-level only — not curriculum blockers):
multi-continent SLA meshes, kernel TCP byte-splice loss, multi-hub DAO treasury
production ops.

**Program complete through Phase N.** Further work requires a new charter
(new platform surface, new operator topology product, or fresh friction).

---

## Related

- [TRACKER.md](./TRACKER.md) — checkbox delivery status  
- [FEATURE_CATALOG.md](./FEATURE_CATALOG.md) — feature inventory  
- [APP_CATALOG.md](./APP_CATALOG.md) — app matrix  
- [STAGES.md](./STAGES.md) — phase narrative  
- [API_FRICTION.md](./API_FRICTION.md) — usability discovery backlog  
- [OPERATE.md](./OPERATE.md) — runbooks + CI suite  
