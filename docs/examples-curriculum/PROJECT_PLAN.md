# Curriculum Examples — Living Project Plan

**Last updated:** 2026-08-05 (Phase H ACTIVE — FQN RPC naming + Med friction + universal obs)  
**Owner drive:** sequential iterative completion of curriculum **and** long-term
platform correctness: API unification, operator ergonomics, latency/throughput
observability in every example path — **not** batch-and-stop.

Legend: `[x]` done · `[~]` partial · `[>]` in progress · `[ ]` not started

---

## 1. North-star goals

| # | Goal | Success measure | Status |
|---|------|-----------------|--------|
| G1 | **Full-power curriculum** — every honest platform surface taught at the lowest level that can prove it | FEATURE_CATALOG rows `shipped` (or explicit non-claim if product gap) | [x] prioritized surfaces taught; residual platform-only gaps are non-claims (mTLS F12, live WS chaos F10) |
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
| G13 | **Universal example observability** | Every curriculum app emits latency/throughput via probe (default-on) | [>] Phase H |
| G14 | **Close remaining Med friction** | F2–F8, F17 fixed in platform (or honest non-claim) | [>] Phase H |

---

## 2. Current baseline

| Metric | Value |
|--------|------:|
| Shipped apps | **70** |
| Smoke apps | 8 |
| Suite apps | 70 (all registry `suite=True`) |
| Feature IDs in `features.py` constants | ~140+ |
| FEATURE_CATALOG prioritized gaps 1–8 | **closed** (honest non-claims where needed) |
| Thin apps (scen<2 or L1+ ens<5) | **0** |
| Branch vs origin | main ahead local only (no push unless asked) |
| Last new-app validation | Phase F final wave each `mpreg-example run` green |
| Last unit | `pytest tests/examples_apps -m unit` → **85 passed** |
| Last full suite | **70/70 passed** |
| Phase G | **COMPLETE** — DX fixes + ExampleProbe + 8 apps obs-proven |
| Phase H | **ACTIVE** — Med friction F2–F8/F17 + universal probe |

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
| PG10 | Optional: CLI aliases / doctor URL | F2/F3 deferred (Med; apps document real paths) | [~] deferred |

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

### Phase H — Remaining Med friction + universal observability (**ACTIVE**)

Continue sole-consumer platform ownership: close remaining **Med** friction in
source, and make **every** curriculum app prove latency/throughput (not only
the Phase G representative eight).

#### H goals (detailed)

| ID | Goal | Success measure | Status |
|----|------|-----------------|--------|
| PH1 | Universal ExampleProbe | `app_run` defaults `probe=True`; scenario auto-timing | [x] |
| PH2 | CLI ergonomics F2/F3/F14 | Top-level `mpreg call` / `mpreg dns` aliases; doctor WS URL detect; `--targets` alias | [~] |
| PH3 | RPC FQN + namespace deny (supersedes F4 short-name denylist) | Every wire name is dotted FQN; bare → active ns; `mpreg.*` user-deny; optional hierarchical `bound_rpc_namespace` | [x] |
| PH4 | Cache event listeners F7 | `add_event_listener` callbacks fire on `notify_cache_event` | [x] |
| PH5 | Cache invalidate kwargs F8 | Keyword-only `invalidate` + helpful TypeError on bad kwargs | [x] |
| PH6 | TopicPattern F17 | `{param}` templates match as single-segment wildcards in `matches_topic` | [x] |
| PH7 | Versioned RPC F5/F6 | Multi-version same-node where registry already allows; loud errors on collision; version_mismatch path proven | [~] |
| PH8 | Friction log + apps | FIXED rows; affected apps drop workarounds / prove fixes | [>] |
| PH9 | README universal note | Shared convention: `◆ obs` appears on every app run | [ ] |
| PH10 | Full validation | unit + suite green; plan 100% | [ ] |

#### Phase H waves (sequential)

| Wave | Deliverables | Status |
|------|--------------|--------|
| **H0** | Living plan Phase H section + dashboard | [x] |
| **H1** | Runtime: default probe + scenario auto-record | [x] |
| **H2** | CLI F2/F3/F14 aliases + doctor URL detect | [~] |
| **H3** | FQN namespace deny (not short-name list); F7 listeners; F8 invalidate; F17 template match | [x] |
| **H4** | F5/F6 version registry clarity + app proof | [~] |
| **H5** | Update friction/TRACKER/READMEs; suite + unit | [>] |
| **H6** | Commit; plan → Phase H complete | [ ] |

#### Phase H exit criteria

- [ ] `app_run` probe default-on; ≥60 apps show `◆ obs` in suite (or all that record ops)  
- [ ] F2, F3, F7, F8, F17 fixed in platform  
- [ ] F4 superseded by FQN + **namespace deny** (`mpreg.*`); short-name denylist removed  
- [ ] F5/F6 improved or honest non-claim with loud errors  
- [ ] Friction log FIXED section updated  
- [ ] `uv run pytest tests/examples_apps -m unit` green  
- [ ] `uv run mpreg-example suite` green  
- [ ] PROJECT_PLAN Phase H dashboard 100%  

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

---

## 4. Target app matrix growth

| Band | Now | Target | Notes |
|------|----:|-------:|-------|
| L0 hellos | 8 | 6–8 | + hello_queue, hello_dns |
| L1 product/planes | ~24 | 18+ | + rate limit, SLO/trace, … |
| L2 product/integ | ~21 | 14+ | + shipping, router, dependency |
| L3 complex | ~16 | 12+ | + hubs, leader election |
| L4 world | 2 | 2–3 | global_edge + multi_pop |
| **Total** | **70** | **50–70** | **band complete** |

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

### Phase H (Med friction + universal obs) — active program

| Area | Weight | Done | Notes |
|------|-------:|-----:|-------|
| H0 living plan | 5% | 5 | [x] drafted |
| H1 universal probe default | 20% | 20 | [x] app_run probe=True |
| H2 CLI F2/F3/F14 | 15% | 10 | [~] aliases partial |
| H3 FQN/F7/F8/F17 platform | 30% | 30 | [x] namespace deny + F7/F8/F17 |
| H4 F5/F6 version DX | 10% | 5 | [~] multi-version registry OK; app proof pending |
| H5 docs + suite proof | 15% | 5 | [>] friction log updated; suite pending |
| H6 commit + plan close | 5% | 0 | |
| **Phase H overall** | **100%** | **~75%** | FQN + F7/F8/F17 landed; finish CLI/F5/suite |

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
| F2 | CLI IA | Operators guess `mpreg dns …` / `mpreg call …`; real paths are `mpreg client dns-*` / `mpreg client call` | Med | Top-level aliases or clearer `--help` epilog | `ops_cli_tour` |
| F3 | `mpreg doctor` | `--url` is **monitoring HTTP**, not WS RPC; WS URL fails opaquely | Med | Dual URL flags + better error | `ops_cli_tour` |
| F4 | RPC registry | Built-in name `echo` already registered | Med | Document reserved builtins | `ops_cli_tour` |
| F5 | Versioned RPC | Same `function_id` cannot register two versions on one node | Med | Multi-version map or loud docs | `rpc_versioned_topic` |
| F6 | Version miss | Bad constraint → generic command-not-found | Med | Structured version_mismatch | `rpc_versioned_topic` |
| F7 | Cache events | `add_event_listener` is registration-only | Med | Fire listeners or rename | `cache_event_bus` |
| F8 | Cache invalidation | Wrong kwargs easy to guess | Med | Keyword-only + clear TypeError | `cache_event_bus` |
| F9 | CircuitBreaker | `timeout_seconds` ≠ `current_timeout` for half-open | Med | Sync fields on init | `fabric_graph_resilience`  **FIXED Phase G** (CB timeout sync) |
| F10 | Chaos model | `FaultInjector` is lab-only | Info | Server partition hooks | `chaos_*` |
| F11 | Auth | Client `auth_token` not enforced on local WS RPC by default | Info | Optional require_auth | `client_auth_token` |
| F12 | mTLS | No turnkey local-cert curriculum path | Info | Dev self-signed helper | non-claim |
| F13 | Cross-cluster | Peers alone ≠ fabric bridge | Info | Clearer “no fabric route” errors | `multi_region_dns_policy` |
| F14 | DNS CLI | `--target` not `--targets` | Low | Alias | `ops_cli_tour` |
| F15 | M2 deadline | Handler still runs after client fail-closed | Info | Docs / cooperative cancel | `rpc_deadline_budget` |
| F16 | Port categories | Fixed enum — typos raise late | Low | (error already lists keys) | general |
| F17 | TopicPattern | `matches_topic` on `{param}` templates → False | Med | Separate format vs wildcard match | `topic_taxonomy_tour` |
| F18 | SQLite backend | `db_path` must be `Path`, not `str` | Med | Coerce str→Path | `persistence_kv`  **FIXED Phase G** (Path coerce) |
| F19 | RaftOracle | Dual-leader raises on `observe_role` | Info | Document fail-fast timing | `routing_oracle_lab` |
| F20 | DiscoveryRateLimiter | `max_keys` soft cap → steady `max_keys+1` | Med | Prune to max_keys-1 before insert | `discovery_rate_limit`  **FIXED Phase G** (hard max_keys) |
| F21 | TopicQueueRouter | `successful_routes` only on `send_via_topic` | Med | Bump on pure route or rename | `topic_queue_router_lab`  **FIXED Phase G** (route success stats) |

**Process:** when a new app hits friction, append a row here **and** a `step("friction: …")` in the app so operators see it live.

See also: [API_FRICTION.md](./API_FRICTION.md).

---

## 10. Program status

**Curriculum program (Phases A–F): COMPLETE at 100%.**

**Phase G (platform DX + observability proof): COMPLETE at 100%.**  
F1/F9/F18/F20/F21 fixed; ExampleProbe + ServerMetricsTracker.snapshot; 8 apps.

**Phase H (remaining Med friction + universal obs): ACTIVE ~5%.**  
Close F2–F8/F17 in platform; default-on probe for all curriculum apps;
operator CLI aliases and doctor URL clarity.

Still honest residual non-claims until productized: F10 live WS chaos hooks,
F12 mTLS turnkey helper (Info).

**Next sequential work:** H1 runtime default probe → H2 CLI → H3 platform Med
fixes → H4 version DX → validate → commit → plan 100%.

---

## Related

- [TRACKER.md](./TRACKER.md) — checkbox delivery status  
- [FEATURE_CATALOG.md](./FEATURE_CATALOG.md) — feature inventory  
- [APP_CATALOG.md](./APP_CATALOG.md) — app matrix  
- [STAGES.md](./STAGES.md) — phase narrative  
- [API_FRICTION.md](./API_FRICTION.md) — usability discovery backlog  
- [OPERATE.md](./OPERATE.md) — runbooks + CI suite  
