# Curriculum Examples — Living Project Plan

**Last updated:** 2026-08-05 (E7–E14 + Phase F verticals wave)  
**Owner drive:** sequential iterative completion of every feature-catalog gap,
depth contract, automation, validation, **and API usability discovery** —
**not** batch-and-stop.

Legend: `[x]` done · `[~]` partial · `[>]` in progress · `[ ]` not started

---

## 1. North-star goals

| # | Goal | Success measure | Status |
|---|------|-----------------|--------|
| G1 | **Full-power curriculum** — every honest platform surface taught at the lowest level that can prove it | FEATURE_CATALOG rows `shipped` (or explicit non-claim if product gap) | [~] E1–E14 closed prioritized gaps 1–8; residual polish remains |
| G2 | **Depth contract on every app** | ≥2 scenarios; L0 ≥3 ensures; L1+ ≥5 ensures; `app_run` summary | [x] 0 thin (E6); new apps meet contract on land |
| G3 | **Entrypoint-only UX** | `uv run mpreg-example` / `mpreg examples` / `mpreg demo` only | [x] |
| G4 | **Demo-as-test** | live `main()` via pytest + smoke/suite green | [x] unit 71; per-app runs green; full suite pending E15 |
| G5 | **Feature join integrity** | every app tagged; pytest `test_every_app_has_feature_tags` | [x] |
| G6 | **50–70 apps when catalog requires** | expand matrix beyond thin 35; no bulk-empty shells | [>] **62** shipped (target **≥50** met; drive toward **70**) |
| G7 | **Living plan** | this file + TRACKER + friction log updated every commit slice | [>] |
| G8 | **Commit-as-you-go** | clean what/why/how messages per layer | [x] practice; continue |
| G9 | **API discovery / usability** | curriculum apps surface edge cases + improvement candidates | [>] see §9 API friction log |

---

## 2. Current baseline

| Metric | Value |
|--------|------:|
| Shipped apps | **62** |
| Smoke apps | 8 |
| Suite apps | 56 (all registry `suite=True`) |
| Feature IDs in `features.py` constants | ~130+ |
| FEATURE_CATALOG prioritized gaps 1–8 | **closed** (honest non-claims where needed) |
| Thin apps (scen<2 or L1+ ens<5) | **0** |
| Branch vs origin | main ahead **95+** (local only; no push unless asked) |
| Last new-app validation | E7–E14 + F verticals each `mpreg-example run` green |
| Last unit | `pytest tests/examples_apps -m unit` → **71 passed** |
| Last full suite | pending (E15) |

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

### Phase E — Feature-catalog gap fill + depth (ACTIVE → nearly complete)

**Goal:** close prioritized FEATURE_CATALOG gaps with **real multi-scenario apps**,
deepen thin apps, keep automation/tests green, grow matrix toward 50+ **and**
log API usability friction for platform follow-ups.

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
| **E15** | Full suite automation + coverage report | suite 56/56 + docs | [x] |
| **E16** | APP_CATALOG / STAGES / FEATURE_CATALOG sync | docs | [x] |

**Phase E exit criteria**

- [x] ≥50 shipped apps in registry (**56**)  
- [x] Prioritized gaps 1–8 from FEATURE_CATALOG closed or honestly non-claimed  
- [x] Zero apps below depth contract  
- [x] `uv run mpreg-example suite` **56/56** (~75s)  
- [~] `uv run pytest tests/examples_apps` unit 71; full live via suite path  
- [x] FEATURE_CATALOG + APP_CATALOG coverage matrix updated  
- [x] PROJECT_PLAN completion % ≥ 85% for Phase E waves E0–E16  

### Phase F — Breadth to 60–70 + operator polish (STARTED)

| Item | Status |
|------|--------|
| Additional product verticals (billing, notifications, inventory) | [x] `billing_ledger`, `notification_fanout`, `inventory_reserve` |
| Multi-region + DNS + policy composition (L3/L4) | [x] `multi_region_dns_policy` |
| L0 hellos for queue/dns | [x] `hello_queue`, `hello_dns` |
| Deadline budget teaching | [x] `rpc_deadline_budget` + `deadline_hop_budget` |
| Topic→queue bridge integration | [x] `topic_queue_bridge` |
| Taxonomy / persistence / profiles / oracles / intermediate | [x] +6 → **62** |
| Nightly suite in CI docs | [ ] |
| Curriculum BOOK chapter sync | [ ] |
| More verticals / second L4 world | [ ] toward 70 |

---

## 4. Target app matrix growth

| Band | Now | Target | Notes |
|------|----:|-------:|-------|
| L0 hellos | 8 | 6–8 | + hello_queue, hello_dns |
| L1 product/planes | ~20 | 18+ | + versioned RPC, auth, DLQ, … |
| L2 product/integ | ~16 | 14+ | + ops, notify, billing, inventory, bridge |
| L3 complex | ~11 | 12+ | + graph, watch, chaos_transport, dns policy, deadline |
| L4 world | 1 | 2–3 | optional second world tour |
| **Total** | **62** | **50–70** | ≥50 met; mid Phase F toward 70 |

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

---

## 7. Completion dashboard

| Area | Weight | Done | Notes |
|------|-------:|-----:|-------|
| Runner + foundation | 10% | 10 | [x] |
| L0–L4 baseline matrix | 20% | 20 | [x] 35+ apps |
| Feature-catalog tagging | 10% | 9 | [~] join tests; catalog docs catch-up |
| Gap apps (E1–E14) | 30% | 28 | [x] E0–E14 landed |
| Depth contract all apps | 10% | 10 | [x] 0 thin |
| Phase F breadth 50–70 | 10% | 8 | [>] 62 apps; mid-band |
| Docs/plan living sync | 5% | 4 | [~] plan+tracker+friction |
| Automation/report (E15) | 5% | 5 | [x] suite 56/56 |
| **Overall curriculum program** | **100%** | **~95%** | F to 70 + friction triage + nightly docs → 100 |

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
| F1 | CLI nesting | `mpreg` click handlers call `asyncio.run` → **cannot** be invoked from an already-running event loop | High | Offer async entrypoints or `anyio`/`asyncioRunner` compatible path; document thread offload | `ops_cli_tour` |
| F2 | CLI IA | Operators guess `mpreg dns …` / `mpreg call …`; real paths are `mpreg client dns-*` / `mpreg client call` | Med | Top-level aliases or clearer `--help` epilog | `ops_cli_tour` |
| F3 | `mpreg doctor` | `--url` is **monitoring HTTP**, not WS RPC; WS URL fails opaquely | Med | Accept `ws://` and derive/reject with explicit message; or dual `--rpc-url` / `--monitoring-url` defaults | `ops_cli_tour` |
| F4 | RPC registry | Built-in name `echo` already registered → user `register_command("echo", …)` raises | Med | Document reserved builtins; `register_command` error should list conflict source | `ops_cli_tour` |
| F5 | Versioned RPC | Same `function_id` **cannot** register two versions on **one** node (`Function id conflict`); multi-version needs multi-node | Med | Allow multi-version map per function_id on one registry **or** document clearly | `rpc_versioned_topic` |
| F6 | Version miss | Impossible `version_constraint` surfaces as generic `Command not found` | Med | Distinct `version_mismatch` / structured error always | `rpc_versioned_topic` |
| F7 | Cache events | `add_event_listener` is **registration-only**; delivery is via topic exchange / `notifications_sent` | Med | Rename or dual-path docs; fire listeners on notify | `cache_event_bus` |
| F8 | Cache invalidation | `broadcast_cache_invalidation(namespace=)` wrong kwargs — needs `cache_key=` / `pattern=` | Med | Keyword-only API + better TypeError | `cache_event_bus` |
| F9 | CircuitBreaker | Setting only `timeout_seconds` leaves `current_timeout` at default 60s — half-open demos hang | Med | Sync `current_timeout` when constructing / document both fields | `fabric_graph_resilience` |
| F10 | Chaos model | `FaultInjector` is lab-only; live WS partition not wired through injector | Info | Server-side partition hooks when ready | `chaos_*` |
| F11 | Auth | `auth_token` on client wires transport security; local WS RPC does **not** enforce it by default | Info | Optional server `require_auth_token`; document monitoring vs RPC auth split | `client_auth_token` |
| F12 | mTLS | No turnkey local-cert curriculum path yet | Info | Dev profile self-signed helper | `client_auth_token` non-claim |
| F13 | Cross-cluster | Peers alone ≠ fabric bridge; EU locs from US client may `Command not found` without fabric mode | Info | Clearer errors: “no fabric route to cluster X” | `multi_region_dns_policy` |
| F14 | DNS CLI | Under `client` group; `--target` repeatable (not `--targets`) | Low | Alias `--targets` | `ops_cli_tour` |
| F15 | M2 deadline | Handler `time.sleep` still runs after client fail-closed (no server preemption) | Info | Document; optional cooperative cancellation | `rpc_deadline_budget` |
| F16 | Port categories | `port_range_context` categories are fixed enum — typos raise late | Low | Clearer error already lists available | general |

**Process:** when a new app hits friction, append a row here **and** a `step("friction: …")` in the app so operators see it live.

See also: [API_FRICTION.md](./API_FRICTION.md) (same log, expandable).

---

## 10. Next sequential work (do not stop)

1. ~~E15 full suite~~ **56/56 green**  
2. ~~E16 catalog sync~~ APP_CATALOG lists 56  
3. **F polish** — more verticals / second L4 toward **60–70**  
4. **Platform follow-ups** — triage F1–F9 with maintainers (DX wins from curriculum)  
5. Optional coverage report script under `scripts/`  
6. Keep validating + committing every slice  

---

## Related

- [TRACKER.md](./TRACKER.md) — checkbox delivery status  
- [FEATURE_CATALOG.md](./FEATURE_CATALOG.md) — feature inventory  
- [APP_CATALOG.md](./APP_CATALOG.md) — app matrix  
- [STAGES.md](./STAGES.md) — phase narrative  
- [API_FRICTION.md](./API_FRICTION.md) — usability discovery backlog  
