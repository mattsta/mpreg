# Curriculum Examples — Living Project Plan

**Last updated:** 2026-08-05  
**Owner drive:** sequential iterative completion of every feature-catalog gap,
depth contract, automation, and validation — **not** batch-and-stop.

Legend: `[x]` done · `[~]` partial · `[>]` in progress · `[ ]` not started

---

## 1. North-star goals

| # | Goal | Success measure | Status |
|---|------|-----------------|--------|
| G1 | **Full-power curriculum** — every honest platform surface taught at the lowest level that can prove it | FEATURE_CATALOG rows `shipped` (or explicit non-claim if product gap) | [~] E1–E5 closed top gaps; ~35 catalog rows still `gap` |
| G2 | **Depth contract on every app** | ≥2 scenarios; L0 ≥3 ensures; L1+ ≥5 ensures; `app_run` summary | [~] 9/35 thin (see §4) |
| G3 | **Entrypoint-only UX** | `uv run mpreg-example` / `mpreg examples` / `mpreg demo` only | [x] |
| G4 | **Demo-as-test** | live `main()` via pytest + smoke/suite green | [x] 35/35 suite (baseline); grows with apps |
| G5 | **Feature join integrity** | every app tagged; pytest `test_every_app_has_feature_tags` | [x] |
| G6 | **50–70 apps when catalog requires** | expand matrix beyond thin 35; no bulk-empty shells | [>] **40** → target **≥50** shipped |
| G7 | **Living plan** | this file + TRACKER updated every commit slice | [>] |
| G8 | **Commit-as-you-go** | clean what/why/how messages per layer | [x] practice; continue |

---

## 2. Current baseline (start of Phase E drive)

| Metric | Value |
|--------|------:|
| Shipped apps | **40** |
| Smoke apps | 8 |
| Suite apps | 40 |
| Feature IDs in `features.py` constants | ~105 |
| FEATURE_CATALOG rows marked `gap` | **~35** (was ~46) |
| Thin apps (scen<2 or ens<5) | **9** (E6 next) |
| Branch vs origin | main ahead **92+** (local only; no push unless asked) |
| Last new-app validation | E1–E5 each `mpreg-example run` green |
| Last full suite | pending after E1–E5 commit |

### Thin backlog (must deepen before claiming depth-complete)

| App | scen | ens | Action |
|-----|-----:|----:|--------|
| `tier3_expansion` | 1 | 0 | Inline multi-scenario; stop pure legacy wrap |
| `signed_route_border` | 1 | 2 | Multi-scenario security tour + ensures |
| `fabric_snapshot_restart` | 1 | 2 | Multi-scenario snapshot/restart ensures |
| `plane_fabric` | 2 | 3 | Explicit bridge + more ensures |
| `plane_monitoring` | 3 | 4 | +1 ensure / health drill |
| `plane_queue` | 5 | 4 | +1 ensure (ack/receive) |
| `job_queue_worker` | 5 | 4 | +1 ensure |
| `hello_trace` | 3 | 4 | +1 ensure |
| `cache_plus_federation` | 3 | 4 | +1 ensure / second key |

---

## 3. Phases

### Phase A–D — Foundation through L4 flagship

| Phase | Scope | Status | Exit |
|-------|-------|--------|------|
| A | Runtime, runner, L0/L1, smoke | [x] | smoke green |
| B | L2 product + integrations | [x] | suite includes L2 |
| C | L3 mesh / chaos / fabric security | [x] | suite includes L3 |
| D | L4 `global_edge_control_plane` | [x] | suite includes L4 |

### Phase E — Feature-catalog gap fill + depth (ACTIVE)

**Goal:** close prioritized FEATURE_CATALOG gaps with **real multi-scenario apps**,
deepen thin apps, keep automation/tests green, grow matrix toward 50+.

| Wave | Deliverables | Apps (new or deepen) | Status |
|------|--------------|----------------------|--------|
| **E0** | Living PROJECT_PLAN + tracker refresh | docs | [x] |
| **E1** | Cache atomic / structures / ns ops | `cache_atomic_ops` (L1 plane) | [x] |
| **E2** | Namespace policy wire path | `namespace_policy_gate` (L1 plane) | [x] |
| **E3** | DNS plane register/list/describe/resolve | `plane_dns` (L1 plane) | [x] |
| **E4** | Unified client four-plane façade | `unified_client_tour` (L1 product) | [x] |
| **E5** | Pubsub request/reply | `pubsub_request_reply` (L1 product) | [x] |
| **E6** | Deepen 9 thin apps to depth contract | listed in §2 | [ ] |
| **E7** | Discovery watches + summary query | `discovery_watch_summary` (L2/L3) | [ ] |
| **E8** | Fabric graph / resilience drills | `fabric_graph_resilience` (L3) | [ ] |
| **E9** | Chaos extras (clock skew, dup) | deepen `chaos_checkout` or `chaos_transport` | [ ] |
| **E10** | Ops CLI teaching apps (call/dns/ns) | `ops_cli_tour` (L2 legacy/ops) | [ ] |
| **E11** | Cache pubsub events integration | `cache_event_bus` (L2) | [ ] |
| **E12** | Queue DLQ path | deepen `plane_queue` / `job_queue_dlq` | [ ] |
| **E13** | Topic-aware / versioned RPC | `rpc_versioned_topic` when API honest | [ ] |
| **E14** | TLS / auth_token path | `client_auth_token` when local-cert story ready | [ ] |
| **E15** | Full suite automation + coverage report script | `scripts/` + pytest | [ ] |
| **E16** | APP_CATALOG / STAGES / FEATURE_CATALOG sync to 50+ | docs | [ ] |

**Phase E exit criteria**

- [ ] ≥50 shipped apps in registry  
- [ ] Prioritized gaps 1–6 from FEATURE_CATALOG closed or honestly non-claimed  
- [ ] Zero apps below depth contract  
- [ ] `uv run mpreg-example suite` 100%  
- [ ] `uv run pytest tests/examples_apps` 100%  
- [ ] FEATURE_CATALOG coverage matrix updated  
- [ ] PROJECT_PLAN completion % ≥ 85% for Phase E waves E0–E12  

### Phase F — Breadth to 60–70 + operator polish (planned)

| Item | Status |
|------|--------|
| Additional product verticals (billing, notifications, inventory) | [ ] |
| Multi-region + DNS + policy composition (L3/L4) | [ ] |
| Nightly suite in CI docs | [ ] |
| Curriculum BOOK chapter sync | [ ] |

---

## 4. Target app matrix growth

| Band | Now | Target | Notes |
|------|----:|-------:|-------|
| L0 hellos | 6 | 6–8 | optional hello_queue / hello_dns |
| L1 product | 8 | 10+ | + unified client, reply |
| L1 planes | 9 | 10+ | + plane_dns, atomic, ns policy |
| L2 product/integ | 10 | 14+ | + cache events, ops tour |
| L3 complex | 7 | 12+ | + graph, watch, dlq, deepen legacy |
| L4 world | 1 | 2–3 | optional second world tour |
| **Total** | **40** | **50–70** | quality over empty shells |

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
6. Update FEATURE_CATALOG depth + APP_CATALOG row  
7. Update this plan accomplishments + %  
8. **Commit** with what/why/how (no push unless asked)

---

## 6. Accomplishments log

| Date | Slice | Result |
|------|-------|--------|
| 2026-08-05 | Phases A–D + feature depth pass | 35 apps; suite 35/35; feature tags + join tests |
| 2026-08-05 | PROJECT_PLAN.md created | Phase E waves E0–E16 defined; thin list; gates |
| 2026-08-05 | E1–E5 gap apps | +5 apps → **40**; atomic/ns/dns/unified/reply all green |

*(append every commit slice below)*

---

## 7. Completion dashboard

| Area | Weight | Done | Notes |
|------|-------:|-----:|-------|
| Runner + foundation | 10% | 10 | [x] |
| L0–L4 baseline matrix | 25% | 25 | [x] 35 apps |
| Feature-catalog tagging | 10% | 8 | [~] join tests; gaps remain |
| Gap apps (E1–E5, E7–E14) | 30% | 12 | [~] E1–E5 done; E7+ open |
| Depth contract all apps | 15% | 10 | [~] 9 thin (E6 next) |
| Docs/plan living sync | 5% | 4 | [~] plan+tracker+catalogs updated |
| Automation/report (E15) | 5% | 1 | [~] suite exists |
| **Overall curriculum program** | **100%** | **~70%** | drive to 100 via E6–E16 + F |

---

## 8. Working rules (non-negotiable)

1. **Do not stop and claim done** after a small batch — update plan and continue next wave.  
2. **No bulk-thin apps** — every new id meets depth contract on first land.  
3. **Honesty** — non-claims for unproven surfaces; never invent APIs.  
4. **Entrypoints only** — never document `python -m`.  
5. **Commit as you go** — each wave or app slice is commit-ready.  
6. **Validate every step** — run the app before the next one.

---

## Related

- [TRACKER.md](./TRACKER.md) — checkbox delivery status  
- [FEATURE_CATALOG.md](./FEATURE_CATALOG.md) — API inventory + gaps  
- [APP_CATALOG.md](./APP_CATALOG.md) — matrix + bundles  
- [STAGES.md](./STAGES.md) — learning path phases  
- [APP_CONVENTIONS.md](./APP_CONVENTIONS.md) — depth contract  
