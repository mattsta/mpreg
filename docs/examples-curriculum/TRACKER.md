# Growth Cycle Tracker

Last updated: 2026-08-05

Legend: `[x]` done · `[~]` partial · `[ ]` not started

## Phase A — Foundation + L0/L1

| Item | Status | Notes |
|------|--------|-------|
| Curriculum docs tree | [x] | `docs/examples-curriculum/` |
| `apps/_shared` runtime + registry | [x] | kinds, aliases, bundles |
| Console script `mpreg-example` | [x] | pyproject entrypoint |
| Central CLI `mpreg examples` | [x] | list/run/smoke/suite/describe/path |
| `mpreg demo` → unified runner | [x] | no more direct tier imports |
| L0 hellos (6) | [x] | rpc, cluster, trace, pubsub, cache, ports |
| L1 product + planes | [x] | HA, queue, url, sensor, session, auto_port, 6 planes |
| Pytest `tests/examples_apps/` | [x] | live main() per app |
| Smoke/suite scripts | [x] | entrypoint only |

## Phase B — L2 moderate

| Item | Status | Notes |
|------|--------|-------|
| `order_intake` | [x] | |
| `media_pipeline` | [x] | |
| `feature_flag_mesh` | [x] | |
| `webhook_dispatcher` | [x] | |
| `config_reload_live` | [x] | was persistence_restart_demo |
| tier2 integrations as apps | [x] | rpc_plus_cache, pubsub_plus_queue, cache_plus_federation |
| `ml_inference_mesh` | [x] | |

## Phase C — L3 complex

| Item | Status | Notes |
|------|--------|-------|
| `multi_region_shop` | [x] | |
| `signed_route_border` | [x] | was fabric_route_security_demo |
| `partition_safe_counter` | [x] | FaultInjector quorum teaching |
| `discovery_join` | [x] | |
| `chaos_checkout` | [x] | |
| `fabric_snapshot_restart` | [x] | |
| `tier3_expansion` | [x] | fixed indent bug so it actually runs |

## Phase D — L4 world

| Item | Status | Notes |
|------|--------|-------|
| `global_edge_control_plane` | [x] | hub + US/EU + monitoring |

## Runner coverage

| Command | Status |
|---------|--------|
| `mpreg-example list` | [x] |
| `mpreg-example describe` | [x] |
| `mpreg-example run` | [x] |
| `mpreg-example smoke` | [x] |
| `mpreg-example suite` | [x] |
| `mpreg-example demo` | [x] |
| `mpreg-example bundles` | [x] |
| `mpreg-example path` | [x] |
| Alias resolution | [x] | legacy names → canonical ids |

## Verification log

| Date | Command | Result |
|------|---------|--------|
| 2026-08-05 | `uv run mpreg-example smoke` | **8/8** passed |
| 2026-08-05 | `uv run mpreg-example suite` | **35/35** passed |
| 2026-08-05 | `uv run pytest tests/examples_apps` | **91 passed** |

## 2026-08-05 — Feature-catalog depth pass

- FEATURE_CATALOG.md inventories platform APIs (rpc/client/pubsub/queue/cache/fabric/disco/mon/chaos/pers/…).
- Shared `features.py` + registry `features` field + pytest join tests.
- L0–L4 apps deepened with multi-scenario API drills (not thin wrappers).
- Planes inlined (no 18-line tier1-only shims).

## Phase E — Gap fill (COMPLETE)

Living plan: [PROJECT_PLAN.md](./PROJECT_PLAN.md)

| Wave | Item | Status | Notes |
|------|------|--------|-------|
| E0–E16 | All Phase E waves | [x] | gaps + depth + suite + catalog |

## Phase F — Breadth to 70 (COMPLETE)

| Wave | Item | Status | Notes |
|------|------|--------|-------|
| F | Verticals + hellos + oracles | [x] | → 62 |
| F | Final breadth → 70 | [x] | +8 apps incl. second L4 |
| F | CI / BOOK / OPERATE / EXAMPLES sync | [x] | nightly suite docs |
| F | Friction F20–F21 | [x] | rate limit soft cap; router stats |

**App count:** 35 → 40 → 44 → 56 → 62 → **70** shipped (50–70 band **complete**).

**API discovery:** [API_FRICTION.md](./API_FRICTION.md) (F1–F21).

**Program:** Phases A–F at **100%** per PROJECT_PLAN dashboard.

## Verification log (append)

| Date | Command | Result |
|------|---------|--------|
| 2026-08-05 | `mpreg-example run cache_atomic_ops` | pass (5 scen, ~21 ens) |
| 2026-08-05 | `mpreg-example run namespace_policy_gate` | pass (6 scen, ~9 ens) |
| 2026-08-05 | `mpreg-example run plane_dns` | pass (5 scen, ~9 ens) |
| 2026-08-05 | `mpreg-example run unified_client_tour` | pass (4 scen, ~12 ens) |
| 2026-08-05 | `mpreg-example run pubsub_request_reply` | pass (4 scen, ~12 ens) |
| 2026-08-05 | E6 deepen thin apps | all 9 green; 0 thin |
| 2026-08-05 | E7–E12 four apps | all green; 44 apps |
| 2026-08-05 | E9–E14 + F twelve apps | all green; **56** apps; unit 71 |
| 2026-08-05 | `pytest tests/examples_apps -m unit` | **71 passed** |
| 2026-08-05 | `mpreg-example suite` | **56/56 passed** |
| 2026-08-05 | F breadth +6 | **62/62 suite** |
| 2026-08-05 | F final +8 each `run` | all green |
| 2026-08-05 | `mpreg-example suite` (70) | see latest suite run |
| 2026-08-05 | `pytest tests/examples_apps -m unit` | see latest unit run |

## Phase G — Platform DX + observability (**COMPLETE**)

| Item | Status | Notes |
|------|--------|-------|
| F9 CircuitBreaker timeout sync | [x] | `__post_init__` |
| F18 SQLite Path\|str coerce | [x] | |
| F20 DiscoveryRateLimiter hard cap | [x] | prune max_keys-1 |
| F21 TopicQueueRouter successful_routes | [x] | on route match |
| F1 CLI `run_coro` nested-safe | [x] | async_utils |
| ExampleProbe + app_run(probe=True) | [x] | `_shared/obs.py` |
| ServerMetricsTracker.snapshot() | [x] | |
| ≥8 apps latency/throughput ensures | [x] | 8 apps |
| README Observability annotations | [x] | |
| unit + suite validation | [x] | |
