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

## Phase E — Gap fill (ACTIVE)

Living plan: [PROJECT_PLAN.md](./PROJECT_PLAN.md)

| Wave | Item | Status | Notes |
|------|------|--------|-------|
| E0 | Living PROJECT_PLAN | [x] | goals, waves, dashboard |
| E1 | `cache_atomic_ops` | [x] | CAS/incr/structures/ns bulk |
| E2 | `namespace_policy_gate` | [x] | validate/apply/status/export/audit |
| E3 | `plane_dns` | [x] | register/list/describe + UDP/TCP resolve |
| E4 | `unified_client_tour` | [x] | MPREGClient RPC+cache+queue |
| E5 | `pubsub_request_reply` | [x] | publish_with_reply |
| E6 | Deepen 9 thin apps | [x] | 0 thin remaining; tier3 inlined |
| E7+ | Remaining catalog gaps | [ ] | graph, watches, dlq, ops, … |

**App count:** 35 → **40** shipped.

## Verification log (append)

| Date | Command | Result |
|------|---------|--------|
| 2026-08-05 | `mpreg-example run cache_atomic_ops` | pass (5 scen, ~21 ens) |
| 2026-08-05 | `mpreg-example run namespace_policy_gate` | pass (6 scen, ~9 ens) |
| 2026-08-05 | `mpreg-example run plane_dns` | pass (5 scen, ~9 ens) |
| 2026-08-05 | `mpreg-example run unified_client_tour` | pass (4 scen, ~12 ens) |
| 2026-08-05 | `mpreg-example run pubsub_request_reply` | pass (4 scen, ~12 ens) |

| 2026-08-05 | E6 deepen thin apps | all 9 green; 0 thin |
