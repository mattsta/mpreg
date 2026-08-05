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
- Gaps remain: DNS plane app, namespace policy app, atomic cache ops, unified client queue/cache RPC on live servers.
