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

## Phase H — FQN + Med friction + universal obs (**COMPLETE**)

| Item | Status | Notes |
|------|--------|-------|
| Universal probe default-on | [x] | `app_run(probe=True)` |
| CLI F2/F3/F14 | [x] | call/dns aliases; doctor URL; --targets |
| FQN + `mpreg.*` namespace deny | [x] | `rpc_naming.py` |
| F5/F6/F7/F8/F17 | [x] | version, cache events/invalidate, TopicPattern |
| unit + suite at 70 | [x] | |

## Phase I — Residual polish + FQN teach + docs (**COMPLETE**)

| Item | Status | Notes |
|------|--------|-------|
| `rpc_fqn_namespace` L1 | [x] | bare/FQN/deny/bound proof |
| F13 route_not_found fabric hint | [x] | `errors.route_not_found` + multi_region app |
| F15 deadline preemption honesty | [x] | `rpc_deadline_budget` scenario |
| F16 `list_port_categories` | [x] | + hello_ports teach |
| F19 RaftOracle fail-fast docs | [x] | + routing_oracle_lab |
| F10/F11/F12 honest non-claims | [x] | no fake fixes |
| FEATURE_CATALOG gap→shipped truth | [x] | |
| TRACKER/STAGES/APP_CATALOG sync | [x] | **71** apps |
| unit + suite validation | [x] | see verification log |

## Verification log (Phase I)

| Date | Command | Result |
|------|---------|--------|
| 2026-08-05 | `mpreg-example run rpc_fqn_namespace` | pass (6 scen) |
| 2026-08-05 | `mpreg-example run hello_ports` | pass (F16) |
| 2026-08-05 | `mpreg-example run multi_region_dns_policy` | pass (F13) |
| 2026-08-05 | `mpreg-example run rpc_deadline_budget` | pass (F15) |
| 2026-08-05 | `mpreg-example run routing_oracle_lab` | pass (F19) |
| 2026-08-05 | `pytest tests/examples_apps -m unit` | **86 passed** |
| 2026-08-05 | `mpreg-example suite` | **71/71 passed** (~82s) |

**App count:** 35 → … → 70 → **71** (`rpc_fqn_namespace`).

**Program:** Phases A–I at **100%** per PROJECT_PLAN dashboard.

## Phase J — Productize F10–F12 + catalog residual teach (**COMPLETE**)

| Item | Status | Notes |
|------|--------|-------|
| J0 charter in PROJECT_PLAN | [x] | linear waves J0–J5 |
| F11 `rpc_auth_token` + `client_auth_token` | [x] | handshake gate |
| F12 `dev_certs` + `tls_dev_handshake` | [x] | AKI/SKI PEMs + wss |
| F10 `live_partition_chaos` | [x] | drain/detach/`/ready` |
| `discovery_resolver_audit` | [x] | stats/resync/audit |
| `queue_federation_lab` | [x] | wire codec |
| `transport_health_attach` | [x] | mon.transport |
| `transport_protocol_tour` | [x] | tx.tcp + multi |
| `blockchain_message_lab` | [x] | types + bridge import |
| FEATURE/APP/friction sync | [x] | F10–F12 FIXED J |
| unit + suite at 78 | [x] | see verification log |

## Verification log (Phase J)

| Date | Command | Result |
|------|---------|--------|
| 2026-08-05 | `mpreg-example run client_auth_token` | pass (F11) |
| 2026-08-05 | `mpreg-example run tls_dev_handshake` | pass (F12) |
| 2026-08-05 | `mpreg-example run live_partition_chaos` | pass (F10) |
| 2026-08-05 | `mpreg-example run discovery_resolver_audit` | pass |
| 2026-08-05 | `mpreg-example run queue_federation_lab` | pass |
| 2026-08-05 | `mpreg-example run transport_health_attach` | pass |
| 2026-08-05 | `mpreg-example run transport_protocol_tour` | pass |
| 2026-08-05 | `mpreg-example run blockchain_message_lab` | pass |
| 2026-08-05 | `pytest tests/examples_apps -m unit` | **93 passed** |
| 2026-08-05 | `mpreg-example suite` | **78/78 passed** (~87s) |

**App count:** 71 → **78** (+7 Phase J).

**Program:** Phases A–J at **100%** per PROJECT_PLAN dashboard.

## Phase K — Residual depth + catalog partials (**COMPLETE**)

| Item | Status | Notes |
|------|--------|-------|
| K0 charter serialized into living plans | [x] | G17/G18 + waves K0–K10 |
| K1 API_FRICTION F10–F12 open table cleared | [x] | |
| `discovery_signatures_lab` | [x] | disco.signatures gap closed |
| `rpc_inventory_tour` | [x] | describe + report |
| `client_trace_bind` | [x] | trace + bind |
| `correlation_routing_lab` | [x] | correlation + no-loop |
| `mtls_mesh_handshake` | [x] | CERT_REQUIRED depth |
| `packet_loss_chaos` | [x] | drop rates + drain |
| `blockchain_hub_settlement` | [x] | hub queue settlement |
| FEATURE/APP/STAGES sync | [x] | **85** apps |
| unit + suite | [x] | see verification log |

## Verification log (Phase K)

| Date | Command | Result |
|------|---------|--------|
| 2026-08-05 | all 7 Phase K apps `mpreg-example run` | pass |
| 2026-08-05 | `pytest tests/examples_apps -m unit` | **100 passed** |
| 2026-08-05 | `mpreg-example suite` | **85/85 passed** (~94s) |

**App count:** 78 → **85** (+7 Phase K).

**Program:** Phases A–K at **100%** per PROJECT_PLAN dashboard.

## Phase L — FEATURE partial promotion batch (**COMPLETE**)

| Item | Status | Notes |
|------|--------|-------|
| L0 charter serialized into living plans | [x] | G19 + waves L0–L8 |
| `queue_ack_receive_lab` | [x] | queue.ack/receive/broadcast/fnf |
| `pubsub_client_backlog` | [x] | client.pubsub + backlog |
| `cache_replication_geo` | [x] | geo/repl/L2/invalidate/pers |
| `fabric_policy_modes` | [x] | strict/explicit/catalog/link-state |
| `rpc_concurrency_lab` | [x] | concurrency + M3 + routing_topic |
| `cluster_map_catalog` | [x] | map refresh + catalog_query |
| `mon_logging_json` | [x] | json logs + mon.correlation/health |
| `chaos_crash_recover` | [x] | chaos.crash |
| `tx_circuit_breaker_lab` | [x] | tx.circuit_breaker |
| `ns_engine_direct` | [x] | ns.engine direct |
| FEATURE/APP/STAGES sync | [x] | **95** apps |
| unit + suite | [x] | see verification log |

## Verification log (Phase L)

| Date | Command | Result |
|------|---------|--------|
| 2026-08-05 | all 10 Phase L apps `mpreg-example run` | pass |
| 2026-08-05 | `pytest tests/examples_apps -m unit` | **110 passed** |
| 2026-08-05 | `mpreg-example suite` | **95/95 passed** (~100s) |

**App count:** 85 → **95** (+10 Phase L).

**Program:** Phases A–L at **100%** per PROJECT_PLAN dashboard.

## Phase M — Residual CLI ops + fabric_forward (**COMPLETE**)

| Item | Status | Notes |
|------|--------|-------|
| M0 charter into living plans | [x] | G20 |
| `ops_cli_tour` planes/ns/discovery deepen | [x] | cache/queue/publish/ns/resolver |
| `pubsub_fabric_forward_lab` | [x] | hop metadata |
| FEATURE partials → 0 teachable | [x] | |
| unit + suite | [x] | see log |

## Verification log (Phase M)

| Date | Command | Result |
|------|---------|--------|
| 2026-08-05 | `mpreg-example run` Phase M apps | pass |
| 2026-08-05 | `pytest tests/examples_apps -m unit` | **111 passed** |
| 2026-08-05 | `mpreg-example suite` | **96/96 passed** (~103s) |

**App count:** 95 → **96** (+1 Phase M).

**Program:** Phases A–M at **100%**.

## Phase N — Residual Low DX F22/F23 + planning scrub (**COMPLETE**)

| Item | Status | Notes |
|------|--------|-------|
| N0 charter into living plans | [x] | G21 |
| F22 MessageHeaders Mapping coerce | [x] | `MessageHeaders.coerce` + publish |
| F23 catalog entry_type default/error | [x] | server + CatalogQueryRequest |
| Teach apps (pubsub_client_backlog, cluster_map_catalog) | [x] | deepen |
| POC_NOTES / friction scrub | [x] | stale follow-ups closed |
| unit + suite | [x] | see log |

## Verification log (Phase N)

| Date | Command | Result |
|------|---------|--------|
| 2026-08-05 | `pytest tests/test_message_headers_coerce.py tests/test_fabric_pubsub_forwarding.py` | **9 passed** |
| 2026-08-05 | `pytest tests/examples_apps -m unit` | **111 passed** |
| 2026-08-05 | `mpreg-example run` pubsub_client_backlog + cluster_map_catalog | pass |
| 2026-08-05 | `mpreg-example suite` | **96/96 passed** (~99s) |

**App count:** **96** (no new apps; platform DX + deepen).

**Also in Phase N residual close:** `client.summary` / `call_with_summary`
taught end-to-end in `discovery_watch_summary` (same-cluster path no longer
forces fabric `target_cluster` from summary). FEATURE note un-partialed.

**Program:** Phases A–N at **100%**.

## Phase O — Integrity polish (**COMPLETE**)

| Item | Status | Notes |
|------|--------|-------|
| O0 charter into living plans | [x] | G22 |
| `client_trace_bind` thin deepen | [x] | ≥5 ensures |
| `fabric.gossip` APP_FEATURES join | [x] | discovery_signatures_lab |
| GossipMessage hop/TTL scenario | [x] | fabric.gossip teach |
| OPERATE / APP_CATALOG / README scrub | [x] | no stale planned chaos |
| unit + suite | [x] | see log |

## Verification log (Phase O)

| Date | Command | Result |
|------|---------|--------|
| 2026-08-05 | depth/feature-join audit | thin→0; uncovered→0 |
| 2026-08-05 | `mpreg-example run` client_trace_bind + discovery_signatures_lab | pass |
| 2026-08-05 | `pytest tests/examples_apps -m unit` | **111 passed** |
| 2026-08-05 | `mpreg-example suite` | **96/96** |

**App count:** **96** (no new apps).

**Program:** Phases A–O at **100%**.
