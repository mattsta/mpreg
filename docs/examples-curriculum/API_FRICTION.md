# API Friction & Usability Discovery

**Purpose:** Curriculum example apps are not only teaching material — they are a
**forced integration walk** of public MPREG APIs. Every awkward edge, missing
error code, or CLI surprise gets logged here so platform DX can improve.

**Last updated:** 2026-08-05 (Phase I — residual Info polish + FQN curriculum)  
**Source of truth also summarized in:** [PROJECT_PLAN.md §9](./PROJECT_PLAN.md)

Legend severity: **High** (blocks nested/async use or confuses operators badly) ·
**Med** (wrong guess / weak error / surprising constraint) · **Low/Info** (docs).

---

## Open findings (honest residual non-claims)

| ID | Surface | Finding | Sev | Suggested improvement | App |
|----|---------|---------|-----|----------------------|-----|
| F10 | Chaos | Injector not live-WS wired | Info | Server partition hooks | `chaos_*` |
| F11 | Auth | Client `auth_token` not enforced on local WS RPC | Info | Optional require_auth | `client_auth_token` |
| F12 | mTLS | No local-cert curriculum helper | Info | Dev self-signed profile | non-claim |

These remain **product limits**, not curriculum bugs. Do not fake-fix with
thin demos.

---

## Fixed / documented in platform (Phases G + H + I)

| ID | Fix | Where |
|----|-----|-------|
| F1 | Nested-loop-safe `run_coro` replaces bare `asyncio.run` in CLI handlers | `mpreg/cli/async_utils.py`, `mpreg/cli/main.py` |
| F2 | Top-level `mpreg call` / `mpreg dns` aliases | `mpreg/cli/main.py` |
| F3 | Doctor rejects WS URL with clear monitoring-HTTP guidance | `mpreg/cli/main.py` |
| F4 | **Superseded by FQN + namespace deny** (not a short-name denylist). Wire names are dotted FQNs; bare → active ns (`app` default); users cannot inject into `mpreg.*`; full flexibility elsewhere; optional hierarchical `bound_rpc_namespace` | `mpreg/core/rpc_naming.py`; curriculum: `rpc_fqn_namespace` |
| F5 | Multi-version same-node + loud same-version collision | registry + `rpc_versioned_topic` |
| F6 | VERSION_MISMATCH (1002) when constraint misses other versions | `server._raise_route_miss` |
| F7 | `add_event_listener` callbacks fire on `notify_cache_event` | `mpreg/core/cache_pubsub_integration.py` |
| F8 | Keyword-only `invalidate` + helpful TypeError on bad kwargs | `mpreg/core/global_cache.py` |
| F9 | `CircuitBreaker.__post_init__` syncs `current_timeout` from `timeout_seconds` when default `-1` | `mpreg/fabric/federation_optimized.py` |
| F13 | `route_not_found` details name fabric bridge / peer-gossip limit | `mpreg/core/errors.py`; proven in `multi_region_dns_policy` |
| F14 | DNS CLI `--targets` alias for `--target` | `mpreg/cli/main.py` |
| F15 | **Documented:** client fail-closed ≠ server handler preemption for sync work | `rpc_deadline_budget` scenario + this log |
| F16 | `list_port_categories()` + unknown category lists keys | `mpreg/core/port_allocator.py`; `hello_ports` |
| F17 | `{param}` templates match as single-segment `*` wildcards in `matches_topic` | `mpreg/core/topic_taxonomy.py` |
| F18 | `SQLitePersistenceBackend.db_path: Path \| str` + coerce in `__post_init__` | `mpreg/core/persistence/backend.py` |
| F19 | **Documented:** RaftOracle dual-leader raises on `observe_role` (fail-fast) | `mpreg/testing/oracles.py`; `routing_oracle_lab` |
| F20 | `DiscoveryRateLimiter` prunes to `max_keys-1` before insert → hard cap `≤ max_keys` | `mpreg/core/discovery_rate_limit.py` |
| F21 | `route_message_to_queues` bumps `successful_routes` / `failed_routes`; `send_via_topic` avoids double-count | `mpreg/core/topic_queue_routing.py` |

Also shipped: `ServerMetricsTracker.snapshot()`, shared `ExampleProbe`
(`mpreg/examples/apps/_shared/obs.py`), `app_run(..., probe=True)` + `get_probe()`.

## How to add a finding

1. Hit the issue while building/running an app.  
2. Prefer fixing the **app** with an honest `step("friction: …")` / non-claim.  
3. Append a row here + PROJECT_PLAN §9.  
4. Optionally open a platform issue referencing `F#`.

---

## Closed / mitigated in curriculum (platform may still improve)

| ID | Mitigation in apps |
|----|-------------------|
| F4 | **Platform-fixed:** bare `echo` → `app.echo` (≠ `mpreg.system.echo`); `rpc_fqn_namespace` + `ops_cli_tour` prove user names legal outside `mpreg.*` |
| F5–F6 | `rpc_versioned_topic` multi-version + VERSION_MISMATCH |
| F7–F8 | Platform-fixed; apps drop non-claims |
| F2–F3 / F14 | CLI aliases + doctor URL clarity |
| F13 | Operator-readable fabric route miss |
| F15 / F19 | Documented fail-closed / fail-fast invariants |
| F16 | Discoverable port categories |

## Phase J closes (2026-08-05)

| ID | Resolution |
|----|------------|
| F10 | Live `/mgmt/v1/nodes/drain` + `/peers/detach` + `/ready` taught in `live_partition_chaos` |
| F11 | `MPREGSettings.rpc_auth_token` enforced in `MPREGServer.opened` |
| F12 | `mpreg.core.dev_certs.generate_dev_tls_material` + `tls_*` settings + `tls_dev_handshake` |
