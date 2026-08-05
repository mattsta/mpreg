# API Friction & Usability Discovery

**Purpose:** Curriculum example apps are not only teaching material — they are a
**forced integration walk** of public MPREG APIs. Every awkward edge, missing
error code, or CLI surprise gets logged here so platform DX can improve.

**Last updated:** 2026-08-05 (Phase H — FQN namespace deny + Med friction)  
**Source of truth also summarized in:** [PROJECT_PLAN.md §9](./PROJECT_PLAN.md)

Legend severity: **High** (blocks nested/async use or confuses operators badly) ·
**Med** (wrong guess / weak error / surprising constraint) · **Low/Info** (docs).

---

## Open findings

| ID | Surface | Finding | Sev | Suggested improvement | App |
|----|---------|---------|-----|----------------------|-----|
| F2 | CLI IA | Guessed paths `mpreg dns` / `mpreg call` wrong; real is `mpreg client …` | Med | Top-level aliases or help epilog | `ops_cli_tour` |
| F3 | `doctor` | `--url` = monitoring HTTP only; WS URL fails | Med | Explicit dual URL flags + better error | `ops_cli_tour` |
| F5 | Versioned RPC | Multi-version same-node needs curriculum proof | Med | App proof + loud collision errors | `rpc_versioned_topic` |
| F6 | Version miss | Bad constraint → generic command-not-found | Med | Always raise structured version_mismatch | `rpc_versioned_topic` |
| F10 | Chaos | Injector not live-WS wired | Info | Server partition hooks | `chaos_*` |
| F11 | Auth | Client `auth_token` not enforced on local WS RPC | Info | Optional require_auth | `client_auth_token` |
| F12 | mTLS | No local-cert curriculum helper | Info | Dev self-signed profile | non-claim |
| F13 | Fabric | Peers ≠ cross-cluster route | Info | Better “no fabric route” errors | `multi_region_dns_policy` |
| F14 | DNS CLI | `--target` not `--targets` | Low | Alias | `ops_cli_tour` |
| F15 | Deadlines | Server handler not preempted after client fail-closed | Info | Docs / cooperative cancel | `rpc_deadline_budget` |
| F16 | Ports | Fixed port category enum | Low | (error already lists keys) | general |
| F19 | RaftOracle | Dual-leader raises on `observe_role`, not deferred to `assert_safe` | Info | Document fail-fast invariant timing | `routing_oracle_lab` |

---

## Fixed in platform (Phase G + H)

| ID | Fix | Where |
|----|-----|-------|
| F1 | Nested-loop-safe `run_coro` replaces bare `asyncio.run` in CLI handlers | `mpreg/cli/async_utils.py`, `mpreg/cli/main.py` |
| F4 | **Superseded by FQN + namespace deny** (not a short-name denylist). Wire names are dotted FQNs; bare → active ns (`app` default); users cannot inject into `mpreg.*`; full flexibility elsewhere; optional hierarchical `bound_rpc_namespace` for operator↔client conformance | `mpreg/core/rpc_naming.py`, `server.register_command`, client qualify paths |
| F7 | `add_event_listener` callbacks fire on `notify_cache_event` | `mpreg/core/cache_pubsub_integration.py` |
| F8 | Keyword-only `invalidate` + helpful TypeError on bad kwargs | `mpreg/core/global_cache.py` |
| F9 | `CircuitBreaker.__post_init__` syncs `current_timeout` from `timeout_seconds` when default `-1` | `mpreg/fabric/federation_optimized.py` |
| F17 | `{param}` templates match as single-segment `*` wildcards in `matches_topic` | `mpreg/core/topic_taxonomy.py` |
| F18 | `SQLitePersistenceBackend.db_path: Path \| str` + coerce in `__post_init__` | `mpreg/core/persistence/backend.py` |
| F20 | `DiscoveryRateLimiter` prunes to `max_keys-1` before insert → hard cap `≤ max_keys` | `mpreg/core/discovery_rate_limit.py` |
| F21 | `route_message_to_queues` bumps `successful_routes` / `failed_routes`; `send_via_topic` avoids double-count | `mpreg/core/topic_queue_routing.py` |

Also shipped: `ServerMetricsTracker.snapshot()`, shared `ExampleProbe` (`mpreg/examples/apps/_shared/obs.py`),
`app_run(..., probe=True)` + `get_probe()`.

## How to add a finding

1. Hit the issue while building/running an app.  
2. Prefer fixing the **app** with an honest `step("friction: …")` / non-claim.  
3. Append a row here + PROJECT_PLAN §9.  
4. Optionally open a platform issue referencing `F#`.

---

## Closed / mitigated in curriculum (platform may still improve)

| ID | Mitigation in apps |
|----|-------------------|
| F4 | **Platform-fixed:** bare `echo` → `app.echo` (≠ `mpreg.system.echo`); `ops_cli_tour` proves user `echo` is legal |
| F5 | Two-node demo for v1/v2 of `catalog.price` (multi-version same-node also OK in registry) |
| F7–F8 | Platform-fixed; apps drop non-claims |
| F2–F3 | Apps document real CLI paths / doctor monitoring URL (aliases landing in H2) |
