# API Friction & Usability Discovery

**Purpose:** Curriculum example apps are not only teaching material — they are a
**forced integration walk** of public MPREG APIs. Every awkward edge, missing
error code, or CLI surprise gets logged here so platform DX can improve.

**Last updated:** 2026-08-05  
**Source of truth also summarized in:** [PROJECT_PLAN.md §9](./PROJECT_PLAN.md)

Legend severity: **High** (blocks nested/async use or confuses operators badly) ·
**Med** (wrong guess / weak error / surprising constraint) · **Low/Info** (docs).

---

## Open findings

| ID | Surface | Finding | Sev | Suggested improvement | App |
|----|---------|---------|-----|----------------------|-----|
| F1 | CLI | Handlers use `asyncio.run` → cannot nest on running loop | High | Async-native CLI entry or documented `to_thread` pattern | `ops_cli_tour` |
| F2 | CLI IA | Guessed paths `mpreg dns` / `mpreg call` wrong; real is `mpreg client …` | Med | Top-level aliases or help epilog | `ops_cli_tour` |
| F3 | `doctor` | `--url` = monitoring HTTP only; WS URL fails | Med | Explicit dual URL flags + better error | `ops_cli_tour` |
| F4 | RPC register | Built-in `echo` name collision | Med | Document reserved names; richer ValueError | `ops_cli_tour` |
| F5 | Versioned RPC | One node cannot host two versions of same `function_id` | Med | Multi-version registry **or** loud docs | `rpc_versioned_topic` |
| F6 | Version miss | Bad constraint → generic command-not-found | Med | Always raise structured version_mismatch | `rpc_versioned_topic` |
| F7 | Cache events | `add_event_listener` registration-only | Med | Fire listeners or rename API | `cache_event_bus` |
| F8 | Cache invalidate | Wrong kwarg names easy to guess | Med | Keyword-only + clear TypeError | `cache_event_bus` |
| F9 | CircuitBreaker | `timeout_seconds` ≠ `current_timeout` for half-open | Med | Sync fields on init | `fabric_graph_resilience` |
| F10 | Chaos | Injector not live-WS wired | Info | Server partition hooks | `chaos_*` |
| F11 | Auth | Client `auth_token` not enforced on local WS RPC | Info | Optional require_auth | `client_auth_token` |
| F12 | mTLS | No local-cert curriculum helper | Info | Dev self-signed profile | non-claim |
| F13 | Fabric | Peers ≠ cross-cluster route | Info | Better “no fabric route” errors | `multi_region_dns_policy` |
| F14 | DNS CLI | `--target` not `--targets` | Low | Alias | `ops_cli_tour` |
| F15 | Deadlines | Server handler not preempted after client fail-closed | Info | Docs / cooperative cancel | `rpc_deadline_budget` |
| F16 | Ports | Fixed port category enum | Low | (error already lists keys) | general |
| F17 | TopicPattern | `matches_topic` on `{param}` format templates returns False — templates ≠ AMQP wildcards | Med | Separate `format_match` vs wildcard match APIs or convert `{x}`→`*` in matcher | `topic_taxonomy_tour` |
| F18 | SQLite backend | `db_path` must be `pathlib.Path`; bare `str` fails on `.parent` | Med | Coerce `str→Path` in `__post_init__` | `persistence_kv` |
| F19 | RaftOracle | Dual-leader raises on `observe_role`, not deferred to `assert_safe` | Info | Document fail-fast invariant timing | `routing_oracle_lab` |
| F20 | DiscoveryRateLimiter | `max_keys` prune-then-insert → steady state can be `max_keys+1` (soft cap) | Med | Prune to `max_keys-1` before insert, or document soft cap | `discovery_rate_limit` |
| F21 | TopicQueueRouter | `successful_routes` only increments on `send_via_topic`; `route_message_to_queues` updates `total_routes`/`cache_*` only | Med | Bump successful on pure route match **or** rename counters | `topic_queue_router_lab` |

---

## How to add a finding

1. Hit the issue while building/running an app.  
2. Prefer fixing the **app** with an honest `step("friction: …")` / non-claim.  
3. Append a row here + PROJECT_PLAN §9.  
4. Optionally open a platform issue referencing `F#`.

---

## Closed / mitigated in curriculum (not necessarily fixed in platform)

| ID | Mitigation in apps |
|----|-------------------|
| F1 | `ops_cli_tour` uses `asyncio.to_thread` for CliRunner |
| F4 | Register `ops_echo` / `ops_add` instead of `echo` |
| F5 | Two-node demo for v1/v2 of `catalog.price` |
| F7–F8 | Non-claims + correct kwargs in `cache_event_bus` |
| F9 | Set `current_timeout` explicitly in graph resilience demo |
