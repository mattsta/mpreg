# Unified Correctness Program — Master Plan

| Field | Value |
|-------|--------|
| **Status** | **Active — core tracks complete; residual U5.6/U6.2/U7.4** |
| **Date** | 2026-08-07 |
| **Architecture** | `docs/architecture/` |
| **Burndown** | `docs/plans/UNIFIED_CORRECTNESS_BURNDOWN.md` |
| **Proof ledger** | `docs/plans/UNIFIED_CORRECTNESS_PROOF_LEDGER.md` |
| **Goal** | Single coherent Raft + logging + validation surface: correct algorithms, honest config/docs, full parameterization/observability, unit + integration + Hypothesis + distributed tests aligned to architecture |

## Global rules

1. **Map first.** No microfix without citing `docs/architecture/*`.
2. **Honesty.** Dead features are removed or implemented — never advertised.
3. **Transitions are methods.** No test assigns `current_state` to fake role changes.
4. **Catch + log together.** `OPERATIONAL` without `log_caught_exception` is incomplete.
5. **Gate after each phase.** Snapshot floor + phase suites (below).
6. **`uv run` only** for Python entry points in scripts/docs.
7. **No push** unless explicitly requested.
8. **Non-goals frozen:** Jepsen-class linearizability checker, BFT, WAN chaos as CI — remain `non_claims` unless separately scoped.

## Gate commands

```bash
# Every phase (snapshot floor)
bash scripts/release_gate.sh

# Phase A (Raft)
uv run pytest \
  tests/test_raft_election_timer_semantics.py \
  tests/test_raft_task_manager.py \
  tests/test_production_raft_integration.py \
  tests/test_raft_safety_properties.py \
  tests/invariants/test_raft_*.py \
  tests/test_raft_election_timer_semantics.py \
  -q --tb=line

# Phase B (logging)
uv run pytest tests/test_operational_exception_logging.py -q --tb=line

# Phase C (broader)
uv run pytest tests/test_production_raft_properties.py \
  tests/integration/test_fabric_raft_integration.py \
  tests/test_live_raft_integration.py \
  -q --tb=line
# Multi-run stress + optional full: pytest tests/ -n auto
```

---

# TRACK U0 — Persistent architecture & PM (this track)

| ID | Work | Done when |
|----|------|-----------|
| U0.1 | `docs/architecture/*` system maps with source cites | files exist, linked from docs/README |
| U0.2 | Master plan + burndown + proof ledger | this file + siblings |
| U0.3 | Fix known doc lies that block honesty (pre-vote line in consensus.md) as A1 lands | consensus.md matches code |

---

# TRACK U1 — Raft config & pre-vote honesty (algorithm)

**Problem:** `pre_vote_enabled=True` by default but **zero reads** in algorithm; docs claim pre-vote; minority test enables flag and only asserts backoff.

| ID | Work | Source anchors |
|----|------|----------------|
| U1.1 | Implement Raft **pre-vote** when `pre_vote_enabled=True`: before term++, probe peers with `pre_vote=True` RequestVote; only campaign if majority would grant; pre-vote must not increment term or persist vote | `production_raft.py` RequestVote*, `production_raft_rpcs.py` handle_request_vote, `_start_election` |
| U1.2 | Extend RPC types with `pre_vote: bool = False` (backward compatible) | `production_raft.py`, `raft_codec.py`, fabric messages if needed |
| U1.3 | Metrics: `pre_votes_started`, `pre_votes_passed`, `pre_votes_failed` on `RaftMetrics` + `get_status` | impl |
| U1.4 | When `pre_vote_enabled=False`, behavior = today's campaign path | config |
| U1.5 | Rewrite `tests/invariants/test_raft_prevote_minority.py` to assert **pre-vote** bounds term growth (not only backoff) | test |
| U1.6 | Update `mpreg/server_pkg/consensus.md` pre-vote sentence to match | docs |
| U1.7 | Remove or wire other dead knobs: `pipeline_enabled`, `batch_size`, `max_election_timeout_jitter` — prefer **remove** with changelog note; AE batch stays `max_log_entries_per_request` | RaftConfiguration |

**Exit:** prevote minority test proves term bound via pre-vote path; diag shows pre_vote metrics; no dead True defaults.

---

# TRACK U2 — Raft task lifecycle unification

**Problem:** election callback + vote fanout are bare `create_task`; shutdown/observability incomplete.

| ID | Work |
|----|------|
| U2.1 | Schedule election callback via `RaftTaskManager` (`core`, name `election_callback` or unique id) |
| U2.2 | Track vote RPC tasks in `replication` or `core` group (or single gather owned by election callback — preferred: gather inside callback so no orphan tasks) |
| U2.3 | `stop()` cancels in-flight election work deterministically |
| U2.4 | Surface active election task in `get_status` / task manager status |
| U2.5 | Tests in `test_raft_task_manager.py` + no pending-task warnings on clean stop |

**Exit:** no fire-and-forget election tasks; stop is clean under pytest warnings filters for tasks.

---

# TRACK U3 — Single transition path + test encapsulation

**Problem:** tests assign `current_state` skipping `_pending_task_ops`.

| ID | Work |
|----|------|
| U3.1 | Add test helper `async def force_follower(node)` / public `step_down` usage that always runs deferred ops |
| U3.2 | Replace all `node.current_state = RaftState.*` in tests (grep clean under `tests/`) |
| U3.3 | Integration heal paths use `wait_for_leader` + real partitions, not force-state |
| U3.4 | Optional: `__setattr__` guard or property for `current_state` in debug builds — only if low risk; else lint/grep in release test |

**Exit:** `rg 'current_state\s*=\s*RaftState' tests` → 0 (or only inside ProductionRaft package).

---

# TRACK U4 — Raft observability & parameterization

| ID | Work |
|----|------|
| U4.1 | `RaftConfiguration.command_apply_timeout_seconds` (replace hardcode 5.0) |
| U4.2 | Status includes effective election window, pre_vote_enabled, adaptive bounds |
| U4.3 | `status_dict` includes contact age + skip counters (parity with `get_status`) |
| U4.4 | Document operator fields in OBSERVABILITY.md (done) + doctor/raft if present |

---

# TRACK U5 — Exception logging adoption (operator path)

| ID | Work |
|----|------|
| U5.1 | Raft: dual-catch or LCE on all bare `except Exception` in impl/rpcs/storage/transport that log |
| U5.2 | `raft_task_manager.py`: LCE on operational failures |
| U5.3 | Gold dual-catch template helper optional: `errors.log_boundary(logger, msg)` documenting pattern |
| U5.4 | fabric hot supervisors (membership, hubs, auto_discovery, raft_transport): replace `logger.error(f…{e})` with LCE on logged paths (batch by module, keep control flow) |
| U5.5 | Expand `test_operational_exception_logging.py` + one Raft loop sink test |
| U5.6 | server non-RPC loops: prioritize highest-traffic supervisors only this track |

**Exit:** Raft package LCE-complete for logged catches; fabric raft_transport + ≥1 fabric supervisor module; unit tests green.

---

# TRACK U6 — Test alignment & readiness contract

| ID | Work |
|----|------|
| U6.1 | Raft multi-node tests use `wait_for_leader` / deadlines (purge sleep-until-leader loops where practical) |
| U6.2 | Marker hygiene: apply `integration`/`slow` to live raft; fix README markers or register them |
| U6.3 | `scripts/ci_raft.sh` allowlist: timer + task manager + safety smoke + invariants raft transitions/prevote |
| U6.4 | Wire `ci_raft.sh` into `release_gate.sh` (and optionally GHA) once stable |
| U6.5 | Hypothesis: ensure production_raft_properties + cross-system properties still pass post-refactor |
| U6.6 | Multi-run: 5× `test_production_raft_integration.py` serial |

---

# TRACK U7 — Distributed / fabric / full corpus

| ID | Work |
|----|------|
| U7.1 | `tests/integration/test_fabric_raft_integration.py` green |
| U7.2 | `tests/test_live_raft_integration.py` green (timeouts scaled) |
| U7.3 | DistLab scenario smoke if raft scenario exists; else document gap |
| U7.4 | Full `pytest tests/ -n auto` health run; triage by flake class not one-off skips |
| U7.5 | Update claims.yaml INV-C* proof notes for pre-vote real |
| U7.6 | One logical commit when gates + raft CI + ledger green (**no push**) |

---

## Phase order (sequential)

```text
U0 docs ──► U1 pre-vote ──► U2 tasks ──► U3 encapsulation
                │                           │
                └────────► U4 observability ┘
                              │
                              ▼
                           U5 logging
                              │
                              ▼
                           U6 test gates
                              │
                              ▼
                           U7 fabric + full corpus + commit
```

Parallelization allowed only within a track after its predecessor exit criteria.

## Definition of Done (program)

1. Architecture docs accurate vs code.  
2. Pre-vote real or flag default False with no doc claims.  
3. No dead RaftConfiguration knobs.  
4. No test force-state.  
5. Election tasks tracked; clean stop.  
6. Logging: Raft + raft_transport LCE; dual-catch tests.  
7. `ci_raft.sh` in release_gate; release_gate green.  
8. Fabric raft integration green; multi-run stress green.  
9. Proof ledger filled; claims updated.  
10. Residual full-suite failures classified by flake class with issues/notes — not silent skips.

## Out of scope (explicit)

- Dynamic Raft membership / joint consensus  
- BFT  
- Replacing cache STRONG with Raft  
- Full-tree mypy (separate debt)  
- BLE001 zero across entire fabric in one pass (incremental U5)
