# Observability — Operators & Test/Debug Interfaces

**Authority for:** what humans and tests should read; parameterization surfaces.

---

## 1. Layers

```text
L-ops   doctor / monitor / Prometheus / mgmt JSON
L-raft  get_status / status_dict / RaftPlane.status / DIAG_RAFT
L-log   loguru sinks + exception taxonomy (EXCEPTION_LOGGING.md)
L-test  wait_for_leader, RaftOracle, hang_observe, DistLab history
L-debug tools/debug/* probes (not production path)
```

---

## 2. Raft operator surface

| API                                                            | Consumer                          |
| -------------------------------------------------------------- | --------------------------------- |
| `ProductionRaft.get_status()` → `RaftNodeStatus` / `to_dict()` | tests, tools                      |
| `mpreg.consensus.status_dict(node)`                            | mgmt / doctor                     |
| `server.raft_status()` / `RaftPlane.status()`                  | multi-node                        |
| `RaftMetrics` counters (elections*\*, skip*\*)                 | dashboards / asserts              |
| `MPREG_DEBUG_RAFT=1`                                           | `[DIAG_RAFT]` structured warnings |

**Required fields for readiness debugging:** `state`, `term`, `time_since_leader_contact`, `elections_skipped_recent_contact`, `elections_skipped_backoff`, `election_in_progress`, `coordinator_active`, `consecutive_election_failures`.

---

## 3. Test/debug contract

| Do                                                               | Don't                                               |
| ---------------------------------------------------------------- | --------------------------------------------------- |
| `await ProductionRaft.wait_for_leader(nodes, timeout_seconds=…)` | `asyncio.sleep` until leader                        |
| Assert on `get_status()` fields                                  | Assign `current_state`                              |
| Use `leadership_deadline_for_nodes`                              | Hardcode 30s everywhere without derivation          |
| `RaftOracle` for safety                                          | Ignore dual leaders                                 |
| Enable `MPREG_DEBUG_RAFT` on failure                             | Scrape private `_pending_task_ops` in product tests |

Hang / concurrent: `mpreg/testing/hang_observe.py`, `concurrent_runner.py`.

---

## 4. Exception observability

| Expected                   | Unknown                                               |
| -------------------------- | ----------------------------------------------------- |
| message-only error/warning | stack via `log_caught_exception(..., expected=False)` |

Gold path: server RPC dual-catch. Program expands this to Raft loops + fabric supervisors (Plan B).

---

## 5. Parameterization (target)

| Surface                     | Today                      | Target                                          |
| --------------------------- | -------------------------- | ----------------------------------------------- |
| `RaftConfiguration`         | mix live + dead knobs      | **live only** or documented unimplemented=False |
| Adaptive election window    | live internal              | exposed on status                               |
| `submit_command` apply wait | hardcoded 5.0s             | config field                                    |
| Fabric raft request timeout | settings                   | already somewhat wired                          |
| Concurrency factor (tests)  | `get_concurrency_factor()` | keep; document in harness                       |

---

## 6. Logging config

`mpreg/core/logging.py` — sinks/format/JSON only; does **not** classify exceptions. Classification is solely `errors.py`.
