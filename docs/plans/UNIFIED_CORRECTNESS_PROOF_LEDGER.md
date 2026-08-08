# Unified Correctness — Proof Ledger

Record evidence as tracks complete. Prefer commands + exit codes over narrative.

## U0 — Docs

| Proof                                      | Evidence                                              |
| ------------------------------------------ | ----------------------------------------------------- |
| Architecture index                         | `docs/architecture/README.md`                         |
| System / Raft / logging / validation / obs | `docs/architecture/*.md`                              |
| Master plan                                | `docs/plans/UNIFIED_CORRECTNESS_MASTER_PLAN.md`       |
| consensus.md pre-vote honesty              | `mpreg/server_pkg/consensus.md` matches live pre-vote |

## U1 — Pre-vote

| Proof              | Evidence                                                                                                                                                  |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Algorithm          | `production_raft_implementation.py` `_collect_pre_votes` / `_request_pre_vote_from_node`; `production_raft_rpcs.py` `handle_request_vote` pre_vote branch |
| RPC field          | `RequestVoteRequest.pre_vote`; `raft_codec.deserialize_request_vote` defaults False                                                                       |
| Minority test      | `uv run pytest tests/invariants/test_raft_prevote_minority.py -q` → pass (pre_votes_started asserted)                                                     |
| Timer semantics    | `uv run pytest tests/test_raft_election_timer_semantics.py -q` → pass                                                                                     |
| Dead knobs removed | `RaftConfiguration` no longer has pipeline_enabled/batch_size/max_election_timeout_jitter                                                                 |

## U2 — Tasks

| Proof                             | Evidence                                                                                            |
| --------------------------------- | --------------------------------------------------------------------------------------------------- |
| Election callback via TaskManager | coordinator `_coordinator_loop` → `create_task(core, election_callback, …)`                         |
| Vote fanout owned                 | `_collect_real_votes` / `_collect_pre_votes` use gather (no orphan create_task)                     |
| stop()                            | `stop_coordinator` stops coordinator only (not self-cancel callback); `stop_all_tasks` on full stop |
| Task manager tests                | included in `ci_raft.sh`                                                                            |

## U3 — Encapsulation

| Proof                                     | Evidence                                                   |
| ----------------------------------------- | ---------------------------------------------------------- |
| `reset_to_follower` / `testing_set_state` | public APIs on ProductionRaft                              |
| Grep guard                                | `tests/release/test_u3_raft_no_force_state.py`             |
| Integration heal                          | uses `await node.reset_to_follower(trigger_election=True)` |
| Chaos/snapshot                            | `testing_set_state`                                        |

## U4 — Observability

| Proof                           | Evidence                                         |
| ------------------------------- | ------------------------------------------------ |
| `command_apply_timeout_seconds` | RaftConfiguration + submit_command               |
| Status pre-vote fields          | RaftNodeStatus + get_status + status_dict parity |
| OBSERVABILITY.md                | `docs/architecture/OBSERVABILITY.md`             |

## U5 — Logging

| Proof                     | Evidence                                                                   |
| ------------------------- | -------------------------------------------------------------------------- |
| operational logging tests | `tests/test_operational_exception_logging.py` incl. dual_catch_log + codec |
| Raft LCE                  | impl, rpcs, task_manager, fabric raft_transport                            |
| `dual_catch_log`          | `mpreg/core/errors.py`                                                     |

## U6 — Gates

| Proof                           | Evidence                  |
| ------------------------------- | ------------------------- |
| `bash scripts/ci_raft.sh`       | 42 passed                 |
| release_gate wires ci_raft      | `scripts/release_gate.sh` |
| Multi-run stress 3× integration | 8 passed × 3              |

## U7 — Distributed / full

| Proof                           | Evidence                                                       |
| ------------------------------- | -------------------------------------------------------------- |
| fabric + properties + live raft | 21 passed (`test_fabric_raft_integration` + properties + live) |
| DistLab raft gap                | `docs/architecture/DISTLAB_RAFT_GAP.md`                        |
| claims INV-C7                   | updated for real pre-vote                                      |
| full suite `-n auto`            | deferred as corpus health (not mythology); run on demand       |
| commit                          | pending user request (no push)                                 |
