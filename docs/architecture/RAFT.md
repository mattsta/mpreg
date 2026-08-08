# ProductionRaft — Architecture Map

**Authority for:** Raft modules, state machine, election, contact time, config honesty, harness.  
**Algorithm home:** `mpreg/datastructures/production_raft_implementation.py` (~2349 lines).

---

## 1. Module ownership

| File                                                     | Owns                                                                                                                                                                  |
| -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `mpreg/datastructures/production_raft.py`                | Types only: `RaftState`, `LogEntry*`, RPC req/resp, `PersistentState` / volatile / leader volatile, storage/transport/SM **protocols**                                |
| `mpreg/datastructures/production_raft_implementation.py` | `ProductionRaft`, `RaftConfiguration`, `ElectionCoordinator`, metrics/status/errors, election, replication, heartbeat, commit, snapshots (local), `_pending_task_ops` |
| `mpreg/datastructures/production_raft_rpcs.py`           | Mixin: `handle_request_vote`, `handle_append_entries`, `handle_install_snapshot`                                                                                      |
| `mpreg/datastructures/raft_task_manager.py`              | Groups `core` / `replication` / `maintenance`; create/stop/status                                                                                                     |
| `mpreg/datastructures/raft_storage_adapters.py`          | Memory / file / SQLite + factory                                                                                                                                      |
| `mpreg/datastructures/raft_codec.py`                     | Wire serialize for fabric                                                                                                                                             |
| `mpreg/fabric/raft_transport.py`                         | Prod transport over CONTROL messages                                                                                                                                  |
| `mpreg/fabric/raft_messages.py`                          | `RaftRpcKind`, topics                                                                                                                                                 |
| `mpreg/fabric/consensus.py`                              | **NOT Raft** — lightweight gossip consensus                                                                                                                           |
| `mpreg/server_pkg/raft_handlers.py`                      | `RaftPlane` register/status/dispatch                                                                                                                                  |
| `mpreg/consensus/__init__.py`                            | App import façade + `status_dict` + `MembershipChangeNotSupported`                                                                                                    |
| `mpreg/datastructures/leader_election.py`                | Deprecated `RaftBasedLeaderElection` wrapper                                                                                                                          |

```text
mpreg.consensus ──► ProductionRaft ──► RPCs mixin
                         │
              RaftTaskManager + storage adapters
                         │
         ┌───────────────┴───────────────┐
   FabricRaftTransport            NetworkAwareTransport (tests)
         │                               │
   peer WS CONTROL                 MockNetwork
```

---

## 2. State machine

Only three live states (`production_raft.py` `RaftState`):

| State       | Meaning                                             |
| ----------- | --------------------------------------------------- |
| `FOLLOWER`  | Default; answers RPCs; election coordinator running |
| `CANDIDATE` | Campaigning                                         |
| `LEADER`    | Heartbeat/replication; coordinator stopped          |

```text
FOLLOWER ──timeout/gates──► CANDIDATE ──majority──► LEADER
    ▲                          │                     │
    └──── fail / higher term / AE ───────────────────┘
    └──── step_down / lost majority / stop ──────────┘
```

| Transition  | Method                 | Triggers                                                                                                   |
| ----------- | ---------------------- | ---------------------------------------------------------------------------------------------------------- |
| → CANDIDATE | `_start_election`      | Coordinator timeout / `trigger_election`                                                                   |
| → LEADER    | `_become_leader`       | Majority votes (or single-node)                                                                            |
| → FOLLOWER  | `_convert_to_follower` | Fail election, higher term, valid AE while candidate/leader, `step_down`, majority-contact timeout, `stop` |
| Term bump   | `_update_term`         | RPC higher term                                                                                            |

**Deferred task ops** (`_pending_task_ops` + `_run_pending_task_ops`): role changes must not cancel/create tasks while holding `state_lock`. Ops: `stop_coordinator`, `start_heartbeat`, `stop_leader_tasks`, `start_coordinator`.

**Encapsulation rule:** assigning `node.current_state = …` **bypasses** deferred ops → zombie heartbeats or silent coordinators. Tests must call transition methods / public APIs.

---

## 3. Election call flow

```text
start()
  load storage/snapshot
  ElectionCoordinator.start_coordinator(callback=_start_election_with_semaphore)
       │
       ▼
  coordinator loop: wait (random ∈ [min,max] + node bias) OR election_trigger
       │ if election_in_progress: skip
       ▼
  fire task → _start_election
       │ gates:
       │   1. already LEADER? skip
       │   2. time_since(last_heartbeat_time) < election_timeout_min? skip (recent contact)
       │   3. should_backoff_election? skip
       ▼
  term++, vote self, CANDIDATE
  fanout RequestVote (parallel tasks)
       │
       ├─ majority → _become_leader
       │     stop_coordinator queued, NOOP entry, _note_leader_contact(become_leader)
       │     start_heartbeat → replicate loop
       └─ fail → elections_lost, backoff, _convert_to_follower
```

| Source                               | Lines (approx)                               |
| ------------------------------------ | -------------------------------------------- |
| Coordinator loop                     | `production_raft_implementation.py` ~209–309 |
| `_start_election` gates              | ~1035–1115                                   |
| Explicit non-stamp on election start | ~1125–1126                                   |
| `_become_leader`                     | ~1313–1407                                   |
| `_convert_to_follower`               | ~2101–2163                                   |
| `_run_pending_task_ops`              | ~2165–2241                                   |

### Parallel tasks

| Task                      | Managed by TaskManager?                         |
| ------------------------- | ----------------------------------------------- |
| `election_coordinator`    | **Yes** (`core`)                                |
| `heartbeat`               | **Yes** (`core`)                                |
| Election callback wrapper | **No** — bare `asyncio.create_task` (gap)       |
| Per-peer vote RPCs        | **No** — bare tasks (gap)                       |
| Apply committed           | **Sync** on commit path (no background applier) |

---

## 4. `last_heartbeat_time` / `_note_leader_contact`

**Semantics:** wall-clock of last **genuine** leader-or-candidate contact that should suppress elections. **Not** “I started an election.”

| Write site                  | Source string        | File                           |
| --------------------------- | -------------------- | ------------------------------ |
| `_note_leader_contact` body | assignment           | impl ~744–750                  |
| grant vote                  | `"grant_vote"`       | `production_raft_rpcs.py` ~216 |
| accepted AppendEntries      | `"append_entries"`   | rpcs ~288                      |
| InstallSnapshot             | `"install_snapshot"` | rpcs ~441                      |
| become leader               | `"become_leader"`    | impl ~1378                     |

**Must never write:** election start, `_convert_to_follower`, rejected stale AE.

| Read site                                  | Use                                      |
| ------------------------------------------ | ---------------------------------------- |
| `get_status` → `time_since_leader_contact` | observability                            |
| `_start_election` quiet period             | suppress if age < `election_timeout_min` |
| Diag logs                                  | MPREG_DEBUG_RAFT                         |

**Distinct clocks:** `FollowerContactInfo.last_successful_contact`, `last_majority_contact_time`, `ElectionCoordinator.last_election_attempt_time`, coordinator `last_wait_timeout` (diagnostic only).

Regression suite: `tests/test_raft_election_timer_semantics.py`.

---

## 5. Replication / commit

```text
heartbeat_loop (LEADER)
  → _replicate_log_entries
  → gather _replicate_to_follower (AE or snapshot if lag)
  → _handle_append_entries_response → match/next_index
  → _update_commit_index (majority of match_index; current-term only)
  → _apply_committed_entries
  → majority contact check → possible step-down
```

Client: `submit_command` → must be LEADER → append → replicate → wait apply waiter (5s hardcode).

---

## 6. Public observability API (tests + ops)

| API                                                             | Purpose                                                                   |
| --------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `get_status() → RaftNodeStatus`                                 | role, term, votes, commit, contact age, skip counters, coordinator_active |
| `wait_for_leader(nodes, …)`                                     | cluster readiness; raises `RaftLeadershipError` + statuses                |
| `wait_until_leader`                                             | this node is leader                                                       |
| `leadership_deadline_seconds` / `leadership_deadline_for_nodes` | derive waits from live timeouts                                           |
| `mpreg.consensus.status_dict`                                   | operator dict                                                             |
| `server.raft_status()` / `RaftPlane.status()`                   | multi-node                                                                |
| `metrics` / `RaftMetrics`                                       | counters                                                                  |
| `MPREG_DEBUG_RAFT=1`                                            | `[DIAG_RAFT]` logs                                                        |

**Tests should use** wait/status APIs — **not** fixed sleeps or private field assignment (except dedicated timer-semantics units).

---

## 7. Configuration honesty

`RaftConfiguration` (`production_raft_implementation.py` ~312–348):

| Knob                                                                  | Live?                   | Effect                                                    |
| --------------------------------------------------------------------- | ----------------------- | --------------------------------------------------------- |
| `election_timeout_min/max`                                            | **Yes**                 | coordinator wait; quiet period uses **min**; backoff base |
| Adaptive √(n/3) window                                                | **Yes**                 | effective bounds for n>3                                  |
| `timeout_bias_seconds`                                                | **Yes**                 | anti-lockstep (coordinator)                               |
| `heartbeat_interval`                                                  | **Yes**                 | leader loop                                               |
| `rpc_timeout`                                                         | **Yes**                 | AE wait floor                                             |
| `max_log_entries_per_request`                                         | **Yes**                 | AE batch                                                  |
| `snapshot_threshold` / `max_log_entries_behind`                       | **Yes**                 | compact / catch-up                                        |
| `max_retry_attempts` / backoff\_\*                                    | **Yes**                 | AE retries                                                |
| Election failure backoff                                              | **Yes**                 | coordinator internal                                      |
| `pre_vote_enabled`                                                    | **LIVE** (default True) | Pre-vote probe before term++; see U1                      |
| `command_apply_timeout_seconds`                                       | **LIVE**                | `submit_command` apply wait                               |
| ~~`pipeline_enabled` / `batch_size` / `max_election_timeout_jitter`~~ | **REMOVED**             | were no-ops; AE batch = `max_log_entries_per_request`     |

Docs that claim pre-vote (`server_pkg/consensus.md`) must match implementation after each phase.

---

## 8. Test harness

| Piece                                   | Location                                                                                   |
| --------------------------------------- | ------------------------------------------------------------------------------------------ |
| `MockNetwork` + `NetworkAwareTransport` | `tests/test_production_raft_integration.py`                                                |
| Delivery                                | sibling `asyncio.create_task` + shield (avoid cancel recursion)                            |
| Time                                    | **wall clock** — no virtual clock                                                          |
| xdist scale                             | `get_concurrency_factor()` → 4.0 under xdist worker                                        |
| Fabric live                             | `tests/integration/test_fabric_raft_integration.py`, `tests/test_live_raft_integration.py` |
| Safety / properties                     | `test_raft_safety_properties.py`, `test_production_raft_properties.py`                     |
| Timer semantics                         | `test_raft_election_timer_semantics.py`                                                    |
| Invariants INV-C\*                      | `tests/invariants/test_raft_*.py`                                                          |
| Claims                                  | `tests/invariants/claims.yaml` INV-C\*                                                     |

### Known force-state sites (must be eliminated in Plan A)

| File                                                    | Pattern                    |
| ------------------------------------------------------- | -------------------------- |
| `tests/test_production_raft_integration.py` ~796        | `current_state = FOLLOWER` |
| `tests/test_production_raft_properties.py` ~386,527,654 | same                       |
| `tests/invariants/test_raft_snapshot.py` ~66,103        | same                       |
| `tests/chaos/test_t11/t12/t13/t14_residuals.py`         | FOLLOWER/LEADER assign     |

---

## 9. Architectural risks (ranked)

1. Dead pre-vote + misleading docs/tests
2. Dual consensus APIs (Raft vs fabric LW vs LE wrapper)
3. Test force-state bypassing task ops
4. Untracked election/vote tasks
5. xdist × wall-clock starvation
6. Dead config knobs
7. Static membership only (`MembershipChangeNotSupported`)
8. Apply under `state_lock` (SM latency blocks RPC)
9. Fabric timeout stacking

---

## 10. Design intent (one paragraph)

ProductionRaft is a mixin-split, task-manager-centered CFT Raft: types in one module, RPC handlers in another, orchestration in `ProductionRaft`. Elections use a self-contained coordinator timer with suppress gates on **leader-contact time** and **failure backoff**. Leadership runs heartbeat replication with majority-contact step-down. Production I/O is fabric CONTROL RPC correlation; tests use in-process partitioned `MockNetwork`. Honesty gaps to close: implement or remove pre-vote, single transition path, full task ownership, live config surface only.
