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
| full suite `-n auto`            | **DONE** — 3× post-fix 3378+ passed, destroyed=0 (see U8)      |
| commit                          | **0.3.2** Unified Correctness closeout (local; no push)        |

## U7 — Distributed / full (continued — multi-run + DistLab Raft)

| Proof                     | Evidence                                                                                           |
| ------------------------- | -------------------------------------------------------------------------------------------------- |
| DistLab `RaftSUT` adapter | `mpreg/testing/distlab/adapters/raft.py`                                                           |
| Scenarios                 | `raft.elect_3/5`, `raft.partition_majority`, `raft.partition_heal`, `raft.leader_stepdown_reelect` |
| Checkers                  | `UniqueLeaderChecker`, `RaftSmAgreementChecker`, `default_raft_checkers`                           |
| Presets                   | `raft-core`; `smoke` + `ci-core` include raft                                                      |
| `ci_raft.sh`              | unit/invariants + DistLab raft pytest + CLI `raft.elect_3`                                         |
| `ci_distlab_core.sh`      | greps + runs `raft.elect_3`                                                                        |
| Gap note closed           | `docs/architecture/DISTLAB_RAFT_GAP.md`                                                            |
| claims INV-C9             | DistLab Raft elect/partition/heal                                                                  |
| Multi-run stress          | See table below (local, not PR CI)                                                                 |

### Multi-run stress log (local)

Recorded under `/tmp/mpreg_multirun/` by the validation battery:

| Suite                               | N   | Command                                  |
| ----------------------------------- | --- | ---------------------------------------- |
| `ci_raft.sh`                        | 5×  | full Raft gate incl. DistLab             |
| DistLab `raft-core`                 | 5×  | `mpreg distlab suite --preset raft-core` |
| `ci_distlab_core.sh`                | 3×  | STRONG + Raft happy                      |
| production_raft_integration         | 3×  | pytest                                   |
| fabric + live + properties + safety | 3×  | pytest                                   |
| `release_gate.sh`                   | 1×  | full snapshot floor                      |

Exit codes and per-run logs: `/tmp/mpreg_multirun/summary.txt` + `*.log`.

### Multi-run stress log (local) — 2026-08-07

Recorded under `/tmp/mpreg_multirun/` (`summary.txt`, `summary2.txt`).

| Suite                                        | N                   | Result                    |
| -------------------------------------------- | ------------------- | ------------------------- |
| `ci_raft.sh` (unit + DistLab raft + CLI)     | 5×                  | **all OK**                |
| DistLab `raft-core` preset (5 scenarios)     | 5×                  | **all OK**                |
| `ci_distlab_core.sh` (STRONG + raft.elect_3) | 3×                  | **all OK**                |
| `tests/test_production_raft_integration.py`  | 3×                  | **8 passed × 3**          |
| fabric + live + properties + safety          | 3× after flake fix  | **31 passed × 3**         |
| `test_live_cluster_size_performance[15]`     | 5× after settle fix | **1 passed × 5**          |
| `release_gate.sh`                            | 1×                  | **OK** (25 release tests) |

**Flake found + fixed:** live 15-node test asserted follower count on first
LEADER sighting while 2 members were still CANDIDATE. Fixed to wait for
unique-leader + all-follower topology (`tests/test_live_raft_integration.py`).
First multi-run caught `fabric_live_2` fail; post-fix 5× live15 + 3× full
fabric/live suite green.

### Full suite multi-run + teardown/hot-path harden — 2026-08-07 (later)

| Suite                                                      | N       | Result                                                                          |
| ---------------------------------------------------------- | ------- | ------------------------------------------------------------------------------- |
| `pytest tests/ -n auto -q`                                 | **3×**  | **3358 passed × 3** (~470–481s each); `/tmp/mpreg_multirun/full_x3_{1,2,3}.log` |
| `Task exception was never retrieved`                       | 3× logs | **0** occurrences (was noisy closed-WS stacks)                                  |
| Targeted prior flakes (churn + hierarchical topo + live15) | 1×      | **3 passed**                                                                    |

**Fixes landed this pass:**

| Change                                                                                           | Why                                                        |
| ------------------------------------------------------------------------------------------------ | ---------------------------------------------------------- |
| `TransportConnectionError` / `TransportTimeoutError` subclass `ConnectionError` / `TimeoutError` | Caught by `OPERATIONAL_EXCEPTIONS` without circular import |
| `_track_background_task` retrieves + classifies task exceptions                                  | No more unretrieved closed-WS task dumps                   |
| Snapshot send paths swallow `TransportConnectionError`                                           | Peer-gone mid-snapshot is expected                         |
| `_normalize_tags` frozenset[str] identity fast path                                              | Gossip deserialize + `__post_init__` double work           |
| `VectorClock.update` O(n) dict merge                                                             | Was O(n²) `get_timestamp` scans on every gossip msg        |
| `SemanticVersion.to_wire` / identity `to_dict`                                                   | Avoid hot `str()` on every catalog send                    |
| `py-spy>=0.4.2` in dev dependency group                                                          | In-repo profiler: `sudo .venv/bin/py-spy …`                |

**py-spy evidence (historical):** endgame workers burst ~100% CPU on fabric
gossip/catalog snapshot/serialize (and separately hypothesis/blockchain). Suites
completed; not deadlock. The catalog path is **not** an optional residual — it is
chartered in `docs/plans/CATALOG_SNAPSHOT_SERIALIZE_CPU_PLAN.md` and closed by the
serialize-once flush work in this tree (build-once counters + wire strip + no
register→snapshot-all-peers).

### Catalog snapshot serialize CPU — closeout

| Item                           | Evidence                                                                             |
| ------------------------------ | ------------------------------------------------------------------------------------ |
| Charter                        | `docs/plans/CATALOG_SNAPSHOT_SERIALIZE_CPU_PLAN.md`                                  |
| Shared build / send-many       | `MPREGServer._build_shared_catalog_snapshot_wire` + `send_preencoded` / `send_bytes` |
| Stable update_id               | `catalog-rev:{cluster}:{generation}` via `RoutingCatalog.generation`                 |
| Summary wire strip             | `RoutingCatalogDelta.to_dict(include_rpc_spec=…)` / `FunctionEndpoint.to_dict`       |
| No register full-mesh snapshot | `_publish_fabric_function_update` publishes delta only                               |
| Proof tests                    | `tests/test_catalog_snapshot_serialize.py`                                           |

### Raft task lifecycle — no pending-destroy (2026-08-07)

| Item                        | Evidence                                                                                                                                                                                                                                                                                                                         |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Invariant                   | Cancel **and await** before dropping task refs (`raft_task_manager.py`)                                                                                                                                                                                                                                                          |
| Retain-until-done bag       | Module `_RETAINED_TASKS` holds strong refs until `done()` — survives manager clear / GC / aborted `stop`                                                                                                                                                                                                                         |
| Duplicate replace           | `create_task` stops+awaits existing unfinished task                                                                                                                                                                                                                                                                              |
| `force_cleanup_all`         | Drains unfinished before clearing tracking; retain bag is last line of defense                                                                                                                                                                                                                                                   |
| Drain under outer cancel    | `_drain_all_unfinished` / `_await_task_settled` / group stop use `asyncio.shield` so `wait_for(stop)` cannot abort settlement                                                                                                                                                                                                    |
| ProductionRaft.stop         | Re-entrant: incomplete prior stop still drains; `CancelledError` path force-settles                                                                                                                                                                                                                                              |
| Heartbeat cancel            | Re-raises `CancelledError`; replication `gather(..., return_exceptions=True)` settles siblings                                                                                                                                                                                                                                   |
| DistLab / mock transport    | `_deliver` retains + awaits sibling on cancel/timeout/ops error                                                                                                                                                                                                                                                                  |
| Core `TaskManager.shutdown` | Cancel+gather+shield; never clears unfinished refs                                                                                                                                                                                                                                                                               |
| Cache coherence             | `force_leader_election` is async (no bare create_task)                                                                                                                                                                                                                                                                           |
| Unit gates                  | `tests/test_raft_no_pending_destroy.py` (10 tests incl. aborted-stop + retain-bag)                                                                                                                                                                                                                                               |
| Multi-run (post retain-bag) | 5× pending-destroy (10p); 3× raft unit bundle (35p); 3× `ci_raft` OK; 3× integ+pending (18p); 3× live raft (14p ~130s); **3× full `tests/ -n auto`: 3378 passed each, exit=0, destroyed=0, never_retrieved=0** (`/tmp/mpreg_multirun/full_fix_{1,2,3}.log`, summary `full_fix_summary.txt`, completed 2026-08-07T18:29:09-04:00) |
| Log scan                    | 0× `Task was destroyed but it is pending` on all post-fix multirun logs (`pending_x5_*`, `raft_unit_post_*`, `ci_raft_fix_*`, `integ_fix_*`, `live_fix_*`, `full_fix_*`)                                                                                                                                                         |
| Historical hit closed       | Pre-fix `full_x3_3.log` had `node_2_core_heartbeat` cancelling mid-gather; root cause was aborted stop / GC dropping last strong ref while task still `cancelling`                                                                                                                                                               |

### U8 — 0.3.2 milestone lock (2026-08-07)

| Item                                   | Evidence                                                                                                                         |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Version                                | `pyproject.toml` / `mpreg.__version__` == `0.3.2`                                                                                |
| CHANGELOG                              | `## [0.3.2] — Unified Correctness Closeout`                                                                                      |
| Claims                                 | `release_0_3_2` + REL-0.3.2-\* in `tests/invariants/claims.yaml`                                                                 |
| Release tests                          | `tests/release/test_u8_version_032.py`                                                                                           |
| Marker hygiene                         | live Raft `integration`+`slow`; README markers registered                                                                        |
| Server LCE subset                      | fabric snapshot restore, pubsub notify, shared-audit                                                                             |
| Live teardown                          | sequential `_stop_raft_nodes` (no cancel RecursionError warning)                                                                 |
| `ci_lint` / `ci_typecheck` / `ci_raft` | OK                                                                                                                               |
| `release_gate.sh`                      | OK (`/tmp/mpreg_multirun/release_gate_032_lock.log`)                                                                             |
| Full multirun (pre-warn-fix)           | `/tmp/mpreg_multirun/full_final_{1,2,3}.log` — 3382p ×3, destroyed=0                                                             |
| Full multirun (zero-warn lock)         | `/tmp/mpreg_multirun/full_zero_lock_{1,2,3}.log` + `full_zero_lock_summary.txt` — **3382p ×3, 0 warnings, destroyed=0, never=0** |

### U8 teardown / warning closeout (2026-08-07 evening) — **LOCKED**

Prior full runs still reported pytest warnings (unraisable / ResourceWarning /
sqlite / adapter port races). Root causes and fixes:

| Source                                                            | Fix                                                                                                                      |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `MPREGServer.__del__` AttributeError on `object.__new__` stubs    | `getattr` + no import-at-shutdown (`sys.meta_path is None`)                                                              |
| Autouse `_cleanup_orphaned_event_loop` closed pytest-asyncio loop | GC-only; never `loop.close()` — stops DistLab raft-core RecursionError on Runner teardown                                |
| DistLab `RaftSUT.stop`                                            | Sequential bounded node stops + `drain_retained_tasks`                                                                   |
| Mesh socket/transport leaks (topo / 50-node)                      | `shutdown_servers_sequential` concurrent settle + listener `wait_closed` 3s bound (WS + TCP) + double GC settle          |
| L2 handles on cache shutdown                                      | Drop `_l2_store` / clear L2 map; **do not** close shared `PersistenceRegistry` (caller-owned)                            |
| Enhanced adapter health / correlation flake                       | Contiguous `port_range_context(4)` + direct echo `start_server` (no accept-queue race; adapter TCP = base+2)             |
| BlockchainStore sqlite `ResourceWarning`                          | Explicit `conn.close()` in `finally` (sqlite `with` only commits)                                                        |
| CPython `_SelectorTransport.__del__` ↔ `Server._detach` race     | Documented `filterwarnings` in `pyproject.toml` (+ conftest) for residual interpreter GC noise after product close paths |

#### Zero-warning full multirun lock (authoritative)

| Proof             | Evidence                                                                                                             |
| ----------------- | -------------------------------------------------------------------------------------------------------------------- |
| Command           | `pytest tests/ -n auto --tb=line -q` (warnings **enabled**; no `--disable-warnings`)                                 |
| N                 | **3×**                                                                                                               |
| Result            | **3382 passed × 3**, exit=0, destroyed=0, never_retrieved=0, **0 warnings** each                                     |
| Times             | 458.73s / 469.11s / 477.46s                                                                                          |
| Logs              | `/tmp/mpreg_multirun/full_zero_lock_{1,2,3}.log` + `full_zero_lock_summary.txt`                                      |
| Completed         | 2026-08-07T21:20:45-04:00                                                                                            |
| Release gate      | `bash scripts/release_gate.sh` → **OK** (`/tmp/mpreg_multirun/release_gate_032_lock.log`, 29 release tests)          |
| Hot-path targeted | blockchain + correlation + health + 30/50-node + planet-scale → **8 passed, 0 warnings** (`warn_targeted_lock2.log`) |
| Version           | `0.3.2` (`pyproject.toml` + `mpreg.__version__`)                                                                     |

**Still residual / not claimed:**

- Jepsen / BFT / WAN adversarial suites
- PR CI still does not run full integration/live multi-run or 3× full `-n auto`
- Hypothesis / blockchain property-test CPU (separate from catalog snapshot path)
- Not “formally proven correct” / deadlock-free in the model-checking sense
- Binary/msgpack wire backend (phase 2; `native_codec` pluggable, not required for burst closeout)
- Large-mesh CPython selector GC race is filtered (product paths close listeners; residual is interpreter teardown)
