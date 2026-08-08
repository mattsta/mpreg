#!/usr/bin/env bash
# Raft correctness gate — timer semantics, pre-vote, task manager, core invariants,
# DistLab first-class Raft scenarios. Wired into release_gate. Not a substitute
# for full integration / live multi-process suites.
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=(uv run)
echo "== ci_raft: unit + invariants =="
"${RUN[@]}" pytest \
  tests/test_raft_election_timer_semantics.py \
  tests/test_raft_task_manager.py \
  tests/invariants/test_raft_prevote_minority.py \
  tests/invariants/test_raft_transitions.py \
  tests/invariants/test_raft_snapshot.py \
  tests/invariants/test_consensus_facade.py \
  tests/test_operational_exception_logging.py \
  -q --tb=line
echo "== ci_raft: DistLab raft scenarios =="
"${RUN[@]}" pytest \
  tests/testing/test_distlab_raft_scenarios.py \
  -q --tb=line
echo "== ci_raft: DistLab CLI raft.elect_3 =="
"${RUN[@]}" mpreg distlab run raft.elect_3
echo "ci_raft: OK"
