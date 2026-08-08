#!/usr/bin/env bash
# Raft correctness gate — timer semantics, pre-vote, task manager, core invariants.
# Wired into release_gate once stable. Not a substitute for full integration suite.
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
echo "ci_raft: OK"
