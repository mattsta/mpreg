#!/usr/bin/env bash
# R1: invariants + property smoke for STRONG/audit claims surface
set -euo pipefail
cd "$(dirname "$0")/.."
if command -v uv >/dev/null 2>&1; then
  RUN=(uv run)
else
  RUN=()
fi
echo "== ci_invariants =="
"${RUN[@]}" pytest \
  tests/invariants/test_cache_strong_properties.py \
  tests/invariants/test_shared_audit_properties.py \
  tests/invariants/test_cache_strong_history.py \
  -q --tb=line
echo "ci_invariants OK"
