#!/usr/bin/env bash
# R7: full 0.3.0 production snapshot gate
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
echo "======== MPREG 0.3.0 release_gate ========"
bash "$ROOT/scripts/ci_lint.sh"
bash "$ROOT/scripts/ci_typecheck.sh"
bash "$ROOT/scripts/ci_unit_fast.sh"
bash "$ROOT/scripts/ci_invariants.sh"
bash "$ROOT/scripts/ci_distlab_core.sh"
bash "$ROOT/scripts/run_demo_smoke.sh"
bash "$ROOT/scripts/ci_security_deps.sh"
bash "$ROOT/scripts/ci_package_smoke.sh"
bash "$ROOT/scripts/ci_perf_smoke.sh"
if command -v uv >/dev/null 2>&1; then
  uv run pytest tests/release/ -q --tb=short
else
  pytest tests/release/ -q --tb=short
fi
echo "======== release_gate OK ========"
