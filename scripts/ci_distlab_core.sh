#!/usr/bin/env bash
# R1: DistLab core — CLI list + happy path + registry smoke
set -euo pipefail
cd "$(dirname "$0")/.."
if command -v uv >/dev/null 2>&1; then
  RUN=(uv run)
else
  RUN=()
fi
echo "== ci_distlab_core: list =="
"${RUN[@]}" mpreg distlab list | tee /tmp/mpreg_distlab_list.txt
grep -q "strong.happy_3" /tmp/mpreg_distlab_list.txt
echo "== ci_distlab_core: run strong.happy_3 =="
"${RUN[@]}" mpreg distlab run strong.happy_3
echo "== ci_distlab_core: pytest registry =="
"${RUN[@]}" pytest \
  tests/testing/test_distlab_registry.py::test_registry_run_happy_3 \
  tests/testing/test_distlab_registry.py::test_registry_run_suite_smoke_preset \
  tests/testing/test_distlab_strong_scenarios.py::test_distlab_strong_happy_3 \
  -q --tb=line
echo "ci_distlab_core OK"
