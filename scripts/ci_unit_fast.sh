#!/usr/bin/env bash
# R1: fast unit/integration subset (excludes slow/chaos/example suites)
set -euo pipefail
cd "$(dirname "$0")/.."
if command -v uv >/dev/null 2>&1; then
  RUN=(uv run)
else
  RUN=()
fi
echo "== ci_unit_fast =="
"${RUN[@]}" pytest \
  tests/test_config_check_cli.py \
  tests/test_slo_helpers.py \
  tests/core/test_cache_strong.py \
  tests/server_pkg/test_strong_audit_metrics.py \
  tests/test_strong_audit_monitoring_endpoints.py \
  tests/release/ \
  -m "not slow and not chaos and not example_suite and not example_apps" \
  -q --tb=line
echo "ci_unit_fast OK"
