#!/usr/bin/env bash
# H1: fast unit/integration subset (excludes slow/chaos/example suites)
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
  tests/core/test_cache_strong_gcm.py \
  tests/core/test_cache_strong_gcm_bridge.py \
  tests/server_pkg/test_strong_audit_metrics.py \
  tests/test_strong_audit_monitoring_endpoints.py \
  tests/test_unified_client.py \
  tests/test_client_trace_metadata.py \
  tests/test_type_annotation_runtime_contracts.py \
  tests/release/ \
  -m "not slow and not chaos and not example_suite and not example_apps" \
  -q --tb=line
echo "ci_unit_fast OK"
