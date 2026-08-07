#!/usr/bin/env bash
# R1: lint gate
# - E9 (syntax/runtime errors) on full tree — must be clean
# - Full project rule set on release-gate surface (must stay clean)
# Note: broader F/UP rules across the whole monorepo remain a post-0.3.0
# cleanup track; many F821 hits are postponed-annotation forward refs.
set -euo pipefail
cd "$(dirname "$0")/.."
if command -v uv >/dev/null 2>&1; then
  RUN=(uv run)
else
  RUN=()
fi
echo "== ci_lint: E9 critical (mpreg + tests) =="
"${RUN[@]}" ruff check mpreg tests --select E9
echo "== ci_lint: release surface (full project rules) =="
"${RUN[@]}" ruff check tests/release/ tests/test_config_check_cli.py
echo "ci_lint OK"
