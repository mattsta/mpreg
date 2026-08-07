#!/usr/bin/env bash
# H2: lint gate
# - E9 critical on full tree
# - I/F401/UP035 on mpreg (kept clean via autofix)
# - Full project rules on release-gate surface
set -euo pipefail
cd "$(dirname "$0")/.."
if command -v uv >/dev/null 2>&1; then
  RUN=(uv run)
else
  RUN=()
fi
echo "== ci_lint: E9 critical (mpreg + tests) =="
"${RUN[@]}" ruff check mpreg tests --select E9
echo "== ci_lint: mpreg I/F401/UP035 =="
"${RUN[@]}" ruff check mpreg --select I,F401,UP035
echo "== ci_lint: release surface (full project rules) =="
"${RUN[@]}" ruff check tests/release/ tests/test_config_check_cli.py
echo "ci_lint OK"
