#!/usr/bin/env bash
# H2: lint gate — full-tree ruff (project rules)
set -euo pipefail
cd "$(dirname "$0")/.."
if command -v uv >/dev/null 2>&1; then
  RUN=(uv tool run)
else
  RUN=()
fi
echo "== ci_lint: full-tree ruff check =="
# Prefer uv tool run ruff (matches local tooling); fall back to uv run
if command -v uv >/dev/null 2>&1; then
  uv tool run ruff check mpreg tests tools
else
  ruff check mpreg tests tools
fi
echo "ci_lint OK"
