#!/usr/bin/env bash
# Unified demo suite — entrypoints only (never python -m / uv run python).
set -euo pipefail

echo "== MPREG Demo Suite (unified mpreg-example) =="

if command -v uv >/dev/null 2>&1; then
  RUN=(uv run)
else
  RUN=()
fi

"${RUN[@]}" mpreg-example suite
echo
echo "Demo suite completed successfully."
