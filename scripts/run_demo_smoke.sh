#!/usr/bin/env bash
# Unified demo smoke — entrypoints only (never python -m / uv run python).
set -euo pipefail

echo "== MPREG Demo Smoke (unified mpreg-example) =="

if command -v uv >/dev/null 2>&1; then
  RUN=(uv run)
else
  RUN=()
fi

"${RUN[@]}" mpreg-example smoke
echo
echo "Demo smoke completed successfully."
