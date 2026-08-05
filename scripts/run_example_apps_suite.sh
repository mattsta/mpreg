#!/usr/bin/env bash
# Fail-fast suite for all shipped curriculum example apps.
# Entrypoint only — never python -m / uv run python.
set -euo pipefail

echo "== MPREG Curriculum Example Apps Suite =="

if command -v uv >/dev/null 2>&1; then
  uv run mpreg-example suite
else
  mpreg-example suite
fi

echo
echo "Curriculum suite completed successfully."
