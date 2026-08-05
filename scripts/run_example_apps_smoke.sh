#!/usr/bin/env bash
# Fail-fast smoke for curriculum example apps (L0 + selected L1).
# Entrypoint only — never python -m / uv run python.
set -euo pipefail

echo "== MPREG Curriculum Example Apps Smoke =="

if command -v uv >/dev/null 2>&1; then
  uv run mpreg-example smoke
else
  mpreg-example smoke
fi

echo
echo "Curriculum smoke completed successfully."
