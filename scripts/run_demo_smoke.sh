#!/usr/bin/env bash
set -euo pipefail

echo "== MPREG Demo Smoke Suite =="
echo "Running fast validation for CI."
echo

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required to run the demo smoke suite." >&2
  exit 1
fi

run() {
  echo
  echo "==> $*"
  "$@"
}

run uv run python mpreg/examples/tier1_single_system_full.py --system rpc
run uv run python mpreg/examples/tier2_integrations.py

echo
echo "Demo smoke suite completed successfully."
