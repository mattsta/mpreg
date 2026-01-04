#!/usr/bin/env bash
set -euo pipefail

echo "== MPREG Demo Suite =="
echo "Running tiered demos with fail-fast validation."
echo

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required to run the demo suite." >&2
  exit 1
fi

run() {
  echo
  echo "==> $*"
  "$@"
}

run uv run python mpreg/examples/tier1_single_system_full.py --system rpc
run uv run python mpreg/examples/tier1_single_system_full.py --system pubsub
run uv run python mpreg/examples/tier1_single_system_full.py --system queue
run uv run python mpreg/examples/tier1_single_system_full.py --system cache
run uv run python mpreg/examples/tier1_single_system_full.py --system fabric
run uv run python mpreg/examples/tier1_single_system_full.py --system monitoring

run uv run python mpreg/examples/tier2_integrations.py
run uv run python mpreg/examples/tier3_full_system_expansion.py

run uv run python mpreg/examples/quick_demo.py
run uv run python mpreg/examples/simple_working_demo.py
run uv run python mpreg/examples/real_world_examples.py

run uv run mpreg demo all

echo
echo "Demo suite completed successfully."
