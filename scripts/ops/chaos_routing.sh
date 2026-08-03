#!/usr/bin/env bash
# Optional L3 live routing chaos helper (A5). Not required for demo CI.
# Usage: MPREG_MONITORING_URL=http://127.0.0.1:9090 ./scripts/ops/chaos_routing.sh
set -euo pipefail
BASE="${MPREG_MONITORING_URL:-}"
if [[ -z "${BASE}" ]]; then
  echo "Set MPREG_MONITORING_URL to monitoring base URL" >&2
  exit 2
fi
BASE="${BASE%/}"
echo "== doctor deep =="
mpreg doctor --url "${BASE}" --deep --format json || true
echo "== link-state =="
curl -fsS "${BASE}/routing/link-state" | head -c 2000 || true
echo
echo "== decisions =="
curl -fsS "${BASE}/routing/decisions?limit=10" | head -c 2000 || true
echo
echo "== raft =="
curl -fsS "${BASE}/mgmt/v1/raft" | head -c 2000 || true
echo
echo "chaos_routing probe complete"
