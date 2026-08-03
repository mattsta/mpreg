#!/usr/bin/env bash
# Optional L3 live Raft chaos probe (B4/P1). Not required for demo CI.
# Usage: MPREG_MONITORING_URL=http://127.0.0.1:9090 ./scripts/ops/chaos_raft.sh
set -euo pipefail
BASE="${MPREG_MONITORING_URL:-}"
if [[ -z "${BASE}" ]]; then
  echo "Set MPREG_MONITORING_URL to monitoring base URL" >&2
  exit 2
fi
BASE="${BASE%/}"
echo "== doctor deep =="
mpreg doctor --url "${BASE}" --deep --format json || true
echo "== raft status =="
curl -fsS "${BASE}/mgmt/v1/raft" | head -c 4000 || true
echo
echo "== health =="
curl -fsS "${BASE}/health/summary" | head -c 2000 || true
echo
echo "chaos_raft probe complete"
