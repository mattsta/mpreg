#!/usr/bin/env bash
set -euo pipefail
URL="${MPREG_MONITORING_URL:?set MPREG_MONITORING_URL}"
TOKEN="${MPREG_MONITORING_TOKEN:-}"
HDR=()
if [[ -n "$TOKEN" ]]; then
  HDR=(-H "Authorization: Bearer ${TOKEN}")
fi
curl -fsS "${HDR[@]}" "${URL%/}/metrics/prometheus"
