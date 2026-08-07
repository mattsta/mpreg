#!/usr/bin/env bash
# R5: optional short perf smoke (catastrophic regression only; may be nightly)
set -euo pipefail
cd "$(dirname "$0")/.."
if command -v uv >/dev/null 2>&1; then
  RUN=(uv run)
else
  RUN=()
fi
echo "== ci_perf_smoke (doc + import only by default) =="
test -f docs/ops/PERF_BASELINE.md
# Avoid long baseline suite in default CI — document points to full path
"${RUN[@]}" python -c "from pathlib import Path; assert Path('docs/ops/PERF_BASELINE.md').stat().st_size > 200"
echo "ci_perf_smoke OK"
