#!/usr/bin/env bash
# R1: typecheck / import gate for 0.3.0 Production Snapshot.
#
# Full-tree `mypy mpreg` under strict pyproject settings still reports large
# pre-existing debt (1000+ notes). That cleanup is post-0.3.0. This gate proves:
#   1) Core packages import on the release interpreter
#   2) Release-surface modules typecheck cleanly under project mypy config
#   3) CLI entry module has no syntax/import breakage
set -euo pipefail
cd "$(dirname "$0")/.."
if command -v uv >/dev/null 2>&1; then
  RUN=(uv run)
else
  RUN=()
fi

echo "== ci_typecheck: import smoke =="
"${RUN[@]}" python - <<'PY'
import mpreg
import mpreg.cli.main
import mpreg.core.config
import mpreg.core.cache_strong
import mpreg.server
assert mpreg.__version__ == "0.3.0", mpreg.__version__
print("imports ok", mpreg.__version__)
PY

echo "== ci_typecheck: mypy release surface =="
"${RUN[@]}" mypy \
  tests/release/ \
  mpreg/__init__.py \
  mpreg/core/cache_strong.py \
  --pretty \
  --follow-imports=skip \
  --ignore-missing-imports
echo "ci_typecheck OK"
