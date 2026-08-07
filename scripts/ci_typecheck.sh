#!/usr/bin/env bash
# H1: typecheck / import gate for Production Snapshot + Hardening
#
# Full-tree strict mypy remains large pre-existing debt (tracked post-0.3.1).
# This gate proves core imports + a growing release-surface mypy set.
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
import mpreg.core.errors
import mpreg.client.client_api
import mpreg.client.cluster_client
import mpreg.server
from importlib.metadata import version as _v

ver = mpreg.__version__
assert ver.count(".") >= 2, ver
# Prefer metadata when installed; source tree must match pyproject major.minor
print("imports ok", ver, "metadata=", end=" ")
try:
    print(_v("mpreg"))
except Exception as exc:  # pragma: no cover
    print("n/a", type(exc).__name__)
PY

echo "== ci_typecheck: mypy release surface =="
"${RUN[@]}" mypy \
  tests/release/ \
  mpreg/__init__.py \
  mpreg/core/cache_strong.py \
  mpreg/core/errors.py \
  mpreg/client/call_policy.py \
  --pretty \
  --follow-imports=skip \
  --ignore-missing-imports
echo "ci_typecheck OK"
