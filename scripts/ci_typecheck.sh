#!/usr/bin/env bash
# H1: typecheck / import gate
#
# Full-tree strict mypy still has large JsonValue/union debt. This gate covers:
# - core import smoke
# - growing mypy surface: release + modules with runtime-critical typing fixes
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
from mpreg.server import Cluster
from mpreg.core.transport.interfaces import TransportInterface
assert hasattr(Cluster, "remove_server")
assert hasattr(TransportInterface, "close")
from importlib.metadata import version as _v

ver = mpreg.__version__
assert ver.count(".") >= 2, ver
print("imports ok", ver, "metadata=", end=" ")
try:
    print(_v("mpreg"))
except Exception as exc:  # pragma: no cover
    print("n/a", type(exc).__name__)
PY

echo "== ci_typecheck: mypy release + hardened surface =="
"${RUN[@]}" mypy \
  tests/release/ \
  mpreg/__init__.py \
  mpreg/core/cache_strong.py \
  mpreg/core/errors.py \
  mpreg/core/transport/interfaces.py \
  mpreg/client/call_policy.py \
  mpreg/client/cluster_client.py \
  mpreg/datastructures/vector_clock.py \
  mpreg/datastructures/leader_election.py \
  mpreg/datastructures/graph_algorithms.py \
  mpreg/datastructures/blockchain_crypto.py \
  --pretty \
  --follow-imports=silent \
  --ignore-missing-imports
echo "ci_typecheck OK"
