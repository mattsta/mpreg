#!/usr/bin/env bash
# R4/H4: build wheel and smoke entry points in isolated venv (standard CPython)
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== ci_package_smoke: build =="
rm -rf dist
uv build
WHEEL=$(ls -1 dist/mpreg-*.whl | head -1)
echo "Built $WHEEL"
EXPECT_VER=$(sed -n 's/^version = "\([^"]*\)"/\1/p' pyproject.toml | head -1)
echo "Expect version $EXPECT_VER"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

PY="${MPREG_PACKAGE_SMOKE_PYTHON:-}"
if [[ -z "$PY" ]]; then
  for cand in \
    /opt/homebrew/bin/python3.14 \
    /usr/local/bin/python3.14 \
    "$HOME/.pyenv/versions/3.14.5/bin/python3.14" \
    /opt/homebrew/bin/python3 \
    python3.14 \
    python3
  do
    if command -v "$cand" >/dev/null 2>&1 || [[ -x "$cand" ]]; then
      if "$cand" -c "import sys; raise SystemExit(0 if (not hasattr(sys,'_is_gil_enabled') or sys._is_gil_enabled()) else 1)" 2>/dev/null; then
        PY="$cand"
        break
      fi
    fi
  done
fi
if [[ -z "${PY}" ]]; then
  echo "ERROR: no standard (GIL) Python found for package smoke" >&2
  exit 1
fi
echo "Using Python: $PY"
uv venv "$TMP/venv" --python "$PY"
# shellcheck disable=SC1091
source "$TMP/venv/bin/activate"
uv pip install "$WHEEL"
mpreg --help >/dev/null
mpreg-example --help >/dev/null
python -c "from importlib.metadata import version; v=version('mpreg'); print(v); assert v=='${EXPECT_VER}', (v, '${EXPECT_VER}')"
deactivate
echo "ci_package_smoke OK"
