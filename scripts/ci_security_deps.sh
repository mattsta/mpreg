#!/usr/bin/env bash
# R1: dependency vulnerability scan via pip-audit when available
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== ci_security_deps =="
if ! command -v uv >/dev/null 2>&1; then
  echo "WARN: uv not found; skip dep audit"
  exit 0
fi
# Export locked prod deps when possible
EXPORT_OUT="/tmp/mpreg-reqs-prod-$$.txt"
if uv export --no-dev --no-emit-project -o "$EXPORT_OUT" 2>/dev/null; then
  :
elif uv pip compile pyproject.toml -o "$EXPORT_OUT" 2>/dev/null; then
  :
else
  # Fallback: list installed project deps from pyproject names only
  echo "WARN: could not export lock; running pip-audit on environment"
  EXPORT_OUT=""
fi
set +e
if [[ -n "$EXPORT_OUT" && -f "$EXPORT_OUT" ]]; then
  uvx pip-audit -r "$EXPORT_OUT"
  RC=$?
else
  uvx pip-audit .
  RC=$?
fi
set -e
rm -f "$EXPORT_OUT" 2>/dev/null || true
if [[ "$RC" -ne 0 ]]; then
  echo "pip-audit failed (exit $RC)" >&2
  exit "$RC"
fi
echo "ci_security_deps OK"
