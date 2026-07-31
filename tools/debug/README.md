# Debug and investigation scripts

This directory holds **one-off investigation scripts** used during development
and incident response. They are **not** part of the supported public API and
are **not** run in CI.

## Policy

1. Prefer `mpreg doctor`, `mpreg monitor *`, and `mpreg config-check` for routine ops.
2. New debug scripts must include a one-line purpose comment at the top.
3. Do not import `tools.debug` from production `mpreg/` packages.
4. When a script graduates to a supported tool, move it to `scripts/ops/`.

## Promoted ops scripts

See `scripts/ops/` for supported operator helpers.
