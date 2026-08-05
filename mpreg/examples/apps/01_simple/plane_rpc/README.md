# plane_rpc (L1)

## Story

Full RPC plane capability tour (dependency graph + multi-resource).

## Lesson

Single-plane depth tour for `rpc` (same asserts as legacy tier1).

## Run

```bash
uv run mpreg-example run plane_rpc
```

## What it proves

- tier1 `demo_rpc` invariants hold

## Architecture

```text
mpreg-example → tier1.demo_rpc
```

## Non-claims

- Capability tour, not a product vertical.

## Production exit ramp

- Prefer product apps for learning; use plane_* for depth drills.
- Legacy: `mpreg demo tier1` now routes here via mpreg-example.
