# plane_fabric (L1)

## Story

Full fabric plane capability tour (multi-cluster RPC).

## Lesson

Single-plane depth tour for `fabric` (same asserts as legacy tier1).

## Run

```bash
uv run mpreg-example run plane_fabric
```

## What it proves

- tier1 `demo_fabric` invariants hold

## Architecture

```text
mpreg-example → tier1.demo_fabric
```

## Non-claims

- Capability tour, not a product vertical.

## Production exit ramp

- Prefer product apps for learning; use plane\_\* for depth drills.
- Legacy: `mpreg demo tier1` now routes here via mpreg-example.
