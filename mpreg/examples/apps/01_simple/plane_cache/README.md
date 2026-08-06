# plane_cache (L1)

## Story

Full cache plane capability tour (L3/L4 federation scope).

## Lesson

Single-plane depth tour for `cache` (same asserts as legacy tier1).

## Run

```bash
uv run mpreg-example run plane_cache
```

## What it proves

- tier1 `demo_cache` invariants hold

## Architecture

```text
mpreg-example → tier1.demo_cache
```

## Non-claims

- Capability tour, not a product vertical.

## Production exit ramp

- Prefer product apps for learning; use plane\_\* for depth drills.
- Legacy: `mpreg demo tier1` now routes here via mpreg-example.
