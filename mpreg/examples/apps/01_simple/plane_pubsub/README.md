# plane_pubsub (L1)

## Story

Full pubsub plane capability tour (wildcards + fan-out).

## Lesson

Single-plane depth tour for `pubsub` (same asserts as legacy tier1).

## Run

```bash
uv run mpreg-example run plane_pubsub
```

## What it proves

- tier1 `demo_pubsub` invariants hold

## Architecture

```text
mpreg-example → tier1.demo_pubsub
```

## Non-claims

- Capability tour, not a product vertical.

## Production exit ramp

- Prefer product apps for learning; use plane\_\* for depth drills.
- Legacy: `mpreg demo tier1` now routes here via mpreg-example.
