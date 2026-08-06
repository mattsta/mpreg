# plane_queue (L1)

## Story

Full queue plane capability tour (at-least-once + quorum).

## Lesson

Single-plane depth tour for `queue` (same asserts as legacy tier1).

## Run

```bash
uv run mpreg-example run plane_queue
```

## What it proves

- tier1 `demo_queue` invariants hold

## Architecture

```text
mpreg-example → tier1.demo_queue
```

## Non-claims

- Capability tour, not a product vertical.

## Production exit ramp

- Prefer product apps for learning; use plane\_\* for depth drills.
- Legacy: `mpreg demo tier1` now routes here via mpreg-example.
