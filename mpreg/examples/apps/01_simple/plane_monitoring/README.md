# plane_monitoring (L1)

## Story

Full monitoring plane capability tour (correlation timeline).

## Lesson

Single-plane depth tour for `monitoring` (same asserts as legacy tier1).

## Run

```bash
uv run mpreg-example run plane_monitoring
```

## What it proves

- tier1 `demo_monitoring` invariants hold

## Architecture

```text
mpreg-example → tier1.demo_monitoring
```

## Non-claims

- Capability tour, not a product vertical.

## Production exit ramp

- Prefer product apps for learning; use plane_* for depth drills.
- Legacy: `mpreg demo tier1` now routes here via mpreg-example.
