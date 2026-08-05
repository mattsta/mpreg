# pubsub_plus_queue (L2)

## Story

Pubsub fan-out feeds durable queue

## Lesson

Two-plane integration (legacy tier2, now first-class curriculum app).

## Run

```bash
uv run mpreg-example run pubsub_plus_queue
```

## What it proves

- tier2 `pubsub_plus_queue` invariants hold

## Architecture

```text
mpreg-example → tier2.pubsub_plus_queue
```

## Non-claims

- Integration drill, pair with product apps for narrative.

## Production exit ramp

- Legacy mpreg demo tier2 routes through mpreg-example suite.
