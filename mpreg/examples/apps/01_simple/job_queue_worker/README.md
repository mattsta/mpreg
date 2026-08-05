# job_queue_worker (L1)

## Story

A jobs queue with two workers. One message uses at-least-once delivery; another
requires quorum acknowledgments.

## Lesson

Choose `DeliveryGuarantee` deliberately; quorum needs multiple subscribers.

## Run

```bash
uv run mpreg-example run job_queue_worker
```

## What it proves

- Four received payloads (fan-out to two workers × two messages).
- Both task names appear.

## Non-claims

- Not a networked multi-process worker pool (in-process manager for teaching).

## Production exit ramp

- Persist queues (see persistence demos / settings).
- Compose with RPC intake in `order_intake`.
