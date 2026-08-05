# job_queue_dlq (L1 · product)

## Story

Poison messages must not block a queue forever — after bounded retries they land
in a dead-letter queue for operator inspection.

## Lesson

`QueueConfiguration(max_retries=…, enable_dead_letter_queue=True)` plus a
non-acking worker demonstrates the DLQ path.

## Run

```bash
uv run mpreg-example run job_queue_dlq
```

## What it proves

- Failing worker is retried (≥3 attempts with max_retries=2)
- Message appears in `dead_letter_queue`
- Stats show requeues/failures
- Healthy messages still deliver after poison handling

## Non-claims

- Not exactly-once.
- Not multi-node DLQ federation.
- Ack timeout values are demo-tuned (sub-second).

## Production exit ramp

- Wire `on_dlq` to Prometheus (server attaches metrics sink)
- Next: `job_queue_worker`, `plane_queue`, `ops` runbooks
