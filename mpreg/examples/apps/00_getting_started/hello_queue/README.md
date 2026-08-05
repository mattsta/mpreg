# hello_queue (L0 · product)

## Story

A first durable job lands on a local queue and a worker callback receives it.

## Lesson

`MessageQueue` + `subscribe` + `send_message(..., AT_LEAST_ONCE)` is the
smallest honest queue hello (deeper tours live in `plane_queue` / DLQ apps).

## Run

```bash
uv run mpreg-example run hello_queue
```

## What it proves

- Worker subscription on topic pattern
- At-least-once send delivers payload
- Second message also delivers

## Non-claims

- Not multi-node quorum queue.
- Not DLQ (see `job_queue_dlq`).

## Production exit ramp

- Next: `job_queue_worker`, `plane_queue`, `job_queue_dlq`
