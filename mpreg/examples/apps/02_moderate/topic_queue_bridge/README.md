# topic_queue_bridge (L2 · integration)

## Story

Transient topic events become durable queue work — the classic pubsub→queue
bridge pattern used by webhooks and notifications.

## Lesson

Match on `events.#`, then `send_message` per hit into a reliable queue worker.

## Run

```bash
uv run mpreg-example run topic_queue_bridge
```

## What it proves

- Wildcard match drives enqueue
- Non-matching topics stay out
- Burst publish delivers N jobs
- Headers on PubSubMessage

## Non-claims

- Not exactly-once cross-plane transactions.
- Not multi-node bridge HA.

## Production exit ramp

- See `webhook_dispatcher`, `pubsub_plus_queue`
