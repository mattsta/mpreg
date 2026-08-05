# webhook_dispatcher (L2)

## Story

Domain events hit pubsub, then a durable queue worker represents outbound webhooks.

## Lesson

Backpressured egress: fan-in topics, durable out.

## Run

```bash
uv run mpreg-example run webhook_dispatcher
```

## What it proves

- 2 events matched and queued
- Worker sees signup + paid

## Architecture

```text
events.* → TopicExchange → egress queue → webhook-worker
```

## Non-claims

- Does not call real HTTP endpoints.
- At-least-once ≠ exactly-once delivery to partners.

## Production exit ramp

- Next: order_intake
- Production: DLQ + retry budgets + monitoring
