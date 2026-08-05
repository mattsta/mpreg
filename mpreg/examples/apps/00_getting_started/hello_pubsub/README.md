# hello_pubsub (L0)

## Story

Topic exchange hello: wildcards route login/order/payment events to the right subscribers.

## Lesson

Pub/sub patterns (`*`, `#`) and multi-subscriber fan-out.

## Run

```bash
uv run mpreg-example run hello_pubsub
```

## What it proves

- Exactly 3 notifications for 3 matching topics
- Unrelated topic produces zero matches

## Architecture

```text
Publisher → TopicExchange → auth-svc / orders-svc
```

## Non-claims

- Not durable; not cross-cluster; in-process exchange only.

## Production exit ramp

- Next: sensor_ingest_pubsub
- Production: server topic plane + monitoring
