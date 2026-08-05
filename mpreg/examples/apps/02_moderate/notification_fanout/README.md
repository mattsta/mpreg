# notification_fanout (L2 · product)

## Story

Welcome emails, push badges, and an audit trail all subscribe to notification
topics with different wildcard depths.

## Lesson

`TopicExchange` + `TopicPattern` (`*` vs `#`) fans one publish to N subscribers.

## Run

```bash
uv run mpreg-example run notification_fanout
```

## What it proves

- `notify.email.*` and `notify.push.*` isolation
- `notify.#` audit catches both
- Unrelated topics do not wake workers
- Headers survive publish path

## Non-claims

- Not mobile push provider integration.
- Not durable offline inbox (pair with queue).

## Production exit ramp

- Bridge hits into `webhook_dispatcher` / queue egress
