# topic_queue_router_lab (L2 · integration)

## Story

Durable queues should accept work by AMQP-style topic patterns, not only by
hard-coded queue names — fanout, round-robin, and load-balanced strategies.

## Lesson

`TopicQueueRouter` / `TopicRoutedQueue` / `create_topic_queue_router` +
`create_high_performance_topic_router`.

## Run

```bash
uv run mpreg-example run topic_queue_router_lab
```

## Proves

- Multi-pattern register + fanout match
- Unregister removes routes
- `send_via_topic` strategy metadata
- Routing statistics counters
- High-performance factory defaults
- No-match returns empty

## Non-claims

- Does not prove federated cross-cluster queue routing end-to-end.
