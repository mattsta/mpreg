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

## Observability (Phase G)

This app enables `app_run(..., probe=True)` so every measured operation
feeds an in-process :class:`ExampleProbe`.

Look for these annotations in the run log:

- `◆ feature:mon.slo` — latency/throughput scenario
- `◆ obs:` lines — per-op count, avg/p50/p95/p99 ms, and `throughput_ops_s`
- `server-metrics` steps when a live `ServerMetricsTracker.snapshot()` is available

```bash
uv run mpreg-example run topic_queue_router_lab
# … scenarios …
#   ◆ obs: app=<id> ops=N errors=0 elapsed_s=… throughput_ops_s=…
#   ◆ obs:   rpc.call: n=… avg_ms=… p95_ms=… p99_ms=…
```

## Non-claims

- Does not prove federated cross-cluster queue routing end-to-end.
