# shipping_fulfillment (L2 · product)

## Story

A fulfillment desk creates shipping labels, tracks status in cache, and hands
packages to a durable dispatch queue for the carrier handoff worker.

## Lesson

Compose RPC (idempotent create) + L1 cache snapshot + AT_LEAST_ONCE queue.

## Run

```bash
uv run mpreg-example run shipping_fulfillment
```

## Proves

- Idempotent `create_shipment` replay
- Tracking key cache put/get
- Status advance + cache refresh
- Dispatch queue worker delivery
- `get_shipment` read model + unknown order

## Observability (Phase G)

This app enables ``app_run(..., probe=True)`` so every measured operation
feeds an in-process :class:`ExampleProbe`.

Look for these annotations in the run log:

- ``◆ feature:mon.slo`` — latency/throughput scenario
- ``◆ obs:`` lines — per-op count, avg/p50/p95/p99 ms, and ``throughput_ops_s``
- ``server-metrics`` steps when a live ``ServerMetricsTracker.snapshot()`` is available

```bash
uv run mpreg-example run shipping_fulfillment
# … scenarios …
#   ◆ obs: app=<id> ops=N errors=0 elapsed_s=… throughput_ops_s=…
#   ◆ obs:   rpc.call: n=… avg_ms=… p95_ms=… p99_ms=…
```

## Non-claims

- Not a real carrier integration or label PDF printer.
