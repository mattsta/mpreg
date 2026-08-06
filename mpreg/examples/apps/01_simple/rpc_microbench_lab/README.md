# rpc_microbench_lab (L1)

**Phase P / PERF** — in-process RPC microbench with client `ExampleProbe` and
server `ServerMetricsTracker.snapshot()` depth (samples, min/max, percentiles).

## Run

```bash
uv run mpreg-example run rpc_microbench_lab
```

## API drill-down

- `ExampleProbe.measure("rpc.nop")` around `MPREGClientAPI.call`
- `ServerMetricsTracker.snapshot()` → `rpc.total|samples|p50_ms|p95_ms|min_ms|max_ms|rps`
- `format_server_snapshot` / `absorb_server_tracker` for curriculum reporting

## Ensures

Timed nop loop, multi-op probe, snapshot key depth, min≤avg≤max, p95 budget.
