# observability_slo_trace (L1 · plane)

## Story

Operators need golden-signal SLOs and multi-hop W3C `traceparent` without a
hard OpenTelemetry dependency on every node.

## Lesson

`GOLDEN_SIGNALS` / `prometheus_alert_rules_yaml` + `trace_context` helpers
(`generate_traceparent`, `bind_current_trace`, `inject_trace_metadata`).

## Run

```bash
uv run mpreg-example run observability_slo_trace
```

## Proves

- Four golden signals (traffic, errors, latency, saturation)
- Example Prometheus alert YAML generation
- W3C traceparent shape and sample flags
- ensure/extract on metadata maps
- ContextVar bind for RPC-ingress continuation
- inject preference order (explicit → metadata → bind → generate)

## Observability (Phase G)

This app enables `app_run(..., probe=True)` so every measured operation
feeds an in-process :class:`ExampleProbe`.

Look for these annotations in the run log:

- `◆ feature:mon.slo` — latency/throughput scenario
- `◆ obs:` lines — per-op count, avg/p50/p95/p99 ms, and `throughput_ops_s`
- `server-metrics` steps when a live `ServerMetricsTracker.snapshot()` is available

```bash
uv run mpreg-example run observability_slo_trace
# … scenarios …
#   ◆ obs: app=<id> ops=N errors=0 elapsed_s=… throughput_ops_s=…
#   ◆ obs:   rpc.call: n=… avg_ms=… p95_ms=… p99_ms=…
```

## Non-claims

- Does not start Prometheus or export OTel spans — library helpers only.
