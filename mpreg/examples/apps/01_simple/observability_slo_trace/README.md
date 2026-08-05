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

## Non-claims

- Does not start Prometheus or export OTel spans — library helpers only.
