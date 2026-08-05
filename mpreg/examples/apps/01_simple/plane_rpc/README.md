# plane_rpc (L1)

## Story

Full RPC plane capability tour (dependency graph + multi-resource).

## Lesson

Single-plane depth tour for `rpc` (same asserts as legacy tier1).

## Run

```bash
uv run mpreg-example run plane_rpc
```

## What it proves

- tier1 `demo_rpc` invariants hold

## Architecture

```text
mpreg-example → tier1.demo_rpc
```

## Observability (Phase G)

This app enables ``app_run(..., probe=True)`` so every measured operation
feeds an in-process :class:`ExampleProbe`.

Look for these annotations in the run log:

- ``◆ feature:mon.slo`` — latency/throughput scenario
- ``◆ obs:`` lines — per-op count, avg/p50/p95/p99 ms, and ``throughput_ops_s``
- ``server-metrics`` steps when a live ``ServerMetricsTracker.snapshot()`` is available

```bash
uv run mpreg-example run plane_rpc
# … scenarios …
#   ◆ obs: app=<id> ops=N errors=0 elapsed_s=… throughput_ops_s=…
#   ◆ obs:   rpc.call: n=… avg_ms=… p95_ms=… p99_ms=…
```

## Non-claims

- Capability tour, not a product vertical.

## Production exit ramp

- Prefer product apps for learning; use plane_* for depth drills.
- Legacy: `mpreg demo tier1` now routes here via mpreg-example.
