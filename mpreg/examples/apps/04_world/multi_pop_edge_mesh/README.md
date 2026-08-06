# multi_pop_edge_mesh (L4 · product)

## Story

A second world-tour: one control hub plus three edge POPs (US / EU / AP). The
hub geo-routes; edges serve health and cart paths; monitoring correlates the
tour with a W3C traceparent stamp.

## Lesson

Four-cluster permissive fabric + multi-POP RPC locs + unified monitoring timeline.

## Run

```bash
uv run mpreg-example run multi_pop_edge_mesh
```

## Proves

- Hub `route_plan` for us/eu/ap
- Tri-edge health pings over fabric
- Edge echo payload round-trip
- Path matrix (3 regions × 2 paths)
- Correlation timeline + traceparent shape

## Observability (Phase G)

This app enables `app_run(..., probe=True)` so every measured operation
feeds an in-process :class:`ExampleProbe`.

Look for these annotations in the run log:

- `◆ feature:mon.slo` — latency/throughput scenario
- `◆ obs:` lines — per-op count, avg/p50/p95/p99 ms, and `throughput_ops_s`
- `server-metrics` steps when a live `ServerMetricsTracker.snapshot()` is available

```bash
uv run mpreg-example run multi_pop_edge_mesh
# … scenarios …
#   ◆ obs: app=<id> ops=N errors=0 elapsed_s=… throughput_ops_s=…
#   ◆ obs:   rpc.call: n=… avg_ms=… p95_ms=… p99_ms=…
```

## Non-claims

- Lab topology on loopback — not multi-continent latency or SLA proof.
- Complements `global_edge_control_plane` (hub+2) with a third POP.
