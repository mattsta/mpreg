# discovery_rate_limit (L1 · plane)

## Story

Discovery catalog and summary queries can be abused by a hot viewer. Operators
need a per-viewer, per-command sliding window before the wire path.

## Lesson

`DiscoveryRateLimiter` + `DiscoveryRateLimitKey` + `DiscoveryRateLimitConfig`.

## Run

```bash
uv run mpreg-example run discovery_rate_limit
```

## Proves

- Disabled config always allows
- `max_requests=0` hard-denies
- Burst then deny inside the window
- Window expiry re-allows
- Isolation by viewer / command / namespace
- `max_keys` prunes under pressure

## Observability (Phase G)

This app enables `app_run(..., probe=True)` so every measured operation
feeds an in-process :class:`ExampleProbe`.

Look for these annotations in the run log:

- `◆ feature:mon.slo` — latency/throughput scenario
- `◆ obs:` lines — per-op count, avg/p50/p95/p99 ms, and `throughput_ops_s`
- `server-metrics` steps when a live `ServerMetricsTracker.snapshot()` is available

```bash
uv run mpreg-example run discovery_rate_limit
# … scenarios …
#   ◆ obs: app=<id> ops=N errors=0 elapsed_s=… throughput_ops_s=…
#   ◆ obs:   rpc.call: n=… avg_ms=… p95_ms=… p99_ms=…
```

## Non-claims

- Not wired automatically into every discovery RPC path — this app teaches the
  limiter library surface; production attach points vary by plane.
