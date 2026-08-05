# hello_rpc (L0)

## Story

A single-node “hello world” service that registers math helpers and runs a
two-step RPC dependency chain: `add` → `multiply`.

## Lesson

Register commands with resource tags (`locs`) and compose results by name.

## Run

```bash
uv run mpreg-example run hello_rpc
```

## What it proves

- `sum = 20+22 = 42`
- `scaled = sum*3 = 126`

## Architecture

```text
Client ──WS──► MPREGServer (cpu,math)
                 add, multiply
```

## Observability (Phase G)

This app enables ``app_run(..., probe=True)`` so every measured operation
feeds an in-process :class:`ExampleProbe`.

Look for these annotations in the run log:

- ``◆ feature:mon.slo`` — latency/throughput scenario
- ``◆ obs:`` lines — per-op count, avg/p50/p95/p99 ms, and ``throughput_ops_s``
- ``server-metrics`` steps when a live ``ServerMetricsTracker.snapshot()`` is available

```bash
uv run mpreg-example run hello_rpc
# … scenarios …
#   ◆ obs: app=<id> ops=N errors=0 elapsed_s=… throughput_ops_s=…
#   ◆ obs:   rpc.call: n=… avg_ms=… p95_ms=… p99_ms=…
```

## Non-claims

- Not multi-node; not HA; not durable.

## Production exit ramp

- Prefer `MPREGClientAPI.call` for single-function calls.
- Add `MPREGClusterClient` when you have multiple seeds (`ha_client_failover`).
- Next: `hello_cluster`.
