# rpc_deadline_budget (L3 · product)

## Story

Soft real-time callers need a single wall-clock budget across retries; async
throughput callers may reset per attempt.

## Lesson

`ClientCallPolicy.for_mode(M1|M2|M3, deadline_seconds=…)` controls
`share_deadline_across_attempts`. M2/M3 fail closed when the shared budget is
exhausted.

## Run

```bash
uv run mpreg-example run rpc_deadline_budget
```

## What it proves

- M1 does not share deadline by default
- M2/M3 share wall budget
- Fast call under M2 succeeds
- Slow call under tight M2 fails closed quickly

## Non-claims

- Not server-side preemption of running handlers (handler may still sleep).
- Not HA multi-endpoint deadline (see `ha_client_failover` / cluster client).

## Production exit ramp

- Pair with `chaos_checkout` and cluster `deadline_mono` path
