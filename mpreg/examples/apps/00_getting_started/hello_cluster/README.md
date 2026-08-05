# hello_cluster (L0)

## Story

A tiny two-node mesh: CPU node does arithmetic; GPU node scores the result.
One client call drives a three-step dependency graph across resources.

## Lesson

`resources` + `locs` + `peers` form a local cluster without manual routing.

## Run

```bash
uv run mpreg-example run hello_cluster
```

## What it proves

- Cross-node resolution of `model_score` after `add`/`multiply`.
- Score ≈ 1.26 for the demo inputs.

## Architecture

```text
Client ──► CPU (math) ──mesh──► GPU (ml)
```

## Non-claims

- Not multi-region fabric; not partition-tolerant leadership.

## Production exit ramp

- Name resources by domain capability, not hostnames.
- Next: `hello_trace`, then `ha_client_failover`.
