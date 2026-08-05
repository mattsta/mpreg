# rpc_plus_cache (L2)

## Story

RPC output cached and reused

## Lesson

Two-plane integration (legacy tier2, now first-class curriculum app).

## Run

```bash
uv run mpreg-example run rpc_plus_cache
```

## What it proves

- tier2 `rpc_plus_cache` invariants hold

## Architecture

```text
mpreg-example → tier2.rpc_plus_cache
```

## Non-claims

- Integration drill, pair with product apps for narrative.

## Production exit ramp

- Legacy mpreg demo tier2 routes through mpreg-example suite.
