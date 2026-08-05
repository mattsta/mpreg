# rpc_versioned_topic (L1 · product)

## Story

Two pricing nodes advertise the same stable `function_id` at different semantic
versions. Callers pin or range-select without renaming the command.

## Lesson

`register_command(..., function_id=, version=)` + client
`version_constraint=` (`==`, `>=`, `<`) route to the matching endpoint.

## Run

```bash
uv run mpreg-example run rpc_versioned_topic
```

## What it proves

- Exact pin `==1.0.0` and `==2.0.0`
- Range `>=2.0.0` and upper bound `<2.0.0`
- Impossible constraint fails closed

## Non-claims

- Not rolling blue/green deploy orchestration.
- Not multi-cluster version federation SLA.

## Production exit ramp

- Prefer reverse-DNS `function_id` values
- Next: `plane_rpc`, fabric catalog version filters
