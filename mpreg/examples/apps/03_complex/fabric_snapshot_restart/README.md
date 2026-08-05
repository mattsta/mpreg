# fabric_snapshot_restart (L3)

## Story

Fabric catalog and route key state survive process restart.

## Lesson

Persistence of fabric control-plane snapshots.

## Run

```bash
uv run mpreg-example run fabric_snapshot_restart
```

## What it proves

- Legacy fabric_snapshot_restart_demo invariants hold

## Architecture

```text
MPREGServer + fabric persistence → restart → catalog/routes
```

## Non-claims

- See demo source for exact asserts.

## Production exit ramp

- Supersedes fabric_snapshot_restart_demo.py as the user-facing id
