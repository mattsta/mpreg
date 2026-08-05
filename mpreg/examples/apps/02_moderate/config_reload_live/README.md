# config_reload_live (L2)

## Story

Write cache+queue state, kill the process, restart on the same data dir — state returns.

## Lesson

SQLite persistence across restart for operator-friendly config/job durability.

## Run

```bash
uv run mpreg-example run config_reload_live
```

## What it proves

- jobs queue present after restart
- theme=dark cache hit on L2

## Architecture

```text
MPREGServer + PersistenceMode.SQLITE → stop → start → read
```

## Non-claims

- Not hot config reload without restart.
- Not multi-node consensus on config.

## Production exit ramp

- Supersedes legacy persistence_restart_demo.py
- Next: fabric_snapshot_restart
