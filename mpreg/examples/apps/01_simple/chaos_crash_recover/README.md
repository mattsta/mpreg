# chaos_crash_recover (L1 · plane)

`FaultInjector.crash` / `recover` delivery gating composed with partitions.

```bash
uv run mpreg-example run chaos_crash_recover
```

## Proves

- `chaos.crash` — crash/recover membership on `NetworkView`
- Compose with `chaos.partition` + `chaos.heal`

## Non-claims

- Live process kill of OS-level MPREG server PIDs
