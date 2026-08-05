# chaos_transport (L3 · plane)

## Story

Before wiring chaos into live sockets, operators need a deterministic model for
clock skew, duplicates, reorders, and plane-separated drops.

## Lesson

`mpreg.testing.faults.FaultInjector` records decisions and separates control vs
data plane drop/delay rates.

## Run

```bash
uv run mpreg-example run chaos_transport
```

## What it proves

- Per-node `set_clock_skew` / `clear_clock_skew` + `now_for`
- `duplicate_rate` sampling
- `should_reorder` buffer-length gate
- Control vs data `can_deliver` plane separation
- Composition with partition + crash

## Non-claims

- Not live WebSocket message mutation.
- Not production traffic shaping.

## Production exit ramp

- Compose with `chaos_checkout` / `partition_safe_counter`
- Server-side partition hooks when available
