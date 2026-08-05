# inventory_reserve (L2 · product)

## Story

Stock is reserved under an idempotent hold id; insufficient stock fails closed;
release returns inventory and refreshes the cache snapshot.

## Lesson

RPC inventory commands + L1 cache snapshot is a usable product vertical before
full consensus-backed stock systems.

## Run

```bash
uv run mpreg-example run inventory_reserve
```

## What it proves

- `available` / `reserve` / `release` RPC
- Idempotent hold replay
- Insufficient stock rejection
- Cache refresh after release

## Non-claims

- Not distributed lock / Raft-backed inventory.
- Not multi-warehouse allocation.

## Production exit ramp

- Compose with `order_intake` and `billing_ledger`
