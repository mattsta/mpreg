# billing_ledger (L2 · product)

## Story

A charge is authorized via RPC (idempotent), the balance is cached, and a
settlement job is queued for async capture.

## Lesson

Compose RPC + cache + queue into a billing-shaped vertical without claiming a
full payments processor.

## Run

```bash
uv run mpreg-example run billing_ledger
```

## What it proves

- Idempotent `charge` RPC
- Balance `cache.put/get`
- Settlement queue delivery
- `get_txn` lookup

## Non-claims

- Not PCI / card network integration.
- Not multi-currency ledger correctness proofs.

## Production exit ramp

- Pair with `order_intake` and `notification_fanout`
