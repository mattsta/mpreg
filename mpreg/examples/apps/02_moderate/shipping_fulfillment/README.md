# shipping_fulfillment (L2 · product)

## Story

A fulfillment desk creates shipping labels, tracks status in cache, and hands
packages to a durable dispatch queue for the carrier handoff worker.

## Lesson

Compose RPC (idempotent create) + L1 cache snapshot + AT_LEAST_ONCE queue.

## Run

```bash
uv run mpreg-example run shipping_fulfillment
```

## Proves

- Idempotent `create_shipment` replay
- Tracking key cache put/get
- Status advance + cache refresh
- Dispatch queue worker delivery
- `get_shipment` read model + unknown order

## Non-claims

- Not a real carrier integration or label PDF printer.
