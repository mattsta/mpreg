# order_intake (L2)

## Story

An order API creates orders with application-level idempotency, mirrors the
record in cache, publishes a created event, and enqueues fulfillment work.

## Lesson

Compose **RPC + cache + pubsub + queue** as separate planes with clear
boundaries — the shape of a real intake service.

## Run

```bash
uv run mpreg-example run order_intake
```

## What it proves

- Second `create_order` with same idempotency key is a replay (same order_id).
- Cache holds the order record.
- Topic pattern `orders.*.created` matches.
- Fulfill queue worker sees the order_id.

## Architecture

```text
Client ─RPC─► Order API ─┬─► Cache (idem mirror)
                         ├─► TopicExchange (notify)
                         └─► Queue (fulfill worker)
```

## Non-claims

- In-process cache/queue/pubsub for teaching speed — not multi-host durability.
- Idempotency is app-level, not a platform exactly-once guarantee.

## Production exit ramp

- Persist queue + cache (`persistence_restart_demo`, profiles).
- HA client in front of API nodes (`ha_client_failover`).
- Multi-region catalog: `multi_region_shop`.
- Monitoring correlation: `hello_trace` + OPERATE.md.
