# unified_client_tour (L1 · product)

## Story

Application code often needs RPC, cache, and queue without juggling three
clients. `MPREGClient` is the four-plane façade over one WebSocket session.

## Lesson

With `enable_default_cache` + `enable_default_queue`, the server registers
plane RPC commands and the unified client exercises them end-to-end.

## Run

```bash
uv run mpreg-example run unified_client_tour
```

## What it proves

- `call` RPC with resource locs
- `cache_put` / `cache_get` success and value equality
- `queue_create` → `queue_send` → `queue_receive` delivery
- Multi-key cache isolation through the same façade

## Architecture

- One node, default cache + queue managers attached at boot
- Single `MPREGClient` context manager

## API drill-down

| Call                                            | Feature ID                   |
| ----------------------------------------------- | ---------------------------- |
| `MPREGClient.call`                              | `client.unified`, `rpc.call` |
| `cache_put` / `cache_get`                       | `cache.rpc_surface`          |
| `queue_create` / `queue_send` / `queue_receive` | `queue.rpc_surface`          |

## Non-claims

- Pubsub publish/subscribe via unified client is not fully drilled here
  (`pubsub_request_reply` covers reply).
- Not HA multi-seed (see `ha_client_failover`).
- Not exactly-once queue semantics.

## Production exit ramp

- Prefer `MPREGClient` in apps that touch ≥2 planes
- HA: wrap seeds with `MPREGClusterClient` patterns from L1 HA app
- Next: `order_intake`, `ops` CLI planes
