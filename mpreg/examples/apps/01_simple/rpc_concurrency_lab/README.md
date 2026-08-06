# rpc_concurrency_lab (L1 · plane)

Concurrent independent RPCs, M3 streaming policy factory, `routing_topic`.

```bash
uv run mpreg-example run rpc_concurrency_lab
```

## Proves

- `rpc.concurrency` — `asyncio.gather` of independent `call`s
- `client.policy.m3` — `ClientCallPolicy.for_mode(M3_STREAMING)`
- `rpc.routing_topic` — `call(..., routing_topic=)`

## Non-claims

- Progressive intermediate streaming payload frames end-to-end (M3 mode defaults)
