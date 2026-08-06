# live_partition_chaos (L3 · plane)

## Story

Operators need **live** admission control — not only a lab `FaultInjector`.
This app drains a node via `/mgmt/v1/nodes/drain` (forces `/ready` 503),
clears drain, detaches a peer, and contrasts the lab partition model.

## Lesson

- **Drain** flips `server._mgmt_draining` and fails `/ready` closed.
- **Detach** closes a peer connection through the mgmt mutation path.
- `FaultInjector` remains a teaching model for abstract partitions (F10 closed
  for *live admission*; full wire-level packet drop is still a different layer).

## Run

```bash
uv run mpreg-example run live_partition_chaos
```

## What it proves

- Baseline mesh RPC + `/ready` 200
- Drain → `/ready` 503; clear → 200
- Peer detach HTTP applied
- Lab injector still models abstract groups

## Non-claims

- Does not inject packet loss on the raw TCP/WS socket.
- Cross-cluster fabric bridges are separate (`multi_region_*`).

## Production exit ramp

- `mpreg admin drain` / `mpreg admin detach`
- Wire drain into load-balancer health checks via `/ready`
