# routing_oracle_lab (L3 · plane)

## Story

Before trusting a router or Raft group, lab oracles check next-hop BFS plans and
single-leader-per-term safety.

## Lesson

`mpreg.testing.oracles.RoutingOracle` + `RaftOracle`.

## Run

```bash
uv run mpreg-example run routing_oracle_lab
```
