# leader_election_lab (L3 · plane)

## Story

Cache and control namespaces need a single leader chosen by fitness metrics or
quorum rules — without always running full Raft.

## Lesson

`MetricBasedLeaderElection` + `QuorumBasedLeaderElection` +
`LeaderElectionMetrics.fitness_score`.

## Run

```bash
uv run mpreg-example run leader_election_lab
```

## Proves

- Metrics validation bounds
- Metric elect prefers healthier peer
- Self-win when best
- `step_down` clears namespace
- Quorum elect with peers
- Sub-quorum elects self
- Independent namespace leaders

## Non-claims

- Does not prove Byzantine agreement or multi-datacenter Raft under partition
  (see `routing_oracle_lab` / `partition_safe_counter` for related teaching).
