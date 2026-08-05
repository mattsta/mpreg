# partition_safe_counter (L3)

## Story

Five-node mental model: majority can commit, minority cannot; heal restores mesh.

## Lesson

FaultInjector partitions + quorum arithmetic (honest Raft teaching).

## Run

```bash
uv run mpreg-example run partition_safe_counter
```

## What it proves

- Healthy mesh fully connected
- Cross-partition blocked
- Minority size < quorum
- Heal restores

## Architecture

```text
FaultInjector.partition(majority, minority) → can_communicate oracle
```

## Non-claims

- Does NOT run ProductionRaft log replication here.
- Full Raft proofs: tests/test_production_raft_* and invariants/.
- Not Byzantine fault tolerance.

## Production exit ramp

- ProductionRaft integration tests for real commit path
- chaos_checkout for client-visible faults
