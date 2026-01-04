# MPREG Fabric Developer Quick Reference

## Critical Gotchas

### 1. Byzantine Detection False Positives

Only store **conflicting** responses. Do not store the first response as a
conflict.

```python
# Correct: only add if response differs from an existing one
responses = consensus_round.conflicting_responses.setdefault(cluster_id, [])
if response_payload not in responses:
    responses.append(response_payload)
```

### 2. Cluster Weight Synchronization

All coordinators must use the same cluster weights when running quorum
operations.

```python
cluster_weights = {
    "cluster-a": ConsensusClusterWeight(cluster_id="cluster-a", weight=1.0),
    "cluster-b": ConsensusClusterWeight(cluster_id="cluster-b", weight=1.0),
}
coordinator.cluster_weights = cluster_weights
```

### 3. Automatic Queue Acknowledgments

Queue federation ACKs are automatic. Avoid manual ACK tests.

## Essential Testing Patterns

### Multi-Cluster Setup

```python
server_a, server_b = await _start_queue_servers(ctx, port_manager)

await server_a._fabric_queue_federation.create_queue("jobs")
await server_b._fabric_queue_federation.create_queue("jobs")
```

### Consensus Testing

```python
result = await server_a._fabric_queue_delivery.deliver_with_global_quorum(
    queue_name="jobs",
    topic="jobs.created",
    payload={"id": 123},
    target_clusters={"cluster-a", "cluster-b"},
)
assert result.success
```

## Key Metrics to Monitor

### Consensus Health

```python
stats = coordinator.delivery_stats
assert stats.byzantine_faults_detected == 0
assert stats.consensus_success_rate() > 0.95
```

### Queue Federation Health

```python
stats = manager.get_federation_statistics()
assert stats.success_rate() >= 0.0
```

## Common Debug Hooks

```python
logger.info(f"Consensus rounds: {list(coordinator.active_consensus_rounds.keys())}")
logger.info(f"Votes: {consensus_round.votes_received}")
logger.info(f"Conflicts: {consensus_round.conflicting_responses}")
logger.info(f"Has quorum: {consensus_round.has_quorum()}")
```

## File Locations

- Fabric queue manager: `mpreg/fabric/queue_federation.py`
- Queue delivery coordinator: `mpreg/fabric/queue_delivery.py`
- Cluster messenger: `mpreg/fabric/cluster_messenger.py`
- Queue federation tests: `tests/test_fabric_queue_federation.py`
- Queue delivery tests: `tests/test_fabric_queue_delivery.py`
- Live integration: `tests/integration/test_fabric_queue_federation.py`
