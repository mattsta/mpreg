# MPREG Federation Developer Quick Reference

## 🚨 Critical Gotchas and Edge Cases

### 1. Byzantine Detection False Positives

**⚠️ NEVER track first responses as conflicts**

```python
# ❌ WRONG - causes false Byzantine detection
consensus_round.conflicting_responses[cluster] = [response]

# ✅ CORRECT - only track actual conflicts
if cluster in conflicting_responses and response not in existing_responses:
    conflicting_responses[cluster].append(response)
```

### 2. Cluster Weight Synchronization

**⚠️ All delivery coordinators must have identical cluster weights**

```python
# ✅ CORRECT - ensure consistency across all coordinators
cluster_weights = {
    "cluster-1": ClusterWeight(cluster_id="cluster-1", weight=1.0),
    "cluster-2": ClusterWeight(cluster_id="cluster-2", weight=1.0),
}
coordinator1.cluster_weights = cluster_weights
coordinator2.cluster_weights = cluster_weights  # Must be identical
```

### 3. Automatic Federation Acknowledgments

**⚠️ Don't test manual acknowledgments - federation bridge auto-acknowledges**

```python
# ❌ WRONG - testing manual acknowledgment
await manager.acknowledge_federated_message(ack_token="dummy")

# ✅ CORRECT - test automatic acknowledgment
await asyncio.sleep(2.0)  # Allow processing time
stats = manager.get_federation_statistics()
assert stats.successful_cross_cluster_deliveries >= 1
```

## 🔧 Essential Testing Patterns

### Multi-Cluster Test Setup

```python
# Always test with at least 3 clusters for quorum scenarios
cluster1_servers, cluster2_servers, cluster3_servers = federation_clusters

# Create delivery coordinators for ALL clusters
delivery_coord1 = FederatedDeliveryCoordinator(
    cluster_id="cluster-1",
    federation_bridge=manager1.federation_bridge,
    federated_queue_manager=manager1,
)
```

### Consensus Testing

```python
# Test consensus with proper timing
await asyncio.sleep(2.0)  # Allow federation setup

result = await delivery_coord1.deliver_with_global_quorum(
    message=test_message,
    target_clusters={"cluster-1", "cluster-2", "cluster-3"},
    required_weight_threshold=0.67,  # 67% quorum
    timeout_seconds=30.0,  # Sufficient for test environment
)

assert result.success
assert len(result.delivered_to) >= 2  # At least 2/3 clusters
```

## 📊 Key Metrics to Monitor

### Consensus Health

```python
stats = coordinator.get_delivery_statistics()

# Critical metrics
assert stats.byzantine_faults_detected == 0  # No false positives
assert stats.consensus_success_rate() > 0.95  # High success rate
assert stats.average_consensus_time_ms < 10000  # Under 10 seconds
```

### Federation Health

```python
stats = manager.get_federation_statistics()

# Critical metrics
assert stats.federation_success_rate() > 0.95  # High success rate
assert stats.total_federated_messages > 0  # Messages being sent
assert stats.federation_latency_ms < 5000  # Under 5 seconds
```

## 🐛 Common Debug Commands

### Consensus Debugging

```python
# Add these logs to debug consensus issues
logger.info(f"Active consensus rounds: {list(coordinator.active_consensus_rounds.keys())}")
logger.info(f"Votes received: {consensus_round.votes_received}")
logger.info(f"Conflicting responses: {consensus_round.conflicting_responses}")
logger.info(f"Byzantine clusters: {consensus_round.detect_byzantine_faults()}")
logger.info(f"Has quorum: {consensus_round.has_quorum()}")
```

### Federation Debugging

```python
# Add these logs to debug federation issues
logger.info(f"Local cluster ID: {manager.local_cluster_id}")
logger.info(f"Remote queues: {dict(manager.remote_queues)}")
logger.info(f"Federation bridge connected: {manager.federation_bridge.is_connected()}")
logger.info(f"Target cluster discovered: {await manager._discover_queue_cluster(queue_name)}")
```

## 🔍 File Locations

### Core Components

- **Federation Bridge**: `mpreg/federation/federation_bridge.py`
- **Federated Queue Manager**: `mpreg/core/federated_message_queue.py`
- **Delivery Coordinator**: `mpreg/core/federated_delivery_guarantees.py`

### Critical Methods

- **Byzantine Detection**: `GlobalConsensusRound.detect_byzantine_faults()` (line ~157)
- **Vote Handling**: `FederatedDeliveryCoordinator.handle_consensus_vote()` (line ~425)
- **Consensus Waiting**: `FederatedDeliveryCoordinator._wait_for_consensus()` (line ~529)

### Test Files

- **Live Integration Tests**: `tests/test_federated_message_queue_live.py`
- **Unit Tests**: `tests/test_federated_message_queue.py`

## ⚡ Performance Tips

### Optimize Consensus Timeouts

```python
# Adjust based on network conditions
consensus_timeout = 30.0  # Testing
consensus_timeout = 120.0  # Production with high latency
consensus_timeout = 10.0   # Low-latency production
```

### Batch Operations

```python
# Avoid creating too many concurrent consensus rounds
# Use semaphores to limit concurrent operations
consensus_semaphore = asyncio.Semaphore(3)  # Max 3 concurrent consensus
```

### Monitor Resource Usage

```python
# Check background worker performance
logger.info(f"Active tasks: {len(coordinator._task_manager.tasks)}")
logger.info(f"Active consensus rounds: {len(coordinator.active_consensus_rounds)}")
```

## 🚨 Emergency Procedures

### If Consensus Always Fails

1. Check Byzantine detection logs for false positives
2. Verify cluster weight synchronization
3. Increase consensus timeout
4. Test with single cluster first

### If Federation Messages Not Delivered

1. Verify federation bridge connectivity
2. Check cluster discovery and advertisements
3. Validate message topic patterns
4. Inspect federation forwarding logs

### If Performance Degrades

1. Monitor consensus completion times
2. Check for consensus round cleanup issues
3. Validate background worker performance
4. Inspect network bandwidth utilization

## 📝 Code Review Checklist

### Consensus Changes

- [ ] No false positive Byzantine detection
- [ ] Proper vote tracking without conflicts
- [ ] Consistent cluster weight handling
- [ ] Adequate timeout values
- [ ] Comprehensive test coverage

### Federation Changes

- [ ] Proper message targeting and routing
- [ ] Correct cluster ID handling
- [ ] Statistics tracking accuracy
- [ ] Error handling and recovery
- [ ] Cross-cluster compatibility

### Testing Requirements

- [ ] Multi-cluster integration tests
- [ ] Byzantine fault simulation
- [ ] Network partition scenarios
- [ ] Performance regression tests
- [ ] Edge case coverage

---

**⚠️ Remember**: The federation system is complex and fragile. Always test changes with live multi-cluster scenarios, and never assume that unit tests alone are sufficient.

**Last Updated**: 2025-07-20
