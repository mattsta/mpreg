# MPREG Fabric Architecture and Fault Tolerance

This document describes the unified fabric queue delivery system, including
consensus behavior, fault tolerance, and key edge cases.

## Fabric Architecture Overview

The legacy federated queue stack has been replaced with the unified fabric:

- **FabricQueueFederationManager** (`mpreg/fabric/queue_federation.py`)
  - Advertises queues via the fabric catalog
  - Routes cross-cluster queue messages over UnifiedMessage envelopes
- **FabricQueueDeliveryCoordinator** (`mpreg/fabric/queue_delivery.py`)
  - Performs quorum-based delivery and broadcast delivery
  - Uses fabric control messages for consensus voting
- **ClusterMessenger** (`mpreg/fabric/cluster_messenger.py`)
  - Enforces hop budgets and path tracking

Consensus and queue routing now flow through the same fabric transport and
routing headers.

## Byzantine Fault Tolerant Consensus

### Consensus Flow (Unified Fabric)

```
Initiator                    Remote Cluster
   │                                │
   ├─► mpreg.queue.consensus.request│
   │                                │
   │◄─ mpreg.queue.consensus.vote   │
   │                                │
   ├─► quorum reached               │
   │
   └─► deliver queue message(s)
```

### Critical Implementation Details

#### Vote Collection and Conflict Tracking

**Location**: `QueueConsensusRound.record_vote()`

Only conflicting responses are stored. This prevents false Byzantine detection
from first votes:

```python
responses = self.conflicting_responses.setdefault(voter, [])
if response_payload not in responses:
    responses.append(response_payload)
```

#### Byzantine Detection

**Location**: `QueueConsensusRound.detect_byzantine_faults()`

Clusters are flagged only when multiple _different_ responses are observed:

```python
for cluster_id, responses in self.conflicting_responses.items():
    if len(responses) > 1:
        byzantine_clusters.add(cluster_id)
```

#### Quorum Calculation

**Location**: `QueueConsensusRound.has_quorum()`

Uses weighted votes with a small epsilon to avoid floating-point edge cases.

## Critical Edge Cases

1. **Timeout vs. Network Latency**
   - Consensus timeouts should reflect expected RTTs across clusters.
2. **Cluster Weight Consistency**
   - Weights must be configured consistently across clusters.
3. **Causal Ordering Buffers**
   - Causal dependencies are buffered until prerequisites clear.

## Delivery Guarantees

- **AT_LEAST_ONCE**: Standard queue delivery with acknowledgments
- **BROADCAST**: Delivered to all target clusters
- **GLOBAL_QUORUM**: Requires weighted quorum before delivery

## Tests

- Unit: `tests/test_fabric_queue_delivery.py`
- Integration: `tests/integration/test_fabric_queue_federation.py`

All tests use the port allocator; fixed ports are not allowed.
