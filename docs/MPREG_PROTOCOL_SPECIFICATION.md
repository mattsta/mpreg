# MPREG Protocol Specification v2.0

## Table of Contents

1. [Overview](#overview)
2. [Transport Layer](#transport-layer)
3. [Message Format](#message-format)
4. [RPC Protocol](#rpc-protocol)
5. [Pub/Sub Protocol](#pubsub-protocol)
6. [Gossip Protocol](#gossip-protocol)
7. [Federation Protocol](#federation-protocol)
8. [Metrics Protocol](#metrics-protocol)
9. [Global Distributed Caching Protocol](#global-distributed-caching-protocol)
10. [Federated Message Queue Protocol](#federated-message-queue-protocol)
11. [Security Model](#security-model)
12. [Error Handling](#error-handling)
13. [Client Implementation Guide](#client-implementation-guide)
14. [Code References](#code-references)

---

## Overview

MPREG (Multi-Provider REGistry) is a planet-scale distributed computing platform that provides:

- **Multi-Protocol RPC**: Function calls across distributed clusters
- **Topic-Based Pub/Sub**: High-performance messaging with pattern matching
- **Federated Message Queues**: SQS-like queuing with cross-cluster delivery guarantees
- **Gossip-Based Clustering**: State synchronization and membership management
- **Federation Routing**: Multi-hop message routing across global clusters
- **Distributed Caching**: Multi-tier caching with intelligent eviction
- **Real-Time Metrics**: Comprehensive performance and health monitoring

### Protocol Stack

```
┌─────────────────────────────────────────────────┐
│  Application Layer (RPC, Pub/Sub, Queues, Cache)│
├─────────────────────────────────────────────────┤
│  Federation Layer (Graph Routing, Bridges)     │
├─────────────────────────────────────────────────┤
│  Gossip Layer (State Sync, Membership, Queues) │
├─────────────────────────────────────────────────┤
│  Message Layer (JSON/MessagePack Serialization)│
├─────────────────────────────────────────────────┤
│  Transport Layer (WebSocket, TCP, TLS)         │
└─────────────────────────────────────────────────┘
```

---

## Transport Layer

### Supported Transports

| Protocol         | URL Scheme | Default Port | Use Case          | Max Message Size |
| ---------------- | ---------- | ------------ | ----------------- | ---------------- |
| WebSocket        | `ws://`    | 6666         | Real-time clients | 20MB             |
| WebSocket Secure | `wss://`   | 6667         | Secure real-time  | 20MB             |
| TCP              | `tcp://`   | 6668         | High-performance  | 20MB             |
| TCP Secure       | `tcps://`  | 6669         | Secure high-perf  | 20MB             |

### Connection Types

- **CLIENT**: User-facing connections (20MB/100MB limits)
- **INTERNAL**: Node-to-node connections (5MB/20MB limits)

### Transport Features

- **Multi-Protocol Adapters**: Multiple transports active simultaneously
- **Streaming Support**: Large data transfers via chunked streaming
- **Connection Pooling**: Efficient connection reuse
- **Circuit Breakers**: Automatic fault tolerance

---

## Message Format

### Base Message Structure

All MPREG messages follow a consistent JSON structure:

```json
{
  "role": "message-type",
  "u": "unique-request-id",
  "timestamp": 1640995200.123,
  "version": "2.0",
  "...": "type-specific-fields"
}
```

#### Core Fields

- `role`: Message type identifier (required)
- `u`: Unique request/correlation ID (required)
- `timestamp`: Unix timestamp with microseconds (optional)
- `version`: Protocol version (optional, defaults to "2.0")

### Message Types

| Role                  | Purpose                 | Direction           |
| --------------------- | ----------------------- | ------------------- |
| `rpc`                 | Function call request   | Client → Server     |
| `rpc-response`        | Function call response  | Server → Client     |
| `internal-rpc`        | Internal RPC request    | Node → Node         |
| `internal-answer`     | Internal RPC response   | Node → Node         |
| `pubsub-publish`      | Publish message         | Client → Server     |
| `pubsub-subscribe`    | Subscribe to topics     | Client → Server     |
| `pubsub-notification` | Message delivery        | Server → Client     |
| `gossip`              | Gossip protocol message | Node → Node         |
| `federation`          | Cross-cluster message   | Cluster → Cluster   |
| `metrics`             | Metrics reporting       | Component → Monitor |
| `cache-request`       | Cache operation         | Client → Cache      |
| `cache-response`      | Cache operation result  | Cache → Client      |

---

## RPC Protocol

### RPC Request

**Message Type**: `rpc`

```json
{
  "role": "rpc",
  "u": "req-12345",
  "cmds": [
    {
      "name": "command-name",
      "fun": "function_name",
      "args": [arg1, arg2, ...],
      "kwargs": {"key": "value", ...},
      "locs": ["resource1", "resource2"]
    }
  ]
}
```

#### Fields

- `cmds`: Array of RPC commands to execute
- `name`: Command identifier
- `fun`: Function name to invoke
- `args`: Positional arguments
- `kwargs`: Keyword arguments
- `locs`: Resource locations/constraints

### RPC Response

**Message Type**: `rpc-response`

```json
{
  "role": "rpc-response",
  "u": "req-12345",
  "r": "result-data",
  "error": null
}
```

#### Success Response

- `r`: Function result (any JSON-serializable type)
- `error`: null

#### Error Response

```json
{
  "role": "rpc-response",
  "u": "req-12345",
  "r": null,
  "error": {
    "type": "ValueError",
    "message": "Invalid argument",
    "traceback": "...",
    "code": "INVALID_ARGUMENT"
  }
}
```

### Internal RPC

**Message Type**: `internal-rpc`

Used for node-to-node communication within a cluster:

```json
{
  "role": "internal-rpc",
  "u": "internal-67890",
  "command": "execute_distributed_task",
  "args": ["param1", "param2"],
  "kwargs": { "option": "value" },
  "results": { "partial": "data" }
}
```

### Server Lifecycle Messages

#### Hello Message

```json
{
  "what": "HELLO",
  "funs": ["function1", "function2"],
  "locs": ["location1", "location2"],
  "cluster_id": "cluster-abc123",
  "advertised_urls": ["ws://node1:6666", "tcp://node1:6668"]
}
```

#### Goodbye Message

```json
{
  "what": "GOODBYE"
}
```

### Code References

- **Generation**: `mpreg/client/client_api.py:call()`
- **Processing**: `mpreg/server.py:message_handler()`
- **Models**: `mpreg/core/model.py:RPCRequest`

---

## Pub/Sub Protocol

### Message Publication

**Message Type**: `pubsub-publish`

```json
{
  "role": "pubsub-publish",
  "u": "pub-11111",
  "message": {
    "topic": "sensors.temperature.building1.floor2",
    "payload": {
      "value": 23.5,
      "unit": "celsius",
      "sensor_id": "temp_001"
    },
    "timestamp": 1640995200.123,
    "message_id": "msg-abc123",
    "publisher": "sensor-gateway-01",
    "headers": {
      "content-type": "application/json",
      "priority": "normal"
    },
    "routing_path": ["cluster1", "cluster2"],
    "current_hop": 0
  }
}
```

### Topic Subscription

**Message Type**: `pubsub-subscribe`

```json
{
  "role": "pubsub-subscribe",
  "u": "sub-22222",
  "subscription": {
    "subscription_id": "sub-xyz789",
    "patterns": [
      {
        "pattern": "sensors.*.building1.#",
        "exact_match": false
      },
      {
        "pattern": "alerts.critical.*",
        "exact_match": false
      }
    ],
    "subscriber": "monitoring-dashboard",
    "created_at": 1640995200.123,
    "get_backlog": true,
    "backlog_seconds": 300
  }
}
```

#### Pattern Matching

MPREG uses AMQP-style pattern matching:

- `*`: Matches exactly one topic level
- `#`: Matches zero or more topic levels
- `.`: Level separator

**Examples**:

- `sensors.*` matches `sensors.temperature` but not `sensors.temperature.room1`
- `sensors.#` matches `sensors.temperature`, `sensors.temperature.room1`, etc.
- `sensors.*.room1` matches `sensors.temperature.room1`, `sensors.humidity.room1`

### Message Notification

**Message Type**: `pubsub-notification`

```json
{
  "role": "pubsub-notification",
  "u": "notif-33333",
  "message": {
    "topic": "sensors.temperature.building1.floor2",
    "payload": {...},
    "timestamp": 1640995200.123,
    "message_id": "msg-abc123",
    "publisher": "sensor-gateway-01",
    "headers": {...}
  },
  "subscription_id": "sub-xyz789"
}
```

### Backlog Support

The topic exchange maintains time-windowed message backlogs:

- **Backlog Window**: Configurable time window (default: 5 minutes)
- **Late Subscribers**: Receive historical messages within window
- **Performance**: Optimized for millions of topics

### Code References

- **Generation**: `mpreg/client/pubsub_client.py:publish()`
- **Processing**: `mpreg/core/topic_exchange.py:TopicExchange`
- **Models**: `mpreg/core/model.py:PubSubMessage`

---

## Gossip Protocol

### Gossip Message Structure

**Message Type**: `gossip`

```json
{
  "role": "gossip",
  "u": "gossip-44444",
  "message_id": "gossip-abc123",
  "message_type": "state_update",
  "sender_id": "node-1",
  "payload": {
    "key": "cluster.node-2.status",
    "value": "healthy",
    "version": 15,
    "timestamp": 1640995200.123,
    "source_node": "node-2",
    "ttl": 300
  },
  "vector_clock": {
    "node-1": 42,
    "node-2": 38,
    "node-3": 51
  },
  "sequence_number": 1337,
  "ttl": 5,
  "hop_count": 0,
  "max_hops": 3,
  "digest": "sha256:abc123...",
  "checksum": "crc32:def456",
  "created_at": 1640995200.123,
  "expires_at": 1640995500.123,
  "propagation_path": ["node-1"],
  "seen_by": ["node-1", "node-3"]
}
```

### Message Types

| Type                 | Purpose                 | Payload                  |
| -------------------- | ----------------------- | ------------------------ |
| `state_update`       | State synchronization   | Key-value with version   |
| `membership_update`  | Node join/leave/update  | Node info and event type |
| `config_update`      | Configuration changes   | Config key-value         |
| `heartbeat`          | Liveness indication     | Node health metrics      |
| `anti_entropy`       | State reconciliation    | State digest             |
| `rumor`              | Information propagation | Generic data             |
| `consensus_proposal` | Distributed consensus   | Proposal data            |
| `consensus_vote`     | Consensus voting        | Vote information         |
| `membership_probe`   | Node health check       | Probe request            |
| `membership_ack`     | Probe acknowledgment    | Health response          |

### Vector Clock

Provides causal ordering of events:

```json
{
  "vector_clock": {
    "node-1": 42,
    "node-2": 38,
    "node-3": 51
  }
}
```

**Operations**:

- **Increment**: `clock[node_id] += 1` on local events
- **Update**: `clock[node] = max(local[node], remote[node])` on message receipt
- **Compare**: Determine causal relationship between events

### Anti-Entropy Protocol

Periodic state reconciliation between nodes:

1. **Digest Exchange**: Nodes exchange state digests
2. **Difference Detection**: Compare digests to find inconsistencies
3. **State Synchronization**: Exchange missing/outdated state
4. **Convergence**: Eventually consistent state across cluster

### Code References

- **Generation**: `mpreg/federation/federation_gossip.py:_perform_gossip_cycle()`
- **Processing**: `mpreg/federation/federation_gossip.py:_handle_*_update()`
- **Models**: `mpreg/federation/federation_gossip.py:GossipMessage`

---

## Federation Protocol

### Federation Architecture

MPREG federation enables planet-scale deployment through:

- **Graph-Based Routing**: Intelligent multi-hop message routing
- **Hierarchical Structure**: Hub-spoke and mesh topologies
- **Geographic Awareness**: Location-based optimization
- **Fault Tolerance**: Redundant paths and circuit breakers

### Cluster Identity

```json
{
  "cluster_id": "us-west-1-prod",
  "cluster_name": "US West Production",
  "region": "us-west-1",
  "bridge_url": "wss://us-west-1.mpreg.example.com:6667",
  "public_key_hash": "sha256:abc123...",
  "created_at": 1640995200.123,
  "geographic_coordinates": [37.7749, -122.4194],
  "network_tier": 1,
  "max_bandwidth_mbps": 10000,
  "preference_weight": 0.95
}
```

### Federation Graph

#### Node Types

- **CLUSTER**: Computing cluster (leaf node)
- **HUB**: Regional hub (aggregation point)
- **RELAY**: Routing relay (forwarding only)

#### Graph Node

```json
{
  "node_id": "hub-us-west",
  "node_type": "HUB",
  "region": "us-west",
  "coordinates": [37.7749, -122.4194],
  "max_capacity": 1000.0,
  "current_load": 150.0,
  "health_score": 0.98,
  "processing_latency_ms": 5.2,
  "bandwidth_mbps": 10000.0,
  "reliability_score": 0.995
}
```

#### Graph Edge

```json
{
  "source_id": "cluster-sf-1",
  "target_id": "hub-us-west",
  "latency_ms": 15.3,
  "bandwidth_mbps": 1000.0,
  "reliability_score": 0.99,
  "current_utilization": 0.25,
  "priority_class": 1
}
```

### Federation Message Flow

**Message Type**: `federation`

```json
{
  "role": "federation",
  "u": "fed-55555",
  "routing_header": {
    "source_cluster": "us-west-1-prod",
    "target_cluster": "eu-west-1-prod",
    "message_type": "pubsub-notification",
    "priority": "normal",
    "max_hops": 5,
    "current_hop": 2,
    "routing_path": ["us-west-1-prod", "hub-us", "hub-eu"],
    "quality_requirements": {
      "max_latency_ms": 100,
      "min_bandwidth_mbps": 10,
      "min_reliability": 0.99
    }
  },
  "payload": {
    "role": "pubsub-notification",
    "message": {...},
    "subscription_id": "global-alerts"
  }
}
```

### Routing Algorithm

1. **Destination Resolution**: Determine target cluster(s)
2. **Path Planning**: Calculate optimal routes using Dijkstra/A\*
3. **Quality Assessment**: Evaluate path quality (latency, bandwidth, reliability)
4. **Load Balancing**: Distribute traffic across available paths
5. **Circuit Breaking**: Avoid failed or overloaded paths

### Code References

- **Generation**: `mpreg/federation/federated_topic_exchange.py:publish_to_federation()`
- **Processing**: `mpreg/federation/federation_bridge.py:route_message()`
- **Models**: `mpreg/federation/federation_graph.py:FederationGraphNode`

---

## Metrics Protocol

### Metrics Collection

**Message Type**: `metrics`

```json
{
  "role": "metrics",
  "u": "metrics-66666",
  "source": "cluster-us-west-1",
  "timestamp": 1640995200.123,
  "metrics": {
    "performance": {
      "total_clusters": 45,
      "healthy_clusters": 43,
      "federation_avg_latency_ms": 25.3,
      "federation_total_throughput_rps": 15420.0,
      "total_cross_cluster_messages": 1234567,
      "avg_cpu_usage_percent": 45.2,
      "federation_health_score": 0.956
    },
    "cache": {
      "hits": 98765,
      "misses": 1234,
      "evictions": 56,
      "memory_bytes": 1073741824,
      "key_memory_bytes": 52428800,
      "value_memory_bytes": 1021313024,
      "entry_count": 12345
    },
    "topic_exchange": {
      "active_subscriptions": 2500,
      "messages_published": 45678,
      "messages_delivered": 45234,
      "delivery_ratio": 0.9903,
      "trie_stats": {
        "node_count": 15420,
        "leaf_count": 8765,
        "max_depth": 12,
        "avg_depth": 4.2
      },
      "backlog_stats": {
        "total_messages": 1234,
        "oldest_message_age_seconds": 45.2,
        "memory_usage_bytes": 5242880
      }
    },
    "gossip": {
      "total_nodes": 12,
      "healthy_nodes": 11,
      "total_messages_sent": 5678,
      "total_messages_received": 5432,
      "convergence_time_ms": 125.3,
      "state_version": 1337
    },
    "rpc": {
      "total_requests": 98765,
      "successful_requests": 97890,
      "failed_requests": 875,
      "avg_latency_ms": 15.2,
      "p95_latency_ms": 45.8,
      "p99_latency_ms": 123.4,
      "active_connections": 156
    }
  }
}
```

### Alerting Thresholds

```json
{
  "role": "metrics-alert",
  "u": "alert-77777",
  "alert_type": "threshold_exceeded",
  "severity": "warning",
  "metric": "federation_avg_latency_ms",
  "current_value": 125.3,
  "threshold": 100.0,
  "source": "cluster-us-west-1",
  "timestamp": 1640995200.123,
  "description": "Federation latency exceeds threshold"
}
```

### Code References

- **Generation**: `mpreg/core/statistics.py:get_comprehensive_stats()`
- **Processing**: `mpreg/federation/federation_graph_monitor.py:collect_metrics()`
- **Models**: `mpreg/core/statistics.py:FederationPerformanceMetrics`

---

## Global Distributed Caching Protocol

### Cache Architecture

MPREG implements a sophisticated multi-tier caching system:

```
┌─────────────────────────────────────────────────┐
│  L1: Memory Cache (S4LRU, Cost-Based Eviction) │
├─────────────────────────────────────────────────┤
│  L2: Persistent Cache (SSD/NVMe Storage)       │
├─────────────────────────────────────────────────┤
│  L3: Distributed Cache (Gossip-Based)          │
├─────────────────────────────────────────────────┤
│  L4: Federation Cache (Global Replication)     │
└─────────────────────────────────────────────────┘
```

### Cache Request

**Message Type**: `cache-request`

```json
{
  "role": "cache-request",
  "u": "cache-88888",
  "operation": "get",
  "key": {
    "namespace": "compute.results",
    "identifier": "sha256:abc123...",
    "version": "v1.2.3",
    "tags": ["expensive", "ml-model"]
  },
  "options": {
    "include_metadata": true,
    "consistency_level": "eventual",
    "timeout_ms": 5000,
    "cache_levels": ["L1", "L2", "L3"],
    "replication_factor": 2
  }
}
```

### Cache Response

**Message Type**: `cache-response`

```json
{
  "role": "cache-response",
  "u": "cache-88888",
  "status": "hit",
  "cache_level": "L1",
  "entry": {
    "key": {...},
    "value": {"result": "computed-data", "metadata": {...}},
    "creation_time": 1640995200.123,
    "access_count": 42,
    "last_access_time": 1640995800.456,
    "computation_cost_ms": 15420.0,
    "size_bytes": 1048576,
    "dependencies": ["input:abc123", "model:def456"],
    "ttl_seconds": 3600
  },
  "performance": {
    "lookup_time_ms": 0.05,
    "network_hops": 0,
    "cache_efficiency": 0.95
  }
}
```

### Cache Operations

#### Get Operation

```json
{
  "role": "cache-request",
  "u": "cache-11111",
  "operation": "get",
  "key": {...},
  "options": {
    "consistency_level": "strong"
  }
}
```

#### Put Operation

```json
{
  "role": "cache-request",
  "u": "cache-22222",
  "operation": "put",
  "key": {...},
  "value": {...},
  "metadata": {
    "computation_cost_ms": 5000.0,
    "dependencies": ["input:xyz789"],
    "ttl_seconds": 7200,
    "replication_policy": "async"
  }
}
```

#### Delete Operation

```json
{
  "role": "cache-request",
  "u": "cache-33333",
  "operation": "delete",
  "key": {...},
  "options": {
    "cascade_dependencies": true,
    "replication_propagate": true
  }
}
```

#### Invalidate Operation

```json
{
  "role": "cache-request",
  "u": "cache-44444",
  "operation": "invalidate",
  "pattern": "compute.results.*",
  "options": {
    "reason": "dependency_changed",
    "source_change": "input:abc123"
  }
}
```

### Global Cache Replication

#### Replication Strategy

```json
{
  "replication_policy": {
    "strategy": "geographic",
    "min_replicas": 2,
    "max_replicas": 5,
    "preferred_regions": ["us-west", "eu-west", "ap-southeast"],
    "consistency_model": "eventual",
    "conflict_resolution": "last_writer_wins"
  }
}
```

#### Cache Gossip

Cache state is synchronized using the gossip protocol:

```json
{
  "role": "gossip",
  "message_type": "cache_update",
  "payload": {
    "operation": "put",
    "key": {...},
    "value_hash": "sha256:def456...",
    "metadata": {...},
    "vector_clock": {...},
    "replication_sites": ["us-west-1", "eu-west-1"]
  }
}
```

### Eviction Policies

#### S4LRU (Segmented LRU)

- **4 Segments**: S0 (new), S1 (accessed once), S2 (accessed twice), S3 (hot)
- **Promotion**: Cache hits promote items to higher segments
- **Eviction**: Cascades from S3 → S2 → S1 → S0

#### Cost-Based Eviction

```json
{
  "eviction_score": {
    "computation_cost": 5000.0,
    "access_frequency": 0.15,
    "recency_factor": 0.8,
    "size_penalty": 0.9,
    "dependency_weight": 1.2,
    "final_score": 4320.5
  }
}
```

### Cache Analytics

```json
{
  "role": "cache-analytics",
  "u": "analytics-99999",
  "period": {
    "start": 1640995200.123,
    "end": 1640998800.123,
    "duration_seconds": 3600
  },
  "statistics": {
    "operations": {
      "get_requests": 45678,
      "put_requests": 1234,
      "delete_requests": 56,
      "invalidate_requests": 12
    },
    "performance": {
      "hit_ratio_l1": 0.85,
      "hit_ratio_l2": 0.12,
      "hit_ratio_l3": 0.025,
      "miss_ratio": 0.005,
      "avg_lookup_time_ms": 0.15,
      "cache_efficiency": 0.995
    },
    "cost_savings": {
      "computation_time_saved_ms": 1234567.0,
      "network_requests_avoided": 4321,
      "estimated_cost_savings_usd": 45.67
    }
  }
}
```

### Advanced Cache Operations

MPREG extends basic caching with advanced operations that enable the cache to function as a distributed data structure and micro-database server.

#### Atomic Operations

**Message Type**: `cache-request` with `operation: "atomic"`

```json
{
  "role": "cache-request",
  "u": "atomic-12345",
  "operation": "atomic",
  "key": {
    "namespace": "locks",
    "identifier": "resource_abc",
    "version": "v1"
  },
  "atomic_operation": {
    "operation_type": "test_and_set",
    "expected_value": null,
    "new_value": {
      "owner": "worker_001",
      "acquired_at": 1640995200.123
    },
    "ttl_seconds": 300,
    "conditions": {
      "if_not_exists": true
    },
    "create_if_missing": true
  },
  "options": {
    "consistency_level": "strong",
    "timeout_ms": 5000
  }
}
```

**Atomic Operation Types**:

| Operation          | Purpose                    | Parameters                      |
| ------------------ | -------------------------- | ------------------------------- |
| `test_and_set`     | Conditional updates        | `expected_value`, `new_value`   |
| `compare_and_swap` | Atomic replacement         | `expected_value`, `new_value`   |
| `increment`        | Atomic numeric increment   | `increment_by`, `initial_value` |
| `decrement`        | Atomic numeric decrement   | `increment_by`, `initial_value` |
| `append`           | Atomic string/list append  | `new_value`                     |
| `prepend`          | Atomic string/list prepend | `new_value`                     |
| `extend_ttl`       | TTL extension              | `ttl_seconds`                   |

#### Data Structure Operations

**Message Type**: `cache-request` with `operation: "structure"`

```json
{
  "role": "cache-request",
  "u": "struct-67890",
  "operation": "structure",
  "key": {
    "namespace": "user_permissions",
    "identifier": "user_123"
  },
  "structure_operation": {
    "structure_type": "set",
    "operation": "add",
    "values": ["read_posts", "write_comments"],
    "options": {
      "create_if_missing": true,
      "return_size": true
    }
  }
}
```

**Supported Data Structures**:

##### Set Operations

```json
{
  "structure_type": "set",
  "operation": "add|remove|contains|size|union|intersection|difference",
  "values": ["item1", "item2"],
  "options": { "other_sets": ["namespace:other_set_key"] }
}
```

##### List Operations

```json
{
  "structure_type": "list",
  "operation": "append|prepend|pop_front|pop_back|get_range|length|insert",
  "values": [{ "item": "data" }],
  "index": 5,
  "range_spec": { "start": 0, "end": 10 }
}
```

##### Map Operations

```json
{
  "structure_type": "map",
  "operation": "set_field|get_fields|remove_fields|increment_field|keys|values",
  "field_updates": {
    "user.last_login": 1640995200.123,
    "user.login_count": { "operation": "increment", "value": 1 }
  },
  "fields": ["user.email", "user.preferences.theme"]
}
```

##### Sorted Set Operations

```json
{
  "structure_type": "sorted_set",
  "operation": "add_scored|remove|get_top|get_bottom|get_range|get_score",
  "scored_values": [{ "value": "player_123", "score": 95000 }],
  "limit": 10,
  "range_spec": { "min_score": 1000, "max_score": 10000 }
}
```

#### Namespace Operations

**Message Type**: `cache-request` with `operation: "namespace"`

```json
{
  "role": "cache-request",
  "u": "ns-54321",
  "operation": "namespace",
  "namespace_operation": {
    "operation_type": "clear",
    "namespace": "temp_computations",
    "pattern": "expired_*",
    "conditions": {
      "ttl_remaining": { "$lt": 60 },
      "access_count": { "$eq": 0 }
    },
    "max_entries": 1000,
    "dry_run": false
  }
}
```

**Namespace Operation Types**:

| Operation    | Purpose             | Parameters                                  |
| ------------ | ------------------- | ------------------------------------------- |
| `clear`      | Bulk delete entries | `pattern`, `conditions`, `max_entries`      |
| `statistics` | Namespace metrics   | `include_detailed_breakdown`, `sample_size` |
| `list_keys`  | Enumerate keys      | `pattern`, `limit`, `offset`                |
| `backup`     | Export namespace    | `backup_format`, `compression`              |
| `restore`    | Import namespace    | `data`, `merge_strategy`                    |

#### Cache-Pub/Sub Integration

Cache operations can trigger pub/sub notifications for real-time updates:

```json
{
  "role": "cache-request",
  "u": "notify-77777",
  "operation": "put",
  "key": {...},
  "value": {...},
  "options": {
    "notify_on_change": true,
    "notification_topic": "cache.inventory.updates",
    "notification_payload": {
      "change_type": "stock_level_update",
      "product_id": "product_123"
    },
    "notification_condition": {
      "field_path": "stock_level",
      "threshold": {"$lt": 10}
    }
  }
}
```

When conditions are met, automatic pub/sub messages are published:

```json
{
  "role": "pubsub-publish",
  "u": "cache-notify-88888",
  "message": {
    "topic": "cache.inventory.updates",
    "payload": {
      "change_type": "stock_level_update",
      "product_id": "product_123",
      "cache_key": {...},
      "old_value": {...},
      "new_value": {...}
    },
    "timestamp": 1640995200.123,
    "publisher": "cache-system"
  }
}
```

### Implementation References

**Advanced Cache Components**:

```python
# mpreg/core/global_cache.py
class GlobalCacheManager:
    """Multi-tier global cache with gossip-based replication."""

    async def get(self, key: CacheKey, options: CacheOptions = None) -> CacheEntry | None
    async def put(self, key: CacheKey, value: Any, metadata: CacheMetadata = None) -> bool
    async def delete(self, key: CacheKey, options: CacheOptions = None) -> bool
    async def invalidate(self, pattern: str, options: CacheOptions = None) -> int

# mpreg/core/cache_gossip.py
class CacheGossipProtocol:
    """Gossip-based cache state synchronization."""

    async def propagate_cache_update(self, operation: str, key: CacheKey, metadata: dict)
    async def handle_cache_gossip(self, message: GossipMessage)
    async def sync_cache_state(self, peer_node: str)

# mpreg/federation/global_cache_federation.py
class GlobalCacheFederation:
    """Federation-aware cache with geographic replication."""

    async def replicate_across_regions(self, key: CacheKey, value: Any, policy: ReplicationPolicy)
    async def resolve_cache_conflicts(self, conflicting_entries: list[CacheEntry])
    async def optimize_cache_placement(self, access_patterns: dict)
```

---

## Message Queue Protocol

### Overview

MPREG provides comprehensive message queuing capabilities with two operational modes:

1. **Local Message Queues**: SQS-like queuing within a single cluster
2. **Federated Message Queues**: Cross-cluster queuing with federation-aware delivery guarantees

The federated protocol extends the local protocol with additional routing and consensus capabilities.

### Local Message Queue Protocol

#### Basic Queue Operations

##### Queue Creation

```json
{
  "type": "create_queue",
  "payload": {
    "queue_name": "orders-processing",
    "queue_type": "priority",
    "max_size": 10000,
    "default_visibility_timeout_seconds": 30.0,
    "default_acknowledgment_timeout_seconds": 300.0,
    "enable_deduplication": true,
    "deduplication_window_seconds": 300.0,
    "enable_dead_letter_queue": true,
    "max_retries": 3,
    "message_ttl_seconds": 3600.0
  }
}
```

##### Message Send

```json
{
  "type": "send_message",
  "payload": {
    "queue_name": "orders-processing",
    "topic": "order.new",
    "message_payload": {
      "order_id": "ORD-123456",
      "customer_id": "CUST-789",
      "amount": 99.99,
      "items": [{ "sku": "WIDGET-001", "quantity": 2, "price": 49.99 }]
    },
    "delivery_guarantee": "at_least_once",
    "priority": 10,
    "delay_seconds": 0.0,
    "visibility_timeout_seconds": 30.0,
    "max_retries": 3,
    "acknowledgment_timeout_seconds": 300.0,
    "headers": {
      "correlation_id": "corr-123",
      "request_id": "req-456"
    }
  }
}
```

##### Queue Subscription

```json
{
  "type": "subscribe_queue",
  "payload": {
    "queue_name": "orders-processing",
    "subscriber_id": "order-processor-1",
    "topic_pattern": "order.*",
    "auto_acknowledge": true,
    "metadata": {
      "service_name": "order-service",
      "version": "2.1.0"
    }
  }
}
```

##### Message Acknowledgment

```json
{
  "type": "acknowledge_message",
  "payload": {
    "queue_name": "orders-processing",
    "message_id": "msg-550e8400-e29b-41d4-a716-446655440000",
    "subscriber_id": "order-processor-1",
    "success": true
  }
}
```

#### Delivery Guarantees

##### Fire-and-Forget

```json
{
  "type": "message_delivered",
  "payload": {
    "message_id": "msg-660e8400-e29b-41d4-a716-446655440001",
    "topic": "analytics.user_action",
    "message_payload": {
      "user_id": 12345,
      "action": "page_view",
      "page": "/products"
    },
    "delivery_guarantee": "fire_and_forget",
    "delivered_to": ["analytics-processor-1", "analytics-processor-2"],
    "delivery_timestamp": 1703001234.567
  }
}
```

##### At-Least-Once

```json
{
  "type": "message_pending_ack",
  "payload": {
    "message_id": "msg-770e8400-e29b-41d4-a716-446655440002",
    "topic": "payment.process",
    "message_payload": {
      "order_id": "ORD-123456",
      "amount": 99.99,
      "payment_method": "credit_card"
    },
    "delivery_guarantee": "at_least_once",
    "delivered_to": ["payment-processor-1"],
    "acknowledgment_timeout": 1703001534.567,
    "retry_count": 0,
    "max_retries": 3
  }
}
```

##### Broadcast

```json
{
  "type": "broadcast_message",
  "payload": {
    "message_id": "msg-880e8400-e29b-41d4-a716-446655440003",
    "topic": "system.config_update",
    "message_payload": {
      "config_key": "feature_flags",
      "new_value": { "feature_x": true }
    },
    "delivery_guarantee": "broadcast",
    "delivered_to": ["service-1", "service-2", "service-3", "service-4"],
    "acknowledged_by": ["service-1", "service-2", "service-3"],
    "pending_acknowledgments": ["service-4"]
  }
}
```

##### Quorum

```json
{
  "type": "quorum_message",
  "payload": {
    "message_id": "msg-990e8400-e29b-41d4-a716-446655440004",
    "topic": "consensus.leadership_election",
    "message_payload": {
      "candidate_id": "node-5",
      "term": 42,
      "vote_request": true
    },
    "delivery_guarantee": "quorum",
    "delivered_to": ["node-1", "node-2", "node-3", "node-4", "node-5"],
    "acknowledged_by": ["node-1", "node-3", "node-5"],
    "required_acknowledgments": 3,
    "quorum_reached": true
  }
}
```

#### Queue Statistics

```json
{
  "type": "queue_statistics",
  "payload": {
    "queue_name": "orders-processing",
    "messages_sent": 15420,
    "messages_received": 15398,
    "messages_acknowledged": 15390,
    "messages_failed": 8,
    "messages_expired": 0,
    "messages_requeued": 22,
    "current_queue_size": 145,
    "current_in_flight_count": 8,
    "success_rate": 0.9995,
    "average_processing_time_seconds": 2.34
  }
}
```

## Federated Message Queue Protocol

### Overview

MPREG's Federated Message Queue Protocol extends the local protocol to work transparently across federated clusters with strong consistency guarantees:

- **Cross-Cluster Queue Discovery**: Automatic discovery of queues across federation via gossip protocol
- **Federation-Aware Delivery Guarantees**: Fire-and-forget, at-least-once, broadcast, and quorum across clusters
- **Multi-Hop Acknowledgment Routing**: Acknowledgments traverse back through federation paths
- **Byzantine Fault Tolerance**: Consensus operations handle Byzantine faults in federated clusters
- **Causal Consistency**: Vector clock-based message ordering across distributed systems
- **Circuit Breaker Protection**: Graceful degradation during federation failures

### Queue Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Federated Queue Manager                     │
├─────────────────┬─────────────────┬─────────────────┬───────────┤
│  Local Queues   │ Remote Queues   │  Federation     │  Gossip   │
│                 │                 │   Routing       │ Protocol  │
│  ┌─────────────┐│ ┌─────────────┐ │ ┌─────────────┐ │ ┌───────┐ │
│  │ FIFO Queue  ││ │Advertisement│ │ │Multi-hop    │ │ │Queue  │ │
│  │Priority Q.  ││ │  Cache      │ │ │ Routing     │ │ │Ads    │ │
│  │Delay Queue  ││ │Circuit      │ │ │ACK Routing  │ │ │Sync   │ │
│  └─────────────┘│ │Breakers     │ │ │Consensus    │ │ │       │ │
│                 │ └─────────────┘ │ └─────────────┘ │ └───────┘ │
└─────────────────┴─────────────────┴─────────────────┴───────────┘
```

### Message Types

#### Queue Advertisement Message

Distributed via gossip protocol to advertise queue availability:

```json
{
  "type": "queue_advertisement",
  "payload": {
    "advertisement_id": "ad-550e8400-e29b-41d4-a716-446655440000",
    "cluster_id": "us-east-1",
    "queue_name": "orders-processing",
    "supported_guarantees": [
      "fire_and_forget",
      "at_least_once",
      "broadcast",
      "quorum"
    ],
    "current_subscribers": 12,
    "queue_health": "healthy",
    "advertised_at": 1703001234.567,
    "ttl_seconds": 300.0,
    "metadata": {
      "queue_type": "priority",
      "max_size": "50000"
    }
  },
  "vector_clock": {
    "us-east-1": 42,
    "eu-central-1": 38
  },
  "sender_id": "us-east-1",
  "timestamp": 1703001234.567
}
```

#### Federated Message Delivery

Cross-cluster message delivery with federation routing:

```json
{
  "type": "federated_message",
  "payload": {
    "ack_token": "ack-660e8400-e29b-41d4-a716-446655440001",
    "queue_name": "global-payments",
    "message_topic": "payment.process",
    "message_payload": {
      "order_id": "ORD-123456",
      "amount": 99.99,
      "currency": "USD",
      "payment_method": "credit_card"
    },
    "delivery_guarantee": "quorum",
    "source_cluster": "us-west-2",
    "target_cluster": "eu-central-1",
    "federation_path": ["us-west-2", "us-east-1", "eu-central-1"],
    "required_cluster_acks": 3,
    "message_options": {
      "priority": 10,
      "max_retries": 5,
      "acknowledgment_timeout_seconds": 300
    }
  },
  "sender_id": "us-west-2",
  "timestamp": 1703001235.123
}
```

#### Federated Acknowledgment

Acknowledgment routed back through federation path:

```json
{
  "type": "federated_acknowledgment",
  "payload": {
    "ack_token": "ack-660e8400-e29b-41d4-a716-446655440001",
    "message_id": "msg-770e8400-e29b-41d4-a716-446655440002",
    "acknowledging_cluster": "eu-central-1",
    "acknowledging_subscriber": "payment-processor-eu",
    "return_path": ["eu-central-1", "us-east-1", "us-west-2"],
    "vector_clock": {
      "eu-central-1": 156,
      "us-east-1": 89,
      "us-west-2": 203
    },
    "success": true,
    "ack_timestamp": 1703001236.789
  },
  "sender_id": "eu-central-1",
  "timestamp": 1703001236.789
}
```

#### Global Consensus Request

Byzantine fault tolerant consensus for critical operations:

```json
{
  "type": "global_consensus_request",
  "payload": {
    "consensus_id": "consensus-880e8400-e29b-41d4-a716-446655440003",
    "initiating_cluster": "us-east-1",
    "message_id": "msg-990e8400-e29b-41d4-a716-446655440004",
    "message_topic": "payment.critical",
    "message_payload": {
      "transaction_id": "TXN-789012",
      "amount": 50000.0,
      "requires_consensus": true
    },
    "target_clusters": [
      "us-east-1",
      "us-west-2",
      "eu-central-1",
      "ap-southeast-1"
    ],
    "required_weight_threshold": 0.67,
    "byzantine_fault_threshold": 1,
    "timeout_seconds": 120.0,
    "cluster_weights": {
      "us-east-1": { "weight": 2.0, "reliability_score": 0.98 },
      "us-west-2": { "weight": 2.0, "reliability_score": 0.95 },
      "eu-central-1": { "weight": 1.5, "reliability_score": 0.92 },
      "ap-southeast-1": { "weight": 1.0, "reliability_score": 0.88 }
    }
  },
  "sender_id": "us-east-1",
  "timestamp": 1703001237.456
}
```

#### Consensus Vote Response

Response to consensus request with vote and metadata:

```json
{
  "type": "consensus_vote",
  "payload": {
    "consensus_id": "consensus-880e8400-e29b-41d4-a716-446655440003",
    "voting_cluster": "eu-central-1",
    "vote": true,
    "vector_clock": {
      "eu-central-1": 157,
      "us-east-1": 90
    },
    "vote_metadata": {
      "processing_time_ms": 45.2,
      "local_queue_health": "healthy",
      "resource_utilization": 0.72
    },
    "voter_signature": "eu-central-1-vote-hash-abc123"
  },
  "sender_id": "eu-central-1",
  "timestamp": 1703001238.123
}
```

### Delivery Guarantees

#### Fire-and-Forget

Best-effort delivery with no acknowledgment tracking:

```python
# Client sends message
result = await federated_queue_manager.send_message_globally(
    queue_name="analytics-events",
    topic="user.action",
    payload={"user_id": 12345, "action": "page_view"},
    delivery_guarantee=DeliveryGuarantee.FIRE_AND_FORGET
)

# No acknowledgment required - highest throughput
```

#### At-Least-Once

Guaranteed delivery with retry logic and acknowledgment:

```python
# Client sends with acknowledgment requirement
result = await federated_queue_manager.send_message_globally(
    queue_name="order-processing",
    topic="order.new",
    payload={"order_id": "ORD-123", "amount": 99.99},
    delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
    max_retries=5,
    acknowledgment_timeout_seconds=300
)

# System automatically retries on failure until acknowledged
```

#### Broadcast

Delivery to all matching subscribers across federation:

```python
# Broadcast configuration update to all clusters
result = await federated_queue_manager.send_message_globally(
    queue_name="system-config",
    topic="config.update",
    payload={"feature_flags": {"new_feature": True}},
    delivery_guarantee=DeliveryGuarantee.BROADCAST
)

# Delivered to all subscribers in all federated clusters
```

#### Quorum

Requires N acknowledgments for consensus:

```python
# Critical financial transaction requiring majority consensus
result = await delivery_coordinator.deliver_with_global_quorum(
    message=payment_message,
    target_clusters={"us-east", "us-west", "eu-central", "ap-southeast"},
    required_weight_threshold=0.67,  # 67% of total weight
    byzantine_fault_threshold=1,     # Tolerate 1 Byzantine fault
    timeout_seconds=120.0
)

# Only succeeds if 67% of weighted clusters acknowledge
```

### Queue Discovery Protocol

#### Discovery Request

Gossip-based queue discovery across federation:

```json
{
  "type": "queue_discovery_request",
  "payload": {
    "discovery_id": "disc-aa0e8400-e29b-41d4-a716-446655440005",
    "requesting_cluster": "ap-southeast-1",
    "queue_pattern": "orders-*",
    "vector_clock": {
      "ap-southeast-1": 45
    }
  },
  "sender_id": "ap-southeast-1",
  "timestamp": 1703001239.789
}
```

#### Discovery Response

Response with matching queue advertisements:

```json
{
  "type": "queue_discovery_response",
  "payload": {
    "discovery_id": "disc-aa0e8400-e29b-41d4-a716-446655440005",
    "responding_cluster": "us-east-1",
    "matching_queues": [
      {
        "queue_name": "orders-us",
        "cluster_id": "us-east-1",
        "supported_guarantees": ["at_least_once", "broadcast"],
        "current_subscribers": 8,
        "queue_health": "healthy"
      },
      {
        "queue_name": "orders-priority",
        "cluster_id": "us-east-1",
        "supported_guarantees": ["quorum", "at_least_once"],
        "current_subscribers": 3,
        "queue_health": "healthy"
      }
    ]
  },
  "sender_id": "us-east-1",
  "timestamp": 1703001240.123
}
```

### Federation Routing

Messages are automatically routed through the federation using optimal paths:

```
Client (ap-southeast) → Target Queue (eu-central)

Path Discovery:
ap-southeast-1 → us-west-2 → us-east-1 → eu-central-1

Message Flow:
1. Client sends to local federated queue manager
2. Manager discovers target cluster via gossip
3. Message routed through optimal federation path
4. Delivered to target queue in eu-central-1
5. Acknowledgment routed back through same path
```

### Error Handling and Circuit Breakers

#### Federation Failures

```json
{
  "type": "federation_error",
  "payload": {
    "error_code": "CLUSTER_UNREACHABLE",
    "message": "Target cluster eu-central-1 unreachable",
    "failed_cluster": "eu-central-1",
    "alternative_clusters": ["eu-west-1", "eu-north-1"],
    "circuit_breaker_status": "OPEN",
    "retry_after_seconds": 60
  },
  "sender_id": "us-east-1",
  "timestamp": 1703001241.456
}
```

#### Byzantine Fault Detection

```json
{
  "type": "byzantine_fault_detected",
  "payload": {
    "consensus_id": "consensus-880e8400-e29b-41d4-a716-446655440003",
    "suspected_clusters": ["malicious-cluster-1"],
    "evidence": {
      "conflicting_responses": 2,
      "timing_anomalies": true,
      "signature_validation_failed": false
    },
    "action_taken": "EXCLUDE_FROM_CONSENSUS",
    "updated_cluster_weights": {
      "malicious-cluster-1": { "weight": 0.0, "is_trusted": false }
    }
  },
  "sender_id": "us-east-1",
  "timestamp": 1703001242.789
}
```

### Client API Examples

#### Global Queue Subscription

```python
# Subscribe to queues across entire federation
subscription_id = await federated_queue_manager.subscribe_globally(
    subscriber_id="payment-processor",
    queue_pattern="payments-*",  # Matches any payment queue
    topic_pattern="payment.*",   # Matches payment topics
    delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
    target_clusters=None,        # Auto-discover all clusters
    callback=process_payment_message
)
```

#### Cross-Cluster Message Send

```python
# Send message to any cluster hosting target queue
result = await federated_queue_manager.send_message_globally(
    queue_name="inventory-updates",
    topic="inventory.sync",
    payload={"sku": "WIDGET-001", "quantity": 500},
    delivery_guarantee=DeliveryGuarantee.BROADCAST,
    target_cluster=None  # Auto-discover optimal cluster
)
```

#### Federated Acknowledgment

```python
# Acknowledge message received via federation
success = await federated_queue_manager.acknowledge_federated_message(
    ack_token="ack-660e8400-e29b-41d4-a716-446655440001",
    subscriber_id="inventory-manager",
    success=True
)
```

### Performance Characteristics

- **Queue Discovery**: O(log N) convergence via epidemic gossip
- **Message Routing**: Optimal path selection through federation graph
- **Consensus Operations**: Byzantine fault tolerant with configurable thresholds
- **Throughput**: 100K+ messages/second per cluster with federation overhead <10%
- **Latency**: Cross-cluster delivery <100ms for single-hop, <300ms for multi-hop
- **Availability**: 99.99% uptime with graceful degradation during federation failures

---

## Security Model

### Authentication Methods

#### Bearer Token

```http
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

#### API Key

```http
X-API-Key: mpreg_api_key_abc123...
```

#### Client Certificate (mTLS)

```json
{
  "tls_config": {
    "client_cert": "/path/to/client.crt",
    "client_key": "/path/to/client.key",
    "ca_cert": "/path/to/ca.crt",
    "verify_hostname": true
  }
}
```

### Message Security

#### Digital Signatures

```json
{
  "role": "rpc",
  "u": "req-12345",
  "cmds": [...],
  "signature": {
    "algorithm": "RS256",
    "signature": "base64-encoded-signature",
    "public_key_id": "key-abc123",
    "timestamp": 1640995200.123
  }
}
```

#### Message Encryption

For sensitive payloads:

```json
{
  "role": "rpc",
  "u": "req-12345",
  "encrypted": true,
  "encryption": {
    "algorithm": "AES-256-GCM",
    "key_id": "encryption-key-123",
    "iv": "base64-iv",
    "tag": "base64-auth-tag"
  },
  "payload": "base64-encrypted-data"
}
```

---

## Error Handling

### Error Response Format

```json
{
  "role": "error-response",
  "u": "req-12345",
  "error": {
    "type": "TransportError",
    "code": "CONNECTION_TIMEOUT",
    "message": "Connection timeout after 30 seconds",
    "details": {
      "timeout_seconds": 30,
      "attempted_host": "cluster-node-1",
      "retry_count": 3
    },
    "timestamp": 1640995200.123,
    "request_id": "req-12345"
  }
}
```

### Error Categories

| Code           | Category       | Description               |
| -------------- | -------------- | ------------------------- |
| `TRANSPORT_*`  | Transport      | Network/connection errors |
| `AUTH_*`       | Authentication | Authentication failures   |
| `PERMISSION_*` | Authorization  | Permission denied         |
| `PROTOCOL_*`   | Protocol       | Message format errors     |
| `TIMEOUT_*`    | Timeout        | Operation timeouts        |
| `RESOURCE_*`   | Resource       | Resource constraints      |
| `FEDERATION_*` | Federation     | Cross-cluster errors      |
| `CACHE_*`      | Caching        | Cache operation errors    |

### Retry Logic

```json
{
  "retry_policy": {
    "max_retries": 3,
    "base_delay_ms": 1000,
    "max_delay_ms": 30000,
    "backoff_multiplier": 2.0,
    "jitter": true,
    "retryable_errors": [
      "CONNECTION_TIMEOUT",
      "NETWORK_ERROR",
      "SERVICE_UNAVAILABLE"
    ]
  }
}
```

---

## Client Implementation Guide

### Python Client Example

```python
import asyncio
import json
import websockets
from typing import Any, Dict

class MPREGClient:
    def __init__(self, url: str, auth_token: str = None):
        self.url = url
        self.auth_token = auth_token
        self.websocket = None
        self.request_id = 0

    async def connect(self):
        headers = {}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        self.websocket = await websockets.connect(
            self.url,
            extra_headers=headers
        )

    async def call(self, function: str, *args, **kwargs) -> Any:
        request_id = f"req-{self.request_id}"
        self.request_id += 1

        message = {
            "role": "rpc",
            "u": request_id,
            "cmds": [{
                "name": "call",
                "fun": function,
                "args": args,
                "kwargs": kwargs,
                "locs": []
            }]
        }

        await self.websocket.send(json.dumps(message))

        response = await self.websocket.recv()
        data = json.loads(response)

        if data.get("error"):
            raise Exception(f"RPC Error: {data['error']}")

        return data.get("r")

    async def publish(self, topic: str, payload: Any, **headers) -> None:
        request_id = f"pub-{self.request_id}"
        self.request_id += 1

        message = {
            "role": "pubsub-publish",
            "u": request_id,
            "message": {
                "topic": topic,
                "payload": payload,
                "timestamp": time.time(),
                "message_id": f"msg-{uuid.uuid4()}",
                "publisher": "python-client",
                "headers": headers
            }
        }

        await self.websocket.send(json.dumps(message))

    async def subscribe(self, patterns: list[str], callback) -> str:
        subscription_id = f"sub-{uuid.uuid4()}"
        request_id = f"sub-{self.request_id}"
        self.request_id += 1

        message = {
            "role": "pubsub-subscribe",
            "u": request_id,
            "subscription": {
                "subscription_id": subscription_id,
                "patterns": [
                    {"pattern": p, "exact_match": False}
                    for p in patterns
                ],
                "subscriber": "python-client",
                "created_at": time.time(),
                "get_backlog": True,
                "backlog_seconds": 300
            }
        }

        await self.websocket.send(json.dumps(message))

        # Handle notifications in background
        asyncio.create_task(self._notification_handler(callback))

        return subscription_id

    async def _notification_handler(self, callback):
        async for message in self.websocket:
            data = json.loads(message)
            if data.get("role") == "pubsub-notification":
                await callback(data["message"])

# Usage example
async def main():
    client = MPREGClient("ws://localhost:6666", "your-auth-token")
    await client.connect()

    # RPC call
    result = await client.call("compute_fibonacci", 10)
    print(f"Fibonacci(10) = {result}")

    # Pub/Sub
    await client.publish("events.user.login", {
        "user_id": "user123",
        "timestamp": time.time()
    })

    def handle_notification(message):
        print(f"Received: {message}")

    await client.subscribe(["events.user.*"], handle_notification)

    # Keep connection alive
    await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
```

### Go Client Example

```go
package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/url"
    "time"

    "github.com/gorilla/websocket"
)

type MPREGClient struct {
    conn      *websocket.Conn
    requestID int
}

type RPCRequest struct {
    Role string      `json:"role"`
    U    string      `json:"u"`
    Cmds []RPCCommand `json:"cmds"`
}

type RPCCommand struct {
    Name   string                 `json:"name"`
    Fun    string                 `json:"fun"`
    Args   []interface{}          `json:"args"`
    Kwargs map[string]interface{} `json:"kwargs"`
    Locs   []string               `json:"locs"`
}

type RPCResponse struct {
    Role  string      `json:"role"`
    U     string      `json:"u"`
    R     interface{} `json:"r"`
    Error interface{} `json:"error"`
}

func NewMPREGClient(urlStr, authToken string) (*MPREGClient, error) {
    u, err := url.Parse(urlStr)
    if err != nil {
        return nil, err
    }

    headers := make(map[string][]string)
    if authToken != "" {
        headers["Authorization"] = []string{"Bearer " + authToken}
    }

    conn, _, err := websocket.DefaultDialer.Dial(u.String(), headers)
    if err != nil {
        return nil, err
    }

    return &MPREGClient{
        conn:      conn,
        requestID: 0,
    }, nil
}

func (c *MPREGClient) Call(function string, args ...interface{}) (interface{}, error) {
    c.requestID++
    requestID := fmt.Sprintf("req-%d", c.requestID)

    request := RPCRequest{
        Role: "rpc",
        U:    requestID,
        Cmds: []RPCCommand{{
            Name:   "call",
            Fun:    function,
            Args:   args,
            Kwargs: make(map[string]interface{}),
            Locs:   []string{},
        }},
    }

    if err := c.conn.WriteJSON(request); err != nil {
        return nil, err
    }

    var response RPCResponse
    if err := c.conn.ReadJSON(&response); err != nil {
        return nil, err
    }

    if response.Error != nil {
        return nil, fmt.Errorf("RPC Error: %v", response.Error)
    }

    return response.R, nil
}

func main() {
    client, err := NewMPREGClient("ws://localhost:6666", "your-auth-token")
    if err != nil {
        log.Fatal(err)
    }
    defer client.conn.Close()

    // Call remote function
    result, err := client.Call("compute_fibonacci", 10)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Fibonacci(10) = %v\n", result)
}
```

### JavaScript Client Example

```javascript
class MPREGClient {
  constructor(url, authToken = null) {
    this.url = url;
    this.authToken = authToken;
    this.websocket = null;
    this.requestId = 0;
    this.pendingRequests = new Map();
  }

  async connect() {
    return new Promise((resolve, reject) => {
      this.websocket = new WebSocket(this.url, [], {
        headers: this.authToken
          ? {
              Authorization: `Bearer ${this.authToken}`,
            }
          : {},
      });

      this.websocket.onopen = () => resolve();
      this.websocket.onerror = (error) => reject(error);
      this.websocket.onmessage = (event) => this.handleMessage(event);
    });
  }

  async call(functionName, ...args) {
    const requestId = `req-${++this.requestId}`;

    const message = {
      role: "rpc",
      u: requestId,
      cmds: [
        {
          name: "call",
          fun: functionName,
          args: args,
          kwargs: {},
          locs: [],
        },
      ],
    };

    return new Promise((resolve, reject) => {
      this.pendingRequests.set(requestId, { resolve, reject });
      this.websocket.send(JSON.stringify(message));
    });
  }

  async publish(topic, payload, headers = {}) {
    const requestId = `pub-${++this.requestId}`;

    const message = {
      role: "pubsub-publish",
      u: requestId,
      message: {
        topic: topic,
        payload: payload,
        timestamp: Date.now() / 1000,
        message_id: `msg-${Math.random().toString(36).substr(2, 9)}`,
        publisher: "javascript-client",
        headers: headers,
      },
    };

    this.websocket.send(JSON.stringify(message));
  }

  async subscribe(patterns, callback) {
    const subscriptionId = `sub-${Math.random().toString(36).substr(2, 9)}`;
    const requestId = `sub-${++this.requestId}`;

    const message = {
      role: "pubsub-subscribe",
      u: requestId,
      subscription: {
        subscription_id: subscriptionId,
        patterns: patterns.map((p) => ({ pattern: p, exact_match: false })),
        subscriber: "javascript-client",
        created_at: Date.now() / 1000,
        get_backlog: true,
        backlog_seconds: 300,
      },
    };

    this.websocket.send(JSON.stringify(message));
    this.subscriptionCallbacks = this.subscriptionCallbacks || new Map();
    this.subscriptionCallbacks.set(subscriptionId, callback);

    return subscriptionId;
  }

  handleMessage(event) {
    const data = JSON.parse(event.data);

    if (data.role === "rpc-response") {
      const pending = this.pendingRequests.get(data.u);
      if (pending) {
        this.pendingRequests.delete(data.u);
        if (data.error) {
          pending.reject(new Error(`RPC Error: ${JSON.stringify(data.error)}`));
        } else {
          pending.resolve(data.r);
        }
      }
    } else if (data.role === "pubsub-notification") {
      const callback = this.subscriptionCallbacks?.get(data.subscription_id);
      if (callback) {
        callback(data.message);
      }
    }
  }
}

// Usage example
async function main() {
  const client = new MPREGClient("ws://localhost:6666", "your-auth-token");
  await client.connect();

  // RPC call
  try {
    const result = await client.call("compute_fibonacci", 10);
    console.log(`Fibonacci(10) = ${result}`);
  } catch (error) {
    console.error("RPC Error:", error);
  }

  // Pub/Sub
  await client.publish("events.user.login", {
    user_id: "user123",
    timestamp: Date.now() / 1000,
  });

  await client.subscribe(["events.user.*"], (message) => {
    console.log("Received:", message);
  });
}

main().catch(console.error);
```

---

## Code References

### Core Components

| Component         | File                                    | Purpose                                 |
| ----------------- | --------------------------------------- | --------------------------------------- |
| Message Models    | `mpreg/core/model.py`                   | All message type definitions            |
| Transport Layer   | `mpreg/core/transport/`                 | WebSocket/TCP transport implementations |
| Topic Exchange    | `mpreg/core/topic_exchange.py`          | Pub/Sub message routing                 |
| Gossip Protocol   | `mpreg/federation/federation_gossip.py` | State synchronization                   |
| Federation Bridge | `mpreg/federation/federation_bridge.py` | Cross-cluster communication             |
| Federation Graph  | `mpreg/federation/federation_graph.py`  | Graph-based routing                     |
| Statistics        | `mpreg/core/statistics.py`              | Metrics collection                      |
| Caching           | `mpreg/core/caching.py`                 | Multi-tier cache implementation         |

### Client Components

| Component      | File                            | Purpose            |
| -------------- | ------------------------------- | ------------------ |
| RPC Client     | `mpreg/client/client.py`        | RPC communication  |
| API Client     | `mpreg/client/client_api.py`    | High-level API     |
| Pub/Sub Client | `mpreg/client/pubsub_client.py` | Pub/Sub operations |

### Server Components

| Component          | File                                | Purpose                        |
| ------------------ | ----------------------------------- | ------------------------------ |
| MPREG Server       | `mpreg/server.py`                   | Main server implementation     |
| Message Processing | `mpreg/server.py:message_handler()` | Message routing and processing |

---

This comprehensive protocol specification provides complete coverage of all MPREG communication patterns and includes the proposed global distributed caching system. External developers can use this specification to implement compatible clients in any programming language, while the code references point to specific implementation details within the MPREG codebase.
