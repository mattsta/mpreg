# MPREG Protocol Specification v2.0

Note: Example endpoints in this document use placeholder ports. For live runs,
allocate ports dynamically (for example, via `allocate_port("servers")`) and
propagate the chosen URLs to clients.

## Table of Contents

1. [Overview](#overview)
2. [Transport Layer](#transport-layer)
3. [Message Format](#message-format)
4. [RPC Protocol](#rpc-protocol)
5. [Pub/Sub Protocol](#pubsub-protocol)
6. [Fabric Gossip Protocol](#fabric-gossip-protocol)
7. [Fabric Routing and Federation](#fabric-routing-and-federation)
8. [Metrics Protocol](#metrics-protocol)
9. [Fabric Cache Protocol](#fabric-cache-protocol)
10. [Fabric Queue Protocol](#fabric-queue-protocol)
11. [Security Model](#security-model)
12. [Error Handling](#error-handling)
13. [Client Implementation Guide](#client-implementation-guide)
14. [Code References](#code-references)

---

## Overview

MPREG (Multi-Provider REGistry) is a distributed computing platform that provides:

- **Dependency-Resolving RPC**: Function calls with topological execution across clusters
- **Topic-Based Pub/Sub**: High-performance messaging with pattern matching
- **Fabric Queues**: SQS-like queuing with cross-cluster delivery guarantees
- **Fabric Control Plane**: Gossip + catalog propagation for discovery and membership
- **Path-Vector Routing**: Multi-hop routing across federated clusters
- **Distributed Caching**: Multi-tier caching with intelligent eviction
- **Real-Time Metrics**: Comprehensive performance and health monitoring

### Protocol Stack

```
┌────────────────────────────────────────────────────────┐
│  Application Layer (RPC, Pub/Sub, Queue, Cache)        │
├────────────────────────────────────────────────────────┤
│  Fabric Control Plane (Catalog + Route Control + Policy)│
├────────────────────────────────────────────────────────┤
│  Fabric Gossip Plane (Catalog/Route/Membership Deltas) │
├────────────────────────────────────────────────────────┤
│  Unified Message Envelope (UnifiedMessage + Headers)   │
├────────────────────────────────────────────────────────┤
│  Transport Layer (WebSocket, TCP, TLS)                 │
└────────────────────────────────────────────────────────┘
```

---

## Transport Layer

### Supported Transports

| Protocol         | URL Scheme | Default Port | Use Case          | Max Message Size |
| ---------------- | ---------- | ------------ | ----------------- | ---------------- |
| WebSocket        | `ws://`    | `<port>`     | Real-time clients | 20MB             |
| WebSocket Secure | `wss://`   | `<port>`     | Secure real-time  | 20MB             |
| TCP              | `tcp://`   | `<port>`     | High-performance  | 20MB             |
| TCP Secure       | `tcps://`  | `<port>`     | Secure high-perf  | 20MB             |

### Connection Types

- **CLIENT**: User-facing connections (20MB/100MB limits)
- **INTERNAL**: Node-to-node connections (5MB/20MB limits)

### Transport Features

- **Multi-Protocol Adapters**: Multiple transports active simultaneously
- **Streaming Support**: Large data transfers via chunked streaming
- **Connection Pooling**: Efficient connection reuse
- **Circuit Breakers**: Automatic fault tolerance
- **Auto Port Assignment**: `base_port=0` uses the port allocator and reports
  assigned endpoints via `port_assignment_callback`.

Example (auto-assigned ports):

```python
from mpreg.core.transport.factory import (
    MultiProtocolAdapter,
    MultiProtocolAdapterConfig,
)
from mpreg.core.transport.interfaces import TransportProtocol

assigned = []

def on_assign(assignment):
    assigned.append(assignment)
    print(f"{assignment.protocol.value} -> {assignment.endpoint}")

config = MultiProtocolAdapterConfig(
    base_port=0,
    port_assignment_callback=on_assign,
)
adapter = MultiProtocolAdapter(config)
await adapter.start([TransportProtocol.WEBSOCKET])
```

---

## Message Format

### Base Message Structures

MPREG uses two canonical envelope shapes:

1. **Client/Server envelopes** (RPC, pub/sub, cache):

```json
{
  "role": "message-type",
  "u": "unique-request-id",
  "timestamp": 1640995200.123,
  "version": "2.0",
  "...": "type-specific-fields"
}
```

2. **Fabric envelopes** (node-to-node control/data):

```json
{
  "role": "fabric-message",
  "payload": {
    "message_id": "msg-123",
    "topic": "mpreg.rpc.execute.compute_fibonacci",
    "message_type": "rpc",
    "delivery": "at_least_once",
    "payload": { "...": "message-specific data" },
    "headers": { "...": "routing headers" },
    "timestamp": 1700000000.0
  }
}
```

#### Core Fields (Client/Server)

- `role`: Message type identifier (required)
- `u`: Unique request/correlation ID (required)
- `timestamp`: Unix timestamp with microseconds (optional)
- `version`: Protocol version (optional, defaults to "2.0")

### Message Types

| Role                  | Purpose                                | Direction           |
| --------------------- | -------------------------------------- | ------------------- |
| `rpc`                 | Function call request                  | Client → Server     |
| `rpc-response`        | Function call response                 | Server → Client     |
| `server`              | Server lifecycle/status events         | Node → Node         |
| `fabric-message`      | Unified fabric envelope (data/control) | Node → Node         |
| `fabric-gossip`       | Fabric gossip message envelope         | Node → Node         |
| `pubsub-publish`      | Publish message                        | Client → Server     |
| `pubsub-subscribe`    | Subscribe to topics                    | Client → Server     |
| `pubsub-unsubscribe`  | Unsubscribe from topics                | Client → Server     |
| `pubsub-notification` | Message delivery                       | Server → Client     |
| `pubsub-ack`          | Pub/Sub acknowledgment                 | Client → Server     |
| `consensus-proposal`  | Consensus proposal broadcast           | Node → Node         |
| `consensus-vote`      | Consensus vote broadcast               | Node → Node         |
| `metrics`             | Metrics reporting                      | Component → Monitor |
| `cache-request`       | Cache operation                        | Client → Cache      |
| `cache-response`      | Cache operation result                 | Cache → Client      |

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

### Fabric RPC (UnifiedMessage)

**Message Type**: `fabric-message` (UnifiedMessage + FabricRPCRequest payload)

Used for node-to-node RPC forwarding within and across clusters. Routing uses
fabric headers (`routing_path`, `hop_budget`) and federation path tracking.

```json
{
  "role": "fabric-message",
  "payload": {
    "message_id": "req-12345",
    "topic": "mpreg.rpc.execute.execute_distributed_task",
    "message_type": "rpc",
    "delivery": "at_least_once",
    "payload": {
      "kind": "rpc-request",
      "request_id": "req-12345",
      "command": "execute_distributed_task",
      "args": ["param1", "param2"],
      "kwargs": { "option": "value" },
      "resources": ["gpu", "region-us-east"],
      "function_id": "func-exec-task",
      "version_constraint": ">=1.0.0,<2.0.0",
      "target_cluster": "cluster-b",
      "target_node": "ws://node-b:<port>",
      "reply_to": "ws://node-a:<port>",
      "federation_path": ["cluster-a"],
      "federation_remaining_hops": 3
    },
    "headers": {
      "correlation_id": "req-12345",
      "source_cluster": "cluster-a",
      "routing_path": ["ws://node-a:<port>"],
      "hop_budget": 3
    },
    "timestamp": 1700000000.0
  }
}
```

### UnifiedMessage Envelope

The routing fabric uses a single canonical envelope for all node-to-node traffic.
The schema is defined in `mpreg/fabric/message.py` and is summarized here.

- `message_id`: Unique message identifier (string).
- `topic`: Routing topic (string).
- `message_type`: `rpc` | `pubsub` | `queue` | `cache` | `control` | `data`.
- `delivery`: `fire_and_forget` | `at_least_once` | `exactly_once` | `broadcast` | `quorum`.
- `payload`: Message-specific payload (object).
- `headers`: Routing headers (object, see below).
- `timestamp`: Unix timestamp (float).

#### Routing Headers

- `correlation_id`: Correlates request/response chains (string).
- `source_cluster`: Originating cluster id (string, optional).
- `target_cluster`: Intended target cluster id (string, optional).
- `routing_path`: Node ids traversed so far (list of strings).
- `federation_path`: Cluster ids traversed so far (list of strings).
- `hop_budget`: Remaining hop budget (int, optional).
- `priority`: `critical` | `high` | `normal` | `low` | `bulk`.
- `metadata`: Free-form metadata (object).

### Topic Taxonomy (Canonical Namespaces)

The platform uses a stable set of topic namespaces. The detailed patterns and
examples live in `mpreg/core/topic_taxonomy.py` and should be treated as the
source of truth.

- `mpreg.rpc.*`: RPC execution, progress, and response events.
  - Example: `mpreg.rpc.execute.compute_fibonacci`
  - Example: `mpreg.rpc.response`
- `mpreg.queue.*`: Queue delivery, acknowledgments, and consensus.
  - Example: `mpreg.queue.orders`
  - Example: `mpreg.queue.ack.orders`
  - Example: `mpreg.queue.consensus.request`
- `mpreg.pubsub.*`: Pub/Sub topics and subscription lifecycle.
  - Example: `mpreg.pubsub.metrics.cpu`
- `mpreg.cache.*`: Cache federation, sync, invalidation, and analytics.
  - Example: `mpreg.cache.sync.operation`
  - Example: `mpreg.cache.invalidation.user_data.user_123`
- `mpreg.fabric.*`: Control plane, routing, and cluster-level events.
  - Example: `mpreg.fabric.route.discovered.west_coast`
  - Example: `mpreg.fabric.cluster.west_coast.join`
  - Example: `mpreg.fabric.raft.rpc`

### Server Lifecycle Messages

#### Status Message

```json
{
  "what": "STATUS",
  "server_url": "ws://node1:<port>",
  "cluster_id": "cluster-abc123",
  "status": "ok",
  "active_clients": 42,
  "peer_count": 5,
  "funs": ["function1", "function2"],
  "locs": ["location1", "location2"],
  "advertised_urls": ["ws://node1:<port>"]
}
```

Function discovery is propagated via fabric catalog gossip (`CATALOG_UPDATE`),
not through server lifecycle messages.

#### Goodbye Message

```json
{
  "what": "GOODBYE",
  "departing_node_url": "ws://node1:<port>",
  "cluster_id": "cluster-abc123",
  "reason": "graceful_shutdown",
  "timestamp": 1700000000.0
}
```

### Fabric Routing Extensions

- **Enablement**: `fabric_routing_enabled` toggles the fabric routing plane.
- **Identity**: RPCs can specify `function_id` and semantic `version_constraint`.
- **Name uniqueness**: each function name maps to a single `function_id`.
- **Resources**: `locs` must be a subset of advertised resources for a route.
- **Hop budget**: `hop_budget` limits forwards; `routing_path` prevents loops.
- **Federation security**: cross-cluster advertisements are filtered by
  federation allow/block lists.

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

## Fabric Gossip Protocol

The fabric control plane uses gossip to disseminate catalog deltas, route
announcements, membership events, and consensus messages. Gossip always travels
in the `fabric-gossip` envelope; the payload is a serialized `GossipMessage`.

### Fabric Gossip Envelope

**Message Type**: `fabric-gossip`

```json
{
  "role": "fabric-gossip",
  "payload": {
    "message_id": "gossip-abc123",
    "message_type": "catalog_update",
    "sender_id": "node-1",
    "payload": {
      "delta": {
        "additions": ["function endpoint(s)"],
        "removals": [],
        "timestamp": 1700000000.0
      }
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
    "created_at": 1640995200.123,
    "expires_at": 1640995500.123,
    "propagation_path": ["node-1"],
    "seen_by": ["node-1", "node-3"]
  }
}
```

### Message Types

| Type                        | Purpose                     | Payload                  |
| --------------------------- | --------------------------- | ------------------------ |
| `state_update`              | State synchronization       | Key-value with version   |
| `membership_update`         | Node join/leave/update      | Node info and event type |
| `config_update`             | Configuration changes       | Config key-value         |
| `catalog_update`            | Routing catalog delta       | `RoutingCatalogDelta`    |
| `route_advertisement`       | Path-vector route update    | `RouteAnnouncement`      |
| `route_withdrawal`          | Path-vector route removal   | `RouteWithdrawal`        |
| `link_state_update`         | Link-state adjacency update | `LinkStateUpdate`        |
| `heartbeat`                 | Liveness indication         | Node health metrics      |
| `anti_entropy`              | State reconciliation        | State digest             |
| `rumor`                     | Information propagation     | Generic data             |
| `consensus_proposal`        | Distributed consensus       | Proposal data            |
| `consensus_vote`            | Consensus voting            | Vote information         |
| `membership_probe`          | Node health check           | Probe request            |
| `membership_ack`            | Probe acknowledgment        | Health response          |
| `membership_indirect_probe` | Indirect health probe       | Relay probe              |

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

- **Generation**: `mpreg/fabric/gossip.py:_perform_gossip_cycle()`
- **Processing**: `mpreg/fabric/gossip.py:_handle_*_update()`
- **Models**: `mpreg/fabric/gossip.py:GossipMessage`

---

## Fabric Routing and Federation

### Fabric Routing Overview

The routing fabric provides a unified control plane for all cross-node traffic
(RPC, pub/sub, queues, cache). Discovery is catalog-driven, and routing decisions
are made using a path-vector route table with policy scoring.

### Routing Catalog NodeDescriptor

`NodeDescriptor` entries describe peer nodes for discovery and connection setup.
They include advertised transport endpoints so peers can select the best
internal connection target when multiple protocols are available.

```json
{
  "node_id": "ws://127.0.0.1:12000",
  "cluster_id": "cluster-a",
  "resources": ["cpu", "gpu"],
  "capabilities": ["rpc", "cache"],
  "transport_endpoints": [
    {
      "connection_type": "internal",
      "protocol": "ws",
      "host": "127.0.0.1",
      "port": 12000,
      "endpoint": "ws://127.0.0.1:12000"
    }
  ],
  "advertised_at": 1700000000.0,
  "ttl_seconds": 30.0
}
```

Endpoint selection rules (peer connections):

- Prefer `connection_type=internal` endpoints.
- Prefer secure protocols when available (`wss`/`tcps`), otherwise `ws`/`tcp`.
- Fall back to `node_id` if no endpoints are advertised.

### Route Control (Path-Vector)

Route announcements are distributed via gossip and stored in a local route
table. Each route is a path-vector record with TTL and quality metrics.
Announcements may include optional `route_tags` for policy filtering and
signature fields (`signature`, `public_key`, `signature_algorithm`) when
route signing is enabled.

```json
{
  "destination": { "cluster_id": "cluster-b" },
  "path": { "hops": ["cluster-a", "cluster-b"] },
  "metrics": {
    "hop_count": 1,
    "latency_ms": 18.4,
    "bandwidth_mbps": 1000,
    "reliability_score": 0.995,
    "cost_score": 0.3
  },
  "advertiser": "cluster-a",
  "advertised_at": 1700000000.0,
  "ttl_seconds": 30.0,
  "epoch": 3,
  "route_tags": ["gold", "low-latency"],
  "signature": "deadbeef...",
  "public_key": "cafebabe...",
  "signature_algorithm": "ed25519"
}
```

Notes:

- When a route key registry is configured, verifiers prioritize registry keys
  and use the payload `public_key` only as a fallback.
- Key rotation is supported by overlapping old/new public keys during a grace
  window; both keys can validate the same announcement stream during rollover.

#### Route Withdrawal

Withdrawals are broadcast when a previously advertised path is no longer
reachable. Withdrawals carry the path being removed along with optional
signature metadata when route signing is enabled.

```json
{
  "destination": { "cluster_id": "cluster-b" },
  "path": { "hops": ["cluster-a", "cluster-b"] },
  "advertiser": "cluster-a",
  "withdrawn_at": 1700000010.0,
  "epoch": 4,
  "route_tags": ["gold", "low-latency"],
  "signature": "deadbeef...",
  "public_key": "cafebabe...",
  "signature_algorithm": "ed25519"
}
```

#### Link-State Updates (Optional)

Link-state mode is an optional control-plane feature that advertises each
cluster's direct neighbor set (adjacency list). When enabled, link-state
updates are gossiped through the fabric and converted into a global graph for
shortest-path routing.

```json
{
  "origin": "cluster-a",
  "area": "area-a",
  "neighbors": [
    {
      "cluster_id": "cluster-b",
      "latency_ms": 4.0,
      "bandwidth_mbps": 1000,
      "reliability_score": 0.99,
      "cost_score": 0.1
    }
  ],
  "advertised_at": 1700000020.0,
  "ttl_seconds": 30.0,
  "sequence": 12
}
```

The optional `area` field scopes link-state updates to a named area. When
configured, only updates from the same area are accepted and used for routing.

Link-state routing is disabled by default. Enable it with:

```python
settings = MPREGSettings(
    fabric_link_state_mode=LinkStateMode.PREFER,
    fabric_link_state_ttl_seconds=30.0,
    fabric_link_state_announce_interval_seconds=10.0,
    fabric_link_state_ecmp_paths=1,
    fabric_link_state_area=None,
    fabric_link_state_area_policy=None,
)
```

### UnifiedMessage Routing Headers

The fabric uses routing headers on every `fabric-message` envelope:

- `source_cluster`: Originating cluster id.
- `target_cluster`: Intended destination cluster id.
- `routing_path`: Node ids traversed (prevents node-level loops).
- `federation_path`: Cluster ids traversed (prevents cluster-level loops).
- `hop_budget`: Remaining hop budget for forwarding.
- `priority`: Routing priority for policy selection.

### Routing Flow (All Systems)

1. **Resolve candidates** from the routing catalog using function identity,
   version constraints, queue/topic names, and resource filters.
2. **Select a route** from the path-vector table using policy weights.
3. **Forward** using a `fabric-message` envelope and update routing headers.
4. **Execute locally** when a matching endpoint exists on the current node.
5. **Return replies** using the recorded `routing_path`, falling back to the
   route table if a direct hop is unavailable.

#### Deterministic Tie-Breakers (Example)

Route selection first uses weighted metrics. When multiple routes produce the
same score, deterministic tie-breakers are applied in order to keep selection
stable across nodes. The default order is:

1. Lowest `hop_count`
2. Lowest `latency_ms`
3. Highest `reliability_score`
4. Stable `advertiser` ordering

Example: two routes tie on score, both with `hop_count=1`, but one has
`latency_ms=15` and the other `latency_ms=40`. The lower-latency route wins.
If both latency and reliability are equal, the `advertiser` id is used to
break the tie deterministically.

### Failure Handling

- **TTL expiry** removes stale routes and catalog entries.
- **Hop budgets** bound propagation and prevent runaway forwarding.
- **Path tracking** (`routing_path`, `federation_path`) prevents loops.
- **Withdrawals** remove invalid paths before TTL expiry.
- **Hold-down/suppression** dampens unstable routes when enabled.

### Code References

- **Routing core**: `mpreg/fabric/router.py` + `mpreg/fabric/route_control.py`
- **Announcements**: `mpreg/fabric/route_announcer.py`
- **Transport**: `mpreg/fabric/server_transport.py`

---

## Metrics Protocol

### Metrics Collection

**Message Type**: `metrics`

Metrics payloads use `federation_*` field names to report fabric federation
health and routing performance (cross-cluster latency, throughput, error rate).

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

Route control metrics are included in server status payloads under
`route_metrics` when the fabric control plane is enabled. The payload includes:

- `routes_active_total`: total non-expired routes stored locally.
- `destinations_tracked`: number of destinations with active routes.
- `routes_per_destination`: map of destination cluster id to route count.
- `convergence_seconds_avg`: average convergence duration per destination.
- `convergence_seconds_max`: maximum convergence duration observed.
- `hold_down_rejects`, `suppression_rejects`, `withdrawals_*` counters.

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
- **Processing**: `mpreg/fabric/federation_graph_monitor.py:collect_metrics()`
- **Models**: `mpreg/core/statistics.py:FederationPerformanceMetrics`

---

## Fabric Cache Protocol

### Cache Architecture

MPREG implements a sophisticated multi-tier caching system:

```
┌─────────────────────────────────────────────────┐
│  L1: Memory Cache (S4LRU, Cost-Based Eviction) │
├─────────────────────────────────────────────────┤
│  L2: Persistent Cache (SSD/NVMe Storage)       │
├─────────────────────────────────────────────────┤
│  L3: Distributed Cache (Fabric Gossip)        │
├─────────────────────────────────────────────────┤
│  L4: Federation Cache (Fabric Replication)    │
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

#### Cache Sync (Fabric)

Cache state is synchronized over the routing fabric using the unified envelope:

```json
{
  "role": "fabric-message",
  "payload": {
    "message_id": "cache-op-123",
    "topic": "mpreg.cache.sync.operation",
    "message_type": "cache",
    "delivery": "at_least_once",
    "payload": {
      "kind": "cache_operation",
      "operation": {
        "operation_type": "put",
        "key": {...},
        "value": {...},
        "metadata": {...},
        "vector_clock": {...}
      }
    },
    "headers": {
      "correlation_id": "cache-op-123",
      "source_cluster": "cluster-a",
      "routing_path": ["ws://cluster-a/node-1"],
      "hop_budget": 4
    },
    "timestamp": 1700000000.0
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
    """Multi-tier global cache with fabric-based L3 sync."""

    async def get(self, key: CacheKey, options: CacheOptions = None) -> CacheEntry | None
    async def put(self, key: CacheKey, value: Any, metadata: CacheMetadata = None) -> bool
    async def delete(self, key: CacheKey, options: CacheOptions = None) -> bool
    async def invalidate(self, pattern: str, options: CacheOptions = None) -> int

# mpreg/fabric/cache_federation.py
class FabricCacheProtocol:
    """Fabric cache protocol for L3/L4 distributed sync."""

    async def propagate_cache_operation(self, operation_type: CacheOperationType, key: CacheKey, ...)
    async def handle_cache_message(self, message: CacheOperationMessage) -> bool
    async def sync_cache_state(self, peer_node: str) -> bool

# mpreg/fabric/cache_transport.py
class CacheTransport:
    """Transport abstraction for cache federation."""

    async def send_operation(self, peer_id: str, message: CacheOperationMessage) -> bool
    async def fetch_digest(self, peer_id: str) -> CacheDigest | None
    async def fetch_entry(self, peer_id: str, key: CacheKey) -> CacheEntry | None

```

---

## Fabric Queue Protocol

### Overview

MPREG provides comprehensive message queuing capabilities with two operational modes:

1. **Local Message Queues**: SQS-like queuing within a single cluster
2. **Fabric Queue Federation**: Cross-cluster queuing with fabric-aware delivery guarantees

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

### Delivery Guarantees

#### Fire-and-Forget

Best-effort delivery with no acknowledgment tracking:

```python
result = await fabric_queue_federation.send_message_globally(
    queue_name="analytics-events",
    topic="user.action",
    payload={"user_id": 12345, "action": "page_view"},
    delivery_guarantee=DeliveryGuarantee.FIRE_AND_FORGET,
)
```

#### At-Least-Once

Guaranteed delivery with acknowledgment:

```python
result = await fabric_queue_federation.send_message_globally(
    queue_name="order-processing",
    topic="order.new",
    payload={"order_id": "ORD-123", "amount": 99.99},
    delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
)
```

#### Broadcast

Delivered to all matching subscribers:

```python
result = await fabric_queue_federation.send_message_globally(
    queue_name="system-config",
    topic="config.update",
    payload={"feature_flags": {"new_feature": True}},
    delivery_guarantee=DeliveryGuarantee.BROADCAST,
)
```

#### Global Quorum

Requires weighted quorum before delivery:

```python
result = await fabric_queue_delivery.deliver_with_global_quorum(
    queue_name="payments",
    topic="payment.authorize",
    payload={"payment_id": "pay-123"},
    target_clusters={"us-east", "us-west", "eu-central"},
    required_weight_threshold=0.67,
    byzantine_fault_threshold=1,
    timeout_seconds=120.0,
)
```

### Queue Discovery

Queue discovery is handled by the fabric catalog. The RoutingIndex is the
canonical query surface for known queues:

```python
matches = fabric_control_plane.index.find_queues(QueueQuery(queue_name="orders-*"))
```

### Federation Routing

```
1. Client sends to local FabricQueueFederationManager
2. RoutingIndex selects cluster hosting the queue
3. ClusterMessenger forwards UnifiedMessage (MessageType.QUEUE)
4. QueueFederationAck returns to source cluster
```

### Client API Examples

#### Global Queue Subscription

```python
subscription_id = await fabric_queue_federation.subscribe_globally(
    subscriber_id="payment-processor",
    queue_pattern="payments-*",
    topic_pattern="payment.*",
    delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
    callback=process_payment_message,
)
```

#### Cross-Cluster Message Send

```python
result = await fabric_queue_federation.send_message_globally(
    queue_name="inventory-updates",
    topic="inventory.sync",
    payload={"sku": "WIDGET-001", "quantity": 500},
    delivery_guarantee=DeliveryGuarantee.BROADCAST,
)
```

#### Acknowledgments

Queue federation acknowledgments are automatic; there is no explicit client
acknowledgment API for cross-cluster delivery.

### Performance Characteristics

- **Queue Discovery**: O(log N) convergence via epidemic gossip
- **Message Routing**: Optimal path selection through the fabric route table
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

Internal Python usage should go through `MPREGClientAPI` or the transport factory
(`mpreg.core.transport.TransportFactory`). The external protocol examples below
use WebSocket framing and raw message envelopes.

### Python Client Example (Recommended)

```python
from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.pubsub_client import MPREGPubSubClient

async def handle_notification(message):
    print(f"Received: {message.topic} -> {message.payload}")

async def main():
    async with MPREGClientAPI("ws://localhost:<port>") as client:
        result = await client.call(
            "compute_fibonacci",
            10,
            function_id="math.fibonacci",
            version_constraint=">=1.0.0,<2.0.0",
        )
        print(f"Fibonacci(10) = {result}")

        pubsub = MPREGPubSubClient(base_client=client)
        await pubsub.start()
        await pubsub.subscribe(["events.user.*"], handle_notification)
        await pubsub.publish(
            "events.user.login",
            {"user_id": "user123"},
        )
        await pubsub.stop()
```

### External Protocol Examples (Reference)

#### Go Client Example

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
    Name          string                 `json:"name"`
    Fun           string                 `json:"fun"`
    Args          []interface{}          `json:"args"`
    Kwargs        map[string]interface{} `json:"kwargs"`
    Locs          []string               `json:"locs"`
    TargetCluster string                 `json:"target_cluster,omitempty"`
    RoutingTopic  string                 `json:"routing_topic,omitempty"`
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
            Name:          "call",
            Fun:           function,
            Args:          args,
            Kwargs:        make(map[string]interface{}),
            Locs:          []string{},
            TargetCluster: "cluster-b",
            RoutingTopic:  "mpreg.rpc." + function,
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
    client, err := NewMPREGClient("ws://localhost:<port>", "your-auth-token")
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

#### JavaScript Client Example

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
  const client = new MPREGClient("ws://localhost:<port>", "your-auth-token");
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

| Component            | File                               | Purpose                                  |
| -------------------- | ---------------------------------- | ---------------------------------------- |
| Message Models       | `mpreg/core/model.py`              | External message envelopes               |
| Fabric Envelope      | `mpreg/fabric/message.py`          | UnifiedMessage + routing headers         |
| Fabric Router        | `mpreg/fabric/router.py`           | Routing decisions + forwarding           |
| Fabric Control Plane | `mpreg/fabric/control_plane.py`    | Catalog + gossip + route control         |
| Routing Catalog      | `mpreg/fabric/catalog.py`          | Function/queue/cache/topic registrations |
| Route Control        | `mpreg/fabric/route_control.py`    | Path-vector route records                |
| Gossip Protocol      | `mpreg/fabric/gossip.py`           | Control plane dissemination              |
| Transport Layer      | `mpreg/core/transport/`            | WebSocket/TCP transport implementations  |
| Fabric Transport     | `mpreg/fabric/server_transport.py` | Cross-cluster message delivery           |
| Topic Exchange       | `mpreg/core/topic_exchange.py`     | Pub/Sub message routing                  |
| Cache Protocol       | `mpreg/core/cache_protocol.py`     | Cache request/response models            |
| Cache Federation     | `mpreg/fabric/cache_federation.py` | Cross-cluster cache synchronization      |
| Queue Federation     | `mpreg/fabric/queue_federation.py` | Cross-cluster queue routing              |
| Statistics           | `mpreg/core/statistics.py`         | Metrics collection                       |

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
