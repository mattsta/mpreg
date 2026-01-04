# MPREG Client Guide

This guide covers the three main types of MPREG clients and how to use them over network protocols.
Example URLs use placeholder ports; for live runs, allocate ports dynamically and share the
resulting `MPREG_URL` with clients.

## Overview

MPREG provides three distinct client types, each optimized for different use cases:

1. **MPREG RPC Client** - For distributed function calls and compute workloads
2. **MPREG PubSub Client** - For topic-based messaging and event routing
3. **MPREG Cache Client** - For direct cache operations and data structures

All clients operate over standard network protocols (WebSocket, TCP) and can be used from any programming language.
For in-repo usage, prefer the unified `MPREGClientAPI` or the transport factory rather than
opening raw sockets directly. External clients should follow the wire protocol described in
`docs/MPREG_PROTOCOL_SPECIFICATION.md`.

---

## 1. MPREG RPC Client

**Use Case**: Distributed computing, microservices, function orchestration

Use `target_cluster` to constrain execution to a federated cluster and
`routing_topic` to apply fabric routing policies when needed.

### Python Example

```python
from mpreg.client.client_api import MPREGClientAPI

async def main():
    async with MPREGClientAPI("ws://cache-server:<port>") as client:
        # Simple function call
        result = await client.call("fibonacci", 10)
        print(f"fib(10) = {result}")

        # Function with identity + version constraints
        result = await client.call(
            "fibonacci",
            10,
            function_id="math.fibonacci",
            version_constraint=">=1.0.0,<2.0.0",
        )
        print(f"fib(10) v1.x = {result}")

        # Function with caching
        ml_result = await client.call("ml_predict", "bert-large", [1.0, 2.0, 3.0])
        print(f"Prediction: {ml_result}")

        # Dependency chain execution
        from mpreg.core.model import RPCCommand
        result = await client._client.request([
            RPCCommand(
                name="step1",
                fun="process_data",
                args=(data,),
                function_id="pipeline.process_data",
                version_constraint=">=2.0.0,<3.0.0",
            ),
            RPCCommand(name="step2", fun="analyze", args=("step1",)),  # Uses step1 result
            RPCCommand(name="final", fun="summarize", args=("step2",))
        ])
```

### External Clients

For JavaScript/Go/Rust/etc., implement the MPREG wire protocol and message
envelopes. See `docs/MPREG_PROTOCOL_SPECIFICATION.md` for the exact payloads
and framing details.

---

## 2. MPREG PubSub Client

**Use Case**: Event-driven architecture, real-time notifications, message routing

Pub/sub integrates with the fabric control plane for discovery and
cross-cluster forwarding; no separate pub/sub gossip plane exists.

### Python Example

```python
from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.pubsub_client import MPREGPubSubClient

async def main():
    base_client = MPREGClientAPI("ws://cache-server:<port>")
    await base_client.connect()
    pubsub = MPREGPubSubClient(base_client=base_client)
    await pubsub.start()

    # Subscribe to cache events
    await pubsub.subscribe(
        patterns=["cache.events.#"],
        callback=handle_cache_event,
        get_backlog=True,
    )

    # Publish cache invalidation
    await pubsub.publish(
        topic="cache.invalidation.pattern.ml.*",
        payload={"pattern": "ml.*", "reason": "model_updated"},
    )

    await pubsub.stop()
    await base_client.disconnect()

async def handle_cache_event(message):
    print(f"Cache event: {message.topic}")
    print(f"Payload: {message.payload}")
```

---

## 3. Global Cache Manager (In-Process)

**Use Case**: Direct cache manipulation, cache federation, and cache-level control

### Python Example

```python
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport
from mpreg.core.global_cache import (
    CacheLevel,
    CacheMetadata,
    CacheOptions,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)

async def main():
    cache_transport = InProcessCacheTransport()
    cache_protocol = FabricCacheProtocol("node-a", transport=cache_transport)

    cache = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=True,
            enable_l4_federation=True,
            local_cluster_id="cluster-a",
        ),
        cache_protocol=cache_protocol,
    )

    key = GlobalCacheKey.from_data("user_sessions", {"session_id": "session_123"})
    options = CacheOptions(cache_levels=frozenset([CacheLevel.L1, CacheLevel.L4]))
    await cache.put(key, {"user_id": "user_456"}, CacheMetadata(ttl_seconds=300.0), options=options)

    result = await cache.get(key, options=options)
    print("Cache hit:", result.success)
    await cache.shutdown()
```

### Cache Protocol Messages

For implementing cache clients in other languages, here are the key message formats:

#### Cache GET Request

```json
{
  "role": "cache-request",
  "u": "cache-12345",
  "operation": "get",
  "key": {
    "namespace": "user_sessions",
    "identifier": "session_123",
    "version": "v1.0.0"
  },
  "options": {
    "consistency_level": "eventual",
    "timeout_ms": 5000
  }
}
```

#### Cache PUT Request

```json
{
  "role": "cache-request",
  "u": "cache-12346",
  "operation": "put",
  "key": {
    "namespace": "user_sessions",
    "identifier": "session_123"
  },
  "value": {
    "user_id": "user_456",
    "login_time": 1640995200.123
  },
  "options": {
    "consistency_level": "strong"
  }
}
```

#### Atomic Test-and-Set Request

```json
{
  "role": "cache-request",
  "u": "atomic-12347",
  "operation": "atomic",
  "key": {
    "namespace": "locks",
    "identifier": "resource_abc"
  },
  "atomic_operation": {
    "operation_type": "test_and_set",
    "expected_value": null,
    "new_value": {
      "owner": "worker_001",
      "acquired_at": 1640995200.123
    },
    "ttl_seconds": 300
  },
  "options": {
    "consistency_level": "strong"
  }
}
```

#### Data Structure Operation Request

```json
{
  "role": "cache-request",
  "u": "struct-12348",
  "operation": "structure",
  "key": {
    "namespace": "user_permissions",
    "identifier": "user_456"
  },
  "structure_operation": {
    "structure_type": "set",
    "operation": "add",
    "values": ["admin", "moderator"]
  }
}
```

#### Namespace Operation Request

```json
{
  "role": "cache-request",
  "u": "namespace-12349",
  "operation": "namespace",
  "namespace_operation": {
    "operation_type": "clear",
    "namespace": "temp_data",
    "pattern": "expired_*",
    "max_entries": 1000
  }
}
```

---

## Cache Demonstrations

Run the cache-focused demos:

```bash
uv run python mpreg/examples/tier1_single_system_full.py --system cache
uv run python mpreg/examples/tier2_integrations.py
```

---

## Client Comparison

| Feature           | RPC Client              | PubSub Client         | Cache Manager                  |
| ----------------- | ----------------------- | --------------------- | ------------------------------ |
| **Primary Use**   | Function calls          | Event messaging       | Data storage                   |
| **Communication** | Request/Response        | Publish/Subscribe     | In-process API                 |
| **Data Model**    | Function arguments      | Topic messages        | Key-value + structures         |
| **Consistency**   | Per-function            | Eventually consistent | Configurable (eventual/strong) |
| **Caching**       | Automatic (server-side) | Message backlog       | Direct control                 |
| **Atomicity**     | Function-level          | Message-level         | Operation-level                |
| **Federation**    | Multi-cluster routing   | Topic federation      | Geographic replication         |

---

## Network Protocols

All MPREG clients support multiple transport protocols:

### WebSocket (Recommended)

- **URL**: `ws://server:port` or `wss://server:port` (TLS)
- **Use Case**: Real-time applications, web browsers
- **Features**: Bi-directional, low latency, firewall friendly

### TCP

- **URL**: `tcp://server:port` or `tcps://server:port` (TLS)
- **Use Case**: High-throughput, server-to-server
- **Features**: Maximum performance, connection pooling

### Connection Example

```python
# WebSocket
client = MPREGClientAPI("ws://cache-server:<port>")

# WebSocket with TLS
client = MPREGClientAPI("wss://cache-server:<port>")

# TCP
client = MPREGClientAPI("tcp://cache-server:<port>")

# TCP with TLS
client = MPREGClientAPI("tcps://cache-server:<port>")
```

---

## Production Considerations

### High Availability

```python
# Client with multiple server endpoints
servers = [
    "ws://cache-1.example.com:<port>",
    "ws://cache-2.example.com:<port>",
    "ws://cache-3.example.com:<port>"
]

for server_url in servers:
    try:
        async with MPREGClientAPI(server_url) as client:
            await client.call("echo", "ping")
        break
    except ConnectionError:
        continue  # Try next server
```

### Authentication

```python
# Bearer token authentication
headers = {"Authorization": "Bearer eyJhbGciOiJIUzI1NiIs..."}
client = MPREGClientAPI("wss://secure-cache:<port>", headers=headers)

# API key authentication
headers = {"X-API-Key": "mpreg_api_key_abc123..."}
client = MPREGClientAPI("wss://secure-cache:<port>", headers=headers)
```

### Connection Pooling

```python
from mpreg.core.transport.factory import create_transport_pool

# Create connection pool
pool = create_transport_pool(
    urls=["ws://cache-1:<port>", "ws://cache-2:<port>"],
    pool_size=10,
    max_lifetime_seconds=3600
)

# Use pooled connections
async with pool.get_client() as client:
    result = await client.call("fibonacci", 10)
```

### Monitoring

```python
# Cross-system monitoring
from mpreg.core.monitoring.unified_monitoring import (
    EventType,
    SystemType,
    create_unified_system_monitor,
)

monitor = create_unified_system_monitor()
await monitor.start()
tracking_id = await monitor.record_cross_system_event(
    correlation_id="client-demo",
    event_type=EventType.REQUEST_START,
    source_system=SystemType.RPC,
)
await monitor.stop()
```

This guide provides everything needed to start using MPREG's RPC and PubSub clients plus the in-process cache manager.
