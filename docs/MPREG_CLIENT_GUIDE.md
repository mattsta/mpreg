# MPREG Client Guide

This guide covers the three main types of MPREG clients and how to use them over network protocols.

## Overview

MPREG provides three distinct client types, each optimized for different use cases:

1. **MPREG RPC Client** - For distributed function calls and compute workloads
2. **MPREG PubSub Client** - For topic-based messaging and event routing
3. **MPREG Cache Client** - For direct cache operations and data structures

All clients operate over standard network protocols (WebSocket, TCP) and can be used from any programming language.

---

## 1. MPREG RPC Client

**Use Case**: Distributed computing, microservices, function orchestration

### Python Example

```python
from mpreg.client.client_api import MPREGClientAPI

async def main():
    async with MPREGClientAPI("ws://cache-server:9001") as client:
        # Simple function call
        result = await client.call("fibonacci", 10)
        print(f"fib(10) = {result}")

        # Function with caching
        ml_result = await client.call("ml_predict", "bert-large", [1.0, 2.0, 3.0])
        print(f"Prediction: {ml_result}")

        # Dependency chain execution
        from mpreg.core.model import RPCCommand
        result = await client._client.request([
            RPCCommand(name="step1", fun="process_data", args=(data,)),
            RPCCommand(name="step2", fun="analyze", args=("step1",)),  # Uses step1 result
            RPCCommand(name="final", fun="summarize", args=("step2",))
        ])
```

### JavaScript Example

```javascript
class MPREGRPCClient {
  constructor(url) {
    this.ws = new WebSocket(url);
    this.requestId = 0;
    this.pendingRequests = new Map();
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
      this.ws.send(JSON.stringify(message));
    });
  }
}

// Usage
const client = new MPREGRPCClient("ws://cache-server:9001");
const result = await client.call("fibonacci", 10);
```

---

## 2. MPREG PubSub Client

**Use Case**: Event-driven architecture, real-time notifications, message routing

### Python Example

```python
from mpreg.core.model import PubSubMessage, PubSubSubscription
from mpreg.core.topic_exchange import TopicExchange

async def main():
    exchange = TopicExchange("ws://cache-server:9001", "client_cluster")

    # Subscribe to cache events
    subscription = PubSubSubscription(
        subscription_id="cache-monitor",
        patterns=[{
            "pattern": "cache.events.#",  # All cache events
            "exact_match": False
        }],
        subscriber="monitoring-client",
        get_backlog=True
    )

    await exchange.subscribe(subscription, handle_cache_event)

    # Publish cache invalidation
    message = PubSubMessage(
        topic="cache.invalidation.pattern.ml.*",
        payload={"pattern": "ml.*", "reason": "model_updated"},
        publisher="admin-client"
    )

    await exchange.publish(message)

async def handle_cache_event(message):
    print(f"Cache event: {message.topic}")
    print(f"Payload: {message.payload}")
```

### Go Example

```go
package main

import (
    "encoding/json"
    "log"
    "github.com/gorilla/websocket"
)

type PubSubClient struct {
    conn *websocket.Conn
}

func (c *PubSubClient) Subscribe(pattern string) error {
    message := map[string]interface{}{
        "role": "pubsub-subscribe",
        "u":    "sub-12345",
        "subscription": map[string]interface{}{
            "subscription_id": "go-client",
            "patterns": []map[string]interface{}{
                {"pattern": pattern, "exact_match": false},
            },
            "subscriber": "go-client",
            "get_backlog": true,
        },
    }

    return c.conn.WriteJSON(message)
}

func (c *PubSubClient) Publish(topic string, payload interface{}) error {
    message := map[string]interface{}{
        "role": "pubsub-publish",
        "u":    "pub-12345",
        "message": map[string]interface{}{
            "topic":     topic,
            "payload":   payload,
            "publisher": "go-client",
        },
    }

    return c.conn.WriteJSON(message)
}
```

---

## 3. MPREG Cache Client

**Use Case**: Direct cache manipulation, data structures, atomic operations

### Python Example

```python
from mpreg.examples.cache_client_server_demo import MPREGCacheClient

async def main():
    cache = MPREGCacheClient("ws://cache-server:9001")
    await cache.connect()

    try:
        # Basic cache operations
        await cache.put("user_sessions", "session_123", {
            "user_id": "user_456",
            "login_time": time.time(),
            "permissions": ["read", "write"]
        })

        session = await cache.get("user_sessions", "session_123")
        print(f"Session: {session}")

        # Atomic operations for distributed locking
        lock_acquired = await cache.atomic_test_and_set(
            "locks", "resource_abc",
            expected_value=None,  # Only acquire if not locked
            new_value={"owner": "worker_001", "acquired_at": time.time()}
        )

        if lock_acquired:
            print("🔒 Lock acquired, processing resource...")
            # Do work...
            await cache.put("locks", "resource_abc", None)  # Release lock

        # Server-side data structures
        await cache.set_add("user_permissions", "user_456", "admin")
        await cache.set_add("user_permissions", "user_456", "moderator")

        has_admin = await cache.set_contains("user_permissions", "user_456", "admin")
        print(f"User has admin: {has_admin}")

        # Namespace operations
        cleared = await cache.clear_namespace("temp_data", pattern="expired_*")
        print(f"Cleared {cleared} expired entries")

    finally:
        await cache.disconnect()
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

## Starting Cache-Enabled MPREG Server

### Basic Server Setup

```python
from mpreg.examples.cache_client_server_demo import MPREGCacheServer

async def main():
    server = MPREGCacheServer(port=9001, cluster_id="cache-cluster-1")
    await server.start()  # Runs until interrupted

# Run with: python -m asyncio server_script.py
```

### Command Line

```bash
# Start cache server
poetry run python mpreg/examples/cache_client_server_demo.py --mode server --port 9001

# Run cache client demo
poetry run python mpreg/examples/cache_client_server_demo.py --mode client --url ws://localhost:9001

# Run integrated demo (server + client)
poetry run python mpreg/examples/cache_client_server_demo.py --mode integrated
```

### Docker Deployment

```dockerfile
FROM python:3.11-slim

RUN pip install poetry
COPY . /app
WORKDIR /app
RUN poetry install

EXPOSE 9001

CMD ["poetry", "run", "python", "mpreg/examples/cache_client_server_demo.py", "--mode", "server", "--port", "9001"]
```

```bash
# Build and run
docker build -t mpreg-cache-server .
docker run -p 9001:9001 mpreg-cache-server

# Connect client from another container
docker run --network host mpreg-cache-server poetry run python mpreg/examples/cache_client_server_demo.py --mode client --url ws://host.docker.internal:9001
```

---

## Client Comparison

| Feature           | RPC Client              | PubSub Client         | Cache Client                   |
| ----------------- | ----------------------- | --------------------- | ------------------------------ |
| **Primary Use**   | Function calls          | Event messaging       | Data storage                   |
| **Communication** | Request/Response        | Publish/Subscribe     | Request/Response               |
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
client = MPREGClientAPI("ws://cache-server:9001")

# WebSocket with TLS
client = MPREGClientAPI("wss://cache-server:9001")

# TCP
client = MPREGClientAPI("tcp://cache-server:9002")

# TCP with TLS
client = MPREGClientAPI("tcps://cache-server:9002")
```

---

## Production Considerations

### High Availability

```python
# Client with multiple server endpoints
servers = [
    "ws://cache-1.example.com:9001",
    "ws://cache-2.example.com:9001",
    "ws://cache-3.example.com:9001"
]

for server_url in servers:
    try:
        client = MPREGCacheClient(server_url)
        await client.connect()
        # Client connected successfully
        break
    except ConnectionError:
        continue  # Try next server
```

### Authentication

```python
# Bearer token authentication
headers = {"Authorization": "Bearer eyJhbGciOiJIUzI1NiIs..."}
client = MPREGClientAPI("wss://secure-cache:9001", headers=headers)

# API key authentication
headers = {"X-API-Key": "mpreg_api_key_abc123..."}
client = MPREGClientAPI("wss://secure-cache:9001", headers=headers)
```

### Connection Pooling

```python
from mpreg.core.transport.factory import create_transport_pool

# Create connection pool
pool = create_transport_pool(
    urls=["ws://cache-1:9001", "ws://cache-2:9001"],
    pool_size=10,
    max_lifetime_seconds=3600
)

# Use pooled connections
async with pool.get_client() as client:
    result = await client.call("fibonacci", 10)
```

### Monitoring

```python
# Enable client metrics
client = MPREGCacheClient("ws://cache-server:9001")
await client.connect()

# Get connection statistics
stats = client.get_statistics()
print(f"Requests sent: {stats['requests_sent']}")
print(f"Cache hits: {stats['cache_hits']}")
print(f"Average latency: {stats['avg_latency_ms']}ms")
```

This guide provides everything needed to start using MPREG's three client types for RPC, PubSub, and Cache operations over network protocols!
