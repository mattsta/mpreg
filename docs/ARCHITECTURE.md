# MPREG Architecture Documentation

## Overview

MPREG (Matt's Protocol for Results Everywhere Guaranteed) is a distributed RPC system implementing sophisticated dependency resolution, resource-based routing, and cluster management. This document details the architectural patterns and design decisions that make MPREG production-ready.

## Core Architecture Principles

### 1. Self-Managing Components

Every component in MPREG follows the principle of self-management:

- **Connections** manage their own lifecycle, reconnection, and cleanup
- **Servers** manage their own peer discovery and function registration
- **Clients** manage their own request routing and response correlation
- **Clusters** manage their own membership and gossip protocols

### 2. Encapsulated Interfaces

Objects maintain clean boundaries and handle their own:

- Connection pooling and management
- Error handling and recovery
- Logging and metrics collection
- Resource allocation and cleanup

## Component Architecture

### MPREGServer (`mpreg/server.py`)

The server component implements a self-managing distributed node with:

```python
class MPREGServer:
    cluster: Cluster           # Manages peer relationships
    registry: Registry         # Function registry
    settings: MPREGSettings    # Configuration
    clients: set[Connection]   # Active client connections
```

**Key Features:**

- **Automatic Peer Discovery**: Gossip protocol for cluster formation
- **Function Registry**: Dynamic advertisement of available functions
- **Resource Management**: Tracks and advertises available resources/datasets
- **Connection Pooling**: Maintains persistent connections to other servers
- **Self-Healing**: Automatic reconnection and cluster reformation

### Cluster Management (`mpreg/server.py:Cluster`)

The cluster component handles distributed coordination:

```python
class Cluster:
    servers: dict[str, set[str]]           # Server -> Functions mapping
    functions: dict[str, dict[frozenset, set[str]]]  # Function -> Resources -> Servers
    peer_connections: dict[str, Connection]  # Persistent server connections
    waitingFor: dict[str, asyncio.Event]     # Response correlation
    answer: dict[str, Any]                   # Response storage
```

**Core Responsibilities:**

- **Server Discovery**: Track available servers and their capabilities
- **Function Routing**: Determine optimal server for function execution
- **Dependency Resolution**: Topological sorting of function call graphs
- **Inter-Server Communication**: Handle server-to-server RPC calls

### Connection Management (`mpreg/connection.py`)

The connection component provides robust networking:

```python
class Connection:
    url: str
    websocket: WebSocketClientProtocol | WebSocketServerProtocol | None
    cluster: Cluster | None        # For response routing
    _receive_queue: asyncio.Queue  # Message buffering
    _listener_task: asyncio.Task   # Background message processing
```

**Key Improvements:**

- **Response Routing**: Direct handling of `internal-answer` messages
- **Connection Pooling**: Efficient reuse of established connections
- **Auto-Reconnection**: Exponential backoff retry logic
- **Message Queuing**: Buffered message handling for reliability

### Client Architecture (`mpreg/client.py`)

The client implements concurrent request handling:

```python
class Client:
    websocket: WebSocketClientProtocol | None
    _pending_requests: dict[str, asyncio.Future[RPCResponse]]
    _listener_task: asyncio.Task | None
```

**Concurrency Pattern:**

1. **Request Dispatch**: Send request, create Future for response
2. **Message Listener**: Background task routes responses to correct Future
3. **Response Correlation**: Match responses to requests via unique IDs
4. **Concurrent Execution**: Multiple requests over single connection

## Distributed System Patterns

### 1. Gossip Protocol Implementation

Servers use gossip protocol for cluster discovery:

```python
async def _establish_peer_connection(self, peer_url: str):
    """Establish persistent connection to peer server"""
    connection = Connection(url=peer_url)
    connection.cluster = self
    await connection.connect()
    self.peer_connections[peer_url] = connection
```

**Benefits:**

- Automatic cluster formation
- Fault tolerance through redundant paths
- Dynamic membership updates
- No single point of failure

### 2. Resource-Based Routing

Functions are routed based on resource requirements:

```python
def server_for(self, fun: str, locs: frozenset[str]) -> str | None:
    """Find best server for function with required resources"""
    if fun not in self.functions:
        return None

    # Find servers with all required resources
    candidates = set()
    for resource_set, servers in self.functions[fun].items():
        if locs.issubset(resource_set):
            candidates.update(servers)

    return random.choice(list(candidates)) if candidates else None
```

### 3. Dependency Resolution Engine

Complex workflows are resolved using topological sorting:

```python
class RPC:
    def tasks(self) -> Iterator[set[str]]:
        """Return dependency levels for execution"""
        ts = TopologicalSorter(self.dependencies)
        ts.prepare()
        while ts.is_active():
            ready = ts.get_ready()
            yield set(ready)
            ts.done(*ready)
```

## Message Flow Architecture

### 1. Client Request Flow

```
Client -> Server:
1. Client.request() creates Future and sends RPCRequest
2. Background listener routes response to correct Future
3. Future resolves with RPCResponse
```

### 2. Server-to-Server Communication

```
Server A -> Server B:
1. Server A creates persistent connection to Server B
2. Sends RPCInternalRequest with unique ID
3. Server B processes and sends RPCInternalAnswer
4. Connection listener routes response to waiting Future
```

### 3. Dependency Resolution Flow

```
Multi-Step Workflow:
1. Parse dependencies using topological sort
2. Execute each level in parallel
3. Substitute results into subsequent levels
4. Return final results to client
```

## Error Handling & Recovery

### Connection Recovery

- **Exponential Backoff**: Intelligent retry logic for failed connections
- **Dead Connection Detection**: Automatic cleanup of failed connections
- **Cluster Reformation**: Servers automatically rejoin after network partitions

### Request Timeout Handling

- **Per-Request Timeouts**: Configurable timeout for each RPC call
- **Future Cancellation**: Clean cancellation of timed-out requests
- **Resource Cleanup**: Automatic cleanup of orphaned requests

### Graceful Degradation

- **Partial Cluster Operation**: System continues with remaining healthy nodes
- **Function Redistribution**: Automatic routing around failed servers
- **Client Retry Logic**: Built-in retry for transient failures

## Performance Optimizations

### 1. Connection Pooling

- Persistent connections between servers reduce establishment overhead
- Connection reuse for multiple requests
- Intelligent connection lifecycle management

### 2. Concurrent Processing

- Parallel execution within dependency levels
- Multiple requests over single connections
- Non-blocking I/O throughout the system

### 3. Efficient Serialization

- JSON-based serialization with orjson for performance
- Minimal message overhead
- Streaming-friendly message format

## Testing Strategy

### Test Coverage Areas

1. **Unit Tests**: Individual component functionality
2. **Integration Tests**: Multi-server scenarios
3. **Concurrency Tests**: Parallel request handling
4. **Error Scenarios**: Timeout, disconnection, and failure cases
5. **Performance Tests**: Load and stress testing

### Test Architecture

```python
@pytest.fixture
async def cluster_2_servers():
    """Provides two connected servers for testing"""
    primary = MPREGServer(port=9001, name="Primary")
    secondary = MPREGServer(port=9002, name="Secondary", peers=["ws://127.0.0.1:9001"])
    # ... setup and teardown logic
```

## Security Considerations

### Current Security Model

- **Network Security**: WebSocket connections (can be upgraded to WSS)
- **Cluster Isolation**: Cluster ID validation for message acceptance
- **No Authentication**: Currently operates in trusted network environments

### Future Security Enhancements

- **Authentication**: Token-based or certificate-based auth
- **Authorization**: Role-based access control for functions
- **Encryption**: End-to-end encryption for sensitive data
- **Audit Logging**: Comprehensive audit trail for compliance

## Monitoring & Observability

### Current Logging

- **Structured Logging**: Loguru-based structured logging throughout
- **Request Tracing**: Unique request IDs for distributed tracing
- **Performance Metrics**: Connection timing and request duration

### Future Observability

- **Metrics Collection**: Prometheus-compatible metrics
- **Distributed Tracing**: OpenTelemetry integration
- **Health Checks**: Built-in health and readiness endpoints
- **Dashboard**: Real-time cluster status visualization

## Deployment Patterns

### Single Cluster Deployment

```
[Client] -> [Load Balancer] -> [Server Cluster]
                              ├─ Server A (resources: model-a, dataset-1)
                              ├─ Server B (resources: model-b, dataset-2)
                              └─ Server C (resources: gpu-cluster)
```

### Multi-Cluster Federation

```
[Client] -> [Cluster A] <-> [Cluster B]
            ├─ Server A1    ├─ Server B1
            └─ Server A2    └─ Server B2
```

## Configuration Management

### Server Configuration

```yaml
host: "127.0.0.1"
port: 9001
name: "Primary Server"
cluster_id: "production-cluster"
resources: ["model-a", "dataset-1", "gpu"]
peers: ["ws://server2:9002", "ws://server3:9003"]
gossip_interval: 5.0
```

### Client Configuration

```python
client = MPREGClientAPI(
    url="ws://127.0.0.1:9001",
    full_log=False,  # Disable verbose logging in production
)
```

## Best Practices

### 1. Resource Design

- Use descriptive resource names
- Group related resources logically
- Consider resource dependencies

### 2. Function Design

- Keep functions stateless when possible
- Use clear, descriptive function names
- Handle errors gracefully within functions

### 3. Workflow Design

- Minimize cross-server dependencies
- Use resource constraints to optimize routing
- Design for parallel execution where possible

### 4. Deployment

- Start with simple single-cluster deployments
- Monitor resource utilization across nodes
- Plan for gradual scaling and capacity growth

This architecture enables MPREG to provide a robust, scalable, and maintainable distributed RPC system suitable for production workloads.
