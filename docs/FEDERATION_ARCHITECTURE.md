# MPREG Federation Architecture

## Overview

The MPREG Federation system enables multiple MPREG clusters to operate together as a unified distributed system, providing federated message queues with cross-cluster discovery, message routing, and delivery guarantees. This document describes the complete architecture and message flows.

## Architecture Components

### Core Components

1. **GraphAwareFederationBridge** - The central federation component that manages communication between clusters
2. **TopicExchange** - Routes federation messages and integrates with federation bridges
3. **FederatedMessageQueueManager** - Manages federated queues and handles queue advertisements
4. **ClusterConnection** - Manages WebSocket connections to remote clusters
5. **PubSub Client** - Handles actual message transmission over WebSocket

### Federation Bridge (`GraphAwareFederationBridge`)

The federation bridge is the core component responsible for:

- **Message Forwarding**: Sending local federation messages to remote clusters
- **Message Receiving**: Processing federation messages from remote clusters
- **Cluster Management**: Managing connections to remote clusters
- **Health Monitoring**: Tracking cluster health and connectivity

#### Key Methods

- `_message_forwarding_loop()` - Background task that processes outbound federation messages
- `_federation_message_receiving_loop()` - Background task that processes inbound federation messages
- `_forward_message_to_all_clusters()` - Sends messages to all healthy remote clusters
- `queue_incoming_federation_message()` - Queues incoming messages for processing
- `_process_received_federation_message()` - Processes received federation messages

#### Message Queues

- `_outbound_message_queue` - Queues local federation messages for forwarding
- `_inbound_federation_queue` - Queues incoming federation messages for processing

Both queues are initialized in the event loop during bridge startup to avoid asyncio binding issues.

### Topic Exchange Integration

The `TopicExchange` has been extended to support federation:

- **Federation Bridge Connection**: `set_federation_bridge()` connects the bridge to the topic exchange
- **Message Routing**: `publish_message()` routes federation messages based on headers:
  - Messages with `federation_hop` header → route to federation bridge for processing
  - Messages without `federation_hop` header → queue for forwarding to remote clusters

### Federation Message Flow

#### Outbound Message Flow (Sending)

```
1. FederatedMessageQueueManager.create_federated_queue(advertise_globally=True)
   ↓
2. FederatedMessageQueueManager._advertise_queue()
   ↓
3. FederatedMessageQueueManager._publish_queue_advertisement()
   ↓
4. TopicExchange.publish_message() [local]
   ↓
5. TopicExchange routes to federation bridge (no federation_hop header)
   ↓
6. GraphAwareFederationBridge._queue_message_for_forwarding()
   ↓
7. GraphAwareFederationBridge._message_forwarding_loop() [background task]
   ↓
8. GraphAwareFederationBridge._forward_message_to_all_clusters()
   ↓
9. ClusterConnection.send_message() [for each healthy remote cluster]
   ↓
10. MPREGPubSubClient.publish() [with federation_hop header added]
    ↓
11. WebSocket transmission to remote cluster
```

#### Inbound Message Flow (Receiving)

```
1. WebSocket message received by remote MPREG server
   ↓
2. Server publishes to its TopicExchange.publish_message()
   ↓
3. TopicExchange detects federation_hop header
   ↓
4. TopicExchange routes to GraphAwareFederationBridge.queue_incoming_federation_message()
   ↓
5. Message queued in _inbound_federation_queue
   ↓
6. GraphAwareFederationBridge._federation_message_receiving_loop() [background task]
   ↓
7. GraphAwareFederationBridge._process_received_federation_message()
   ↓
8. Message processed (e.g., queue advertisement stored for discovery)
```

## Critical Implementation Details

### Anti-Loop Protection

The federation system prevents message loops through header-based filtering:

- **Outbound filtering**: `_forward_message_to_all_clusters()` skips messages with `federation_hop` header
- **Inbound filtering**: `queue_incoming_federation_message()` only processes messages WITH `federation_hop` header

### Headers and Message Identification

Federation messages use specific headers:

- `federation_hop: True` - Indicates message came from a remote cluster
- `federation_source: <cluster_id>` - Identifies the source cluster

### WebSocket Connection Management

Each message forwarding creates a temporary WebSocket connection:

- Creates `MPREGClientAPI` and `MPREGPubSubClient`
- Connects, sends message, disconnects
- This prevents WebSocket recv conflicts from multiple concurrent readers

### Topic Exchange Integration

The `TopicExchange.publish_message()` method includes federation routing logic:

```python
# If this is a federation message and we have a federation bridge, handle it
if (message.topic.startswith("mpreg.federation.") and
    self.federation_bridge is not None):

    # If this message has federation_hop header, it came from a remote cluster
    if message.headers.get('federation_hop'):
        # Route to federation bridge for processing
        asyncio.create_task(self.federation_bridge.queue_incoming_federation_message(message))
    else:
        # Queue local federation message for forwarding to remote clusters
        self.federation_bridge._queue_message_for_forwarding(message)
```

## Queue Advertisement and Discovery

### Advertisement Flow

1. `create_federated_queue(advertise_globally=True)` triggers advertisement
2. Creates `QueueAdvertisement` with queue metadata
3. Publishes to topic `mpreg.federation.queue.advertisement.{cluster_id}`
4. Federation bridge forwards to all remote clusters
5. Remote clusters receive and store advertisement for discovery

### Discovery Flow

1. `discover_federated_queues(pattern)` searches local advertisement cache
2. Returns `dict[cluster_id, List[QueueAdvertisement]]` with matching queues
3. Advertisement TTL ensures stale advertisements are removed

## Error Handling and Resilience

### Circuit Breaker Pattern

- Each cluster connection has a circuit breaker
- Failed connections are marked unhealthy
- Automatic recovery when connections restore

### Backpressure Handling

- Federation queues use `await queue.put()` for proper backpressure
- Full queues block message forwarding rather than dropping messages

### Health Monitoring

- Cluster connections track health metrics
- Last heartbeat and success rate monitoring
- Automatic connection health assessment

## Performance Considerations

### Asyncio Event Loop Management

- Federation queues initialized in `start()` method within proper event loop
- Avoids "Queue is bound to a different event loop" errors

### Connection Pooling

- Currently uses ephemeral connections per message
- Future optimization: connection pooling for high-throughput scenarios

### Topic Pattern Matching

- Federation messages use prefix matching for efficient routing
- Topic pattern: `mpreg.federation.*`

## Testing and Debugging

### Debug Tools Created

1. **`tools/debug/trace_federation_flow.py`** - Comprehensive federation flow debugging
2. **`tools/debug/test_simple_forward.py`** - Simple message forwarding test
3. **`tools/debug/test_federation_debug.py`** - Detailed federation debugging
4. **`tools/debug/test_queue_advertisement_simple.py`** - Queue advertisement testing

### Key Debug Points

- Federation message forwarding statistics
- Inbound/outbound queue sizes
- Cluster health status
- Message header inspection
- Advertisement discovery results

## Configuration

### Federation Bridge Setup

```python
# Create federation bridge
bridge = GraphAwareFederationBridge(
    local_cluster=topic_exchange,
    cluster_identity=cluster_identity,
    enable_graph_routing=False,  # Basic federation
    enable_monitoring=True
)

# Connect to topic exchange
topic_exchange.set_federation_bridge(bridge)

# Start federation
await bridge.start()

# Add remote clusters
await bridge.add_cluster(remote_cluster_identity)
```

### Cluster Identity

```python
cluster_identity = ClusterIdentity(
    cluster_id="cluster-1",
    cluster_name="Production Cluster 1",
    region="us-east",
    bridge_url="ws://127.0.0.1:8000",
    public_key_hash="unique_hash",
    created_at=time.time(),
    geographic_coordinates=(40.7128, -74.0060),
    network_tier=1,
    max_bandwidth_mbps=1000,
    preference_weight=1.0
)
```

## Future Enhancements

### Planned Improvements

1. **Connection Pooling** - Persistent connections for high-throughput scenarios
2. **Message Batching** - Batch multiple federation messages for efficiency
3. **Compression** - Message compression for large payloads
4. **Authentication** - Cluster authentication and authorization
5. **Encryption** - End-to-end encryption for sensitive data
6. **Graph Routing** - Advanced routing through federation topology
7. **Load Balancing** - Intelligent routing based on cluster load

### Advanced Features

- **Byzantine Fault Tolerance** - Consensus mechanisms for critical operations
- **Causal Ordering** - Message ordering guarantees across clusters
- **Quorum Consensus** - Multi-cluster agreement protocols
- **Federation Hierarchies** - Multi-level federation topologies

## Troubleshooting

### Common Issues

1. **"Queue is bound to a different event loop"**
   - **Solution**: Ensure queues are initialized in `start()` method

2. **Federation messages not being forwarded**
   - **Check**: `messages_forwarded` statistics
   - **Debug**: Verify cluster health and connections

3. **Federation messages not being received**
   - **Check**: `messages_received` statistics
   - **Debug**: Verify `federation_hop` headers are set correctly

4. **Queue discovery returns empty results**
   - **Check**: Advertisement propagation and TTL
   - **Debug**: Verify federation message processing

### Debugging Steps

1. Enable debug logging for federation components
2. Check federation bridge statistics and queue sizes
3. Verify cluster health and connectivity
4. Inspect message headers and routing
5. Use debug tools to trace complete message flow

## Conclusion

The MPREG Federation architecture provides a robust, scalable foundation for distributed message queuing across multiple clusters. The event-driven design with proper asyncio integration, comprehensive error handling, and extensive debugging capabilities makes it suitable for production deployments.

The architecture successfully implements the core federation primitives (message forwarding, queue advertisement, discovery) while providing a foundation for advanced features like consensus protocols and graph routing.
