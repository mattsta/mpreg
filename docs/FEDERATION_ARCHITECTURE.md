# MPREG Federation Architecture (Unified Fabric)

## Overview

The federation system is now a single, unified fabric that routes RPC, pub/sub,
queues, and cache operations through one control plane and one message envelope.
Legacy federation bridges and queue-specific gossip are removed in favor of the
fabric catalog, routing engine, and unified transport.

## Core Components

1. **FabricControlPlane** (`mpreg/fabric/control_plane.py`)
   - Owns the RoutingCatalog and RoutingIndex
   - Broadcasts catalog deltas via federation gossip
2. **RoutingCatalog / RoutingIndex** (`mpreg/fabric/catalog.py`, `mpreg/fabric/index.py`)
   - Typed registry for functions, topics, queues, caches, and nodes
3. **ClusterMessenger** (`mpreg/fabric/cluster_messenger.py`)
   - Cluster-aware forwarding with hop budget and path tracking
4. **ServerFabricTransport** (`mpreg/fabric/server_transport.py`)
   - Sends UnifiedMessage envelopes over MPREG peer connections
5. **FabricQueueFederationManager** (`mpreg/fabric/queue_federation.py`)
   - Creates queues, advertises queue endpoints, routes queue messages
6. **FabricQueueDeliveryCoordinator** (`mpreg/fabric/queue_delivery.py`)
   - Consensus-aware delivery (global quorum, broadcast, causal ordering)

## Queue Advertisement + Discovery Flow

```
1. FabricQueueFederationManager.create_queue()
2. FabricQueueAnnouncer.advertise()
3. CatalogBroadcaster publishes RoutingCatalogDelta (queue endpoint)
4. Federation gossip propagates delta
5. Remote clusters update RoutingCatalog via RoutingCatalogApplier
```

Queues are discovered by querying the RoutingIndex; no queue-specific gossip
is required.

## Queue Delivery Flow (Cross-Cluster)

```
1. FabricQueueFederationManager.send_message_globally()
2. If remote, build UnifiedMessage (MessageType.QUEUE)
3. ClusterMessenger selects next hop + updates routing headers
4. ServerFabricTransport sends to peer
5. Remote server handles fabric message -> enqueues locally
6. QueueFederationAck travels back to source cluster
```

Routing headers prevent loops and enforce hop budgets:

- `routing_path` (node path)
- `federation_path` (cluster path)
- `hop_budget`

## Global Subscription Flow

```
1. subscribe_globally() on source cluster
2. Local subscriptions attached to local queues
3. QueueFederationSubscription sent to remote clusters
4. Remote cluster subscribes locally and forwards queue messages
5. Source cluster receives QueueFederationRequest with subscription_id
6. Source callback invoked
```

## Consensus and Delivery Guarantees

The FabricQueueDeliveryCoordinator implements consensus-driven delivery using
control messages over the fabric:

- `mpreg.queue.consensus.request`
- `mpreg.queue.consensus.vote`

Consensus results gate delivery to target clusters for quorum guarantees.
Broadcast delivery is supported without quorum constraints.

## Testing Coverage

- Unit: `tests/test_fabric_cluster_messenger.py`
- Unit: `tests/test_fabric_queue_federation.py`
- Unit: `tests/test_fabric_queue_delivery.py`
- Integration: `tests/integration/test_fabric_queue_federation.py`

All tests use the port allocator; no fixed ports are permitted.
