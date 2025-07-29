# MPREG Cache Federation System - Complete Guide

## Table of Contents

1. [Overview & Purpose](#overview--purpose)
2. [Architecture & Components](#architecture--components)
3. [Integration with Existing MPREG APIs](#integration-with-existing-mpreg-apis)
4. [API Reference](#api-reference)
5. [Real-World Usage Examples](#real-world-usage-examples)
6. [Configuration & Deployment](#configuration--deployment)
7. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
8. [Future Improvements & Roadmap](#future-improvements--roadmap)

## Overview & Purpose

### What is Cache Federation?

Cache Federation is a distributed cache coherence system that keeps cache data synchronized across multiple MPREG clusters. Think of it as a "smart sync system" that ensures when you update cached data in one cluster, all other clusters automatically know about the change.

### Why Do We Need It?

**Without Cache Federation:**

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Cluster A   │    │ Cluster B   │    │ Cluster C   │
│ Cache: {    │    │ Cache: {    │    │ Cache: {    │
│  user:123:  │    │  user:123:  │    │  user:123:  │
│   name: Bob │    │   name: Bob │    │   name: Bob │
│ }           │    │ }           │    │ }           │
└─────────────┘    └─────────────┘    └─────────────┘

App updates user:123 name to "Robert" in Cluster A:

┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Cluster A   │    │ Cluster B   │    │ Cluster C   │
│ Cache: {    │    │ Cache: {    │    │ Cache: {    │
│  user:123:  │    │  user:123:  │    │  user:123:  │
│   name:     │    │   name: Bob │    │   name: Bob │
│   Robert ✓  │    │     ❌      │    │     ❌      │
│ }           │    │ }           │    │ }           │
└─────────────┘    └─────────────┘    └─────────────┘
                        STALE!           STALE!
```

**With Cache Federation:**

```
App updates user:123 name to "Robert" in Cluster A:

┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Cluster A   │    │ Cluster B   │    │ Cluster C   │
│ Cache: {    │────┤ Cache: {    │────┤ Cache: {    │
│  user:123:  │    │  user:123:  │    │  user:123:  │
│   name:     │    │   INVALID   │    │   INVALID   │
│   Robert ✓  │    │     ✓       │    │     ✓       │
│ }           │    │ }           │    │ }           │
└─────────────┘    └─────────────┘    └─────────────┘
      │                  │                  │
      └─── Invalidation ─┴─── Messages ────┘
```

### Key Benefits

1. **Data Consistency**: All clusters see the same cached data
2. **Performance**: Fast local cache access with automatic synchronization
3. **Scalability**: Add more clusters without cache inconsistency
4. **Reliability**: Automatic fallback and error recovery
5. **Monitoring**: Built-in metrics and statistics

## Architecture & Components

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     MPREG Cache Federation                      │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   Cluster A     │  │   Cluster B     │  │   Cluster C     │  │
│  │                 │  │                 │  │                 │  │
│  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │
│  │ │Cache Fed    │ │  │ │Cache Fed    │ │  │ │Cache Fed    │ │  │
│  │ │Bridge       │◄┼──┼─┤Bridge       │◄┼──┼─┤Bridge       │ │  │
│  │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │  │
│  │       │         │  │       │         │  │       │         │  │
│  │ ┌─────▼─────┐   │  │ ┌─────▼─────┐   │  │ ┌─────▼─────┐   │  │
│  │ │Local Cache│   │  │ │Local Cache│   │  │ │Local Cache│   │  │
│  │ │System     │   │  │ │System     │   │  │ │System     │   │  │
│  │ └───────────┘   │  │ └───────────┘   │  │ └───────────┘   │  │
│  │       │         │  │       │         │  │       │         │  │
│  │ ┌─────▼─────┐   │  │ ┌─────▼─────┐   │  │ ┌─────▼─────┐   │  │
│  │ │Topic      │   │  │ │Topic      │   │  │ │Topic      │   │  │
│  │ │Exchange   │   │  │ │Exchange   │   │  │ │Exchange   │   │  │
│  │ └───────────┘   │  │ └───────────┘   │  │ └───────────┘   │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                               │
                     ┌─────────▼─────────┐
                     │   Message Flow    │
                     │                   │
                     │ Invalidations     │
                     │ Replications      │
                     │ Vector Clocks     │
                     │ Statistics        │
                     └───────────────────┘
```

### Core Components

#### 1. Cache Federation Bridge (`CacheFederationBridge`)

- **Purpose**: Coordinates cache operations across clusters
- **Location**: `mpreg.federation.cache_federation_bridge`
- **Key Responsibilities**:
  - Send invalidation messages when cache data changes
  - Handle replication of cache data to other clusters
  - Track pending operations and timeouts
  - Maintain vector clocks for ordering
  - Collect statistics and metrics

#### 2. Federated Cache Datastructures

- **Location**: `mpreg.datastructures.federated_cache_coherence`
- **Key Types**:
  - `FederatedCacheKey`: Cache keys that work across clusters
  - `CacheInvalidationMessage`: Messages to invalidate cache entries
  - `CacheReplicationMessage`: Messages to replicate cache data
  - `CacheCoherenceMetadata`: Tracks cache state and ownership
  - `FederatedCacheStatistics`: Performance metrics

#### 3. Cache Coherence States

Cache entries can be in different states following industry-standard protocols:

```python
from mpreg.datastructures.federated_cache_coherence import CacheCoherenceState

# Basic states
CacheCoherenceState.MODIFIED    # Data modified locally
CacheCoherenceState.SHARED      # Data shared across clusters
CacheCoherenceState.INVALID     # Data is stale/invalid
CacheCoherenceState.EXCLUSIVE   # Single owner of data

# Federation-specific states
CacheCoherenceState.SYNCING     # Being synchronized
CacheCoherenceState.PENDING     # Waiting for federation response
CacheCoherenceState.DEGRADED    # Available but may be inconsistent
```

## Integration with Existing MPREG APIs

### How It Fits Into Current MPREG Architecture

The cache federation system builds on MPREG's existing foundation:

```
Your Existing MPREG Stack:
┌─────────────────────────────────────────┐
│ Your Application Code                   │
├─────────────────────────────────────────┤
│ MPREG Client API (client.py)           │ ◄── You use this
├─────────────────────────────────────────┤
│ MPREG Server (server.py)               │ ◄── You run this
├─────────────────────────────────────────┤
│ Core Systems:                          │
│ • RPC System                           │ ◄── Existing
│ • Topic Pub/Sub                        │ ◄── Existing
│ • Message Queue                         │ ◄── Existing
│ • Cache System                          │ ◄── Existing
├─────────────────────────────────────────┤
│ NEW: Cache Federation Bridge            │ ◄── NEW! This is what we added
└─────────────────────────────────────────┘
```

### Integration Points

#### 1. With MPREG Server

The cache federation bridge integrates with your existing MPREG server through the topic exchange:

```python
# In your server.py (this would be added)
from mpreg.federation.cache_federation_bridge import (
    CacheFederationBridge,
    CacheFederationConfiguration
)

# Add cache federation to your server
class MPREGServerWithCacheFederation:
    def __init__(self, cluster_id: str):
        # Your existing server initialization
        self.topic_exchange = TopicExchange(...)
        self.cache_manager = SmartCacheManager(...)

        # NEW: Add cache federation
        cache_config = CacheFederationConfiguration(cluster_id=cluster_id)
        self.cache_federation = CacheFederationBridge(
            config=cache_config,
            topic_exchange=self.topic_exchange
        )
```

#### 2. With Cache Operations

When your application performs cache operations, the federation bridge automatically handles cross-cluster coordination:

```python
# Your existing cache code works the same way:
cache_manager.put(key, value)  # This still works

# NEW: The federation bridge automatically:
# 1. Detects cache changes
# 2. Sends invalidation messages to other clusters
# 3. Coordinates replication if needed
# 4. Updates coherence metadata
```

#### 3. With Topic Pub/Sub System

Cache federation uses MPREG's existing topic system for message routing:

```
Cache Federation Topics:
┌─────────────────────────────────────────┐
│ mpreg.cache.federation.invalidation.#  │ ◄── Invalidation messages
│ mpreg.cache.federation.replication.#   │ ◄── Replication messages
│ mpreg.cache.federation.coherence.#     │ ◄── Status messages
└─────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ Your Existing Topic Exchange           │ ◄── Uses existing infrastructure
└─────────────────────────────────────────┘
```

## API Reference

### CacheFederationBridge

#### Initialization

```python
from mpreg.federation.cache_federation_bridge import (
    CacheFederationBridge,
    CacheFederationConfiguration
)

# Basic configuration
config = CacheFederationConfiguration(
    cluster_id="my-cluster-01",
    invalidation_timeout_seconds=30.0,
    replication_timeout_seconds=60.0,
    max_pending_operations=1000
)

# Create bridge with existing topic exchange
bridge = CacheFederationBridge(
    config=config,
    topic_exchange=your_topic_exchange
)
```

#### Key Methods

##### `send_cache_invalidation()`

Notify other clusters that cached data has changed:

```python
async def send_cache_invalidation(
    cache_key: FederatedCacheKey,
    target_clusters: frozenset[str] | None = None,
    invalidation_type: str = "single_key",
    reason: str = "manual_invalidation",
    priority: str = "2.0"
) -> str:
    """
    Args:
        cache_key: The federated cache key to invalidate
        target_clusters: Specific clusters to target (None = all clusters)
        invalidation_type: Type of invalidation operation
        reason: Human-readable reason for debugging
        priority: Message priority level

    Returns:
        Operation ID for tracking the invalidation
    """
```

##### `send_cache_replication()`

Replicate cache data to other clusters:

```python
async def send_cache_replication(
    cache_key: FederatedCacheKey,
    cache_value: Any,
    coherence_metadata: CacheCoherenceMetadata,
    target_clusters: frozenset[str],
    strategy: ReplicationStrategy = ReplicationStrategy.ASYNC_REPLICATION
) -> str:
    """
    Args:
        cache_key: The cache key to replicate
        cache_value: The actual data to replicate
        coherence_metadata: Cache coherence state information
        target_clusters: Clusters to replicate to (cannot be empty)
        strategy: ASYNC_REPLICATION or SYNC_REPLICATION

    Returns:
        Operation ID for tracking the replication
    """
```

##### Statistics Methods

```python
# Get federation statistics
stats = bridge.get_federation_statistics()
print(f"Invalidations sent: {stats.invalidations_sent}")
print(f"Success rate: {stats.success_rate():.2%}")

# Get cluster-specific statistics
cluster_stats = bridge.get_cluster_statistics("other-cluster")
print(f"Replications received: {cluster_stats.replications_received}")
```

### FederatedCacheKey

Create cache keys that work across federated clusters:

```python
from mpreg.datastructures.federated_cache_coherence import FederatedCacheKey

# Create a federated cache key
cache_key = FederatedCacheKey.create(
    namespace="users",           # Logical grouping of cache data
    key="profile:12345",        # Unique key within namespace
    cluster_id="cluster-west",  # Which cluster owns this data
    region="us-west-2",         # Geographic or logical region
    partition_id="0"            # Partition for sharding (optional)
)

# Get different representations
global_key = cache_key.global_key()  # "cluster-west:us-west-2:0:users:profile:12345"
local_key = cache_key.local_key()    # "users:profile:12345"

# Create variants for different clusters
east_key = cache_key.with_cluster("cluster-east")
dev_key = cache_key.with_region("development")
```

## Real-World Usage Examples

### Example 1: E-Commerce User Profile Cache

**Scenario**: You have an e-commerce platform with clusters in different regions. When a user updates their profile, you want all regions to see the change immediately.

```python
import asyncio
from mpreg.federation.cache_federation_bridge import (
    CacheFederationBridge, CacheFederationConfiguration
)
from mpreg.datastructures.federated_cache_coherence import (
    FederatedCacheKey, CacheCoherenceMetadata, ReplicationStrategy
)

class UserProfileService:
    def __init__(self, cluster_id: str, topic_exchange):
        # Set up cache federation
        config = CacheFederationConfiguration(cluster_id=cluster_id)
        self.cache_federation = CacheFederationBridge(
            config=config,
            topic_exchange=topic_exchange
        )
        self.cluster_id = cluster_id

    async def update_user_profile(self, user_id: str, profile_data: dict):
        """Update user profile and sync across all clusters."""

        # 1. Create federated cache key
        cache_key = FederatedCacheKey.create(
            namespace="user_profiles",
            key=f"user:{user_id}",
            cluster_id=self.cluster_id,
            region="global"
        )

        # 2. Update local cache
        # (Your existing cache update logic here)
        local_cache.put(cache_key.local_key(), profile_data)

        # 3. Invalidate other clusters
        invalidation_id = await self.cache_federation.send_cache_invalidation(
            cache_key=cache_key,
            reason=f"user_profile_updated_for_{user_id}"
        )

        # 4. Optionally replicate to specific high-priority clusters
        if profile_data.get("vip_user"):
            vip_clusters = frozenset(["cluster-premium-east", "cluster-premium-west"])
            coherence_metadata = CacheCoherenceMetadata.create_initial(self.cluster_id)

            replication_id = await self.cache_federation.send_cache_replication(
                cache_key=cache_key,
                cache_value=profile_data,
                coherence_metadata=coherence_metadata,
                target_clusters=vip_clusters,
                strategy=ReplicationStrategy.SYNC_REPLICATION  # Immediate sync for VIPs
            )

        return {"invalidation_id": invalidation_id, "updated": True}

# Usage
user_service = UserProfileService("cluster-west-1", topic_exchange)
await user_service.update_user_profile("12345", {
    "name": "John Doe",
    "email": "john@example.com",
    "vip_user": True
})
```

### Example 2: Session Cache Management

**Scenario**: User sessions need to be accessible from any cluster, with automatic cleanup when sessions expire.

```python
class SessionManager:
    def __init__(self, cluster_id: str, topic_exchange):
        config = CacheFederationConfiguration(
            cluster_id=cluster_id,
            invalidation_timeout_seconds=15.0  # Fast session invalidation
        )
        self.cache_federation = CacheFederationBridge(config=config, topic_exchange=topic_exchange)
        self.cluster_id = cluster_id

    async def create_session(self, user_id: str, session_data: dict) -> str:
        """Create a new session and replicate to backup clusters."""
        session_id = f"sess_{uuid.uuid4().hex}"

        # Create session cache key
        cache_key = FederatedCacheKey.create(
            namespace="user_sessions",
            key=session_id,
            cluster_id=self.cluster_id,
            region="global"
        )

        # Store locally
        local_cache.put(cache_key.local_key(), session_data, ttl_seconds=3600)

        # Replicate to backup clusters for fault tolerance
        backup_clusters = frozenset(["cluster-backup-1", "cluster-backup-2"])
        coherence_metadata = CacheCoherenceMetadata.create_initial(self.cluster_id)

        await self.cache_federation.send_cache_replication(
            cache_key=cache_key,
            cache_value=session_data,
            coherence_metadata=coherence_metadata,
            target_clusters=backup_clusters,
            strategy=ReplicationStrategy.ASYNC_REPLICATION
        )

        return session_id

    async def invalidate_session(self, session_id: str):
        """Invalidate session across all clusters."""
        cache_key = FederatedCacheKey.create(
            namespace="user_sessions",
            key=session_id,
            cluster_id=self.cluster_id
        )

        # Remove locally
        local_cache.evict(cache_key.local_key())

        # Invalidate everywhere
        await self.cache_federation.send_cache_invalidation(
            cache_key=cache_key,
            reason=f"session_logout_{session_id}"
        )
```

### Example 3: Product Catalog Synchronization

**Scenario**: Product information changes frequently and needs to be consistent across all e-commerce clusters.

```python
class ProductCatalogManager:
    def __init__(self, cluster_id: str, topic_exchange):
        config = CacheFederationConfiguration(cluster_id=cluster_id)
        self.cache_federation = CacheFederationBridge(config=config, topic_exchange=topic_exchange)
        self.cluster_id = cluster_id

    async def update_product_price(self, product_id: str, new_price: float):
        """Update product price with immediate invalidation."""

        # Create specific cache key for price
        price_key = FederatedCacheKey.create(
            namespace="product_prices",
            key=f"product:{product_id}",
            cluster_id=self.cluster_id,
            region="global"
        )

        # Update local cache
        local_cache.put(price_key.local_key(), new_price)

        # Invalidate all other clusters immediately
        await self.cache_federation.send_cache_invalidation(
            cache_key=price_key,
            priority="3.0",  # High priority for price changes
            reason=f"price_update_product_{product_id}"
        )

    async def update_product_inventory(self, product_id: str, quantity: int):
        """Update inventory with pattern-based invalidation."""

        # Update local inventory cache
        inventory_key = FederatedCacheKey.create(
            namespace="product_inventory",
            key=f"product:{product_id}",
            cluster_id=self.cluster_id
        )

        local_cache.put(inventory_key.local_key(), quantity)

        # If inventory is low, invalidate related cache entries
        if quantity < 10:
            # Invalidate product availability cache
            availability_key = FederatedCacheKey.create(
                namespace="product_availability",
                key=f"product:{product_id}",
                cluster_id=self.cluster_id
            )

            await self.cache_federation.send_cache_invalidation(
                cache_key=availability_key,
                reason=f"low_inventory_product_{product_id}"
            )
```

### Example 4: Real-Time Analytics Cache

**Scenario**: Analytics data is computed in one cluster but needs to be available for dashboards in all clusters.

```python
class AnalyticsCache:
    def __init__(self, cluster_id: str, topic_exchange):
        config = CacheFederationConfiguration(cluster_id=cluster_id)
        self.cache_federation = CacheFederationBridge(config=config, topic_exchange=topic_exchange)
        self.cluster_id = cluster_id

    async def publish_analytics_data(self, metric_name: str, data: dict):
        """Compute analytics and replicate to dashboard clusters."""

        cache_key = FederatedCacheKey.create(
            namespace="analytics_metrics",
            key=f"metric:{metric_name}",
            cluster_id=self.cluster_id,
            region="analytics"
        )

        # Store locally
        local_cache.put(cache_key.local_key(), data, ttl_seconds=300)  # 5 min TTL

        # Replicate to dashboard clusters
        dashboard_clusters = frozenset(["cluster-dashboard-east", "cluster-dashboard-west"])
        coherence_metadata = CacheCoherenceMetadata.create_initial(self.cluster_id)

        await self.cache_federation.send_cache_replication(
            cache_key=cache_key,
            cache_value=data,
            coherence_metadata=coherence_metadata,
            target_clusters=dashboard_clusters,
            strategy=ReplicationStrategy.ASYNC_REPLICATION
        )

    async def get_federation_metrics(self):
        """Get metrics about the cache federation itself."""
        stats = self.cache_federation.get_federation_statistics()

        return {
            "federation_health": {
                "success_rate": stats.success_rate(),
                "processing_efficiency": stats.processing_efficiency(),
                "pending_operations": stats.pending_operations_count,
                "average_latency_ms": stats.average_operation_latency_ms
            },
            "traffic": {
                "invalidations_sent": stats.invalidations_sent,
                "invalidations_received": stats.invalidations_received,
                "replications_sent": stats.replications_sent,
                "replications_received": stats.replications_received
            }
        }
```

## Configuration & Deployment

### Basic Configuration

```python
from mpreg.federation.cache_federation_bridge import CacheFederationConfiguration

# Development configuration
dev_config = CacheFederationConfiguration(
    cluster_id="dev-cluster",
    invalidation_timeout_seconds=10.0,    # Fast timeouts for testing
    replication_timeout_seconds=20.0,
    max_pending_operations=100,
    enable_async_replication=True,
    enable_sync_replication=False,        # Disable sync for dev
    topic_prefix="dev.cache.federation"
)

# Production configuration
prod_config = CacheFederationConfiguration(
    cluster_id="prod-cluster-west-1",
    invalidation_timeout_seconds=30.0,    # Conservative timeouts
    replication_timeout_seconds=60.0,
    max_pending_operations=5000,           # Higher capacity
    enable_async_replication=True,
    enable_sync_replication=True,
    default_replication_strategy=ReplicationStrategy.ASYNC_REPLICATION,
    coherence_check_interval_seconds=300.0,  # 5 minute health checks
    topic_prefix="mpreg.cache.federation"
)
```

### Deployment Patterns

#### Pattern 1: Hub-and-Spoke

One central cluster manages cache coordination:

```
    ┌─────────────┐
    │   Hub       │
    │ Cluster     │◄─── All invalidations go here first
    │ (Central)   │
    └─────┬───────┘
          │
    ┌─────┼─────┐
    │     │     │
    ▼     ▼     ▼
┌─────┐ ┌─────┐ ┌─────┐
│Edge │ │Edge │ │Edge │
│Clust│ │Clust│ │Clust│
└─────┘ └─────┘ └─────┘
```

#### Pattern 2: Mesh Network

All clusters communicate directly:

```
┌─────┐     ┌─────┐
│Clust│◄───►│Clust│
│  A  │     │  B  │
└──┬──┘     └──┬──┘
   │           │
   │           │
   ▼           ▼
┌─────┐     ┌─────┐
│Clust│◄───►│Clust│
│  D  │     │  C  │
└─────┘     └─────┘
```

#### Pattern 3: Regional Federation

Clusters federated by geographic region:

```
US-East Region:        US-West Region:
┌─────┐ ┌─────┐       ┌─────┐ ┌─────┐
│Clust│ │Clust│       │Clust│ │Clust│
│  1  │ │  2  │       │  3  │ │  4  │
└─────┘ └─────┘       └─────┘ └─────┘
     │         │           │         │
     └─────────┼───────────┼─────────┘
               │           │
          Cross-Region Federation
```

### Integration with MPREG Server

Here's how to add cache federation to your existing MPREG server:

```python
# server.py modifications
class EnhancedMPREGServer(MPREGServer):
    def __init__(self, cluster_id: str, **kwargs):
        super().__init__(**kwargs)
        self.cluster_id = cluster_id

        # Add cache federation
        self._setup_cache_federation()

    def _setup_cache_federation(self):
        """Set up cache federation with existing infrastructure."""
        config = CacheFederationConfiguration(
            cluster_id=self.cluster_id,
            invalidation_timeout_seconds=30.0,
            replication_timeout_seconds=60.0
        )

        self.cache_federation = CacheFederationBridge(
            config=config,
            topic_exchange=self.topic_exchange  # Use existing topic exchange
        )

        # Subscribe to federation messages
        self._subscribe_to_federation_topics()

    def _subscribe_to_federation_topics(self):
        """Subscribe to cache federation topics."""
        from mpreg.core.model import PubSubSubscription, TopicPattern

        # Subscribe to invalidation messages
        invalidation_subscription = PubSubSubscription(
            subscription_id=f"cache-invalidation-{self.cluster_id}",
            subscriber=f"cache-federation-{self.cluster_id}",
            patterns=[TopicPattern(pattern="mpreg.cache.federation.invalidation.#")],
            get_backlog=False
        )

        self.topic_exchange.add_subscription(invalidation_subscription)

        # Subscribe to replication messages
        replication_subscription = PubSubSubscription(
            subscription_id=f"cache-replication-{self.cluster_id}",
            subscriber=f"cache-federation-{self.cluster_id}",
            patterns=[TopicPattern(pattern="mpreg.cache.federation.replication.#")],
            get_backlog=False
        )

        self.topic_exchange.add_subscription(replication_subscription)
```

## Monitoring & Troubleshooting

### Built-in Metrics

The cache federation system provides comprehensive metrics:

```python
# Get overall federation health
stats = bridge.get_federation_statistics()

print(f"""
Cache Federation Health Report:
==============================

Operations:
- Invalidations sent: {stats.invalidations_sent}
- Invalidations received: {stats.invalidations_received}
- Replications sent: {stats.replications_sent}
- Replications received: {stats.replications_received}

Performance:
- Success rate: {stats.success_rate():.1%}
- Processing efficiency: {stats.processing_efficiency():.1%}
- Average latency: {stats.average_operation_latency_ms:.1f}ms
- Pending operations: {stats.pending_operations_count}

Errors:
- Timeout operations: {stats.timeout_operations}
- Failed operations: {stats.failed_operations}
- Coherence violations: {stats.coherence_violations_detected}
""")

# Get per-cluster statistics
for cluster_id, cluster_stats in bridge.get_all_cluster_statistics().items():
    print(f"""
Cluster {cluster_id}:
- Local hits: {cluster_stats.local_hits}
- Federation hits: {cluster_stats.federation_hits}
- Hit rate: {cluster_stats.local_hit_rate():.1%}
""")
```

### Health Checks

```python
async def check_cache_federation_health(bridge: CacheFederationBridge) -> dict:
    """Comprehensive health check for cache federation."""

    stats = bridge.get_federation_statistics()

    health_status = {
        "healthy": True,
        "issues": [],
        "metrics": {}
    }

    # Check success rate
    success_rate = stats.success_rate()
    if success_rate < 0.95:  # Less than 95% success
        health_status["healthy"] = False
        health_status["issues"].append(f"Low success rate: {success_rate:.1%}")

    # Check pending operations
    if stats.pending_operations_count > 1000:
        health_status["healthy"] = False
        health_status["issues"].append(f"Too many pending operations: {stats.pending_operations_count}")

    # Check average latency
    if stats.average_operation_latency_ms > 5000:  # More than 5 seconds
        health_status["healthy"] = False
        health_status["issues"].append(f"High latency: {stats.average_operation_latency_ms:.1f}ms")

    # Check coherence violations
    if stats.coherence_violations_detected > 0:
        health_status["issues"].append(f"Coherence violations detected: {stats.coherence_violations_detected}")

    health_status["metrics"] = {
        "success_rate": success_rate,
        "pending_operations": stats.pending_operations_count,
        "average_latency_ms": stats.average_operation_latency_ms,
        "coherence_violations": stats.coherence_violations_detected
    }

    return health_status
```

### Common Issues & Solutions

#### Issue 1: High Latency

**Symptoms**: `average_operation_latency_ms` > 5000
**Causes**: Network issues, overloaded clusters, large message payloads
**Solutions**:

```python
# Reduce timeout values
config = CacheFederationConfiguration(
    cluster_id="cluster-1",
    invalidation_timeout_seconds=15.0,  # Reduced from 30
    replication_timeout_seconds=30.0    # Reduced from 60
)

# Use async replication for non-critical data
await bridge.send_cache_replication(
    strategy=ReplicationStrategy.ASYNC_REPLICATION  # Faster than sync
)
```

#### Issue 2: Low Success Rate

**Symptoms**: `success_rate()` < 0.95
**Causes**: Network partitions, cluster failures, configuration errors
**Solutions**:

```python
# Check cluster connectivity
all_stats = bridge.get_all_cluster_statistics()
for cluster_id, stats in all_stats.items():
    if stats.replications_sent > 0 and stats.replications_received == 0:
        print(f"Cluster {cluster_id} may be unreachable")

# Increase retry timeouts
config = CacheFederationConfiguration(
    cluster_id="cluster-1",
    invalidation_timeout_seconds=60.0,  # Increased tolerance
    max_pending_operations=2000         # Allow more pending ops
)
```

#### Issue 3: Memory Usage

**Symptoms**: Too many pending operations
**Causes**: Network delays, cluster failures, insufficient cleanup
**Solutions**:

```python
# More aggressive cleanup
config = CacheFederationConfiguration(
    cluster_id="cluster-1",
    max_pending_operations=500,         # Lower limit
    coherence_check_interval_seconds=60.0  # More frequent cleanup
)

# Manual cleanup
bridge.complete_operation("operation-id", success=False)  # Mark failed ops as complete
```

## Future Improvements & Roadmap

### Near-Term Enhancements (Next 3-6 months)

#### 1. Advanced Replication Strategies

```python
# Quorum-based replication
class QuorumReplicationStrategy:
    def __init__(self, required_replicas: int = 2):
        self.required_replicas = required_replicas

    async def replicate(self, cache_key, cache_value, target_clusters):
        # Wait for at least `required_replicas` confirmations
        pass

# Chain replication for consistency
class ChainReplicationStrategy:
    def __init__(self, chain_order: list[str]):
        self.chain_order = chain_order  # Ordered list of clusters

    async def replicate(self, cache_key, cache_value):
        # Replicate through chain: A -> B -> C -> D
        pass
```

#### 2. Intelligent Routing

```python
# Route based on data locality
class LocalityAwareRouter:
    def determine_target_clusters(self, cache_key: FederatedCacheKey) -> frozenset[str]:
        # Route user data to clusters near users
        if cache_key.base_key.namespace.name == "user_profiles":
            return self.get_user_location_clusters(cache_key.base_key.key)

        # Route product data to e-commerce clusters
        elif cache_key.base_key.namespace.name == "products":
            return self.get_ecommerce_clusters()

        # Default: all clusters
        return self.get_all_clusters()
```

#### 3. Automatic Conflict Resolution

```python
# Last-writer-wins with vector clocks
class ConflictResolver:
    def resolve_conflict(self,
                        local_value: Any,
                        local_metadata: CacheCoherenceMetadata,
                        remote_value: Any,
                        remote_metadata: CacheCoherenceMetadata) -> tuple[Any, CacheCoherenceMetadata]:

        # Use vector clocks to determine causality
        if local_metadata.vector_clock.happens_before(remote_metadata.vector_clock):
            return remote_value, remote_metadata  # Remote is newer

        elif remote_metadata.vector_clock.happens_before(local_metadata.vector_clock):
            return local_value, local_metadata   # Local is newer

        else:
            # Concurrent updates - use application-specific resolution
            return self.application_conflict_resolution(local_value, remote_value)
```

### Medium-Term Features (6-12 months)

#### 1. Cross-Region Federation with WAN Optimization

```python
# Compress and batch messages for WAN
class WANOptimizedBridge(CacheFederationBridge):
    def __init__(self, config, topic_exchange):
        super().__init__(config, topic_exchange)
        self.compression_enabled = True
        self.message_batching = True
        self.batch_size = 100
        self.batch_timeout_ms = 500

    async def send_batch_invalidation(self, operations: list[InvalidationOperation]):
        # Batch multiple invalidations into single message
        # Compress payload for WAN efficiency
        pass
```

#### 2. Tiered Cache Federation

```python
# Different federation strategies by cache tier
class TieredCacheFederation:
    def __init__(self):
        self.l1_strategy = ImmediateReplicationStrategy()  # L1: Immediate
        self.l2_strategy = AsyncReplicationStrategy()       # L2: Async
        self.l3_strategy = LazyReplicationStrategy()        # L3: On-demand

    def get_strategy_for_tier(self, cache_tier: str) -> ReplicationStrategy:
        return {
            "L1": self.l1_strategy,
            "L2": self.l2_strategy,
            "L3": self.l3_strategy
        }[cache_tier]
```

#### 3. Machine Learning-Driven Optimization

```python
# ML model to predict optimal replication strategies
class MLReplicationOptimizer:
    def __init__(self):
        self.model = self.load_trained_model()

    def predict_optimal_strategy(self,
                                cache_key: FederatedCacheKey,
                                access_pattern: dict,
                                cluster_load: dict) -> ReplicationStrategy:
        features = self.extract_features(cache_key, access_pattern, cluster_load)
        prediction = self.model.predict(features)

        return {
            0: ReplicationStrategy.NO_REPLICATION,
            1: ReplicationStrategy.ASYNC_REPLICATION,
            2: ReplicationStrategy.SYNC_REPLICATION,
            3: ReplicationStrategy.QUORUM_REPLICATION
        }[prediction]
```

### Long-Term Vision (1-2 years)

#### 1. Self-Healing Cache Federation

- Automatic detection and repair of cache inconsistencies
- Self-tuning performance parameters based on load patterns
- Predictive scaling based on cache usage trends

#### 2. Global Cache Mesh

- Integration with CDNs and edge computing platforms
- Support for mobile and IoT edge caches
- Hierarchical federation spanning data centers, edge nodes, and devices

#### 3. Integration with External Systems

```python
# Integration with Redis, Memcached, and other cache systems
class ExternalCacheIntegration:
    def __init__(self):
        self.redis_adapter = RedisFederationAdapter()
        self.memcached_adapter = MemcachedFederationAdapter()
        self.s3_adapter = S3CacheFederationAdapter()

    async def federate_external_cache(self, cache_type: str, operation: CacheOperation):
        adapter = self.get_adapter(cache_type)
        await adapter.execute_federated_operation(operation)
```

### Contributing & Extension Points

#### Custom Replication Strategies

```python
from mpreg.datastructures.federated_cache_coherence import ReplicationStrategy

class CustomReplicationStrategy:
    """Implement your own replication logic."""

    async def replicate(self,
                       cache_key: FederatedCacheKey,
                       cache_value: Any,
                       source_cluster: str,
                       target_clusters: frozenset[str]) -> str:
        # Your custom replication logic here
        pass
```

#### Custom Coherence Protocols

```python
class CustomCoherenceProtocol:
    """Implement application-specific coherence rules."""

    def validate_operation(self,
                          operation: CacheOperation,
                          current_state: CacheCoherenceState) -> bool:
        # Your validation logic
        pass

    def next_state(self,
                   current_state: CacheCoherenceState,
                   operation: CacheOperation) -> CacheCoherenceState:
        # Your state transition logic
        pass
```

---

## Summary

The MPREG Cache Federation system provides:

1. **Automatic Cache Synchronization**: Changes in one cluster automatically propagate to others
2. **Flexible Replication Strategies**: Choose between async, sync, and custom replication
3. **Built-in Monitoring**: Comprehensive metrics and health checking
4. **Easy Integration**: Works with existing MPREG infrastructure
5. **Production Ready**: Proper error handling, timeouts, and recovery mechanisms

**Next Steps**:

1. Try the basic examples with your existing MPREG setup
2. Configure cache federation for your specific use case
3. Monitor performance using the built-in metrics
4. Customize replication strategies based on your requirements
5. Consider contributing enhancements back to the MPREG project

The system is designed to grow with your needs, from simple development setups to complex multi-region deployments.
