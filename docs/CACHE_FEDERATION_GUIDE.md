# Fabric Cache Federation Guide

This document describes the unified fabric cache federation path. The legacy cache federation bridge is removed; cache replication now flows through the unified fabric message plane.

## Architecture

- **GlobalCacheManager** (`mpreg/core/global_cache.py`): L1/L2/L3/L4 cache management.
- **FabricCacheProtocol** (`mpreg/fabric/cache_federation.py`): propagation, anti-entropy, conflict handling.
- **CacheTransport** (`mpreg/fabric/cache_transport.py`): fabric-backed delivery over server connections.

L4 (fabric federation) is implemented on top of the same fabric protocol used for L3. The difference is **scope**: L4 enables cross-cluster propagation when federation policy allows it.

## Basic Usage (In-Process)

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

async def demo_cache() -> None:
    transport = InProcessCacheTransport()
    protocol_a = FabricCacheProtocol("node-a", transport=transport)
    protocol_b = FabricCacheProtocol("node-b", transport=transport)

    cache_a = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=True,
            local_cluster_id="cluster-a",
        ),
        cache_protocol=protocol_a,
    )

    cache_b = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=True,
            local_cluster_id="cluster-b",
        ),
        cache_protocol=protocol_b,
    )

    key = GlobalCacheKey.from_data("demo.cache", {"customer": "alice"})
    options = CacheOptions(cache_levels=frozenset({CacheLevel.L4}))

    await cache_a.put(key, {"payload": "cached"}, CacheMetadata(), options=options)
    result = await cache_b.get(key, options=options)
    print("L4 fetch:", result.success)
```

## Server Integration

When `enable_l4_federation=True` the server wires a `ServerCacheTransport` that can address cache roles across clusters allowed by federation policy. Strict isolation limits cache propagation to the local cluster.

## Cache Profiles and Selection

Fabric cache federation now advertises cache node profiles (region, coordinates, capacity, utilization, latency, reliability) into the routing catalog. These profiles are used to pick replication targets for each cache operation based on `CacheMetadata.replication_policy` and `CacheMetadata.geographic_hints`.

Server instances publish profiles automatically alongside status updates. Custom deployments can publish profiles directly with `CacheProfileCatalogAdapter` if needed.

## Observability

Fabric cache operations are visible in:

- `cache_protocol.get_statistics()` (operation and conflict metrics)
- server metrics (`cache_metrics` in status payloads)

## Notes

- **No fixed ports**: tests/examples use the port allocator.
- **No legacy bridge**: all cache federation uses the unified fabric control plane.

## See Also

- `docs/CACHING_SYSTEM.md`
- `docs/ARCHITECTURE.md`
- `docs/MPREG_PROTOCOL_SPECIFICATION.md`
