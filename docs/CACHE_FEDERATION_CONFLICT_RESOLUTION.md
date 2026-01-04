# Fabric Cache Conflict Resolution

Fabric cache federation uses vector clocks and deterministic resolution to handle conflicting updates. The conflict logic lives in `mpreg/fabric/cache_federation.py` and is applied during cache operation processing.

## Fabric Context

Cache conflict resolution is part of the unified fabric control plane. There is
no legacy cache bridge; all conflict handling flows through the fabric cache
protocol and its catalog-advertised replication paths.

## Strategies

- **Vector Clock** (default): compare vector clocks; pick causally newer entries.
- **Last Writer Wins**: resolve by timestamp if clocks are incomparable.
- **Most Recent Access**: prefer entries with more recent access time.
- **Highest Quality**: use `CacheMetadata.quality_score` as tiebreaker.

## How It Works

1. Each cache operation increments a local vector clock.
2. Incoming operations are merged into the local clock.
3. If an entry already exists, conflicts are detected by comparing clocks.
4. The configured strategy resolves to a single entry and updates cache state.

## Testing Guidance

Prefer fabric-level tests:

- Unit: `FabricCacheProtocol` conflict detection + resolution.
- Integration: multi-node cache propagation using `ServerCacheTransport` or `InProcessCacheTransport`.

Legacy cache federation bridge tests were replaced by fabric federation coverage.

## See Also

- `docs/CACHE_FEDERATION_GUIDE.md`
- `docs/CACHING_SYSTEM.md`
