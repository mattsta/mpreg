# cache_replication_geo (L1 · plane)

Geo hints, replication strategy enum, L2 persistent config, pattern invalidate.

```bash
uv run mpreg-example run cache_replication_geo
```

## Proves

- `cache.geo_hints` / `cache.replication` on `CacheMetadata` + `CacheReplicationPolicy`
- `cache.l2` / `pers.cache_l2` via `enable_l2_persistent`
- `pers.mode` — `PersistenceMode.MEMORY` / `SQLITE`
- `cache.invalidate` pattern + F8 keyword-only contract

## Non-claims

- Live multi-region replica quorum under partition
