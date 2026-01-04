# Unified Persistence Framework Plan

## Goals

- Provide a single, well-encapsulated persistence layer shared by cache, queues,
  routing state, and control-plane metadata.
- Ensure crash safety with durable writes, atomic commits, and restart recovery.
- Keep high performance via WAL-backed SQLite, batch writes, and compact
  snapshots.
- Expose ergonomic configuration and startup restore with explicit data
  directories and defaults.

## Current Audit (What Exists Today)

- **Cache**: `GlobalCacheManager` supports L2 persistent cache on disk
  (`persistent_cache_dir`), but the implementation is simplified and not a
  crash-safe store.
- **Queues**: `MessageQueue` and `MessageQueueManager` are in-memory only.
- **Raft**: `datastructures/raft_storage_adapters.py` provides an extensible
  storage protocol with SQLite support (WAL) for Raft state/snapshots.
- **Blockchain**: `datastructures/blockchain_store.py` persists blockchains in
  SQLite.
- **Config**: `MPREGSettings` is a dataclass; there is federation JSON config
  for CLI/auto-discovery, but no unified settings loader for core services.

## Target Architecture

### 1) Core Interfaces (Single Source of Truth)

```
mpreg/core/persistence/
  - backend.py           # StorageBackend protocol + SQLiteBackend
  - kv_store.py          # KeyValueStore (versioned, TTL, namespaces)
  - log_store.py         # Append-only log for queue + event persistence
  - snapshot_store.py    # Snapshot + chunked blobs
  - registry.py          # PersistenceRegistry + lifecycle hooks
```

**StorageBackend (protocol)**

- `open()`, `close()`
- `transaction()` context
- `put/get/delete` for key-value
- `append/read_range` for logs
- `save_snapshot/load_snapshot`
- `stats()`, `compact()`

### 2) Subsystem Integrations

- **Cache**: Replace ad-hoc L2 file storage with `KeyValueStore` + TTL indexes.
- **Queue**: Persist in-flight + pending messages via `LogStore` with replay on
  startup; support ACK state and dead-letter persistence.
- **Route Keys / Policies**: store key registry snapshots in `KeyValueStore`.
- **Fabric Catalog**: optional snapshot + replay to speed restart.
- **Consensus/Raft**: unify adapters so Raft reuses the same backend (SQLite).

### 3) Configuration + Bootstrapping

- Add `PersistenceConfig` to `MPREGSettings`:
  - `enabled`, `data_dir`, `backend` (sqlite/file/memory)
  - WAL + fsync settings
  - retention/compaction thresholds
- Add `MPREGSettings.from_toml()` + `from_json()` using stdlib `tomllib`/`json`.
- Make `mpreg/cli` accept a unified settings file for all services.

## Crash Safety + Performance

- SQLite with WAL mode, batched transactions, and checkpointing.
- Deterministic snapshot versioning and checksums on each write.
- Explicit replay order on startup (queue logs → cache entries → catalog).
- Background compaction with size and time thresholds.

## Testing Plan

- **Unit**: backend contracts, atomic transactions, TTL expiry, WAL recovery.
- **Integration**:
  - Queue restart recovery (pending + in-flight reconciliation).
  - Cache restart recovery (L2 entries restored, TTL respected).
  - Fabric route keys and catalog snapshot replay.
- **E2E**: multi-node restart tests with live servers and persisted queues.

## Implementation Phases

1. **Persistence core**: add `mpreg/core/persistence` + SQLite backend. ✅
2. **Cache integration**: replace L2 file store with `KeyValueStore`. ✅
3. **Queue integration**: add persistent queue store + restart restore. ✅
4. **Config loader**: TOML/JSON settings + CLI wiring. ✅ (basic)
5. **Fabric metadata**: optional snapshots for catalog + route keys. ✅
6. **Docs + examples**: update quickstarts and production guides. ✅ (initial)

## Current Status

- Core persistence layer implemented with memory + SQLite backends.
- Cache L2 now uses the persistence key/value store when configured.
- Queue persistence added with restart restoration support.
- Initial docs updated for cache + queue persistence configuration.
- Integration tests added for server-level queue + cache restart recovery.
- Settings loader added (TOML/JSON + CLI start-config).
- Fabric catalog + route key snapshots added via persistence store.
- Monitoring endpoint exposes persistence snapshot status.
