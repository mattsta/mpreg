# persistence_kv (L1 · plane)

## Story

Config and small blobs need a simple KV with optional TTL — memory for tests,
SQLite for process restart.

## Lesson

`MemoryKeyValueStore` / `SQLitePersistenceBackend(db_path=Path(...))`.

## Run

```bash
uv run mpreg-example run persistence_kv
```

## Non-claims

- Not a full document DB.
- `db_path` must be `pathlib.Path` (F18).
