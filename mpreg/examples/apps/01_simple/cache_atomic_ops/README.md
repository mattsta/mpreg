# cache_atomic_ops (L1 · plane)

## Story

Operators need counters, compare-and-swap flags, and bulk namespace hygiene
without client-side read-modify-write races. This app drills
`AdvancedCacheOperations` on a live `GlobalCacheManager`.

## Lesson

Atomic and structure ops are first-class cache APIs — not ad-hoc JSON patches.

## Run

```bash
uv run mpreg-example run cache_atomic_ops
```

## What it proves

- `INCREMENT` on missing key starts at 0
- `COMPARE_AND_SWAP` succeeds on match and fail-closes on stale expected
- `TEST_AND_SET` + `if_not_exists` creates once
- Set `add` and list `append` structure ops
- Namespace `count` / `list` / `clear` scoped to one namespace

## Architecture

- In-process `GlobalCacheManager` + `FabricCacheProtocol` transport (local only)
- `AdvancedCacheOperations` wrapper for atomic / structure / namespace APIs

## API drill-down

| Call | Feature ID |
|------|------------|
| `atomic_operation(INCREMENT\|CAS\|TEST_AND_SET)` | `cache.atomic` |
| `data_structure_operation(SET/LIST)` | `cache.structures` |
| `namespace_operation(count/list/clear)` | `cache.namespace_ops` |

## Non-claims

- Not multi-node linearizable CAS across clusters (single manager).
- Not third-party cache wire protocol compatibility.
- Does not demo `cache.pubsub_events` (separate app).

## Production exit ramp

- Attach advanced ops beside server default cache (`enable_default_cache`).
- Next: `session_cache`, `feature_flag_mesh`, `plane_cache`.
- See `docs/examples-curriculum/FEATURE_CATALOG.md` cache family.
