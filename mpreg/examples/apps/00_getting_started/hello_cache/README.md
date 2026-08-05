# hello_cache (L0)

## Story

Single-process cache put/get for a session-shaped key.

## Lesson

GlobalCacheKey + GlobalCacheManager happy path.

## Run

```bash
uv run mpreg-example run hello_cache
```

## What it proves

- Put then get returns the same session payload

## Architecture

```text
Client → GlobalCacheManager (L1)
```

## Non-claims

- Not multi-node federation; not persistent across process restart.

## Production exit ramp

- Next: session_cache, plane_cache
- Production: enable L2 SQLite + profiles
