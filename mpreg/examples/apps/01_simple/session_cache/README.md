# session_cache (L1)

## Story

Session store with put/get and in-place role rotation.

## Lesson

Cache as a fast session plane with TTL metadata.

## Run

```bash
uv run mpreg-example run session_cache
```

## What it proves

- Initial session hit
- Rotated roles visible on next get

## Architecture

```text
App → GlobalCacheManager
```

## Non-claims

- TTL expiry race not demonstrated here.
- Not sticky multi-region sessions.

## Production exit ramp

- Next: feature_flag_mesh
- Production: L2 persistence + HA client
