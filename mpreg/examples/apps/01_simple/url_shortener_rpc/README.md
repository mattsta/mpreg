# url_shortener_rpc (L1)

## Story

Tiny URL shortener: shorten + resolve RPC with a cache mirror of codes.

## Lesson

CRUD-shaped RPC plus optional cache for hot keys.

## Run

```bash
uv run mpreg-example run url_shortener_rpc
```

## What it proves

- shorten returns stable code
- resolve returns original URL
- cache holds the mapping

## Architecture

```text
Client → RPC (shorten/resolve) + GlobalCache
```

## Non-claims

- Not multi-tenant auth; not durable DB.

## Production exit ramp

- Next: session_cache, order_intake
