# DistLab T48 — OpenAPI platform cache RPC catalog (Official)

| Field                 | Value                                                                        |
| --------------------- | ---------------------------------------------------------------------------- |
| **Status**            | **Complete**                                                                 |
| **Date**              | 2026-08-06                                                                   |
| **Authority**         | Continuation after T47                                                       |
| **Scope**             | Document `mpreg.cache.*` FQNs incl. strong_retry_abort in OpenAPI components |
| **Point budget**      | **~15 pts**                                                                  |
| **Entry points only** | `uv run pytest …`                                                            |

## Goals

1. `PlatformCacheRpcCatalog` schema in monitoring OpenAPI
2. Includes `mpreg.cache.strong_retry_abort` with honesty flags
3. Tag `platform-rpc`; `x-mpreg-platform-rpc.cache` pointer
4. Residuals + Phase 36

## Non-claims

Catalog is documentation of wire FQNs — not an HTTP invoke path, not SIEM
orchestration, not auto-heal. Handlers remain source of truth.
