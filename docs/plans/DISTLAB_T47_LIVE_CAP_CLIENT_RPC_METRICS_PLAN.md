# DistLab T47 — Live prom cap + client RPC metrics e2e (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T46 (`147ff2b`) |
| **Scope** | Live scrape asserts `mpreg_strong_cap_retry_abort_ops_driven==1`; client RPC retry path on metrics e2e |
| **Point budget** | **~20 pts** |
| **Entry points only** | `uv run pytest …` |

## Goals

1. Live metrics e2e scrapes `mpreg_strong_cap_retry_abort_ops_driven` == 1
2. Client `cache_strong_retry_abort` noop on live mesh after put
3. `/metrics/strong` capabilities.retry_abort_ops_driven true after client path
4. Residuals + Phase 35 honesty

## Non-claims

Cap gauge is honesty (always 1) — not a toggle for automatic heal.
Client noop with empty peers is not residual clear proof (that is T44).
