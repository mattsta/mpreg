# DistLab T42 — Client/RPC strong_retry_abort Surface (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T41 (`9479463`) |
| **Scope** | Expose ops-driven `strong_retry_abort` on platform RPC + MPREGClient |
| **Point budget** | **~40 pts** |
| **Entry points only** | `uv run pytest …` / `uv run mpreg …` |

## Problem

`GlobalCacheManager.strong_retry_abort` is library-only. Operators using
`MPREGClient` / plane RPC cannot re-deliver ABORT after recovery without
in-process access.

## Goals

1. `PlatformRpc.CACHE_STRONG_RETRY_ABORT` (`mpreg.cache.strong_retry_abort`)
2. Plane handler → `manager.strong_retry_abort`
3. `MPREGClient.cache_strong_retry_abort(...)`
4. Promote `quorum_info` / `operation_id` on `CacheOpResult` for STRONG put
5. Tests + docs honesty (ops-driven, not auto-heal)

## Non-claims

Still CFT best-effort. Not automatic heal. Not residual-free while ABORT lost.
Not BFT/WAN. RPC is best-effort ops path, not SIEM orchestration.
