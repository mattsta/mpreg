# DistLab T46 — Client locs pin for strong_retry_abort (Official)

| Field                 | Value                                               |
| --------------------- | --------------------------------------------------- |
| **Status**            | **Complete**                                        |
| **Date**              | 2026-08-06                                          |
| **Authority**         | Continuation after T45 (`6189cd3`)                  |
| **Scope**             | Optional routing pin for ops retry_abort client/CLI |
| **Point budget**      | **~15 pts**                                         |
| **Entry points only** | `uv run pytest …` / `uv run mpreg …`                |

## Problem

T44 showed unpinned `mpreg.cache.strong_retry_abort` may execute on any node
with resource `cache`. Self-target local abort fixes correctness; operators
still want to pin the call to a preferred coordinator (e.g. put origin) when
scraping that node's `retry_abort_*` counters.

## Goals

1. `MPREGClient.cache_strong_retry_abort(..., locs=…)`
2. CLI `--loc` (repeatable)
3. Docs + residuals (still CFT; pin ≠ residual-free / ≠ auto-heal)

## Non-claims

`locs` is ordinary RPC resource routing — not quorum membership, not BFT, not
guarantee the residual peer is excluded unless the pin excludes it.
