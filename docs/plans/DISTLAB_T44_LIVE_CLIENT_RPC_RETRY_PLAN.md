# DistLab T44 — Live client RPC strong_retry_abort e2e (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T43 (`aac82c0`) |
| **Scope** | Live multi-server proof that client/RPC retry_abort clears residual |
| **Point budget** | **~30 pts** |
| **Entry points only** | `uv run pytest …` |

## Problem

T42/T43 proved plane handler + client façade + CLI with mocks / in-process
transport. Operators still need a **live mesh** proof that
`MPREGClient.cache_strong_retry_abort` over `ServerCacheTransport` clears a
seeded peer residual and bumps GCM retry counters.

## Goals

1. Integration test on 3-node live STRONG mesh
2. Seed CFT residual via peer `prepare`+`commit` (lost-ABORT stand-in)
3. Clear via `MPREGClient.cache_strong_retry_abort` → platform RPC
4. Assert `ops_driven` / not `automatic_heal`; GCM `retry_abort_*` counters
5. Fix `retry_abort` self-target: RPC landing on residual peer must local-abort
6. Docs Phase 32 + ledger + residuals

## Product fix (self-target)

Unpinned `mpreg.cache.strong_retry_abort` may execute on any node with resource
`cache`, including the residual peer. Filtering `peers=[self]` to empty and
skipping `local.abort` left residuals. `StrongPutCoordinator.retry_abort` now
always runs `local.abort` and counts self when explicitly targeted.

## Non-claims

Still CFT best-effort. Not automatic heal. Not residual-free under continued
ABORT loss. Not BFT/WAN/Jepsen. Seeded residual is a lab stand-in for partial
COMMIT + lost ABORT — not kernel partition proof.
