# DistLab T43 — CLI cache-strong-retry-abort (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T42 (`a1811fa`) |
| **Scope** | Operator CLI for ops-driven `strong_retry_abort` |
| **Point budget** | **~25 pts** |
| **Entry points only** | `uv run mpreg …` / `uv run pytest …` |

## Problem

T42 exposed RPC + `MPREGClient.cache_strong_retry_abort`, but operators still
need an entry-point CLI smoke for residual repair after network recovery
(consistent with `mpreg client cache-put` / `cache-get`).

## Goals

1. `mpreg client cache-strong-retry-abort --namespace … --key … --op-id …`
2. Optional `--peer` (repeatable), `--version`, `--timeout`, `--json`
3. Honesty banner: ops-driven CFT, not automatic heal / BFT
4. Non-zero exit when `success=False` (still-fail residual peers)
5. Residuals + docs (runbook, client guide, Phase 31, claims, ledger)

## Non-claims

Still CFT best-effort. Not automatic heal. Not residual-free while ABORT lost.
Not BFT/WAN. CLI is a thin client of the T42 RPC — not SIEM orchestration.
