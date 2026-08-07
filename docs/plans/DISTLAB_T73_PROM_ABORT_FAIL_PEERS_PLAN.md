# DistLab T73 — Prometheus abort_fail_peers gauge (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T36/T72 |
| **Scope** | mpreg_strong_abort_fail_peers gauge = len(last_abort_fail_peers) |
| **Point budget** | **~8 pts** |
| **Entry points only** | Prometheus scrape / residual gates |

## Goals

1. Gauge exposes CFT residual candidate count (process-local)
2. HELP text documents not residual-free / not automatic heal
3. Residuals + Phase 61

## Non-claims

Gauge is ops signal only — not residual-free proof, not auto-heal, not WAN SLO.
