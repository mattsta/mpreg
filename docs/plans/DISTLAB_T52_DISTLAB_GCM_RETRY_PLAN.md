# DistLab T52 — DistLab GCM.strong_retry_abort scenario (Official)

| Field                 | Value                                                                   |
| --------------------- | ----------------------------------------------------------------------- |
| **Status**            | **Complete**                                                            |
| **Date**              | 2026-08-06                                                              |
| **Authority**         | Continuation after T51                                                  |
| **Scope**             | First-class DistLab scenario for product library GCM.strong_retry_abort |
| **Point budget**      | **~15 pts**                                                             |
| **Entry points only** | `uv run pytest …` / DistLab suite presets                               |

## Goals

1. `strong.cft_gcm_retry_abort_clears_residual` — residual → GCM clear + counters
2. Registered + in `strong-core` / `ci-core`
3. Residuals + Phase 40

## Non-claims

GCM path is the same ops-driven CFT best-effort as coordinator retry_abort —
not automatic heal, not BFT, not WAN, not SIEM.
