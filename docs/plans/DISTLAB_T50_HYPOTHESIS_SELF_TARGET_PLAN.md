# DistLab T50 — Hypothesis self-target + residual gate (Official)

| Field                 | Value                                                                                     |
| --------------------- | ----------------------------------------------------------------------------------------- |
| **Status**            | **Complete**                                                                              |
| **Date**              | 2026-08-06                                                                                |
| **Authority**         | Continuation after T49                                                                    |
| **Scope**             | Property test peers=[self] clears residual; residual honesty Phase 38; full residual gate |
| **Point budget**      | **~15 pts**                                                                               |
| **Entry points only** | `uv run pytest …`                                                                         |

## Goals

1. Hypothesis `test_cft_retry_abort_self_target_clears_local` (n∈[3,7])
2. Residual tests for DistLab scenario + preset + docs Phase 37/38
3. Claims / ledger / runbook / OPERATE / CACHING_SYSTEM honesty
4. Full residual + property gate green

## Non-claims

Property proves local.abort on self-target under in-process transport — not
kernel partition, not kill -9, not WAN, not automatic background heal.
