# DistLab T66 — Curriculum config-check residual_ops_hint assert (Official)

| Field                 | Value                                                                  |
| --------------------- | ---------------------------------------------------------------------- |
| **Status**            | **Complete**                                                           |
| **Date**              | 2026-08-06                                                             |
| **Authority**         | Continuation after T65                                                 |
| **Scope**             | ops_cli_tour config-check --explain asserts residual_ops_hint ops loop |
| **Point budget**      | **~8 pts**                                                             |
| **Entry points only** | `uv run mpreg-example …` / `uv run pytest …`                           |

## Goals

1. Curriculum `ops_cli_tour` ensures explain output mentions residual_ops_hint /
   cache-strong-retry-abort and CFT honesty (not auto-heal / ops-driven)
2. Residuals + Phase 54

## Non-claims

Curriculum assert is teachable guidance — not live residual clear, not auto-heal.
