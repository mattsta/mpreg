# DistLab T53 — residual_ops_hint on metrics JSON (Official)

| Field                 | Value                                                                                   |
| --------------------- | --------------------------------------------------------------------------------------- |
| **Status**            | **Complete**                                                                            |
| **Date**              | 2026-08-06                                                                              |
| **Authority**         | Continuation after T51                                                                  |
| **Scope**             | Machine-readable `residual_ops_hint` on `/metrics/strong`, GCM `strong_status`, OpenAPI |
| **Point budget**      | **~15 pts**                                                                             |
| **Entry points only** | `uv run pytest …`                                                                       |

## Goals

1. `format_residual_ops_hint` in `cache_strong` (shared with CLI doctor)
2. `strong_status` / `build_strong_metrics` expose `residual_ops_hint`
3. OpenAPI documents the field with honesty wording
4. Client guide ops loop documents metrics → hint → CLI
5. Residuals + Phase 41

## Non-claims

Hint string is operator guidance — not automatic heal, not SIEM, not BFT/WAN.
Empty when no residual candidates.
