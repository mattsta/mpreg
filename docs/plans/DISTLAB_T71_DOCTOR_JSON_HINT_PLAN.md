# DistLab T71 — Doctor JSON residual_ops_hint field (Official)

| Field                 | Value                                                                 |
| --------------------- | --------------------------------------------------------------------- |
| **Status**            | **Complete**                                                          |
| **Date**              | 2026-08-06                                                            |
| **Authority**         | Continuation after T51/T58/T60                                        |
| **Scope**             | doctor metrics_strong/mgmt_strong JSON rows include residual_ops_hint |
| **Point budget**      | **~10 pts**                                                           |
| **Entry points only** | `uv run mpreg doctor --strong` / `uv run pytest …`                    |

## Goals

1. Doctor JSON rows for strong checks always include `residual_ops_hint`
   (empty string when no residual candidates)
2. Unit test mirrors row shape + enriched ns/key path
3. Residuals + Phase 59

## Non-claims

Doctor JSON field is the same operator guidance string — not auto-heal toggle,
not SIEM orchestration, not residual-free proof.
