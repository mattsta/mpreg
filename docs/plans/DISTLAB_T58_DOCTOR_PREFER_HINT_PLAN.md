# DistLab T58 — Doctor prefers server residual_ops_hint (Official)

| Field                 | Value                                                                  |
| --------------------- | ---------------------------------------------------------------------- |
| **Status**            | **Complete**                                                           |
| **Date**              | 2026-08-06                                                             |
| **Authority**         | Continuation after T51/T53                                             |
| **Scope**             | `strong_residual_ops_hint` uses metrics-provided string when non-empty |
| **Point budget**      | **~8 pts**                                                             |
| **Entry points only** | `uv run pytest …`                                                      |

## Goals

1. Prefer `body["residual_ops_hint"]` when non-empty
2. Fall back to `format_residual_ops_hint` from peers/op_id
3. Residuals + Phase 46

## Non-claims

Preference is presentation consistency — not SIEM, not auto-heal.
