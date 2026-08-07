# DistLab T41 — retry_abort_ops_driven Honesty Cap (Official)

| Field                 | Value                                                          |
| --------------------- | -------------------------------------------------------------- |
| **Status**            | **Complete**                                                   |
| **Date**              | 2026-08-06                                                     |
| **Authority**         | Continuation after T40 (`cacd7f9`)                             |
| **Scope**             | Capability honesty: retry_abort is ops-driven, never auto-heal |
| **Entry points only** | `uv run pytest …`                                              |

## Delivered

- Cap `retry_abort_ops_driven=true` on status / config-check / OpenAPI
- Prom `mpreg_strong_cap_retry_abort_ops_driven` + honesty alert
- Doctor fail-closed if false; monitor `retry_ops=`

## Non-claims

Unchanged CFT limits. Cap is honesty only — not a product feature toggle.
