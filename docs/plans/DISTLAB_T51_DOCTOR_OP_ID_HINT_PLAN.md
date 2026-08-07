# DistLab T51 — Doctor op_id + residual ops hint (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T50 |
| **Scope** | Surface `last_abort_fail_op_id` + ops remediation hint on doctor/monitor when residual candidates present |
| **Point budget** | **~15 pts** |
| **Entry points only** | `uv run mpreg doctor …` / `uv run mpreg monitor strong …` / `uv run pytest …` |

## Goals

1. `evaluate_strong_doctor_payload` includes `abort_fail_op_id=`
2. When `abort_fail_peers` non-empty, append ops hint pointing at
   `mpreg client cache-strong-retry-abort` (CFT; not auto-heal)
3. `monitor strong` table/plain shows op_id + same hint
4. Residuals + Phase 39

## Non-claims

Hint is operator guidance after network recovery — not automatic heal, not
SIEM orchestration, not residual-free proof, not BFT/WAN.
