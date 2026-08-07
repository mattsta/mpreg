# DistLab T56 — strong-core membership gate (Official)

| Field                 | Value                                                                       |
| --------------------- | --------------------------------------------------------------------------- |
| **Status**            | **Complete**                                                                |
| **Date**              | 2026-08-06                                                                  |
| **Authority**         | Continuation after T52                                                      |
| **Scope**             | Registry test that strong-core/ci-core include full CFT retry_abort surface |
| **Point budget**      | **~10 pts**                                                                 |
| **Entry points only** | `uv run pytest …`                                                           |

## Goals

1. `test_strong_core_includes_retry_abort_ops_scenarios` asserts required names
2. Factories registered in DistLab registry
3. Residuals + Phase 44

## Non-claims

Preset membership is CI gate coverage — not Jepsen, not WAN, not residual-free.
