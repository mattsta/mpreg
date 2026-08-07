# DistLab T40 — Live retry_abort Metrics E2E (Official)

| Field                 | Value                                                                     |
| --------------------- | ------------------------------------------------------------------------- |
| **Status**            | **Complete**                                                              |
| **Date**              | 2026-08-06                                                                |
| **Authority**         | Continuation after T39 (`258ae3c`)                                        |
| **Scope**             | Live same-host e2e for retry_abort counters + prom scrape; OPERATE polish |
| **Entry points only** | `uv run pytest …`                                                         |

## Delivered

- `test_distlab_live_strong_metrics_e2e` asserts retry counters + prom series
- Live `strong_retry_abort` noop bumps `retry_abort_calls`
- OPERATE documents retry monitor/prom fields

## Non-claims

Same-host only. No WAN drop of ABORT on live mesh in this track (in-process
DistLab covers residual + retry clear). Not automatic heal.
