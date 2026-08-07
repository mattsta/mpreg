# DistLab T39 — retry_abort Prom/Doctor/OpenAPI Surface (Official)

| Field                 | Value                                                                             |
| --------------------- | --------------------------------------------------------------------------------- |
| **Status**            | **Complete**                                                                      |
| **Date**              | 2026-08-06                                                                        |
| **Authority**         | Continuation after T38 (`816d8c9`)                                                |
| **Scope**             | Prometheus series + doctor/monitor + OpenAPI for retry_abort counters; design doc |
| **Entry points only** | `uv run pytest …`                                                                 |

## Delivered

- Prom: `mpreg_strong_retry_abort_{calls,cleared,still_fail}_total`
- Doctor/monitor table: `retry_abort=` / `retry_cleared=`
- OpenAPI schema fields
- Design doc CFT exception mentions retry_abort ops path
- Runbook metric table

## Non-claims

Counters are process-local ops signals. Not automatic heal. Not residual-free proof.
