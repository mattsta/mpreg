# DistLab T23 — Live Audit Caps E2E + Config-Check Parity (Official)

| Field                 | Value                                                                 |
| --------------------- | --------------------------------------------------------------------- |
| **Status**            | **Complete (gated 242)**                                              |
| **Date**              | 2026-08-06                                                            |
| **Authority**         | Continuation after T22 complete (`8e78d90`)                           |
| **Scope**             | Close the loop: live HTTP + config-check advertise same audit honesty |
| **Point budget**      | **~70 pts**                                                           |
| **Entry points only** | `uv run mpreg …` / `uv run mpreg-example …` / `uv run pytest …`       |

## Global rules

1. Prefer production correctness fixes discovered by tests.
2. Lab SLI / suite runners are not WAN SLA.
3. Shared audit is not SIEM / BFT / infinite retention / linearizable cluster ops.
4. STRONG get/delete quorum remains v1.1 non-goal.
5. Never `python -m`.

## Stages

| Stage  | Exit                                                                      |
| ------ | ------------------------------------------------------------------------- |
| T23-S0 | Official plan                                                             |
| T23-S1 | `config-check` `shared_audit.capabilities` parity with metrics            |
| T23-S2 | Live `/metrics/shared-audit` + doctor e2e assert capabilities + evaluator |
| T23-S3 | `claims.yaml` INV-SHARED-AUDIT-01 capability honesty note                 |
| T23-S4 | Tests + full related gate + docs Phase 11 + commit                        |

## Non-claims

Unchanged: not WAN / Elle / Jepsen / BFT / fsync / STRONG quorum get-delete / SIEM.
