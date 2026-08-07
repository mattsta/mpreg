# DistLab T19 — Ops Honesty, Refuse Scenario, Doctor Capabilities (Official)

| Field                 | Value                                       |
| --------------------- | ------------------------------------------- |
| **Status**            | **Complete (gated 225)**                    |
| **Date**              | 2026-08-06                                  |
| **Authority**         | Continuation after T18 complete (`415ba0f`) |
| **Scope**             | STRONG + shared audit + DistLab ops honesty |
| **Point budget**      | **~90 pts**                                 |
| **Entry points only** | `uv run mpreg …` / `uv run pytest …`        |

## Global rules

1. Prefer production correctness fixes discovered by tests.
2. Lab SLI / suite runners are not WAN SLA.
3. STRONG get/delete quorum remains v1.1 non-goal.
4. Never `python -m`.

## Stages

| Stage  | Exit                                                                 |
| ------ | -------------------------------------------------------------------- |
| T19-S0 | Official plan                                                        |
| T19-S1 | Doctor `--strong` surfaces capabilities + refuse counters in detail  |
| T19-S2 | Monitor strong compact human summary (capabilities line)             |
| T19-S3 | Builtin `strong.refuse_get_delete` scenario + smoke preset inclusion |
| T19-S4 | CLI help + unit/live tests                                           |
| T19-S5 | Docs, ledger, honesty Phase 7, gate, commit                          |

## Points

1–10. Plan.
11–30. Doctor semantic detail: capabilities + counters.
31–45. Monitor human-readable capabilities summary.
46–65. DistLab scenario + registry smoke preset update.
66–80. Tests (CLI, scenario, doctor live optional).
81–90. Docs/gate/commit.

## Non-claims

Unchanged from T18.
