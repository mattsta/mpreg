# DistLab T11+ — Ops, Observability, Perf, Live, Capability, Proof (Official)

| Field                 | Value                                                                      |
| --------------------- | -------------------------------------------------------------------------- |
| **Status**            | **Complete (T11–T16 gated)**                                               |
| **Date**              | 2026-08-06                                                                 |
| **Authority**         | Continuation after T1–T10 residual honesty + DistLab                       |
| **Scope**             | STRONG cache put + shared audit + DistLab — **not** whole-platform rewrite |
| **Point budget**      | 6 tracks × ~45–70 = **~340 pts**                                           |
| **Entry points only** | `uv run mpreg …` / `uv run pytest …` — never `python -m`                   |

## Global rules

1. Every track has stages with exit criteria and proof paths.
2. Prefer production metrics/health over test-only asserts.
3. Operator commands use monitoring HTTP (`MPREG_MONITORING_URL`).
4. Never claim WAN / Elle / BFT / fsync / kill-9 cold restart.
5. Gate after T14 and T16.

```bash
uv run mpreg distlab list
uv run pytest tests/testing/ tests/server_pkg/test_shared_audit*.py \
  tests/core/test_cache_strong*.py tests/integration/test_cache_strong*.py \
  tests/integration/test_shared_audit*.py tests/integration/test_strong_audit_coexistence.py \
  tests/chaos/test_strong_chaos_stress.py tests/chaos/test_shared_audit_chaos.py -q
```

---

# T11 — Observability product (60 pts)

## Stages

| Stage  | Exit                                 |
| ------ | ------------------------------------ |
| T11-S0 | Strong metrics on GCM                |
| T11-S1 | `/metrics/strong` + prometheus lines |
| T11-S2 | `/metrics/shared-audit` + prometheus |
| T11-S3 | `/mgmt/v1/strong` health snapshot    |
| T11-S4 | Unit + integration tests             |
| T11-S5 | Docs + gate                          |

## Points (summary)

1–12. `GlobalCacheManager.strong_metrics_snapshot()` — puts*ok/fail, refused_disabled, pending, enabled, replica settings, latency samples.
13–20. `build_strong_metrics(server)` + `build_shared_audit_metrics(server)` in monitoring_metrics.
21–28. Routes `/metrics/strong`, `/metrics/shared-audit`, `/mgmt/v1/strong`.
29–36. Prometheus: `mpreg_strong*_`, `mpreg*shared_audit*_`.
37–45. Wire providers from MPREGServer; openapi surface.
46–55. Tests scrape JSON + prom text.
56–60. OBSERVABILITY doc + claims support_only or proof list.

---

# T12 — Operator CLI + runbooks (55 pts)

## Stages

| Stage  | Exit                            |
| ------ | ------------------------------- |
| T12-S0 | `mpreg monitor strong`          |
| T12-S1 | `mpreg monitor audit`           |
| T12-S2 | `mpreg doctor --strong/--audit` |
| T12-S3 | Runbook section                 |
| T12-S4 | CLI tests                       |
| T12-S5 | Gate                            |

## Points

61–75. Click commands + JSON/table output.
76–85. Doctor probes new endpoints; non-zero on critical.
86–95. `docs/ops/STRONG_AND_SHARED_AUDIT_RUNBOOK.md`.
96–105. OBSERVABILITY_TROUBLESHOOTING links; PRODUCTION_DEPLOYMENT pointer.
106–115. CLI unit tests (httpx mock or live mini).

---

# T13 — Live DistLab expansion (55 pts)

## Stages

| Stage  | Exit                           |
| ------ | ------------------------------ |
| T13-S0 | 4-node strong live             |
| T13-S1 | Mid-put peer kill residual     |
| T13-S2 | Audit late joiner live         |
| T13-S3 | drop_abort in-process scenario |
| T13-S4 | Registry + tests               |
| T13-S5 | Gate                           |

## Points

116–140. Live scenarios in `test_distlab_live.py` + helpers.
141–155. `strong.drop_abort` builtin residual-free.
156–170. Gate live suite green.

---

# T14 — Performance SLIs (50 pts)

## Stages

| Stage  | Exit                              |
| ------ | --------------------------------- |
| T14-S0 | DistLab timing helper             |
| T14-S1 | In-process soak SLI bounds        |
| T14-S2 | Latency samples in strong metrics |
| T14-S3 | Tests                             |
| T14-S4 | Docs honesty (not WAN SLA)        |
| T14-S5 | Gate                              |

## Points

171–190. `sli.py` — wall timers, percentile helper.
191–205. Scenario meta records duration; soak_20 p99 soft bound in-process.
206–215. Metrics record put duration_ms samples (bounded ring).
216–220. non_claim: lab SLI ≠ production WAN SLA.

---

# T15 — Capability + property expansion (55 pts)

## Stages

| Stage  | Exit                                |
| ------ | ----------------------------------- |
| T15-S0 | Audit Hypothesis                    |
| T15-S1 | Strong commit-drop Hypothesis       |
| T15-S2 | Error-code taxonomy in history meta |
| T15-S3 | Useful GCM strong status API        |
| T15-S4 | Tests                               |
| T15-S5 | Gate                                |

## Points

221–245. Hypothesis audit converge / partition-heal.
246–255. Hypothesis commit drops residual-free.
256–265. `strong_status()` on GCM for operators/clients.
266–275. Registry all-audit suite test.

---

# T16 — Proof ledger + claims + full gate (50 pts)

## Stages

| Stage  | Exit                    |
| ------ | ----------------------- |
| T16-S0 | Proof ledger markdown   |
| T16-S1 | claims.yaml updates     |
| T16-S2 | Master plan cross-links |
| T16-S3 | Full related gate       |
| T16-S4 | Commit                  |
| T16-S5 | Clean tree              |

## Points

276–300. `docs/plans/DISTLAB_PROOF_LEDGER.md` point→test→claim.
301–320. claims proof paths + non_claims SLI honesty.
321–340. Gate + commit + status complete.

---

## Status dashboard

| Track             | Pts | Status   |
| ----------------- | --- | -------- |
| T11 Observability | 60  | complete |
| T12 Operator CLI  | 55  | complete |
| T13 Live expand   | 55  | complete |
| T14 Perf SLIs     | 50  | complete |
| T15 Capability    | 55  | complete |
| T16 Proof/gate    | 50  | complete |

## Implementation order

```
T11 → T12 → T13 → T14 → T15 → T16
```
