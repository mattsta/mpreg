# MPREG 0.3.0 Production Snapshot — Master Plan (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-07 |
| **Authority** | Official project planning for next public release milestone |
| **Architecture** | `docs/RELEASE_0_3_PRODUCTION_SNAPSHOT_ARCHITECTURE.md` |
| **Burndown** | `docs/plans/RELEASE_0_3_BURNDOWN.md` |
| **Proof ledger** | `docs/plans/RELEASE_0_3_PROOF_LEDGER.md` |
| **Version target** | `0.3.0` |
| **Point budget** | 7 tracks × ~20–35 pts = **~180 implementation/validation items** |

## Global rules

1. Residual honesty T140+ is **out of scope** unless a P0 product bug appears.
2. Prefer **scripts + CI + tests** over one-off manual steps.
3. Prefer **honesty** over aspirational marketing.
4. Use `uv run …` entry points only (never `python -m` in docs/scripts).
5. Track complete only when: burndown checkbox done, tests green, claims/ledger updated.
6. Non-claims freeze stands: not BFT/WAN/Jepsen/Elle/fsync-as-product/auto-heal/OAuth2-as-shipped.

## Gate command (full release)

```bash
bash scripts/release_gate.sh
```

Equivalent expanded (see script):

```bash
uv run ruff check mpreg tests
uv run mypy mpreg --pretty
bash scripts/ci_unit_fast.sh
bash scripts/ci_invariants.sh
bash scripts/ci_distlab_core.sh
bash scripts/run_demo_smoke.sh
bash scripts/ci_security_deps.sh
bash scripts/ci_package_smoke.sh
uv run pytest tests/release/ -q
```

---

# TRACK R1 — CI quality matrix (~30 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| R1-S0 | Script layout | `scripts/ci_*.sh` exist |
| R1-S1 | Workflow jobs | `.github/workflows/ci.yml` multi-job |
| R1-S2 | Unit-fast curation | stable under 10–15 min |
| R1-S3 | Invariants + DistLab core | green |
| R1-S4 | Security deps job | green or documented allow |
| R1-S5 | Gate | local `release_gate` partial R1 green |

## Points

1. `scripts/ci_lint.sh` — ruff check
2. `scripts/ci_typecheck.sh` — mypy mpreg
3. `scripts/ci_unit_fast.sh` — curated pytest
4. `scripts/ci_invariants.sh` — strong/audit/claims-related
5. `scripts/ci_distlab_core.sh` — list + happy_3 + preset smoke
6. `scripts/ci_security_deps.sh` — pip-audit or uv audit
7. Extend `.github/workflows/ci.yml` lint job
8. typecheck job
9. unit-fast job
10. invariants job
11. distlab-core job
12. security-deps job
13. Keep demo-smoke job
14. demo-suite optional/continue-on-error or separate workflow
15. Document CI matrix in architecture + ops README
16. `tests/release/test_r1_ci_scripts_exist.py`
17. unit-fast excludes slow/chaos/example_suite by default
18. CI uses uv sync + Python 3.14 (match current)
19. Fail-fast false across jobs (see all failures)
20. R1 burndown → complete when scripts + workflow committed

---

# TRACK R2 — Honesty surface (~25 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| R2-S0 | Inventory drift | list aspirational claims |
| R2-S1 | README fix | features match reality |
| R2-S2 | CHANGELOG 0.3.0 | user-facing |
| R2-S3 | claims.yaml release | proof + non_claims |
| R2-S4 | Cross-links | GETTING_STARTED / ARCHITECTURE |
| R2-S5 | Gate | honesty unit tests |

## Points

21. Inventory README lines: OAuth2, Million+, ML balancing, dashboards
22. Move unshipped items strictly under Roadmap
23. Rephrase throughput claims as lab/order-of-magnitude or remove
24. Strengthen top honesty banner with 0.3.0 snapshot pointer
25. CHANGELOG: replace stale Unreleased blob lead with `## [0.3.0]`
26. CHANGELOG: features (fabric, STRONG, audit, DistLab, ops)
27. CHANGELOG: non-goals / non_claims summary
28. CHANGELOG: upgrade notes (profiles, flags default off)
29. claims.yaml: INV or support claim “release 0.3.0 snapshot gates”
30. claims.yaml non_claims: release snapshot ≠ BFT/WAN/OAuth2 shipped
31. ARCHITECTURE.md pointer to release architecture
32. docs/DISTLAB or residual honesty: “release track separate”
33. GETTING_STARTED: link RELEASE checklist / 0.3.0
34. `tests/release/test_r2_readme_honesty.py` — no “OAuth2/OIDC Integration” as done feature
35. `tests/release/test_r2_changelog_030.py` — 0.3.0 section exists
36. README points to SECURITY.md
37. README points to PERF_BASELINE.md (after R5)
38. Freeze note: residual T140+ not required for tag
39–45. Buffer docs polish

---

# TRACK R3 — Security snapshot (~30 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| R3-S0 | SECURITY.md | disclosure + threat model |
| R3-S1 | config-check guards | mon token / strict |
| R3-S2 | Tests | config-check + docs |
| R3-S3 | TLS/mon docs | PRODUCTION + profiles |
| R3-S4 | Gate | R3 tests green |

## Points

46. Write `SECURITY.md` (threat model CFT trusted ops)
47. How to report vulnerabilities
48. Explicit non-promises (BFT, residual-free, etc.)
49. config-check: warn if monitoring_enabled and mon not loopback and no token
50. config-check: warn monitoring_enable_cors=true
51. config-check: `--strict` exits non-zero when critical warnings present
52. Critical set includes: change-me on federated, cors true, exposed mon without token
53. Test: federated change-me + strict → exit ≠ 0
54. Test: cors true → warning
55. Test: SECURITY.md exists and has threat model heading
56. PRODUCTION_DEPLOYMENT: security checklist bullet refresh
57. profiles/README: strict config-check before prod
58. Optional: SUPPORT.md one-pager or section inside SECURITY
59. claims non_claim: SECURITY.md ≠ pen-test certification
60. OpenAPI/docs note unchanged for mon bearer
61–75. Buffer: extra guards if easy (allow_unsigned already warned)

---

# TRACK R4 — Packaging & version (~20 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| R4-S0 | Version bump | 0.3.0 |
| R4-S1 | Metadata | URLs/classifiers |
| R4-S2 | Package smoke script | wheel install |
| R4-S3 | Gate | ci_package_smoke green |

## Points

76. `pyproject.toml` version = 0.3.0
77. project.urls Homepage/Repository/Documentation/Changelog
78. classifiers (Python, License, Typing)
79. `scripts/ci_package_smoke.sh` — uv build + install wheel + mpreg --help
80. Test version string readable from importlib.metadata or pyproject
81. README install snippet uses 0.3.0 / uv
82. Confirm LICENSE Apache-2.0 referenced
83. hatch include profiles + ops alerts
84. Document tag procedure in RELEASE_CHECKLIST (do not push tag in CI without human)
85–95. Buffer

---

# TRACK R5 — Performance evidence (~20 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| R5-S0 | PERF_BASELINE.md | written |
| R5-S1 | Repro command | script or pytest path |
| R5-S2 | Honesty | no false Million+ without caveat |
| R5-S3 | Gate | doc + light test |

## Points

96. `docs/ops/PERF_BASELINE.md` structure
97. Topology: 1-node lab defaults
98. Commands: uv run pytest tests/performance/… or dedicated smoke
99. Non-claims: not WAN SLA; hardware-dependent
100. Link from PRODUCTION_DEPLOYMENT + README
101. `scripts/ci_perf_smoke.sh` optional short path (may be nightly)
102. test_r5_perf_baseline_doc_exists
103. Soft thresholds only if automated
104–115. Buffer

---

# TRACK R6 — Ops golden path (~20 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| R6-S0 | RELEASE_CHECKLIST.md | written |
| R6-S1 | PRODUCTION cross-link | done |
| R6-S2 | ops README | release pointer |
| R6-S3 | Gate | doc tests |

## Points

116. `docs/ops/RELEASE_CHECKLIST.md` pre-tag steps
117. Golden path 6 steps (architecture §10)
118. Link prometheus_alerts.yml
119. Link doctor/monitor residual honesty (not heal)
120. mpreg/ops/README.md release pointer
121. PRODUCTION_DEPLOYMENT observability checklist refresh
122. test_r6_release_checklist_exists
123–135. Buffer

---

# TRACK R7 — Full gate & freeze (~25 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| R7-S0 | release_gate.sh | all tracks |
| R7-S1 | claims + ledger complete | done |
| R7-S2 | Master/burndown status | Complete |
| R7-S3 | Commit | clean tree |

## Points

136. `scripts/release_gate.sh` orchestrates all ci_* + release tests
137. claims.yaml R0.3 proof list complete
138. RELEASE_0_3_PROOF_LEDGER.md rows for R1–R7
139. Burndown all `[x]`
140. Master plan status → Complete
141. Architecture status → Complete
142. Cross-link residual honesty: release milestone separate
143. `tests/release/test_r7_gate_artifacts.py`
144. Run full release_gate locally green
145. Final commit message for 0.3.0 milestone work
146–180. Buffer / fixups from gate failures

---

## Status dashboard

| Track | Pts | Status |
| --- | --- | --- |
| R1 CI quality | ~30 | **complete** |
| R2 Honesty | ~25 | **complete** |
| R3 Security | ~30 | **complete** |
| R4 Packaging | ~20 | **complete** |
| R5 Performance | ~20 | **complete** |
| R6 Ops path | ~20 | **complete** |
| R7 Gate/freeze | ~25 | **complete** |

**Total: ~180 pts. All tracks Complete. `scripts/release_gate.sh` exits 0 (2026-08-07).**

## Implementation order (mandatory)

```
R1 → R2 → R3 → R4 → R5 → R6 → R7
```

R2 may start docs in parallel with R1 scripts; **merge order** keeps R1 first so honesty tests run in CI.

## Bug-fix policy

1. Confirm in-scope for 0.3.0 (not residual T140 micro-polish).
2. Fix product/docs/CI.
3. Add `tests/release/` regression.
4. Check off burndown point.

## Relationship to DistLab residual track

| Track family | Status |
| --- | --- |
| DistLab T1–T7 | Complete |
| Residual honesty T17–T139 | Complete (diminishing returns) |
| **Release 0.3.0 R1–R7** | **This plan** |
| Residual T140+ | Deferred post-tag |
