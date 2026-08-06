# DistLab Entry-Point Architecture + Gap Closure Plan (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** (T8–T10 gated 2026-08-06) |
| **Date** | 2026-08-06 |
| **Authority** | Continuation of `DISTLAB_SEVEN_TRACK_MASTER_PLAN.md` |
| **Why** | (1) Architecture forbids `python -m`; all user/dev tools are pyproject entry points. (2) Seven-track ~420 pts was marked complete early — remaining scenario matrix + entry-point integration must finish. |
| **Point budget** | **T8 entry points (40)** + **T9 gap closure (75)** + **T10 full validation (35)** = **150 pts** |

## Architecture rule (non-negotiable)

| Allowed | Forbidden |
| --- | --- |
| `uv run mpreg …` | `python -m mpreg…` |
| `uv run mpreg-example …` | `uv run python -m …` |
| `uv run pytest …` | `uv run python -m pytest …` |
| `uv run mpreg distlab …` | `python -m mpreg.testing.distlab` |
| `uv run mpreg test concurrent …` | `python -m mpreg.testing.concurrent_runner` |

`__main__.py` under DistLab **must refuse** with exit 2 and point at `uv run mpreg distlab`.

Entry points live in `pyproject.toml` → `[project.scripts]`:

```toml
mpreg = "mpreg.cli:main"
mpreg-example = "mpreg.examples.apps._shared.runner:main"
```

DistLab is a **subcommand group** of `mpreg`, not a second script (single top-level CLI surface).

---

# TRACK T8 — Entry-point integration (40 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| T8-S0 | DistLab CLI module | `mpreg/testing/distlab/cli.py` |
| T8-S1 | `mpreg distlab` group | wired in `mpreg/cli/main.py` |
| T8-S2 | Concurrent runner entry | `mpreg test concurrent` |
| T8-S3 | Block `python -m` | `__main__.py` exit 2 |
| T8-S4 | Scrub docs/tests | zero DistLab `python -m` usage docs |
| T8-S5 | Gate | CLI tests green |

## Points

1. `distlab/cli.py` list/catalog/run pure functions.
2. Help text / epilog cite non_claims.
3. Prog name `mpreg distlab` (not python -m).
4. `@cli.group("distlab")` on main CLI.
5. `mpreg distlab list [--track] [--json]`.
6. `mpreg distlab catalog [--json]`.
7. `mpreg distlab run NAME [--json]` exit 0/1/2.
8. Root CLI docstring lists distlab entry.
9. `@cli.group("test")` + `concurrent` command.
10. Concurrent runner prog string updated.
11. `mpreg/testing/__init__.py` comment no python -m.
12. DistLab `__main__.py` blocks with message.
13. Package `__init__` docstring uses `uv run mpreg distlab`.
14. Master plan CLI lines updated.
15. DISTLAB_AND_SEVEN_TRACKS CLI line updated.
16. ARCHITECTURE DistLab section updated.
17. RESIDUAL_HONESTY CLI line updated.
18. TOPOLOGY / guides: `uv run pytest` not python -m pytest.
19. Registry tests call `uv run mpreg distlab`.
20. Test asserts `python -m mpreg.testing.distlab` exit 2.
21. README DistLab pointer mentions `uv run mpreg distlab`.
22. claims.yaml proof paths unchanged (still tests).
23. No new `[project.scripts]` unless needed (prefer mpreg subcommand).
24. `uv run mpreg distlab --help` works.
25. `uv run mpreg distlab list` non-empty.
26. `uv run mpreg distlab run strong.happy_3` PASS.
27. `uv run mpreg test concurrent --help` works.
28. concurrent_runner docstring entry-point only.
29. scripts/run_demo_suite.sh policy already entry-only (no change needed).
30. examples README policy already entry-only.
31–40. Gate + scrub sweep + complete status.

---

# TRACK T9 — Seven-track gap closure (75 pts)

Remaining items from master plan not fully scenario-covered.

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| T9-S0 | Inventory gaps vs registry | checklist |
| T9-S1 | STRONG missing scenarios | registered + tested |
| T9-S2 | Adversarial matrix rows | registered + tested |
| T9-S3 | Audit residual scenarios | registered + tested |
| T9-S4 | Live expand | optional 4-node / disabled 1012 |
| T9-S5 | T6 regressions | reg_* scenarios |
| T9-S6 | Gate | all registry strong/audit pass |

## Points

### STRONG gaps (master T2)
41. `strong.single_node` min_replicas=1.
42. `strong.sequential_lww` last value wins.
43. `strong.partition_one_peer` still success.
44. `strong.delay_beyond_timeout` fail residual.
45. `strong.crash_recover` crash one + recover put.
46. `strong.soak_50` already registered — exercise in suite.
47. `strong.pending_full_recover` max_pending path.
48. Registry run all `strong.*` (strict except not_bft).
49. Hypothesis commit-drop residual (expand if missing).
50. Nemesis soak sequential body scenario optional.

### Adversarial (master T3)
51. `strong.lie_commit_single_peer`.
52. `strong.not_bft_lie_commit_both` remains not_bft.
53. fail_prepare + wrong_cluster already registered.
54. Malice clear restores happy (interleaved).
55. Handler unit suite still green (prior tests).
56–60. claims non_claims already list DistLab/BFT.

### Audit (master T4)
61. burst_100 registry.
62. duplicate / ineligible / digest / nemesis registry.
63. All `audit.*` registry run green.
64–65. Prior chaos audit green.

### Live (master T5)
66. live happy / multi-key / peer-loss / audit / coex green.
67. live disabled 1012 optional if cheap.
68. port_range_context only.
69–70. Live helpers exported via harness.

### T6 regressions
71. `reg_expired_commit`.
72. `strong.pending_full_recover`.
73. Peer L1 / gossip fixes remain covered by integration.
74–75. T9 complete in status.

---

# TRACK T10 — Full validation + commit (35 pts)

76. `uv run mpreg distlab list` smoke.
77. `uv run mpreg distlab run strong.happy_3` smoke.
78. `uv run mpreg distlab run audit.multi_origin` smoke.
79. `uv run pytest tests/testing/ -q` green.
80. Full related gate (master plan gate cmd) green.
81. Scrub: `rg 'python -m mpreg' docs mpreg/testing` only blocker message.
82. claims.yaml includes live + registry tests.
83. Master plan status remains complete; this plan → complete.
84. RESIDUAL Phase 4 notes entry-point fix.
85. Commit message cites entry points + gap closure.
86–110. Buffer / docs polish.

---

## Status dashboard

| Track | Pts | Status |
| --- | --- | --- |
| T8 Entry points | 40 | **complete** — `mpreg distlab` / `mpreg test concurrent`; python -m blocked |
| T9 Gap closure | 75 | **complete** — expanded builtins + registry all-strong suite |
| T10 Validation | 35 | **complete** — tests/testing 65 + related gate 203 passed |

### Delivered artifacts

| Artifact | Path / command |
| --- | --- |
| DistLab CLI impl | `mpreg/testing/distlab/cli.py` |
| Blocked module path | `mpreg/testing/distlab/__main__.py` (exit 2) |
| CLI groups | `mpreg distlab`, `mpreg test concurrent` in `mpreg/cli/main.py` |
| Builtins (~35) | `mpreg/testing/distlab/builtins.py` |
| Tests | `tests/testing/test_distlab_*.py` |

## Implementation order

```
T8-S0..S5 → T9-S0..S6 → T10
```

## Gate commands (entry points only)

```bash
uv run mpreg distlab list
uv run mpreg distlab run strong.happy_3
uv run pytest tests/testing/ -q
uv run pytest tests/testing/ tests/chaos/test_strong_chaos_stress.py \
  tests/chaos/test_shared_audit_chaos.py tests/chaos/test_t9_residuals.py \
  tests/core/test_cache_strong*.py tests/invariants/test_cache_strong*.py \
  tests/invariants/test_shared_audit_properties.py \
  tests/integration/test_cache_strong*.py tests/integration/test_shared_audit*.py \
  tests/integration/test_strong_audit_coexistence.py \
  tests/server_pkg/test_shared_audit*.py -q
```
