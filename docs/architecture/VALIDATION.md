# Validation Architecture

**Authority for:** what CI proves, test taxonomy, flake classes, honesty scorecard.

---

## 1. Tiered gates (intentional)

```text
PR CI (required)
  ci_lint.sh              ruff mpreg tests tools
  ci_typecheck.sh         import smoke + scoped mypy (not full tree)
  ci_unit_fast.sh         path allowlist only
  ci_invariants.sh        3 files: STRONG + shared audit properties
  ci_distlab_core.sh      list + strong.happy_3 + raft.elect_3 + pytest
  ci_security_deps.sh     pip-audit (soft if no uv)
  ci_package_smoke.sh     wheel install
  demo-smoke              mpreg-example smoke
  demo-suite              continue-on-error

Local release_gate.sh
  above + ci_raft + ci_perf_smoke (doc-only) + pytest tests/release/

Raft gate (release_gate): scripts/ci_raft.sh
  unit/invariants + DistLab raft scenarios + CLI raft.elect_3

NEVER required today
  full pytest tests/ -n auto
  full production_raft_integration / live multi-process (local multi-run)
  chaos/ (~130)
  most integration/
  most invariants (routing/RPC beyond allowlist)
  property_tests/, performance/
  full-tree mypy
```

| Script                       | Allowlist note                             |
| ---------------------------- | ------------------------------------------ |
| `scripts/ci_unit_fast.sh`    | ~12 paths — **no Raft**                    |
| `scripts/ci_invariants.sh`   | 3 STRONG/audit files only                  |
| `scripts/ci_raft.sh`         | Raft unit + DistLab raft track             |
| `scripts/ci_distlab_core.sh` | STRONG + Raft happy CLI + registry pytest  |
| `scripts/release_gate.sh`    | snapshot engineering floor (includes raft) |

Workflow: `.github/workflows/ci.yml`.  
Philosophy: `docs/RELEASE_0_3_PRODUCTION_SNAPSHOT_ARCHITECTURE.md`.

---

## 2. pytest taxonomy (~445 modules)

| Area           | Path                               | CI?                          |
| -------------- | ---------------------------------- | ---------------------------- |
| Root mass      | `tests/test_*.py`                  | almost none                  |
| core STRONG    | `tests/core/`                      | partial unit-fast            |
| integration    | `tests/integration/`               | no                           |
| invariants     | `tests/invariants/`                | 3 files                      |
| chaos          | `tests/chaos/`                     | no (`chaos` marker unused)   |
| property_tests | `tests/property_tests/`            | no                           |
| performance    | `tests/performance/`               | no                           |
| release        | `tests/release/`                   | yes                          |
| DistLab Raft   | `tests/testing/test_distlab_raft*` | **yes** (ci_raft/distlab)    |
| Raft-named     | `test_*raft*`, invariants raft     | **partial** (ci_raft subset) |

Markers registered: `slow`, `integration`, `unit`, `chaos`, `property`,
`performance`, `federation`, `example_*` (`pyproject.toml`).  
Live Raft: `tests/test_live_raft_integration.py` is `@pytest.mark.integration`
+ `@pytest.mark.slow`. Deselect with `-m "not slow"` for fast loops.

Claims ledger: `tests/invariants/claims.yaml` (includes large `non_claims`).

---

## 3. Concurrency model

| Surface     | Behavior                                                   |
| ----------- | ---------------------------------------------------------- |
| CI pytest   | serial allowlists — **no** `-n`                            |
| xdist       | `pytest -n auto`; ConcurrentSuiteRunner default 16 workers |
| Ports       | worker × 200 offset, **cap 20 workers** then collide       |
| Time        | wall clock                                                 |
| ProcessPool | not used in suite                                          |

---

## 4. Flake classes (architectural)

1. Timing / election windows under load
2. Fixed sleep readiness fixtures
3. Shared/hardcoded ports + worker wrap
4. FD exhaustion (macOS)
5. Task/connection leaks
6. MockNetwork ordering + partitions
7. Live mesh/gossip convergence
8. Chaos injector nondeterminism
9. Shutdown timeout races
10. Marker false security (`-m not chaos` does not exclude `tests/chaos/`)

---

## 5. Honest scorecard

| Claim                       | Verdict                                                  |
| --------------------------- | -------------------------------------------------------- |
| CI green ⇒ fully tested     | **False**                                                |
| Raft validated in CI        | **Partial** — `ci_raft` + DistLab raft in release_gate   |
| DistLab first-class Raft    | **True** — elect/partition/heal/reelect scenarios        |
| Multi-run flake-proof       | **Local evidence** — N× gates (see proof ledger); not CI |
| 0.3.x snapshot engineered   | **Defensible** if release_gate green                     |
| All claims.yaml CI-enforced | **False**                                                |
| 100% validated              | **Forbidden** by honesty system                          |

This program **raises** the bar (Raft gate + DistLab raft + logging contract) without claiming Jepsen/BFT/WAN (still non_claims).

---

## 6. Target validation matrix (program end-state)

| Layer                  | Command / surface                                                                         | After track |
| ---------------------- | ----------------------------------------------------------------------------------------- | ----------- |
| Snapshot floor         | `bash scripts/release_gate.sh`                                                            | every track |
| Raft unit+timer        | `pytest tests/test_raft_election_timer_semantics.py tests/test_raft_task_manager.py -q`   | A           |
| Raft integration       | `pytest tests/test_production_raft_integration.py -q`                                     | A/C         |
| Raft safety/properties | `pytest tests/test_raft_safety_properties.py tests/test_production_raft_properties.py -q` | A/C         |
| Raft invariants        | `pytest tests/invariants/test_raft_*.py -q`                                               | A/C         |
| Hypothesis             | property_tests + invariant Hypothesis where present                                       | C           |
| Fabric raft            | `pytest tests/integration/test_fabric_raft_integration.py -q`                             | C           |
| Ops logging            | `pytest tests/test_operational_exception_logging.py -q`                                   | B           |
| Multi-run stress       | N× `ci_raft` + DistLab raft-core + integration/live                                       | C           |
| DistLab Raft           | `mpreg distlab suite --preset raft-core` + `tests/testing/test_distlab_raft_scenarios.py` | C           |
| Full corpus            | `pytest tests/ -n auto` — local **3× green** 2026-08-07 (3358 passed; see proof ledger)   | C final     |
