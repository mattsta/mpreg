# MPREG Performance Baseline (Lab Evidence)

| Field         | Value                                                         |
| ------------- | ------------------------------------------------------------- |
| **Milestone** | 0.3.0 Production Snapshot                                     |
| **Honesty**   | Lab / same-host numbers only — **not** a WAN multi-region SLA |
| **Related**   | `tests/performance/`, `docs/PRODUCTION_DEPLOYMENT.md`         |

## What this document is

Evidence that MPREG has **reproducible** performance measurement hooks and
documented expectations for **local lab** topologies. It deliberately does **not**
claim “million+ msg/s in every deployment” or cloud-region SLOs.

## Hardware class (record yours)

When publishing numbers, record:

- CPU model / core count
- RAM
- OS
- Python version (`python --version`)
- MPREG version (`uv run python -c "import mpreg; print(mpreg.__version__)"`)
- Topology (1-node loopback vs N-node same host)

## Default lab topology

| Role        | Settings sketch                                 |
| ----------- | ----------------------------------------------- |
| Single node | `mpreg/profiles/single-node.toml` or `dev.toml` |
| Transport   | `ws://127.0.0.1` (plain lab)                    |
| Logging     | INFO or WARNING (DEBUG skews latency)           |

## Reproducible commands

### Doc / gate smoke (CI-safe)

```bash
bash scripts/ci_perf_smoke.sh
```

Checks that this baseline document exists and is non-trivial. Does **not** run
long load tests on every PR.

### Full baseline suite (local / nightly)

```bash
uv run pytest tests/performance/test_performance_baselines.py -m slow -q --tb=line
```

Optional federation-oriented scripts under `tools/debug/` (developer tools; not
the release gate):

```bash
# examples only — see tools/debug/README.md
# uv run python tools/debug/benchmark_replication_performance.py
```

Prefer entry-point and pytest forms in automation; do not document `python -m mpreg`.

### DistLab micro-path (correctness under load shape, not SLA)

```bash
uv run mpreg distlab run strong.happy_3
uv run mpreg distlab run strong.soak_20
```

These prove residual/LWW checkers under sequential load — **not** peak pubsub PPS.

## Expected bands (order-of-magnitude, loopback lab)

These are **starting expectations** for a modern laptop/desktop loopback run.
Your numbers will differ. Fail CI only on **catastrophic** regressions (orders
of magnitude), not tight percentile flakes.

| Signal             | Component        | Lab ballpark              | Notes                                 |
| ------------------ | ---------------- | ------------------------- | ------------------------------------- |
| RPC latency p95    | local call       | tens of ms                | Depends on handler work               |
| RPC throughput     | simple echo      | hundreds–thousands RPS    | Single process                        |
| Pub/sub fanout     | topic exchange   | hardware-bound            | Hierarchical topics; not a global SLA |
| STRONG put         | 3-node same-host | much slower than EVENTUAL | Quorum RTTs; flag-gated               |
| Cache EVENTUAL get | local L1         | sub-ms to low ms          | After warm                            |

If README or marketing text mentions high throughput, it must point here and
state **lab/hardware-dependent**.

## Non-claims

- Not a multi-region or WAN latency SLA
- Not Jepsen/Elle linearizability under partition
- Not proof of “million+ msg/s” on arbitrary hardware
- STRONG path is correctness-first CFT, not max-PPS
- Golden-signal SLOs in `docs/ops/SLO_GOLDEN_SIGNALS.md` are **ops thresholds**,
  not marketing benchmarks

## Updating baselines

1. Run the full suite on reference hardware.
2. Record environment in a short PR note or appendix table.
3. Adjust bands only with rationale; never silent tighten to chase green CI.
