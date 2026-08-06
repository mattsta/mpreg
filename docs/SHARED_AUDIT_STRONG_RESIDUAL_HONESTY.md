# Residual Honesty: Shared Audit + STRONG Put (Beyond MVP)

| Field | Value |
| --- | --- |
| **Status** | Shipped — scoped residual hardening (still not Jepsen-class) |
| **Parent** | `docs/SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md` |
| **Date** | 2026-08-06 |
| **Claims** | Extends proof depth for `INV-SHARED-AUDIT-01`, `INV-CACHE-STRONG-01` |

## Motivation

MVP shipped flag-gated shared audit + majority-commit STRONG put with Hypothesis
lattice/quorum properties, multi-node **in-process** integration, and residual-free
chaos paths. Honest residuals remained:

| Residual | MVP gap | This track |
| --- | --- | --- |
| Fabric wire STRONG RR | In-process transport stand-in; peer commits not mirrored into GCM L1 | **Fix + live 3-server put** |
| Live partition / peer loss | Drop sets on fake transport only | Live shutdown mid-barrier + residual checks |
| Shared-audit epidemic churn | Boot + local publish; in-process anti-entropy | Live multi-drain + reconcile wait on real gossip |
| Linearizability checker | Global non_claim (full Jepsen) | **Bounded single-key history checker** (local, not WAN) |
| Byzantine peers | Unstated | Adversarial ACKs / cluster mismatch **fail-closed** (not BFT) |
| WAN / process-kill / fsync | Unclaimed | **Remain non_claims** |

## Production fix (blocking live STRONG)

Peer `StrongLocalBackend.commit` previously updated only the backend `_visible`
map. Origin GCM applied L1 after success; **peers did not**, so `gcm.get` on a
committer missed STRONG values.

**Fix:** optional `on_visible_apply` / `on_visible_uncommit` callbacks on
`StrongLocalBackend`, wired in `MPREGServer` to `_put_to_l1` / L1 evict-or-restore.
GCM `get` also promotes from `_strong_backend.get_visible` on L1 miss (defensive).

## In scope (ship)

1. **Live 3-node STRONG put** over `ServerCacheTransport` RR
   (`enable_default_cache` + `cache_strong_enabled`).
2. **Live peer shutdown** during barrier → client failure residual-free on survivors.
3. **Live shared-audit** multi-origin drains + wait for cluster scope on all nodes.
4. **History checker** for concurrent single-key STRONG puts (LWW + no dirty fail).
5. **Adversarial peer** transport: bad ACK shapes, cluster_id mismatch → fail closed.
6. Claims proof list + non_claims honesty update.

## Out of scope (remain non_claims)

- Full Jepsen / Elle linearizability across WAN and process crash-restart.
- Byzantine fault **tolerance** (quorum of liars).
- Disk fsync durability of STRONG values across replicas.
- Kernel-level / iptables network partitions (we use process stop + drop injectors).
- STRONG get/delete quorum (still v1.1).

## Test map

| File | Proof |
| --- | --- |
| `tests/integration/test_cache_strong_live_mesh.py` | Live wire STRONG + peer kill residual |
| `tests/integration/test_shared_audit_live_churn.py` | Live gossip multi-drain convergence |
| `tests/invariants/test_cache_strong_history.py` | Bounded single-key history / LWW |
| `tests/core/test_cache_strong_adversarial.py` | Adversarial ACK / mismatch fail-closed |
| `tests/core/test_cache_strong_gcm_bridge.py` | Peer apply/uncommit → GCM L1 |

## Success criteria

- Live STRONG put: value readable via GCM get on ≥ Q committers.
- Failed live put after peer stop: no `op_id` visible residual on remaining backends/GCM.
- Live audit: N drains from multiple origins appear in every node’s `scope=cluster`.
- History checker: no successful put lost under LWW; failed ops leave no residual.
- Docs/claims updated; full related suite green.

## Shipped (2026-08-06)

| Item | Result |
| --- | --- |
| Peer → GCM L1 bridge | `StrongLocalBackend.on_visible_apply/uncommit` + GCM get promote |
| Live STRONG 3-node RR | `test_cache_strong_live_mesh.py` green |
| Live peer-loss residual | Pre-put shutdown + concurrent mid-put kill paths |
| Live audit multi-origin + late joiner | `test_shared_audit_live_churn.py` green |
| Gossip slots handler | `GossipProtocol.mgmt_audit_handler` declared field (slots-safe) |
| Epidemic connected-only peers | `_peers()` filters `is_connected`; re-queue delta if `sent==0` |
| History + adversarial | Hypothesis LWW history; lie/mismatch/drop fail-closed |
| Claims | Proof lists + non_claims refined (still not WAN/Jepsen/BFT/fsync) |

## Bottom line

Originally MVP residual honesty said “not sufficient” for live wire / churn /
history. **Now** those scoped residuals have tests and one production bug
(peer L1 blind spot + dead audit handler on slots) was fixed because of them.
Still **not** claimed: WAN/process-kill-9/disk durability, BFT, full Jepsen/Elle,
kernel partitions.

## Phase 2 — Hardening plan (2026-08-06)

Full ~120-point plan: `docs/SHARED_AUDIT_STRONG_HARDENING_PLAN.md`.

| Item | Result |
| --- | --- |
| Chaos harness | `tests/chaos/harness_strong_audit.py` — partition/delay/drop/dup/malice |
| STRONG stress | 5-node majority, soak, multi-key concurrent, Hypothesis drops/history |
| Expired commit reject | `StrongLocalBackend.commit` → `reason=expired` |
| Server pending purge | `_start_strong_pending_purge_loop` best-effort wall-clock GC |
| Multi-origin LWW | Coordinator bumps `logical_ts` above local visible version |
| Handler fuzz | Bad payloads / cluster mismatch never raise; BFT non_claim documented |
| Audit chaos | Partition/heal, dup DELTA, ineligible, outbound drops, watermark |
| Live expand | Multi-origin concurrent keys + STRONG∥audit coexistence |
| Architecture bugs fixed by tests | LWW same-ms multi-origin soak failure → ts bump |

Still **not** claimed: WAN, kill-9 cold restart, fsync, BFT, Jepsen/Elle, kernel partitions.

## Phase 3 — DistLab first-party product (2026-08-06)

Elevated distributed testing into **`mpreg.testing.distlab`**: history +
pluggable checkers + nemesis + scenario runner + STRONG/audit SUTs. The
platform now tests itself through a reusable lab rather than only ad-hoc
test helpers.

| Surface | Path |
| --- | --- |
| Product | `mpreg/testing/distlab/` |
| Plans (7×25 summary) | `docs/DISTLAB_AND_SEVEN_TRACKS.md` |
| Master plan (7×~60 pts) | `docs/plans/DISTLAB_SEVEN_TRACK_MASTER_PLAN.md` |
| Core self-tests | `tests/testing/test_distlab_core.py` |
| Registry/CLI/generators | `tests/testing/test_distlab_registry.py` |
| STRONG scenarios | `tests/testing/test_distlab_strong_scenarios.py` |
| Audit scenarios | `tests/testing/test_distlab_audit_scenarios.py` |
| Live mesh DistLab | `tests/testing/test_distlab_live.py` |
| CLI | `python -m mpreg.testing.distlab list\|run` |
| Re-export | `tests/harness` |

## Phase 4 — Seven-track expansion complete (2026-08-06)

Official master plan executed: generator + registry + live helpers + CLI,
full in-process STRONG/audit scenario catalog, live same-host DistLab suite,
claims/docs gate. See `docs/plans/DISTLAB_SEVEN_TRACK_MASTER_PLAN.md` status
dashboard (T1–T7 complete).

DistLab is **Jepsen-inspired, not Jepsen**. See claims.yaml non_claims.
