# DistLab + Seven-Track Implementation Plans (25 pts each)

| Field | Value |
| --- | --- |
| **Status** | Complete — expanded in official master plan |
| **Date** | 2026-08-06 |
| **Product** | `mpreg.testing.distlab` |
| **Honesty** | Jepsen-inspired, **not** Elle/WAN/BFT/kernel partitions |
| **Master plan** | `docs/plans/DISTLAB_SEVEN_TRACK_MASTER_PLAN.md` (~420 pts, T1–T7) |

## DistLab product (platform tests the platform)

DistLab is a **first-party** distributed testing lab inside MPREG:

| Component | Module | Role |
| --- | --- | --- |
| History | `distlab/history.py` | Append-only invoke/ok/fail/info log |
| Checkers | `distlab/checker.py` | Residual-free, LWW register, G-Set, agreement, composite |
| Nemesis | `distlab/nemesis.py` | Scheduled faults via FaultInjector hooks |
| Scenario | `distlab/scenario.py` | Setup → clients → nemesis → check → teardown |
| Generator | `distlab/generator.py` | Sequential/concurrent puts, audit burst, fault plans |
| Registry | `distlab/registry.py` + `builtins.py` | Named scenario catalog |
| Live | `distlab/live.py` | Same-host MPREGServer helpers + LiveStrongSUT |
| CLI | `uv run mpreg distlab` | list / catalog / run |
| STRONG SUT | `distlab/adapters/strong.py` | In-process majority-commit mesh |
| Audit SUT | `distlab/adapters/audit.py` | In-process G-Set epidemic mesh |
| Re-export | `tests/harness` | Single import surface for suites |

**Uses** existing `mpreg.testing.faults.FaultInjector` / `NetworkView`.

**Does not claim:** full linearizability (Elle), geo-WAN, BFT, fsync, kill-9 cold restart.

---

## T1 — Pluggable chaos harness ecosystem (25)

1. History append is thread-safe and index-monotonic.
2. INVOKE/OK/FAIL pairing is FIFO per process.
3. Open invokes detected by NoOpenInvokeChecker.
4. CompositeChecker fails if any sub-checker fails.
5. ResidualFreeChecker uses state.visible_op_ids.
6. ReplicaAgreementChecker allows null minority (majority quorum).
7. LWWRegisterChecker: final op ∈ successful puts.
8. GSetConvergenceChecker: all nodes share published ids.
9. Nemesis step_once PARTITION/HEAL/DROP/DELAY.
10. Nemesis.stop always heals + clears rates.
11. FaultInjectorNemesisTarget bridges injector + hooks.
12. Scenario runs clients concurrently.
13. Scenario runs body after clients.
14. Scenario attaches history to nemesis.
15. ScenarioSuite run_all stop_on_fail.
16. StrongSUT.create builds N coords + chaos transport.
17. StrongSUT.put records history.
18. StrongSUT.snapshot_state checker-compatible.
19. AuditSUT.publish + flush + reconcile.
20. AuditSUT.snapshot_state gset_ids_by_node.
21. tests/harness re-exports DistLab.
22. mpreg.testing.distlab lazy StrongSUT/AuditSUT.
23. Unit tests for DistLab core (no platform servers).
24. Doc honesty banner in package docstring.
25. Gate: `tests/testing/test_distlab_core.py` green.

## T2 — STRONG stress via DistLab (25)

26. Happy 3-node put scenario residual-free + LWW.
27. Happy 5-node Q=3.
28. Concurrent same-key history checker.
29. Concurrent multi-key no cross residual.
30. Soak 20 sequential multi-origin puts.
31. Partition majority → fail residual-free.
32. Partition one peer → success still possible.
33. Heal after partition → success.
34. Drop prepare majority residual-free.
35. Drop commit residual-free uncommit.
36. Nemesis background during concurrent puts → residual-free or valid LWW.
37. Hypothesis random drop sets residual-free.
38. Hypothesis concurrent ints LWW.
39. Delay within timeout succeeds.
40. Duplicate commit idempotent.
41. Expired pending reject.
42. Pending full 1018 then purge recovers.
43. Interleaved fault/success cycles.
44. Origin-first replica set.
45. LWW uncommit does not clobber winner.
46. Stats in CheckResult populated.
47. Scenario meta carries n/seed.
48. Fail path error_code recorded in history.
49. No open invokes after gather.
50. Gate: `tests/testing/test_distlab_strong_scenarios.py` green.

## T3 — Adversarial / BFT boundary (25)

51. Wrong cluster prepare reject.
52. Bad key / bad version handler reject.
53. Lie prepare_ok → commit fail residual-free.
54. Drop majority residual-free.
55. Handler fuzz never raises.
56. Idempotent prepare/commit.
57. Abort unknown safe.
58. Lie commit_applied documents CFT non_claim (peers may miss).
59. flip_applied / malice matrix residual-free when fail.
60. Corrupt empty ACK dicts tolerant.
61. DistLab history records fail codes for adversarial.
62. Cluster mismatch at transport layer.
63. Explicit non_claim test name contains `not_bft`.
64. Composite checker still residual-free on adversarial fail.
65. No pending after adversarial suite.
66–75. Expand matrix rows (prepare fail, commit lie single peer, etc.).

## T4 — Shared audit epidemic via DistLab (25)

76. Multi-origin publish converge after flush.
77. Partition then heal + reconcile converge.
78. Drop DELTA + digest repair.
79. Duplicate DELTA idempotent.
80. gossip_eligible=false never remote.
81. Cross-cluster insert reject.
82. Outbound queue drop counter.
83. Watermark no resurrection.
84. Reorder buffer converge.
85. Replicator stop/start.
86. Nemesis partition during publish then heal+reconcile.
87. GSetConvergenceChecker drives pass/fail.
88. min_ids enforced.
89. Burst 30 events converge.
90. Late node: create 2-node history, add via reconcile (in-process).
91–100. Hypothesis merge orders / random partition pairs.

## T5 — Live multi-process mesh (25)

101. Live 3-node STRONG via existing mesh helpers.
102. Live multi-origin concurrent keys.
103. Live mid-put kill residual-free or consistent.
104. Live peer pre-shutdown residual-free.
105. Live STRONG disabled 1012.
106. Live audit multi-origin drains.
107. Live audit late joiner.
108. Live STRONG∥audit coexistence.
109. Purge task started when strong enabled.
110. Port allocator only (no fixed ports).
111. Wait helpers timeout with diagnostics.
112. Committer GCM get after put.
113. Backend pending 0 after ops.
114. Coexistence second put after audit churn.
115–125. Optional 5-node skip/slow markers; docs.

## T6 — Architecture fixes from DistLab (25)

126. Expired commit reject (done).
127. Pending purge loop (done).
128. Multi-origin logical_ts bump (done).
129. Peer L1 bridge (done).
130. Gossip slots handler (done).
131. Connected-only epidemic (done).
132. Delta re-queue sent==0 (done).
133. Agreement checker majority-aware (this track).
134. DistLab transport uses FaultInjector.can_deliver.
135. Any new invariant fail → product fix + regression scenario.
136–150. Reserved for bugs found while running T2–T5.

## T7 — Claims / docs / gate (25)

151. claims.yaml INV-CACHE-STRONG-01 proof list + DistLab tests.
152. claims.yaml INV-SHARED-AUDIT-01 + DistLab audit.
153. non_claims DistLab ≠ Jepsen/Elle.
154. non_claims DistLab ≠ BFT.
155. docs/DISTLAB_AND_SEVEN_TRACKS.md (this file).
156. docs/SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md Phase 3.
157. README honesty one-liner DistLab.
158. ARCHITECTURE testing section pointer.
159. Package docstring honesty.
160. tests/harness export complete.
161. Full related suite green.
162. DistLab core unit green.
163. DistLab strong scenarios green.
164. DistLab audit scenarios green.
165. Prior chaos suites still green (compat).
166. Live suites green.
167. Commit message complete.
168. Working tree clean.
169–175. CLI `uv run mpreg distlab` list/catalog/run (shipped; never python -m).

---

## Execution order

T1 core → T2 strong scenarios → T3 adv (reuse) → T4 audit scenarios → T5 live (existing + coexist) → T6 fixes as found → T7 claims/gate/commit.
