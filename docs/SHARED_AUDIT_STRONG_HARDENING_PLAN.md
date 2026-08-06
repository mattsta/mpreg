# Shared Audit + STRONG Hardening Plan (Residual → Stress → Honesty)

| Field | Value |
| --- | --- |
| **Status** | Active implementation track |
| **Parent** | `docs/SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md` |
| **Date** | 2026-08-06 |
| **Claims** | Deepens `INV-SHARED-AUDIT-01`, `INV-CACHE-STRONG-01` without WAN/BFT/Jepsen overclaim |
| **Point count** | ~120 implementation + proof items (this document) |

## 0. North star

Prove, under stress and faults we **can** inject in-process and on same-host
live meshes, exactly what STRONG put and shared audit **do** and **do not**
guarantee. When a test that should pass fails, **strengthen the architecture**
(not the test) unless the failure reveals a true non_claim boundary.

---

## 1. Honesty boundaries (do not cross)

1. Not full Jepsen/Elle multi-process linearizability across WAN.
2. Not Byzantine fault **tolerance** (quorum of colluding liars).
3. Not fsync/disk durability of STRONG values.
4. Not kernel/iptables partitions (process stop + injectors stand in).
5. Not OS `kill -9` + cold restart durability of in-memory STRONG L1.
6. Not STRONG get/delete quorum (v1.1).
7. Not multi-tenant audit isolation beyond `cluster_id` reject.
8. Not SIEM / infinite retention for shared audit.
9. Detectable malice → fail-closed is in scope; silent majority-lie success is out.
10. Same-host multi-process ≠ geo-distributed WAN SLA.

---

## 2. Architecture goals

11. Residual-free failed STRONG put for `op_id` on every replica in R.
12. Peer commits always bridge into GCM L1 (apply) and reverse on abort (uncommit).
13. Pending never visible to get/list/namespace scans.
14. Expired pending cannot become a late visible commit (TTL honesty).
15. Periodic purge of expired pending on live servers (not unit-only).
16. Majority Q = floor(N/2)+1 on frozen replica set.
17. Origin-commit-last default preserved.
18. LWW abort-safe: uncommit must not clobber newer winner.
19. Concurrent multi-key puts do not cross-contaminate residuals.
20. Concurrent single-key puts converge under LWW with history checker.
21. Adversarial ACK shapes never count toward quorum success.
22. Cluster mismatch always fail-closed at handler and coordinator.
23. Shared audit G-Set merge commutative/idempotent under reorder/dup/drop.
24. Watermark advance never resurrects below-floor ids.
25. `gossip_eligible=false` never leaves origin.
26. Epidemic only to **connected** peers; re-queue when fanout is zero.
27. Digest/PULL repairs DELTA loss after heal.
28. Late joiner catch-up via digest/PULL.
29. Publish path never blocks / never raises into mgmt mutation.
30. Bounded outbound queue with observable drop counter.

---

## 3. Chaos harness (infrastructure)

31. `ChaosStrongTransport`: delay, drop, partition, duplicate, reorder, corrupt ACK.
32. Per-peer malice modes: lie_prepare_ok, lie_commit_applied, wrong_cluster, flip_applied.
33. Partition matrix: bidirectional link cut between peer sets.
34. Heal API: clear partitions and flush held messages.
35. Deterministic seed option for Hypothesis integration.
36. Metrics: prepares_sent, commits_sent, aborts_sent, drops, dups, delays.
37. `ChaosAuditTransport` (extend in-process): partition, drop_types, delay, reorder.
38. Shared helpers: `assert_residual_free(backends, key, op_id)`, `assert_backends_agree`.
39. Mesh factory: N backends + N coordinators + chaos transport.
40. Live mesh helpers: wait peers, wait connected, wait audit convergence (shared).

---

## 4. STRONG unit / property waves

41. 3-node happy path residual map + GCM bridge.
42. 5-node replica_factor=5 majority math (Q=3).
43. lab_single_node path still residual-free.
44. Insufficient peers → 1015, no residual.
45. Prepare drop majority → 1015/1016, no residual.
46. Commit drop after prepare → abort uncommit peers.
47. Partial commit ACKs < Q → fail + uncommit.
48. Origin commit LWW loss → 1017 + abort peers.
49. Peer LWW loss during commit → 1017 residual-free.
50. Pending full → 1018 residual-free.
51. Pending TTL expire + purge → no pending; commit of expired → reject.
52. Idempotent prepare same op_id.
53. Idempotent commit after applied.
54. Abort unknown op_id is safe no-op.
55. Abort after commit restores backup / evicts L1 via callback.
56. Concurrent puts different keys all succeed or residual-free fail.
57. Concurrent puts same key: history checker (LWW).
58. Hypothesis: random drop patterns residual-free.
59. Hypothesis: random concurrent int values single-key history.
60. Hypothesis: multi-key concurrent sets no cross-key residual.
61. Soak: 50 sequential STRONG puts on 3-node mesh all residual-clean.
62. Soak: interleaved success + injected fail cycles.
63. max_pending thrash: fill, purge, put again succeeds.
64. Clock skew: logical_ts monotonic per origin under burst.
65. Replica set selection: origin always first when require_origin_in_quorum.

---

## 5. Adversarial / handler waves (not BFT)

66. Wrong cluster_id on prepare → reject, no pending.
67. Wrong cluster_id on commit → reject.
68. Bad key payload → reject.
69. Bad strong_version payload → reject.
70. Missing op_id on commit/abort → reject.
71. Lie prepare_ok without pending → commit fails residual-free.
72. Lie commit_applied without apply → must not alone satisfy Q if need_peers>0.
73. Drop majority peers residual-free.
74. Corrupt ACK missing fields → treated as failure.
75. Duplicate prepare/commit delivery idempotent.
76. Handler fuzz: random payload dicts never raise / never apply garbage.
77. Commit for foreign op_id never applies wrong key.
78. Explicit non_claim test: majority liars can still “succeed” client-side if ACKs look valid — document only, do not “fix” into BFT.

---

## 6. Shared audit stress waves

79. DELTA epidemic 3-node visibility.
80. Drop DELTA → digest/PULL repair.
81. Reorder buffer reverse delivery converges.
82. Duplicate DELTA idempotent.
83. Partition A|{B,C} then heal → converge.
84. Multi-origin concurrent publish converge.
85. Watermark compaction + no resurrection.
86. Cross-cluster record rejected.
87. gossip_eligible=false not in epidemic payload path.
88. Outbound queue overflow increments publish_dropped; store still local.
89. Late joiner after N events catch-up.
90. Hypothesis: random merge order same final set.
91. Replicator stop/start no crash; no double-task.
92. Health dict exposes peers_known, last_delta_at, drops.
93. Live multi-origin drain churn (existing).
94. Live late joiner (existing).
95. Sustained publish burst + reconcile under drop inject (in-process).

---

## 7. Live multi-process waves

96. Live 3-node STRONG put visible on all committers via GCM get.
97. Live peer pre-put shutdown residual-free.
98. Live mid-put peer kill residual-free or consistent success.
99. Live concurrent multi-origin STRONG different keys.
100. Live concurrent multi-origin same key LWW-safe residual-free fails.
101. Live STRONG disabled → 1012.
102. Live audit multi-origin drains converge.
103. Live audit + STRONG coexistence (both flags on one mesh).
104. Live 5-node STRONG (if ports allow) optional/slow.
105. Live residual: after failed put, backend + GCM agree empty for op_id.

---

## 8. Production architecture upgrades (this track)

106. **Pending TTL reject on commit** if `expires_at` passed (reason=`expired`).
107. **Server background purge task** for `_strong_local_backend.purge_expired_pending`.
108. Ensure abort retries remain bounded (already 2).
109. Chaos transport available under `tests/chaos/harness_strong_audit.py` for reuse.
110. Optional metrics counters for strong prepare/commit/abort fails (if cheap).
111. Audit transport partition sets on `InProcessSharedAuditTransport`.
112. Document all new proofs in residual honesty + claims.yaml.

---

## 9. Claims / docs updates

113. Extend INV-CACHE-STRONG-01 proof list with chaos/stress/history/adversarial/live.
114. Extend INV-SHARED-AUDIT-01 proof list with partition-heal + stress.
115. non_claims: TTL purge is best-effort wall clock (not Byzantine time).
116. non_claims: majority-liar BFT still out.
117. non_claims: live 5-node optional not WAN.
118. README / CACHING / ARCHITECTURE honesty one-liners if proof depth changes.
119. Residual honesty doc “Phase 2 shipped” table.
120. This plan checked off as implementation completes.

---

## 10. Validation gate

121. Unit residual + adversarial + bridge green.
122. Invariants history + properties green.
123. Chaos t9 + new strong/audit chaos green.
124. Integration live mesh + live churn green.
125. Related quorum + shared_audit_cluster green.
126. No claim text implies Jepsen/WAN/BFT/fsync.
127. Commit with complete message; working tree clean for this track.

---

## 11. Execution order (iterative)

| Wave | Items | Exit |
| --- | --- | --- |
| W0 | Plan doc (this file) | Written |
| W1 | Chaos harness + arch TTL/purge | Tests can inject; purge scheduled |
| W2 | STRONG stress + Hypothesis expand | Residual-free under chaos |
| W3 | Adversarial/handler expand | Fail-closed matrix green |
| W4 | Audit partition/heal/stress | Converge after heal |
| W5 | Live expand coexistence | Live green |
| W6 | Claims/docs + full suite + commit | Honesty + green |

## 12. Bug-fix policy

When an expected invariant fails:

1. Confirm it is in-scope (not a non_claim).
2. Prefer production fix (backend/coordinator/server/replicator).
3. Add a regression test that would have caught the bug.
4. Only then mark the plan item done.

Examples already fixed in Phase 1: peer L1 blind spot; gossip slots handler;
connected-only epidemic + re-queue.

## 13. DistLab seven-track master plan

Full expansion (generator, registry, live DistLab, ~420 pts across T1–T7) is
tracked in **`docs/plans/DISTLAB_SEVEN_TRACK_MASTER_PLAN.md`**. Residual honesty
Phase 3–4: `docs/SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md`.
