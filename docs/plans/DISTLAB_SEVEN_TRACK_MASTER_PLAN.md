# DistLab Seven-Track Master Plan (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** (T1–T7 gated 2026-08-06) |
| **Date** | 2026-08-06 |
| **Authority** | Official project planning file for residual honesty + DistLab |
| **Product** | `mpreg.testing.distlab` |
| **Related** | `docs/DISTLAB_AND_SEVEN_TRACKS.md`, `docs/SHARED_AUDIT_STRONG_HARDENING_PLAN.md`, `docs/SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md`, `tests/invariants/claims.yaml` |
| **Point budget** | 7 tracks × ~55–70 pts = **~420 implementation/validation items** |

## Global rules

1. Every track has **stages** (S0 design → S1 product → S2 scenarios → S3 gate).
2. Prefer **DistLab scenarios** over one-off asserts; register scenarios in a suite.
3. When a scenario that should pass fails → **fix product code**, add regression scenario.
4. Never claim WAN / Elle / BFT / fsync / kill-9 cold restart / kernel partitions.
5. Use `port_range_context` for all live multi-process work.
6. Gate command (full related):

```bash
uv run pytest tests/testing/ tests/chaos/test_strong_chaos_stress.py \
  tests/chaos/test_shared_audit_chaos.py tests/chaos/test_t9_residuals.py \
  tests/core/test_cache_strong*.py tests/invariants/test_cache_strong*.py \
  tests/invariants/test_shared_audit_properties.py \
  tests/integration/test_cache_strong*.py tests/integration/test_shared_audit*.py \
  tests/integration/test_strong_audit_coexistence.py \
  tests/server_pkg/test_shared_audit*.py -q
```

7. Track complete only when: plan checkbox section updated, tests green, claims list includes new proofs.

---

# TRACK T1 — DistLab core ecosystem (60 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| T1-S0 | Catalog APIs | Modules listed in `__all__` |
| T1-S1 | Generator + registry | `generator.py`, `registry.py` |
| T1-S2 | CLI smoke | `python -m mpreg.testing.distlab` |
| T1-S3 | Core unit matrix | `test_distlab_core.py` expanded |
| T1-S4 | Harness exports | `tests/harness` complete |
| T1-S5 | Gate | Core suite green |

## Points

### T1-S0 Catalog
1. Document DistLab module map in this plan.
2. Package docstring honesty banner.
3. Lazy exports StrongSUT/AuditSUT.
4. `mpreg.testing.distlab` importable without server deps for core.
5. Version/meta field on ScenarioResult.

### T1-S1 Generator + registry
6. `OpGenerator` protocol: yields client work items.
7. `SequentialPutGenerator` for soak.
8. `ConcurrentPutGenerator` for multi-client.
9. `RandomFaultSchedule` seedable.
10. `ScenarioRegistry` name → factory.
11. Register built-in strong scenarios by name.
12. Register built-in audit scenarios by name.
13. `registry.run(name)` convenience.
14. `registry.list()` sorted names.
15. Suite builder from registry subset.

### T1-S2 CLI
16. `__main__.py` list scenarios.
17. `__main__.py` run one in-process scenario by name.
18. Exit code 0/1 on check fail.
19. JSON summary optional `--json`.
20. Help text cites non_claims.

### T1-S3 Core tests
21. History thread-safety smoke (concurrent append).
22. History pairs unmatched invoke.
23. History by_key filter.
24. NoOpenInvokeChecker fail/pass.
25. ResidualFreeChecker with fake state.
26. ReplicaAgreement majority-null OK.
27. ReplicaAgreement conflict fail.
28. LWWRegister success path.
29. LWWRegister fail-op-as-final fail.
30. GSetConvergence missing id fail.
31. GSetConvergence eligible=false ignored.
32. CompositeChecker aggregation.
33. CallableChecker plugin.
34. Nemesis PARTITION_ONE.
35. Nemesis PARTITION_MAJORITY.
36. Nemesis HEAL.
37. Nemesis DROP_RATE + CLEAR.
38. Nemesis DELAY.
39. Nemesis CRASH_ONE + RECOVER_ALL.
40. Nemesis.stop heals.
41. FaultInjectorNemesisTarget partition hooks.
42. Scenario clients concurrent.
43. Scenario body sequential.
44. Scenario nemesis lifecycle.
45. Scenario strict raise.
46. Scenario non-strict returns ok=False.
47. ScenarioSuite stop_on_fail.
48. ScenarioSuite continue on fail when stop_on_fail=False.
49. Generator sequential produces N history pairs.
50. Registry list non-empty after builtins register.

### T1-S4 Exports
51. tests/harness exports registry + generators.
52. tests/harness exports LiveMeshHelpers symbols when present.
53. README DistLab pointer.
54. ARCHITECTURE DistLab section.
55. DISTLAB_AND_SEVEN_TRACKS links to this master plan.

### T1-S5 Gate
56. `tests/testing/test_distlab_core.py` green.
57. `python -m mpreg.testing.distlab list` works.
58. `python -m mpreg.testing.distlab run strong.happy_3` works.
59. No import cycle testing ↔ server for core modules.
60. T1 marked complete in plan status table.

---

# TRACK T2 — STRONG stress via DistLab (65 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| T2-S0 | SUT completeness | StrongSUT API |
| T2-S1 | Happy / soak | scenarios |
| T2-S2 | Fault inject | scenarios |
| T2-S3 | Hypothesis | property tests |
| T2-S4 | Nemesis soak | scenarios |
| T2-S5 | Registry + gate | green |

## Points

### T2-S0
61. StrongSUT.create n=1..7.
62. StrongSUT.put history INVOKE/OK/FAIL.
63. StrongSUT.key auto-register.
64. StrongStateSnapshot pending_count.
65. StrongStateSnapshot visible_op_ids.
66. StrongStateSnapshot replica_views.
67. StrongStateSnapshot final_op_id prefers non-null.
68. StrongChaosTransport FaultInjector.can_deliver.
69. StrongChaosTransport edge_cuts.
70. StrongChaosTransport malice sets.
71. StrongSUT.nemesis_hooks complete.
72. default_strong_checkers composition.

### T2-S1 Happy / soak
73. happy_3 residual + LWW.
74. happy_5 Q=3.
75. happy_7 Q=4.
76. concurrent same-key LWW.
77. concurrent multi-key residual.
78. soak 20 multi-origin.
79. soak 50 multi-origin.
80. sequential overwrite LWW last wins value in history sense.
81. lab_single_node path via min_replicas=1 flag on coord.
82. error_code on fail in history.

### T2-S2 Faults
83. partition majority fail residual.
84. partition one peer still success possible.
85. heal after partition success.
86. drop prepare majority residual.
87. drop commit residual.
88. drop abort still residual after fail (best effort).
89. delay prepare within timeout success.
90. delay beyond timeout fail residual.
91. duplicate commit idempotent.
92. fail_prepare injected residual.
93. wrong_cluster residual.
94. crash_node via nemesis hook residual or heal recovery.
95. interleaved fault/success cycles.

### T2-S3 Hypothesis
96. random prepare drop sets residual-free.
97. random commit drop sets residual-free.
98. concurrent random ints LWW.
99. concurrent random multi-key.
100. random partition-one then heal put.

### T2-S4 Nemesis
101. nemesis during 8 concurrent puts checkers pass.
102. nemesis during soak 15 sequential.
103. nemesis actions recorded in history FAULT/HEAL.
104. after nemesis.stop network healed for final put success.

### T2-S5 Gate
105. All T2 scenarios in registry.
106. `test_distlab_strong_scenarios.py` covers registry run subset.
107. Prior chaos stress still green.
108. Expired pending + purge recover scenario.
109. Pending full 1018 scenario.
110. Origin-first replica set unit.
111. LWW uncommit no clobber.
112. Stats populated on CheckResult.
113. meta n/seed on ScenarioResult.
114. T2 gate green.
115. T2 complete in status table.

---

# TRACK T3 — Adversarial / BFT boundary (55 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| T3-S0 | Handler matrix | unit |
| T3-S1 | Transport malice | DistLab |
| T3-S2 | not_bft docs | explicit |
| T3-S3 | Fuzz | Hypothesis |
| T3-S4 | Gate | green |

## Points

116. Handler wrong cluster prepare.
117. Handler wrong cluster commit.
118. Handler bad key.
119. Handler bad version.
120. Handler missing op_id commit.
121. Handler abort unknown safe.
122. Handler idempotent prepare.
123. Handler idempotent commit.
124. Handler fuzz never raises.
125. ACK from_dict tolerant.
126. DistLab lie_prepare_ok residual.
127. DistLab drop majority residual.
128. DistLab wrong_cluster residual.
129. DistLab fail_prepare residual.
130. DistLab lie_commit single peer (Q still possible).
131. DistLab lie_commit both peers → not_bft demo.
132. Test name contains `not_bft`.
133. History records fail codes adversarial.
134. No pending after adversarial suite.
135. Composite residual on adversarial fail.
136. flip_applied path (if exposed) residual or success consistent.
137. Commit foreign op_id no wrong apply.
138. Prepare not_in_replica_set.
139. Malice clear_faults restores happy path.
140. Adversarial scenarios registered.
141–165. Matrix expansion rows + gate + claims non_claim text + complete.
    (141 handler metadata edge; 142 empty payload; 143 huge payload; 144 unicode key;
     145 concurrent adversarial+honest; 146–155 reserved matrix; 156–160 claims;
     161–165 gate.)

---

# TRACK T4 — Shared audit epidemic (60 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| T4-S0 | AuditSUT API | complete |
| T4-S1 | Converge paths | scenarios |
| T4-S2 | Fault / watermark | scenarios |
| T4-S3 | Hypothesis | properties |
| T4-S4 | Gate | green |

## Points

166. AuditSUT.create n nodes.
167. publish history AUDIT_PUBLISH.
168. flush_all / reconcile_all.
169. snapshot gset_ids_by_node.
170. nemesis_hooks partition/heal.
171. multi-origin converge.
172. partition heal converge.
173. drop delta digest repair.
174. duplicate idempotent.
175. ineligible local only.
176. cross-cluster reject.
177. outbound drop counter.
178. watermark no resurrection.
179. reorder buffer converge.
180. replicator stop/start.
181. burst 30.
182. burst 100.
183. nemesis during publish then converge.
184. late joiner in-process (2 then +1 via new SUT node register).
185. multi-origin concurrent publish clients.
186. digest only path (no delta).
187. health dict fields.
188. Hypothesis random publish order converge.
189. Hypothesis random partition pairs heal.
190. GSet checker min_ids.
191. eligible=false not in expected set.
192–210. Registry + gate + chaos compat + claims + complete.
    (192–200 registry names; 201–205 prior chaos green; 206–210 status.)

---

# TRACK T5 — Live multi-process mesh (60 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| T5-S0 | Live helpers in DistLab | `live.py` |
| T5-S1 | STRONG live scenarios | tests |
| T5-S2 | Audit live scenarios | tests |
| T5-S3 | Coexistence | tests |
| T5-S4 | Gate | green |

## Points

211. `distlab/live.py` StrongLiveMesh settings factory.
212. AuditLiveMesh settings factory.
213. BothFlags mesh factory.
214. wait_cache_peers helper.
215. wait_gossip_connected helper.
216. wait_audit_cluster_events helper.
217. LiveStrongSUT wrap MPREGServer list for history put via GCM.
218. Live put records DistLab history.
219. Live snapshot_state from backends+GCM.
220. port_range_context only.
221. live happy 3-node STRONG.
222. live multi-origin different keys.
223. live peer pre-shutdown residual.
224. live mid-put kill residual-or-ok.
225. live disabled 1012.
226. live audit multi-origin drains.
227. live audit late joiner.
228. live coexistence strong+audit.
229. purge task present when strong on.
230. committer gcm get.
231. pending 0 after ops.
232. second strong put after audit churn.
233. diagnostics on wait timeout.
234. live tests use AsyncTestContext.
235–255. Optional 4-node; docs; gate; claims; complete.
    (235 4-node strong optional; 236–240 helpers unit; 241–250 integration files;
     251–255 gate+status.)

---

# TRACK T6 — Architecture hardening from failures (55 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| T6-S0 | Known fixes verified | regression scenarios |
| T6-S1 | New bugs from T2–T5 | fix+regress |
| T6-S2 | Metrics / observability hooks | optional cheap |
| T6-S3 | Gate | green |

## Points

256. Expired commit reject regression DistLab.
257. Pending purge loop exists on server.
258. Multi-origin logical_ts bump soak.
259. Peer L1 bridge gcm get.
260. Gossip mgmt_audit_handler slots field.
261. Connected-only peers epidemic.
262. Delta re-queue sent==0.
263. Agreement checker majority-aware.
264. FaultInjector delivery on strong transport.
265. Any fail in T2–T5 → fix listed here with scenario name.
266. Commit path lww_lost abort peers.
267. Abort retries bounded.
268. Prepare idempotent same op_id.
269. Backup restore on abort after commit.
270. get promote from strong backend on L1 miss.
271–295. Reserved bug slots + verify suite + complete.
    (271–285 filled as discovered; 286–290 docs; 291–295 gate.)

---

# TRACK T7 — Claims, docs, full gate, commit (50 pts)

## Stages

| Stage | Name | Exit |
| --- | --- | --- |
| T7-S0 | claims.yaml | updated |
| T7-S1 | Docs cross-links | updated |
| T7-S2 | Full gate | green |
| T7-S3 | Commit | clean tree |

## Points

296. INV-CACHE-STRONG-01 proof list all DistLab/live/chaos paths.
297. INV-SHARED-AUDIT-01 proof list DistLab/live/chaos.
298. non_claims DistLab ≠ Elle/Jepsen JVM.
299. non_claims DistLab ≠ BFT.
300. non_claims TTL purge best-effort.
301. non_claims live same-host ≠ WAN.
302. This master plan status → Complete when gate passes.
303. DISTLAB_AND_SEVEN_TRACKS points to master plan.
304. RESIDUAL_HONESTY Phase 4 master plan.
305. HARDENING_PLAN link master.
306. README DistLab.
307. ARCHITECTURE DistLab.
308. CACHING_SYSTEM honesty if needed.
309. Package __main__ help.
310. tests/harness complete exports.
311. Full related pytest green.
312. DistLab CLI list+run smoke.
313. Prior unit/integration green.
314. Commit with complete message.
315. Working tree clean.
316–345. Buffer: changelog note; curriculum pointer optional; support_only claims; double-check non_claims; final status table.

---

## Status dashboard

| Track | Pts | Stages | Status |
| --- | --- | --- | --- |
| T1 DistLab core | 60 | S0–S5 | **complete** — generator/registry/CLI/core tests |
| T2 STRONG stress | 65 | S0–S5 | **complete** — registry builtins + scenario suite |
| T3 Adversarial | 55 | S0–S4 | **complete** — lie/fail/wrong_cluster + not_bft demo |
| T4 Audit epidemic | 60 | S0–S4 | **complete** — burst/partition/digest/nemesis registry |
| T5 Live mesh | 60 | S0–S4 | **complete** — `live.py` + `test_distlab_live.py` |
| T6 Architecture | 55 | S0–S3 | **complete** — prior residuals + live coex drain clear |
| T7 Claims/gate | 50 | S0–S3 | **complete** — claims/docs + full related gate |

**Total: ~405 core pts + buffers. All tracks complete.**

### Delivered surfaces (gate artifacts)

| Artifact | Path |
| --- | --- |
| Generator | `mpreg/testing/distlab/generator.py` |
| Registry | `mpreg/testing/distlab/registry.py` |
| Builtins | `mpreg/testing/distlab/builtins.py` (~27 scenarios) |
| Live helpers | `mpreg/testing/distlab/live.py` |
| CLI | `python -m mpreg.testing.distlab` |
| Tests | `tests/testing/test_distlab_*.py` |

## Implementation order (mandatory)

```
T1-S0..S5 → T2-S0..S5 → T3-S0..S4 → T4-S0..S4 → T5-S0..S4
  → T6 (continuous) → T7-S0..S3
```

Re-run full gate after T5 and after T7.

## Bug-fix policy

1. Confirm in-scope (not non_claim).
2. Fix production module.
3. Add DistLab regression scenario named `reg_<bug>`.
4. Check off T6 point + scenario in registry.
