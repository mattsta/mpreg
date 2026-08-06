# DistLab + STRONG + shared audit — proof ledger (T16)

Point → test → claim mapping for residual honesty. Scope is **in-process and
same-host multi-process**, not WAN / Elle / BFT / fsync.

| Point / track | Proof path | Claim |
| --- | --- | --- |
| T2 happy 3/5/7 | `tests/testing/test_distlab_strong_scenarios.py`, builtins `strong.happy_*` | INV-CACHE-STRONG-01 |
| T2 soak + lab SLI | `strong.soak_20/50`, `tests/testing/test_distlab_sli.py` | support; non_claim WAN SLA |
| T2 drop prepare/commit | builtins + chaos stress | INV-CACHE-STRONG-01 residual-free |
| T13 drop_abort | `strong.drop_abort` + purge TTL | residual-free after GC |
| T13 live 4-node | `test_distlab_live_strong_happy_4` | INV-CACHE-STRONG-01 live |
| T13 mid-put kill | `test_distlab_live_strong_mid_put_peer_kill` | residual-free survivors |
| T13 audit late joiner | `test_distlab_live_audit_late_joiner` | INV-SHARED-AUDIT-01 live |
| T4 audit scenarios | `test_distlab_audit_scenarios.py` | INV-SHARED-AUDIT-01 |
| T15 Hypothesis drops | `test_random_commit_drop_subset_residual_free` | INV-CACHE-STRONG-01 |
| T15 Hypothesis audit heal | `test_partition_pair_heal_converges` | INV-SHARED-AUDIT-01 |
| T11 metrics JSON | `tests/test_strong_audit_monitoring_endpoints.py`, `tests/server_pkg/test_strong_audit_metrics.py` | support_only ops |
| T11 Prometheus | same + `mpreg_strong_*` / `mpreg_shared_audit_*` | support_only ops |
| T12 CLI | `mpreg monitor strong\|audit`, `mpreg doctor --strong/--audit` | support_only ops |
| Coexistence | `test_strong_audit_coexistence`, live both | both INV-* |

| T17 suite CLI | `mpreg distlab suite`, `test_registry_run_suite_*` | support |
| T17 residual non-committer | `test_success_aborts_prepared_non_committers` | INV-CACHE-STRONG-01 |
| T17 live metrics e2e | `test_distlab_live_strong_metrics_e2e` | support ops |
| T17 multi-GCM RYW | `test_mesh_ryw_all_gcms_after_strong_put` | INV-CACHE-STRONG-01 bridge |
| T17 history taxonomy | `test_history_error_code_and_outcome_counts` | support |
| T18 STRONG get/delete 1012 | `test_strong_get_always_refuses_1012`, `test_strong_delete_always_refuses_1012`, property | INV-CACHE-STRONG-01 refuse |
| T18 capabilities + counters | `strong_status`, `build_strong_metrics`, prom refuse series | support ops |
| T18 smoke preset | `mpreg distlab suite --preset smoke`, `test_registry_run_suite_smoke_preset` | support |
| T18 live audit metrics | `test_distlab_live_audit_metrics_e2e` | support ops INV-SHARED-AUDIT-01 |
| T18 commit+abort drop hyp | `test_full_commit_drop_plus_abort_drop_residual_free_after_gc`, minority success GC | INV-CACHE-STRONG-01 |
| T19 doctor honesty | `evaluate_strong_doctor_payload`, `test_doctor_strong_evaluate_payload_honesty` | support ops |
| T19 refuse scenario | `strong.refuse_get_delete`, smoke preset | INV-CACHE-STRONG-01 refuse |
| T19 monitor summary | `mpreg monitor strong --format table` capabilities line | support ops |
| T20 config-check groups | `test_config_check_*_honesty_warnings`, strong_cache/shared_audit groups | support ops |
| T20 live doctor e2e | `test_distlab_live_doctor_strong_audit_e2e` | support ops |
| T21 OpenAPI schemas | `test_t21_openapi_strong_schema_honesty` | support ops |
| T21 curriculum refuse | `cache_strong_quorum` get/delete 1012 + RYW scenario | INV-CACHE-STRONG-01 teach |
| T22 audit capabilities | `test_t22_shared_audit_metrics_capabilities_honesty` | support ops |
| T22 audit doctor | `evaluate_shared_audit_doctor_payload`, CLI honesty test | support ops |
| T22 OpenAPI audit caps | `test_t22_openapi_shared_audit_schema_capabilities` | support ops |
| T22 curriculum audit | `shared_audit_mesh` metrics capabilities scenario | INV-SHARED-AUDIT-01 teach |
| T22 audit-core preset | `test_registry_run_suite_audit_core_preset` | support |
| T23 config-check audit caps | `test_t23_config_check_shared_audit_capabilities_parity` | support ops |
| T23 live audit caps | `test_distlab_live_audit_metrics_e2e` capabilities asserts | support ops |
| T23 live doctor audit eval | `test_distlab_live_doctor_strong_audit_e2e` evaluate_shared_audit | support ops |
| T24 prom cap gauges | `mpreg_strong_cap_*` / `mpreg_shared_audit_cap_*` endpoint test | support ops |
| T24 honesty alerts | `test_t24_prometheus_alerts_include_honesty_rules` | support ops |
| T24 ops_cli monitor | `ops_cli_tour` monitor strong/audit + doctor scenario | support teach |
| T25 live prom strong caps | `test_distlab_live_strong_metrics_e2e` cap gauges 0/1 | support ops |
| T25 live prom audit caps | `test_distlab_live_audit_metrics_e2e` cap gauges 0/1 | support ops |
| T25 ci-core preset | `test_registry_run_suite_ci_core_preset`, `resolve_preset` | support |
| T26 coexistence prom caps | `test_distlab_live_doctor_strong_audit_e2e` dual cap gauges | support ops |
| T27 abort counters | `aborts_peer_ok/fail`, prom series, GCM status | support ops CFT |
| T27 CFT DistLab | `strong.cft_partial_commit_lost_abort` | honesty (not residual-free) |
| T27 CFT Hypothesis | `test_cft_partial_commit_plus_lost_abort_leaves_peer_l1` | honesty CFT limit |
| T27 curriculum honesty | `test_t27_curriculum_honesty_apps_main` | teach |
| T27 doctor CFT | `evaluate_strong_doctor_payload` fails closed on `cft_only`/`abort_best_effort` false | support ops |
| T27 prom CFT alerts | `MPREGStrongCapCftOnlyMissing`, `MPREGStrongCapAbortBestEffortMissing` | support ops |
| T28 monitor CFT | `monitor strong --format table` cft/abort_be/abort_fail | support ops |
| T28 live CFT prom | `test_distlab_live_strong_metrics_e2e` CFT caps + abort series | support ops |
| T28 LWW heal DistLab | `strong.cft_residual_healed_by_lww` | honesty (not reliable ABORT) |
| T28 presets | strong-core/ci-core include CFT scenarios | support |
| T28 ops curriculum | `ops_cli_tour` CFT monitor fields | teach |
| T29 TTL honesty | `pending_ttl_clears_residual_l1=false`, doctor, prom alert | support ops |
| T29 DistLab TTL | `strong.cft_residual_survives_pending_purge` | honesty (not residual GC) |
| T29 Hypothesis TTL | `test_cft_residual_survives_pending_purge` | honesty |
| T29 visible/backups | GCM snapshot `visible_count` / `backups_count` | support ops |
| T30 orphan backup GC | `_prune_orphan_backups`, `strong.cft_orphan_backup_gc` | product fix |
| T31 monitor polish | visible/backups/ttl_gc on `monitor strong` table | support ops |
| T31 live TTL gauge | e2e `pending_ttl_clears_residual_l1` prom 0 | support ops |
| T32 visible/backups prom | `mpreg_strong_visible`, `mpreg_strong_backups`, prune counter | support ops |
| T32 backups_pruned | backend counter + GCM/metrics | product ops |
| T32 config-check CFT | strong_cache caps cft/abort/ttl honesty | support ops |
| T33 curriculum CFT | `cache_strong_quorum` residual + LWW heal scenario | teach |
| T33 claims.yaml | INV-CACHE-STRONG-01 CFT text + non_claims | honesty |
| T33 doctor counts | `ttl_gc` / visible / backups / pruned in doctor detail | support ops |
| T34 CACHING_SYSTEM | CFT honesty in product caching doc | honesty |
| T34 purge prune | `purge_expired_pending` → orphan backup GC | product |
| T34 Hypothesis GC | `test_cft_orphan_backups_bounded_under_repeated_residual` | product |
| T35 design doc | CFT residual-free invariant qualified in design doc | honesty |
| T36 abort_fail peers | `last_abort_fail_peers`, quorum_info.abort_fail_peers | product ops |
| T36 DistLab CFT peers | `strong.cft_partial_commit_lost_abort` asserts n1 ∈ fail peers | honesty |
| T36 client/catalog honesty | client guide, APP/FEATURE catalogs, design alt table | honesty |
| T36 doctor/monitor peers | abort_fail_peers on doctor detail + monitor table | support ops |
| T37 retry_abort | `StrongPutCoordinator.retry_abort` clears residual when ABORT lands | product |
| T37 DistLab retry | `strong.cft_retry_abort_clears_residual` + strong-core/ci-core | product |
| T37 Hypothesis retry | `test_cft_retry_abort_clears_residual_after_heal` | product |
| T37 curriculum | `cache_strong_quorum` retry_abort + LWW; ops_cli abort_fail_peers | teach |
| T38 GCM retry | `GlobalCacheManager.strong_retry_abort` + status counters | product |
| T38 CACHING_SYSTEM | abort_fail_peers + retry_abort product docs | honesty |
| T39 retry prom | `mpreg_strong_retry_abort_*_total` series | support ops |
| T39 doctor/monitor retry | retry_abort= / retry_cleared= fields | support ops |
| T39 design doc retry | CFT exception documents ops-driven retry | honesty |
| T40 live retry e2e | `test_distlab_live_strong_metrics_e2e` retry counters + prom | support ops |
| T40 OPERATE retry | curriculum documents retry prom + monitor fields | teach |
| T41 retry_ops cap | `retry_abort_ops_driven` + prom/doctor/alert | honesty |
| T41 config-check | strong_cache.capabilities.retry_abort_ops_driven | support ops |
| T42 RPC retry | `mpreg.cache.strong_retry_abort` plane handler | product |
| T42 client retry | `MPREGClient.cache_strong_retry_abort` | product |
| T42 CacheOpResult | operation_id + quorum_info on façade | product |
| T42 cache_put wire | operation_id + quorum_info on STRONG put RPC | product |
| T42 honesty | client guide + curriculum + claims non_claim | honesty |
| T43 CLI retry | `mpreg client cache-strong-retry-abort` | support ops |
| T43 CLI honesty | exit≠0 on still-fail + CFT banner | honesty |
| T44 live client RPC | `test_live_client_rpc_strong_retry_abort_clears_residual` | product |
| T44 GCM counters | retry_abort_calls/cleared after live client RPC | support ops |
| T44 self-target fix | retry_abort local.abort when peers includes self | product |
| T45 honesty scan | product-doc banned residual-free/auto-heal phrases | honesty |
| T45 ops_cli retry | cache-strong-retry-abort --help in ops_cli_tour | teach |
| T46 client locs | `cache_strong_retry_abort(..., locs=…)` | product |
| T46 CLI --loc | optional resource pin on retry-abort CLI | support ops |
| T47 live cap scrape | `mpreg_strong_cap_retry_abort_ops_driven==1` live e2e | support ops |
| T47 client RPC metrics | cache_strong_retry_abort on live metrics e2e | product |
| T48 OpenAPI cache RPC | `PlatformCacheRpcCatalog` + strong_retry_abort FQN | support ops |
| T49 GCM curriculum | `cache_strong_quorum` GCM.strong_retry_abort residual→clear | teach |
| T49 DistLab self-target | `strong.cft_retry_abort_self_target` + strong-core/ci-core | product |
| T49 ops_surfaces meta | clears-residual meta lists full ops stack | honesty |
| T50 Hypothesis self-target | `test_cft_retry_abort_self_target_clears_local` | product |
| T50 residual gate | `test_t49_residuals` + `test_t50_residuals` | honesty |
| T51 doctor op_id | `abort_fail_op_id=` on evaluate_strong_doctor_payload | support ops |
| T51 residual ops hint | `strong_residual_ops_hint` → cache-strong-retry-abort | support ops |
| T51 monitor table | abort_fail_op_id + yellow hint on residual candidates | support ops |
| T52 DistLab GCM retry | `strong.cft_gcm_retry_abort_clears_residual` + strong-core | product |
| T53 residual_ops_hint | `format_residual_ops_hint` + metrics/status/OpenAPI field | support ops |
| T53 client guide | metrics → residual_ops_hint → CLI ops loop | teach |
| T54 Hypothesis GCM | `test_cft_gcm_retry_abort_clears_residual_after_heal` | product |
| T55 curriculum ops loop | ops_cli_tour residual_ops_hint step + CACHING/design | teach |
| T56 strong-core membership | registry gate for retry_abort ops scenarios | honesty |
| T57 live residual_ops_hint | live metrics e2e field present + empty after clean put | support ops |
| T58 doctor prefer hint | strong_residual_ops_hint uses server string first | support ops |
| T59 hint key enrich | residual_ops_hint fills ns/key from recent_abort_fails | support ops |
| T60 live enriched hint | `test_distlab_live_residual_ops_hint_enriched_e2e` | support ops |
| T61 OpenAPI hint example | residual_ops_hint + recent_abort_fails examples | support ops |
| T62 DistLab hint enriched | `strong.cft_residual_ops_hint_enriched` + strong-core | product |
| T63 Hypothesis hint enrich | `test_format_residual_ops_hint_enriches_ns_key` | product |
| T64 catalog/docs hint | FEATURE_CATALOG + client guide + CACHING_SYSTEM | teach |
| T65 config-check explain | strong_cache guide residual_ops_hint ops loop | support ops |
| T66 curriculum explain assert | ops_cli_tour residual_ops_hint + CFT honesty | teach |
| T67 design-doc hint polish | SHARED_AUDIT design format_residual_ops_hint | teach |
| T68 ledger + live doctor polish | ledger T66–T71; live doctor/hint e2e | honesty |
| T69 config-check pytest hint | test_config_check_explain residual_ops_hint | support ops |
| T70 APP_CATALOG ops_cli hint | ops_cli_tour product + residual_ops_hint | teach |
| T71 doctor JSON residual_ops_hint | doctor strong rows residual_ops_hint field | support ops |
| T72 live doctor residual_ops_hint | live doctor e2e empty hint + prom gauge 0 | support ops |
| T73 prom abort_fail_peers gauge | mpreg_strong_abort_fail_peers | support ops |
| T74 Hypothesis doctor hint | residual peers⇒hint; dishonest caps fail | product |
| T75 prom residual info alert | MPREGStrongAbortFailPeersPresent | support ops |
| T76 OpenAPI prom residual | /metrics/strong desc abort_fail_peers gauge | support ops |
| T77 docs prom residual | CACHING_SYSTEM + FEATURE_CATALOG gauge/alert | teach |
| T78 curriculum residual_ops_hint | cache_strong_quorum GCM hint assert | teach |
| T79 live enriched + prom gauge | enriched e2e abort_fail_peers >= 1 | support ops |
| T80 abort_fail_peer_count | count_abort_fail_peers + metrics/OpenAPI/GCM | product |
| T81 DistLab peer count | hint_enriched abort_fail_peer_count | product |
| T82 ops_cli doctor JSON hint | doctor --strong --format json residual_ops_hint | teach |
| T83 client guide peer count | MPREG_CLIENT_GUIDE gauge + doctor JSON | teach |
| T84 Hypothesis peer count | count_abort_fail_peers properties | product |
| T85 live peer count | live doctor 0 + enriched >= 1 | support ops |
| T86 design/OPERATE peer count | design + OPERATE abort_fail_peer_count | teach |
| T87 doctor peer count | detail + JSON abort_fail_peer_count | support ops |
| T88 monitor peer count | monitor strong abort_fail_peer_count= | support ops |
| T89 config-check peer count | explain strong_cache abort_fail_peer_count | support ops |
| T90 ops_cli peer count | monitor + doctor JSON curriculum asserts | teach |
| T91 curriculum peer count | cache_strong_quorum abort_fail_peer_count | teach |
| T92 CACHING peer count | CACHING_SYSTEM abort_fail_peer_count | teach |
| T93 runbook peer count | runbook count_abort_fail_peers | teach |
| T94 FEATURE_CATALOG peer count | cache.strong abort_fail_peer_count | teach |
| T95 GCM count helper | strong_status count_abort_fail_peers | product |
| T96 live doctor peer count | detail abort_fail_peer_count=0/>0 | support ops |
| T97 Hypothesis peer-count max | max(reported, len(peers)) | product |
| T98 metrics peer-count unit | build_strong_metrics abort_fail_peer_count | product |
| T99 OPERATE doctor peer count | OPERATE detail/JSON/monitor | teach |
| T100 doctor JSON peer count int | strong_doctor_json_residual_fields int | product |
| T101 doctor JSON peers list | last_abort_fail_peers on doctor JSON | product |
| T102 OpenAPI peer count example | abort_fail_peer_count example: 1 | support ops |
| T103 OBS/SLO peer count | OBSERVABILITY + SLO residual signals | teach |
| T104 claims peer-count closeout | claims.yaml T66–T109 + non_claims | honesty |
| T105 master residual index | DISTLAB + master residual honesty index | teach |
| T106 curriculum doctor JSON types | ops_cli int/list asserts | teach |
| T107 docs doctor JSON types | design/runbook/client/OPERATE/CACHING | teach |
| T108 doctor JSON fields unit | strong_doctor_json_residual_fields unit | product |
| T109 gate T100–T109 | residual closeouts + ledger gate | support |
| T110 doctor JSON op_id | last_abort_fail_op_id on doctor JSON | product |
| T111 live doctor JSON fields | live e2e strong_doctor_json_residual_fields | support ops |
| T112 curriculum doctor op_id | ops_cli last_abort_fail_op_id str | teach |
| T113 Hypothesis doctor JSON types | residual fields type property | product |
| T114 docs doctor op_id | OPERATE/runbook/client op_id | teach |
| T115 design/arch doctor JSON | design + ARCHITECTURE residual types | teach |
| T116 claims T110–T119 | claims.yaml proof + non_claims | honesty |
| T117 master index T110–T119 | DISTLAB residual honesty band | teach |
| T118 catalog doctor op_id | FEATURE/APP_CATALOG | teach |
| T119 gate T110–T119 | residual closeouts + ledger gate | support |
| T120 config-check doctor JSON types | explain strong_cache doctor JSON types | support ops |
| T121 CACHING doctor op_id | CACHING_SYSTEM last_abort_fail_op_id | teach |
| T122 PRODUCTION residual pointer | PRODUCTION residual scrape + retry-abort | teach |
| T123 OpenAPI op_id example | last_abort_fail_op_id example | support ops |
| T124 OpenAPI peers example | last_abort_fail_peers example | support ops |
| T125 config-check pytest doctor JSON | guide doctor JSON type asserts | support ops |
| T126 OpenAPI residual examples unit | count/op_id/peers OpenAPI examples | product |
| T127 claims T120–T129 | claims.yaml proof + non_claims | honesty |
| T128 master index T120–T129 | DISTLAB residual honesty band | teach |
| T129 gate T120–T129 | residual closeouts + ledger gate | support |
| T130 monitor JSON residual ensure | monitor strong JSON setdefault residual fields | product |
| T131 curriculum monitor JSON | ops_cli monitor strong --format json | teach |
| T132 doctor help residual types | doctor --strong help residual field types | support ops |
| T133 GETTING_STARTED residual | GETTING_STARTED doctor/monitor residual loop | teach |
| T134 README residual doctor | README doctor/monitor residual field types | teach |
| T135 doctor help unit | CLI help residual field asserts | support ops |
| T136 claims T130–T139 | claims.yaml proof + non_claims | honesty |
| T137 master index T130–T139 | DISTLAB residual honesty band | teach |
| T138 OPERATE monitor JSON | OPERATE monitor strong JSON residual | teach |
| T139 gate T130–T139 | residual closeouts + ledger gate | support |

## Non-claims (do not market)

- Lab SLI p99 ≠ production WAN SLA
- DistLab ≠ Jepsen/Elle/WAN geo
- STRONG put-only MVP (get/delete not majority)
- Shared audit ≠ SIEM / infinite retention / BFT
- CFT only — Byzantine COMMIT lies are `not_bft`
- Partial peer COMMIT apply + lost ABORT may leave peer L1 (ABORT best-effort)
- `abort_fail_peers` / `last_abort_fail_peers` are CFT residual candidates only
  (not residual-free proof, not automatic residual heal)
- `retry_abort` is ops-driven CFT best-effort — not automatic background heal
- Self-target `peers=[self]` is RPC fan-in local.abort correctness — not
  automatic residual heal, not BFT, not WAN
- Doctor/monitor residual ops hint is operator guidance after recovery — not
  automatic heal, not SIEM orchestration, not residual-free proof
- `residual_ops_hint` JSON field is the same guidance string — not a heal
  toggle, not SIEM, not residual-free proof
- Key enrichment from `recent_abort_fails` is process-local best-effort — not
  durable audit log, not SIEM, not multi-tenant isolation proof
- Live enriched-hint e2e seeds coordinator abort-fail diagnostics + peer
  prepare/commit residual — not kernel drop injectors, not WAN, not auto-heal
- DistLab `strong.cft_residual_ops_hint_enriched` proves guidance only — does
  not clear residual
- Curriculum config-check residual_ops_hint assert is teachable guidance — not
  live residual clear, not automatic heal
- Design-doc residual_ops_hint polish is documentation — not residual-free claim
- config-check pytest guide assert is operator guidance coverage — not auto-heal
- APP_CATALOG ops_cli_tour residual_ops_hint row is teachable inventory — not
  residual-free product claim
- Doctor JSON `residual_ops_hint` field is the same guidance string — not a heal
  toggle, not SIEM orchestration, not residual-free proof
- `mpreg_strong_abort_fail_peers` gauge is CFT residual candidate count — not
  residual-free proof, not automatic heal, not WAN SLO
- Hypothesis doctor residual hint properties are pure unit checks — not live
  mesh, not automatic heal, not SIEM
- `MPREGStrongAbortFailPeersPresent` info alert is ops guidance — not automatic
  heal, not residual-free proof, not WAN/BFT/SIEM
- OpenAPI / docs prom residual gauge mentions are documentation — not auto-heal
- Curriculum residual_ops_hint GCM assert is teachable guidance — not auto-heal
- Live enriched e2e prom gauge >= 1 seeds coordinator fields — not kernel drop,
  not WAN, not automatic heal
- `count_abort_fail_peers` / `abort_fail_peer_count` are process-local ops
  signals — not residual-free proof, not automatic heal, not WAN SLO
- ops_cli doctor JSON residual_ops_hint assert is teachable — not auto-heal
- Client guide peer-count docs are guidance — not residual-free product claim
- Hypothesis count_abort_fail_peers is pure unit formatting — not live mesh,
  not automatic heal
- Live abort_fail_peer_count asserts seed coordinator fields — not kernel drop,
  not WAN, not automatic heal
- Design/OPERATE abort_fail_peer_count docs are guidance — not residual-free claim
- Doctor/monitor abort_fail_peer_count fields are ops presentation — not
  automatic heal, not residual-free proof, not WAN SLO
- Curriculum peer-count asserts are teachable guidance — not auto-heal
- CACHING/runbook/FEATURE_CATALOG peer-count docs are guidance — not
  residual-free product claim
- GCM strong_status count_abort_fail_peers is the same process-local ops
  signal — not residual-free proof, not automatic heal
- Live doctor detail abort_fail_peer_count asserts are same-host multi-process
  — not kernel drop, not WAN, not automatic heal
- Hypothesis max(reported, peers) is pure unit — not live mesh, not auto-heal
- build_strong_metrics peer-count unit tests use mocks — not live mesh
- OPERATE doctor/monitor peer-count docs are guidance — not residual-free claim
- Doctor JSON abort_fail_peer_count int type is ops presentation — not
  automatic heal, not residual-free proof, not SIEM
- Doctor JSON last_abort_fail_peers list is ops presentation — not
  residual-free proof, not automatic heal
- OpenAPI abort_fail_peer_count example is documentation — not auto-heal
- OBSERVABILITY/SLO residual peer-count docs are guidance — not WAN SLO
- claims residual closeout inventory T66–T109 is proof list — not residual-free
  product guarantee under lost ABORT
- Master/DISTLAB residual honesty index is planning cross-link — not Jepsen
- Curriculum doctor JSON int/list asserts are teachable — not auto-heal
- Design/runbook/client doctor JSON type polish is documentation — not
  residual-free claim
- strong_doctor_json_residual_fields unit tests are pure typing — not live mesh
- Residual honesty gate T100–T109 is same-host closeout coverage — not
  WAN/BFT/Jepsen
- Doctor JSON last_abort_fail_op_id is ops presentation — not automatic heal
- Live doctor JSON residual field asserts are same-host multi-process — not
  kernel drop, not WAN, not automatic heal
- Curriculum doctor JSON op_id assert is teachable — not auto-heal
- Hypothesis doctor JSON residual types are pure unit — not live mesh
- OPERATE/runbook/client op_id docs are guidance — not residual-free claim
- Design/ARCHITECTURE doctor JSON type docs are guidance — not residual-free claim
- claims T110–T119 inventory is proof list — not residual-free under lost ABORT
- Master index T110–T119 is planning cross-link — not Jepsen/WAN
- FEATURE/APP_CATALOG doctor op_id rows are teachable inventory — not residual-free claim
- Residual honesty gate T110–T119 is same-host closeout — not WAN/BFT/Jepsen
- config-check doctor JSON types explain is operator guidance — not auto-heal
- CACHING_SYSTEM doctor op_id docs are guidance — not residual-free claim
- PRODUCTION residual pointer is operator guidance — not WAN SLO, not auto-heal
- OpenAPI last_abort_fail_op_id/peers examples are documentation — not auto-heal
- config-check pytest doctor JSON asserts are guidance coverage — not live mesh
- OpenAPI residual examples unit tests are pure schema checks — not live mesh
- claims T120–T129 inventory is proof list — not residual-free under lost ABORT
- Master index T120–T129 is planning cross-link — not Jepsen/WAN
- Residual honesty gate T120–T129 is same-host closeout — not WAN/BFT/Jepsen
- monitor strong JSON residual ensure is ops presentation — not auto-heal
- Curriculum monitor strong JSON asserts are teachable — not auto-heal
- doctor --strong help residual types are guidance — not residual-free claim
- GETTING_STARTED residual ops loop is guidance — not WAN SLO, not auto-heal
- README residual doctor/monitor types are guidance — not residual-free claim
- doctor help unit residual asserts are pure CLI help — not live mesh
- claims T130–T139 inventory is proof list — not residual-free under lost ABORT
- Master index T130–T139 is planning cross-link — not Jepsen/WAN
- OPERATE monitor JSON residual docs are guidance — not residual-free claim
- Residual honesty gate T130–T139 is same-host closeout — not WAN/BFT/Jepsen

## Gate commands

```bash
uv run mpreg distlab list
uv run mpreg distlab suite --preset smoke --json
uv run pytest tests/testing/ tests/server_pkg/test_shared_audit*.py \
  tests/server_pkg/test_strong_audit_metrics.py \
  tests/core/test_cache_strong*.py tests/integration/test_cache_strong*.py \
  tests/integration/test_shared_audit*.py tests/integration/test_strong_audit_coexistence.py \
  tests/chaos/test_strong_chaos_stress.py tests/chaos/test_shared_audit_chaos.py \
  tests/invariants/test_cache_strong*.py tests/invariants/test_shared_audit*.py \
  tests/test_strong_audit_monitoring_endpoints.py \
  tests/test_cli_strong_audit_monitor.py \
  tests/chaos/test_t14_residuals.py::test_erg_t14_01_openapi_matches_route_table \
  tests/chaos/test_t21_residuals.py tests/chaos/test_t22_residuals.py \
  tests/chaos/test_t23_residuals.py tests/chaos/test_t24_residuals.py \
  tests/chaos/test_t25_residuals.py tests/chaos/test_t26_residuals.py \
  tests/chaos/test_t27_residuals.py tests/chaos/test_t28_residuals.py \
  tests/chaos/test_t29_residuals.py tests/chaos/test_t30_residuals.py \
  tests/chaos/test_t31_residuals.py tests/chaos/test_t32_residuals.py \
  tests/chaos/test_t33_residuals.py tests/chaos/test_t34_residuals.py \
  tests/chaos/test_t35_residuals.py tests/chaos/test_t36_residuals.py \
  tests/chaos/test_t37_residuals.py tests/chaos/test_t38_residuals.py \
  tests/chaos/test_t39_residuals.py tests/chaos/test_t40_residuals.py \
  tests/chaos/test_t41_residuals.py tests/chaos/test_t42_residuals.py \
  tests/chaos/test_t43_residuals.py tests/chaos/test_t44_residuals.py \
  tests/chaos/test_t45_residuals.py tests/chaos/test_t46_residuals.py \
  tests/chaos/test_t47_residuals.py tests/chaos/test_t48_residuals.py \
  tests/chaos/test_t60_residuals.py tests/chaos/test_t61_residuals.py \
  tests/chaos/test_t62_residuals.py tests/chaos/test_t63_residuals.py \
  tests/chaos/test_t64_residuals.py tests/chaos/test_t65_residuals.py \
  tests/chaos/test_t66_residuals.py tests/chaos/test_t67_residuals.py \
  tests/chaos/test_t68_residuals.py tests/chaos/test_t69_residuals.py \
  tests/chaos/test_t70_residuals.py tests/chaos/test_t71_residuals.py \
  tests/chaos/test_t72_residuals.py tests/chaos/test_t73_residuals.py \
  tests/chaos/test_t74_residuals.py tests/chaos/test_t75_residuals.py \
  tests/chaos/test_t76_residuals.py tests/chaos/test_t77_residuals.py \
  tests/chaos/test_t78_residuals.py tests/chaos/test_t79_residuals.py \
  tests/chaos/test_t80_residuals.py tests/chaos/test_t81_residuals.py \
  tests/chaos/test_t82_residuals.py tests/chaos/test_t83_residuals.py \
  tests/chaos/test_t84_residuals.py tests/chaos/test_t85_residuals.py \
  tests/chaos/test_t86_residuals.py \
  tests/chaos/test_t87_residuals.py tests/chaos/test_t88_residuals.py \
  tests/chaos/test_t89_residuals.py tests/chaos/test_t90_residuals.py \
  tests/chaos/test_t91_residuals.py tests/chaos/test_t92_residuals.py \
  tests/chaos/test_t93_residuals.py tests/chaos/test_t94_residuals.py \
  tests/chaos/test_t95_residuals.py tests/chaos/test_t96_residuals.py \
  tests/chaos/test_t97_residuals.py tests/chaos/test_t98_residuals.py \
  tests/chaos/test_t99_residuals.py \
  tests/chaos/test_t100_residuals.py tests/chaos/test_t101_residuals.py \
  tests/chaos/test_t102_residuals.py tests/chaos/test_t103_residuals.py \
  tests/chaos/test_t104_residuals.py tests/chaos/test_t105_residuals.py \
  tests/chaos/test_t106_residuals.py tests/chaos/test_t107_residuals.py \
  tests/chaos/test_t108_residuals.py tests/chaos/test_t109_residuals.py \
  tests/chaos/test_t110_residuals.py tests/chaos/test_t111_residuals.py \
  tests/chaos/test_t112_residuals.py tests/chaos/test_t113_residuals.py \
  tests/chaos/test_t114_residuals.py tests/chaos/test_t115_residuals.py \
  tests/chaos/test_t116_residuals.py tests/chaos/test_t117_residuals.py \
  tests/chaos/test_t118_residuals.py tests/chaos/test_t119_residuals.py \
  tests/chaos/test_t120_residuals.py tests/chaos/test_t121_residuals.py \
  tests/chaos/test_t122_residuals.py tests/chaos/test_t123_residuals.py \
  tests/chaos/test_t124_residuals.py tests/chaos/test_t125_residuals.py \
  tests/chaos/test_t126_residuals.py tests/chaos/test_t127_residuals.py \
  tests/chaos/test_t128_residuals.py tests/chaos/test_t129_residuals.py \
  tests/chaos/test_t130_residuals.py tests/chaos/test_t131_residuals.py \
  tests/chaos/test_t132_residuals.py tests/chaos/test_t133_residuals.py \
  tests/chaos/test_t134_residuals.py tests/chaos/test_t135_residuals.py \
  tests/chaos/test_t136_residuals.py tests/chaos/test_t137_residuals.py \
  tests/chaos/test_t138_residuals.py tests/chaos/test_t139_residuals.py \
  tests/integration/test_cache_strong_live_mesh.py \
  tests/testing/test_distlab_live.py::test_distlab_live_strong_metrics_e2e \
  tests/test_config_check_cli.py tests/test_unified_client.py -q
```
