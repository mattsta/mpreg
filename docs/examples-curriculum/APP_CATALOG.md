# App Catalog

Status: `shipped` | `partial` | `planned`

Levels:

- **L0** `00_getting_started`
- **L1** `01_simple`
- **L2** `02_moderate`
- **L3** `03_complex`
- **L4** `04_world`

Kinds: `product` | `plane` | `integration` | `legacy`

**Unified runner (entrypoints only):**

```bash
uv run mpreg-example list
uv run mpreg-example run <id>
uv run mpreg-example smoke
uv run mpreg-example suite
uv run mpreg-example demo tier1|tier2|tier3|quick|all_planes|product_vertical
uv run mpreg examples …   # same runner
uv run mpreg demo tier1   # delegates to mpreg-example
```

**Never** `python -m` / `uv run python`.

Living plan: [PROJECT_PLAN.md](./PROJECT_PLAN.md) · Friction: [API_FRICTION.md](./API_FRICTION.md)

## Matrix (99 shipped)

| ID                          | Level | Kind        | Primary lesson                               | Systems                   |
| --------------------------- | ----- | ----------- | -------------------------------------------- | ------------------------- |
| `hello_rpc`                 | L0    | product     | Register + RPC chain                         | rpc                       |
| `hello_cluster`             | L0    | product     | Multi-node resources + DAG                   | rpc, cluster              |
| `hello_trace`               | L0    | product     | Correlation timeline                         | monitoring                |
| `hello_pubsub`              | L0    | product     | Topic wildcards + fan-out                    | pubsub                    |
| `hello_cache`               | L0    | product     | Cache put/get                                | cache                     |
| `hello_ports`               | L0    | product     | Dynamic port allocation                      | rpc, ports                |
| `hello_queue`               | L0    | product     | Smallest queue send + subscribe              | queue                     |
| `hello_dns`                 | L0    | product     | Minimal DNS register + SRV resolve           | dns, discovery            |
| `ha_client_failover`        | L1    | product     | Multi-seed HA client                         | rpc, ha-client            |
| `job_queue_worker`          | L1    | product     | At-least-once + quorum                       | queue                     |
| `job_queue_dlq`             | L1    | product     | Poison retries → dead-letter                 | queue                     |
| `url_shortener_rpc`         | L1    | product     | CRUD-ish RPC + cache                         | rpc, cache                |
| `sensor_ingest_pubsub`      | L1    | product     | Multi-pattern sensor bus                     | pubsub                    |
| `session_cache`             | L1    | product     | Session TTL put/rotate                       | cache                     |
| `auto_port_bootstrap`       | L1    | legacy      | OS-assigned ports + join                     | rpc, cluster, ports       |
| `plane_rpc`                 | L1    | plane       | Full RPC plane tour                          | rpc                       |
| `plane_pubsub`              | L1    | plane       | Full pubsub plane tour                       | pubsub                    |
| `plane_queue`               | L1    | plane       | Full queue plane tour                        | queue                     |
| `plane_cache`               | L1    | plane       | Full cache plane tour                        | cache                     |
| `plane_fabric`              | L1    | plane       | Full fabric plane tour                       | fabric                    |
| `plane_monitoring`          | L1    | plane       | Full monitoring plane tour                   | monitoring                |
| `cache_atomic_ops`          | L1    | plane       | CAS / incr / structures / ns bulk            | cache                     |
| `namespace_policy_gate`     | L1    | plane       | Validate/apply/status/export/audit           | namespace                 |
| `plane_dns`                 | L1    | plane       | DNS register/list/describe/resolve/unregister | dns, discovery            |
| `unified_client_tour`       | L1    | product     | Four-plane façade + invalidate + map v2      | rpc, cache, queue         |
| `pubsub_request_reply`      | L1    | product     | publish_with_reply round-trip                | pubsub                    |
| `rpc_versioned_topic`       | L1    | product     | function_id + version + bare/opaque id       | rpc                       |
| `rpc_fqn_namespace`         | L1    | product     | Bare→FQN, mpreg.\* deny, bound ns            | rpc                       |
| `client_auth_token`         | L1    | product     | Monitoring bearer + client auth_token        | client, security          |
| `topic_taxonomy_tour`       | L1    | plane       | TopicValidator + taxonomy templates          | pubsub, taxonomy          |
| `persistence_kv`            | L1    | plane       | Memory + SQLite KV TTL                       | persistence               |
| `profile_settings_tour`     | L1    | product     | Packaged TOML profiles via from_path         | boot                      |
| `discovery_rate_limit`      | L1    | plane       | DiscoveryRateLimiter sliding window          | discovery                 |
| `observability_slo_trace`   | L1    | plane       | Golden signals + W3C traceparent             | monitoring                |
| `order_intake`              | L2    | product     | RPC+cache+pubsub+queue                       | multi-plane               |
| `media_pipeline`            | L2    | product     | Multi-stage ETL RPC                          | rpc, cluster              |
| `feature_flag_mesh`         | L2    | product     | Federated L4 flags                           | cache, fabric             |
| `webhook_dispatcher`        | L2    | product     | Events → durable egress                      | pubsub, queue             |
| `config_reload_live`        | L2    | legacy      | Restart durability                           | cache, queue, persistence |
| `rpc_plus_cache`            | L2    | integration | RPC output cached                            | rpc, cache                |
| `pubsub_plus_queue`         | L2    | integration | Fan-out → queue                              | pubsub, queue             |
| `cache_plus_federation`     | L2    | integration | L4 cache federation                          | cache, fabric             |
| `ml_inference_mesh`         | L2    | product     | Router + vision/NLP                          | rpc, cluster              |
| `cache_event_bus`           | L2    | integration | Cache ops → topic events                     | cache, pubsub             |
| `ops_cli_tour`              | L2    | legacy      | mpreg CLI call/dns/doctor friction           | ops, rpc, dns             |
| `notification_fanout`       | L2    | product     | Email/push/audit wildcards                   | pubsub                    |
| `billing_ledger`            | L2    | product     | Charge RPC + balance cache + settle queue    | rpc, cache, queue         |
| `inventory_reserve`         | L2    | product     | Stock reserve/release + cache                | rpc, cache                |
| `topic_queue_bridge`        | L2    | integration | Topic hits → durable queue                   | pubsub, queue             |
| `rpc_intermediate_results`  | L2    | product     | IntermediateResultCollector + DAG            | rpc                       |
| `topic_queue_router_lab`    | L2    | integration | TopicQueueRouter fanout + strategies         | pubsub, queue             |
| `topic_dependency_lab`      | L2    | plane       | TopicDependencyResolver graph + ready set    | rpc, pubsub               |
| `shipping_fulfillment`      | L2    | product     | Label RPC + track cache + dispatch queue     | rpc, cache, queue         |
| `multi_region_shop`         | L3    | product     | Two-cluster fabric RPC                       | fabric, multi-cluster     |
| `signed_route_border`       | L3    | legacy      | Signed routes + policy + rotation            | fabric, security          |
| `partition_safe_counter`    | L3    | product     | Majority vs minority quorum                  | consensus, chaos          |
| `discovery_join`            | L3    | product     | Third node join                              | discovery, cluster        |
| `chaos_checkout`            | L3    | product     | Deadlines + fail-closed partition            | chaos, rpc                |
| `fabric_snapshot_restart`   | L3    | legacy      | Fabric snapshot across restart               | fabric, persistence       |
| `tier3_expansion`           | L3    | legacy      | Full multi-system expansion                  | multi-plane               |
| `discovery_watch_summary`   | L3    | product     | catalog_watch + summary query/watch          | discovery, pubsub         |
| `fabric_graph_resilience`   | L3    | plane       | Dijkstra paths + circuit breaker             | fabric                    |
| `chaos_transport`           | L3    | plane       | Skew/dup/reorder/drop model                  | chaos                     |
| `rpc_deadline_budget`       | L3    | product     | M1 vs M2/M3 shared wall deadline             | rpc, client               |
| `multi_region_dns_policy`   | L3    | product     | US/EU DNS namespaces + regional RPC          | dns, rpc, fabric          |
| `routing_oracle_lab`        | L3    | plane       | RoutingOracle BFS + RaftOracle safety        | fabric, consensus         |
| `deadline_hop_budget`       | L3    | plane       | DeadlineBudget hop header decrement          | rpc, fabric               |
| `fabric_hub_hierarchy`      | L3    | plane       | Global/Regional/Local hub topology (library) | fabric                    |
| `leader_election_lab`       | L3    | plane       | Metric + quorum leader election fitness      | consensus                 |
| `global_edge_control_plane` | L4    | product     | Hub + US/EU edges + timeline                 | fabric, monitoring        |
| `multi_pop_edge_mesh`       | L4    | product     | Hub + US/EU/AP edges (second world tour)     | fabric, monitoring        |
| `shared_audit_mesh`         | L2    | product     | Cluster G-Set shared mgmt audit visibility   | ops, audit, gossip        |
| `cache_strong_quorum`       | L2    | product     | Majority-commit STRONG put + residual-free 1015 | cache                  |

## Legacy → unified mapping

| Legacy path / CLI                        | Canonical app id                                                 |
| ---------------------------------------- | ---------------------------------------------------------------- |
| `tier1_single_system_full --system rpc`  | `plane_rpc`                                                      |
| `… pubsub/queue/cache/fabric/monitoring` | `plane_*`                                                        |
| `tier2_integrations`                     | `rpc_plus_cache` + `pubsub_plus_queue` + `cache_plus_federation` |
| `tier3_multi_system_expansion`           | `tier3_expansion`                                                |
| `federation_hierarchical_demo`           | `multi_region_shop` / `global_edge_control_plane`                |

## Planned / residual (not empty shells)

**Empty of curriculum blockers.** Historical planned rows closed through Phase N;
Phase O only deepens truth (thin ensure count, `fabric.gossip` join).

| ID                            | Level | Notes                                                                          |
| ----------------------------- | ----- | ------------------------------------------------------------------------------ |
| ~~deeper mTLS local-cert~~    | L1    | **shipped** `tls_dev_handshake` (F12)                                          |
| ~~live WS partition chaos~~   | L3    | **shipped** `live_partition_chaos` drain/detach (F10)                          |
| ~~local WS auth enforcement~~ | L1    | **shipped** `rpc_auth_token` in `client_auth_token` (F11)                      |
| platform DX F1–F23            | —     | **G–N closed** (F22/F23 Phase N)                                               |
| operator topology residuals   | —     | multi-continent SLA / kernel TCP loss / multi-hub DAO — honest non-claims only |

### Phase J additions (2026-08-05)

| ID                         | Level | Kind    | Summary                              | Systems                  |
| -------------------------- | ----- | ------- | ------------------------------------ | ------------------------ |
| `tls_dev_handshake`        | L1    | product | Dev CA + wss:// RPC                  | security, transport, rpc |
| `discovery_resolver_audit` | L1    | plane   | resolver stats/resync + access audit | discovery                |
| `transport_health_attach`  | L1    | plane   | TransportHealthAggregator attach     | monitoring, transport    |
| `transport_protocol_tour`  | L1    | plane   | TCP framing + multi-protocol adapter | transport                |
| `queue_federation_lab`     | L2    | plane   | Queue federation wire types          | queue, fabric            |
| `blockchain_message_lab`   | L2    | plane   | BlockchainMessage + routes           | fabric                   |
| `live_partition_chaos`     | L3    | plane   | Live drain/detach + /ready           | chaos, monitoring, ops   |

## Depth contract

Every **shipped** row must satisfy FEATURE_CATALOG depth contract (≥2 scenarios;
L0 ≥3 ensures; L1+ ≥5 ensures) and appear in `features.py` APP_FEATURES.

### Phase K additions (2026-08-05)

| ID                          | Level | Kind    | Summary                                 |
| --------------------------- | ----- | ------- | --------------------------------------- | --------------- |
| `discovery_signatures_lab`  | L1    | plane   | HMAC discovery + gossip signatures      |
| `rpc_inventory_tour`        | L1    | plane   | rpc_describe + rpc_report               |
| `client_trace_bind`         | L1    | plane   | last_trace_context + bind_trace_context |
| `rpc_microbench_lab`        | L1    | plane   | RPC microbench + metrics snapshot depth | rpc, monitoring |
| `correlation_routing_lab`   | L1    | plane   | CorrelationTracker + no-loop assert     |
| `mtls_mesh_handshake`       | L2    | product | CERT_REQUIRED mTLS wss                  |
| `blockchain_hub_settlement` | L2    | plane   | HubMessageQueue settlement path         |
| `packet_loss_chaos`         | L3    | plane   | Plane drops + live drain compose        |

### Phase L additions (2026-08-05)

| ID                       | Level | Kind  | Summary                                     |
| ------------------------ | ----- | ----- | ------------------------------------------- |
| `queue_ack_receive_lab`  | L1    | plane | Poll receive + explicit ack + broadcast/FNF |
| `pubsub_client_backlog`  | L1    | plane | MPREGPubSubClient + get_backlog + unsubscribe |
| `cache_replication_geo`  | L1    | plane | Geo/replication/L2/invalidate/pers.mode     |
| `fabric_policy_modes`    | L1    | plane | Strict/explicit + catalog + link-state      |
| `rpc_concurrency_lab`    | L1    | plane | Concurrent gather + M3 + routing_topic      |
| `cluster_map_catalog`    | L1    | plane | cluster_map + map_v2 + catalog_query        |
| `mon_logging_json`       | L1    | plane | json_logs + correlation/health              |
| `chaos_crash_recover`    | L1    | plane | FaultInjector crash/recover                 |
| `tx_circuit_breaker_lab` | L1    | plane | CircuitBreaker state machine                |
| `ns_engine_direct`       | L1    | plane | In-process NamespacePolicyEngine            |

**App count:** 85 → **95** (+10 Phase L).

### Phase M additions (2026-08-05)

| ID                          | Level | Kind    | Summary                                   |
| --------------------------- | ----- | ------- | ----------------------------------------- |
| `pubsub_fabric_forward_lab` | L1    | plane   | PubSubForwardingMetadata hop/path/headers |
| `ops_cli_tour` (deepen)     | L2    | product | + planes/ns/discovery CLI scenarios       |

**App count:** 95 → **96** (+1 Phase M; ops deepen in place).

### Phase N (2026-08-05) — platform DX, no new apps

F22 Mapping headers coerce; F23 catalog `entry_type` default; `call_with_summary`
taught in `discovery_watch_summary`. **96** apps.

### Phase O (2026-08-05) — integrity polish, no new apps

| ID                             | Change                                                             |
| ------------------------------ | ------------------------------------------------------------------ |
| `client_trace_bind`            | deepen ensures (≥5) — was sole thin L1                             |
| `discovery_signatures_lab`     | `fabric.gossip` APP_FEATURES join + GossipMessage hop/TTL scenario |
| APP_CATALOG / OPERATE / README | scrub stale “planned” language                                     |

**App count:** **96** (no new apps; integrity polish).

### Phase P (2026-08-05) — multi-axis charter +1 app

| ID                                          | Level  | Kind  | Summary                                   |
| ------------------------------------------- | ------ | ----- | ----------------------------------------- |
| `rpc_microbench_lab`                        | L1     | plane | RPC microbench + metrics snapshot depth   |
| `client_trace_bind` / `unified_client_tour` | deepen | —     | W3C last_trace_context + discovery façade |

**App count:** 96 → **97** (+1 Phase P).

### Phase Q–T (2026-08-05) — sequential multi-axis, no new apps

Q: client.summary join; config-check `--explain`; fabric hop snapshot; mgmt audit;
persistence honesty; plane error_code; ops probe; rpc_list façade.
R: full MPREGClient API parity; admin audit CLI.
S: format_server_snapshot fabric; W3C converters; OPERATE `--explain`.
T: docs integrity + snapshot unit coverage.

**App count:** **97** (no new apps Q–T).

### Phase Y (2026-08) — shared audit + STRONG put product

| ID                    | Level | Kind    | Summary                                              |
| --------------------- | ----- | ------- | ---------------------------------------------------- |
| `shared_audit_mesh`   | L2    | product | `mgmt_audit_shared_enabled` G-Set cluster audit      |
| `cache_strong_quorum` | L2    | product | Flag-gated majority-commit STRONG put + 1015 residual |

**App count:** 97 → **99** (+2 Phase Y). Design:
`docs/SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md`. Claims: `INV-SHARED-AUDIT-01`,
`INV-CACHE-STRONG-01`.
