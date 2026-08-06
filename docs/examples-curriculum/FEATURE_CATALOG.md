# MPREG Feature Catalog

**Purpose:** inventory every capability surface the platform exposes, then map
those features into curriculum apps at every complexity level (L0–L4). Apps
are not thin vertical slices — they are **API drill-downs** that prove power.

**How to use**

1. Find a feature below → note its **primary APIs** and **demo depth**.
2. Open the **apps** column → run `uv run mpreg-example run <id>`.
3. Read the app README **API drill-down** section for call signatures and
   invariants.
4. When adding a feature to the product, add a row here and wire at least one
   app at the lowest honest level that can teach it.

**Status keys:** `shipped` (live demo + ensure) · `partial` (touched, thin) ·
`gap` (platform has it; no curriculum app yet).

**Never** `python -m` / `uv run python` — entrypoints only.

---

## Feature families (index)

| Family              | ID prefix  | Primary packages                                                |
| ------------------- | ---------- | --------------------------------------------------------------- |
| RPC                 | `rpc.*`    | `client`, `core.model`, `server`                                |
| Client / HA         | `client.*` | `client_api`, `cluster_client`, `call_policy`, `unified_client` |
| Pub/Sub             | `pubsub.*` | `topic_exchange`, `pubsub_client`                               |
| Queue               | `queue.*`  | `message_queue`, `message_queue_manager`                        |
| Cache               | `cache.*`  | `global_cache`, `advanced_cache_ops`, `fabric.cache_*`          |
| Fabric / federation | `fabric.*` | `fabric/*`, `federation_config`                                 |
| Discovery / DNS     | `disco.*`  | `cluster_map`, `dns_*`, `discovery_*`                           |
| Namespace policy    | `ns.*`     | `namespace_policy`                                              |
| Monitoring / trace  | `mon.*`    | `unified_monitoring`, trace context                             |
| Consensus / Raft    | `cons.*`   | `consensus`, `datastructures/production_raft*`                  |
| Chaos / testing     | `chaos.*`  | `testing.faults`, oracles                                       |
| Persistence         | `pers.*`   | `core.persistence/*`                                            |
| Transport           | `tx.*`     | `core.transport/*`                                              |
| Ports / bootstrap   | `boot.*`   | `port_allocator`, `MPREGSettings`                               |
| CLI / profiles      | `ops.*`    | `cli/*`, `profiles/*`                                           |
| Multi-plane product | `prod.*`   | composed apps                                                   |

---

## 1. RPC plane (`rpc.*`)

| ID                          | Feature                             | Primary APIs                                                               | Depth   | Apps                                                          |
| --------------------------- | ----------------------------------- | -------------------------------------------------------------------------- | ------- | ------------------------------------------------------------- |
| `rpc.register`              | Register command + resource tags    | `MPREGServer.register_command(name, fn, resources)`                        | shipped | `hello_rpc`, `plane_rpc`, all product RPC apps                |
| `rpc.call`                  | Single-function RPC                 | `MPREGClientAPI.call`, `MPREGClient.call`                                  | shipped | `hello_rpc`, `url_shortener_rpc`, …                           |
| `rpc.dag`                   | Multi-command dependency DAG        | `MPREGClientAPI.request` / `call_dag`, `RPCCommand(name, fun, args, locs)` | shipped | `hello_rpc`, `hello_cluster`, `media_pipeline`, `plane_rpc`   |
| `rpc.locs`                  | Resource-location routing           | `RPCCommand.locs` / `call(..., locs=frozenset(...))`                       | shipped | `hello_cluster`, `ml_inference_mesh`, `plane_rpc`             |
| `rpc.concurrency`           | Concurrent independent calls        | multiple `call` / DAG branches                                             | shipped | `rpc_concurrency_lab`                                         |
| `rpc.target_cluster`        | Federated cluster target            | `call(..., target_cluster=)`                                               | shipped | `multi_region_shop`, `global_edge_control_plane`              |
| `rpc.routing_topic`         | Policy routing topic                | `call(..., routing_topic=)`                                                | shipped | `rpc_concurrency_lab`                                         |
| `rpc.function_id` / version | Versioned function identity         | `function_id`, `version_constraint`                                        | shipped | `rpc_versioned_topic`                                         |
| `rpc.list`                  | Capability inventory                | `MPREGClientAPI.rpc_list`                                                  | shipped | `plane_rpc`, `discovery_join`                                 |
| `rpc.describe`              | Spec detail (local/catalog/scatter) | `rpc_describe`                                                             | shipped | `rpc_inventory_tour`                                          |
| `rpc.report`                | Aggregated inventory metrics        | `rpc_report`                                                               | shipped | `rpc_inventory_tour`                                          |
| `rpc.topic_aware`           | Topic-aware RPC progress            | `core.enhanced_rpc`                                                        | shipped | `rpc_versioned_topic`, `topic_dependency_lab`                 |
| `rpc.deadline`              | Per-call timeout                    | `call(..., timeout=)`                                                      | shipped | `chaos_checkout`, `ha_client_failover`, `rpc_deadline_budget` |
| `rpc.fqn`                   | Fully-qualified wire names          | `qualify_rpc_name`, bare→active ns                                         | shipped | `rpc_fqn_namespace`                                           |
| `rpc.namespace_deny`        | Users cannot inject `mpreg.*`       | `assert_registration_allowed`                                              | shipped | `rpc_fqn_namespace`                                           |
| `rpc.bound_namespace`       | Hierarchical register/call lock     | `bound_rpc_namespace`                                                      | shipped | `rpc_fqn_namespace`                                           |

---

## 2. Client surfaces (`client.*`)

| ID                   | Feature                                   | Primary APIs                                                                              | Depth   | Apps                                                           |
| -------------------- | ----------------------------------------- | ----------------------------------------------------------------------------------------- | ------- | -------------------------------------------------------------- |
| `client.api`         | RPC-focused client                        | `MPREGClientAPI`                                                                          | shipped | most apps                                                      |
| `client.unified`     | Four-plane façade + discovery             | `MPREGClient` (call/publish/queue*/cache*/invalidate/list_peers/cluster_map_v2/catalog\*) | shipped | `unified_client_tour`, `order_intake`                          |
| `client.cluster`     | Multi-seed HA client                      | `MPREGClusterClient(seed_urls=…)`                                                         | shipped | `ha_client_failover`                                           |
| `client.cluster_map` | Live cluster map refresh                  | `cluster_map`, `refresh_cluster_map`                                                      | shipped | `cluster_map_catalog`                                          |
| `client.summary`     | Discovery summary routing                 | `summary_query`, `call_with_summary`                                                      | shipped | `discovery_watch_summary` (summary_query + call_with_summary)  |
| `client.policy.m1`   | Async retry policy                        | `ClientCallPolicy.for_mode(M1_ASYNC)`                                                     | shipped | `ha_client_failover`, `plane_rpc`                              |
| `client.policy.m2`   | Soft-RT shared deadline                   | `for_mode(M2_SOFT_RT, deadline_seconds=…)`                                                | shipped | `chaos_checkout`                                               |
| `client.policy.m3`   | Streaming modality defaults               | `for_mode(M3_STREAMING)`                                                                  | shipped | `rpc_concurrency_lab`                                          |
| `client.default_ha`  | HA retry defaults                         | `default_ha_policy()`                                                                     | shipped | `ha_client_failover`                                           |
| `client.pubsub`      | Dedicated pubsub client                   | `MPREGPubSubClient`, `MPREGPubSubExtendedClient`                                          | shipped | `pubsub_client_backlog`                                        |
| `client.dns`         | DNS resolve client                        | `MPREGDnsClient.resolve`                                                                  | shipped | `plane_dns`                                                    |
| `client.trace`       | Last W3C trace context (always after RPC) | `last_trace_context()`; server `RPCResponse` echo                                         | shipped | `client_trace_bind`                                            |
| `client.auth`        | Token / API key on wire                   | `auth_token`, `api_key`, `rpc_auth_token`, `SecurityConfig`                               | shipped | `client_auth_token` (F11 enforced); mTLS = `tls_dev_handshake` |

---

## 3. Pub/Sub (`pubsub.*`)

| ID                      | Feature                     | Primary APIs                       | Depth   | Apps                                           |
| ----------------------- | --------------------------- | ---------------------------------- | ------- | ---------------------------------------------- |
| `pubsub.exchange`       | In-process topic exchange   | `TopicExchange`                    | shipped | `hello_pubsub`, `plane_pubsub`                 |
| `pubsub.wildcard_star`  | Single-segment `*`          | `TopicPattern("user.*.login")`     | shipped | `hello_pubsub`, `sensor_ingest_pubsub`         |
| `pubsub.wildcard_hash`  | Multi-segment `#`           | `TopicPattern("order.#")`          | shipped | `plane_pubsub`, `webhook_dispatcher`           |
| `pubsub.fanout`         | Multi-subscriber match      | multiple `PubSubSubscription`      | shipped | `hello_pubsub`, `plane_pubsub`                 |
| `pubsub.backlog`        | Subscribe with backlog      | `subscribe(..., get_backlog=True)` | shipped | `pubsub_client_backlog`                        |
| `pubsub.publish_reply`  | Request/reply over topics   | `publish_with_reply`               | shipped | `pubsub_request_reply`                         |
| `pubsub.client_wire`    | Wire pubsub via client      | `MPREGClient.publish/subscribe`    | shipped | `pubsub_request_reply`, `sensor_ingest_pubsub` |
| `pubsub.headers`        | Message headers             | `PubSubMessage.headers`            | shipped | `pubsub_client_backlog`, `plane_pubsub`        |
| `pubsub.fabric_forward` | Cross-cluster topic forward | `fabric.pubsub_forwarding`         | shipped | `pubsub_fabric_forward_lab`, `tier3_expansion` |

---

## 4. Queue (`queue.*`)

| ID                  | Feature                      | Primary APIs                               | Depth   | Apps                                      |
| ------------------- | ---------------------------- | ------------------------------------------ | ------- | ----------------------------------------- |
| `queue.create`      | Named queue                  | `MessageQueueManager.create_queue`         | shipped | `job_queue_worker`, `plane_queue`         |
| `queue.send`        | Enqueue with guarantee       | `send_message(..., DeliveryGuarantee)`     | shipped | `job_queue_worker`, `order_intake`        |
| `queue.alo`         | At-least-once                | `DeliveryGuarantee.AT_LEAST_ONCE`          | shipped | `job_queue_worker`, `plane_queue`         |
| `queue.quorum`      | Quorum acks                  | `QUORUM` + `required_acknowledgments`      | shipped | `job_queue_worker`, `plane_queue`         |
| `queue.broadcast`   | Broadcast delivery           | `DeliveryGuarantee.BROADCAST`              | shipped | `queue_ack_receive_lab`, `plane_queue`    |
| `queue.fnf`         | Fire-and-forget              | `FIRE_AND_FORGET`                          | shipped | `queue_ack_receive_lab`, `plane_queue`    |
| `queue.subscribe`   | Worker callback              | `subscribe_to_queue`                       | shipped | all queue apps                            |
| `queue.ack`         | Explicit ack                 | `acknowledge_message` / `queue_ack` RPC    | shipped | `queue_ack_receive_lab`                   |
| `queue.receive`     | Poll receive                 | `receive_message` / client `queue_receive` | shipped | `queue_ack_receive_lab`                   |
| `queue.dlq`         | Dead-letter path             | queue timeout → DLQ                        | shipped | `job_queue_dlq`                           |
| `queue.topic_route` | Topic → queue bridge         | `route_topic_to_queue`                     | shipped | `pubsub_plus_queue`, `webhook_dispatcher` |
| `queue.rpc_surface` | Queue via unified client RPC | `MPREGClient.queue_send/receive/ack`       | shipped | `unified_client_tour`                     |
| `queue.factories`   | Standard / HT / reliable mgr | `create_*_queue_manager`                   | shipped | `plane_queue`                             |

---

## 5. Cache (`cache.*`)

| ID                      | Feature                    | Primary APIs                                                              | Depth   | Apps                                                            |
| ----------------------- | -------------------------- | ------------------------------------------------------------------------- | ------- | --------------------------------------------------------------- |
| `cache.put_get`         | Basic put/get              | `GlobalCacheManager.put/get`, `GlobalCacheKey`                            | shipped | `hello_cache`, `session_cache`                                  |
| `cache.ttl`             | TTL / expiry metadata      | `CacheMetadata(ttl_seconds=…)`                                            | shipped | `session_cache`, `hello_cache`                                  |
| `cache.l1`              | Process-local L1           | default level                                                             | shipped | `hello_cache`                                                   |
| `cache.l2`              | Persistent L2              | `enable_l2_persistent`, `CacheL2Store`                                    | shipped | `cache_replication_geo`                                         |
| `cache.l3`              | Distributed L3             | `enable_l3_distributed`, `CacheOptions(L3)`                               | shipped | `plane_cache`, `cache_plus_federation`                          |
| `cache.l4`              | Federated L4               | `enable_l4_federation`, `CacheOptions(L4)`                                | shipped | `plane_cache`, `feature_flag_mesh`                              |
| `cache.fabric_protocol` | Fabric cache gossip        | `FabricCacheProtocol`, `InProcessCacheTransport`                          | shipped | `plane_cache`, `feature_flag_mesh`                              |
| `cache.sync`            | Explicit peer sync         | `sync_cache_state(peer)`                                                  | shipped | `plane_cache`, `cache_plus_federation`                          |
| `cache.geo_hints`       | Geographic placement hints | `CacheMetadata.geographic_hints`                                          | shipped | `cache_replication_geo`                                         |
| `cache.replication`     | Replication strategy       | `ReplicationStrategy`, `CacheReplicationPolicy`                           | shipped | `cache_replication_geo`                                         |
| `cache.invalidate`      | Pattern invalidate         | `invalidate` / client `cache_invalidate`                                  | shipped | `cache_replication_geo`, `session_cache`, `unified_client_tour` |
| `cache.atomic`          | CAS / incr / append        | `AdvancedCacheOperations.atomic_operation`                                | shipped | `cache_atomic_ops`                                              |
| `cache.structures`      | Set/list/map/counter ops   | `data_structure_operation`                                                | shipped | `cache_atomic_ops`                                              |
| `cache.namespace_ops`   | Clear/list/scan namespace  | `namespace_operation`                                                     | shipped | `cache_atomic_ops`                                              |
| `cache.pubsub_events`   | Cache→pubsub integration   | `CachePubSubIntegration`                                                  | shipped | `cache_event_bus`                                               |
| `cache.rpc_surface`     | Cache via unified client   | `MPREGClient.cache_get/put`                                               | shipped | `unified_client_tour`                                           |
| `cache.strong`          | Majority-commit STRONG put (CFT ABORT best-effort) | `StrongPutCoordinator`, `ConsistencyLevel.STRONG`, `cache_strong_enabled`; ops re-ABORT: `retry_abort` / `MPREGClient.cache_strong_retry_abort`; metrics `residual_ops_hint` / `abort_fail_peer_count`; prom `mpreg_strong_abort_fail_peers` | shipped | `cache_strong_quorum`                                           |

---

## 6. Fabric / federation (`fabric.*`)

| ID                      | Feature                       | Primary APIs                                         | Depth   | Apps                                                 |
| ----------------------- | ----------------------------- | ---------------------------------------------------- | ------- | ---------------------------------------------------- |
| `fabric.permissive`     | Permissive bridging           | `create_permissive_bridging_config`                  | shipped | `plane_fabric`, `multi_region_shop`                  |
| `fabric.strict`         | Strict isolation              | `create_strict_isolation_config`                     | shipped | `fabric_policy_modes`                                |
| `fabric.explicit`       | Explicit bridge allowlist     | `create_explicit_bridging_config`                    | shipped | `fabric_policy_modes`                                |
| `fabric.cluster_id`     | Cluster identity on settings  | `MPREGSettings.cluster_id`                           | shipped | fabric apps                                          |
| `fabric.cross_rpc`      | Cross-cluster RPC DAG         | multi `cluster_id` + `locs`                          | shipped | `plane_fabric`, `multi_region_shop`                  |
| `fabric.catalog`        | Routing catalog types         | `fabric.catalog` Function/Topic/Queue/Cache catalogs | shipped | `fabric_policy_modes`                                |
| `fabric.gossip`         | Membership / gossip           | `fabric.gossip*`, peer directory, HMAC envelopes     | shipped | `discovery_signatures_lab`, `discovery_join`         |
| `fabric.link_state`     | Link-state routing            | `fabric.link_state`                                  | shipped | `fabric_policy_modes`                                |
| `fabric.route_security` | Signed route announcements    | `RouteAnnouncementSigner`, `RouteSecurityConfig`     | shipped | `signed_route_border`                                |
| `fabric.route_policy`   | Neighbor / policy tags        | route policy directory                               | shipped | `signed_route_border`                                |
| `fabric.route_keys`     | Route key rotation            | route keys + gossip                                  | shipped | `signed_route_border`                                |
| `fabric.snapshot`       | Catalog/key persistence       | fabric persistence snapshot                          | shipped | `fabric_snapshot_restart`                            |
| `fabric.hubs`           | Hub hierarchy / edges         | hubs, hub_registry                                   | shipped | `global_edge_control_plane`                          |
| `fabric.graph`          | Graph / Dijkstra routers      | `FederationGraph`, routers                           | shipped | `fabric_graph_resilience`, `fabric_hub_hierarchy`    |
| `fabric.resilience`     | Circuit breakers / recovery   | `FederationHealthMonitor`                            | shipped | `fabric_graph_resilience`                            |
| `fabric.queue_fed`      | Queue federation              | `queue_federation`                                   | shipped | `queue_federation_lab`                               |
| `fabric.blockchain_msg` | Blockchain message federation | `blockchain_message_federation`                      | shipped | `blockchain_message_lab` (types; hub mesh non-claim) |

---

## 7. Discovery / DNS (`disco.*`)

| ID                      | Feature                    | Primary APIs                                | Depth   | Apps                                                                                 |
| ----------------------- | -------------------------- | ------------------------------------------- | ------- | ------------------------------------------------------------------------------------ |
| `disco.list_peers`      | Peer snapshots             | `list_peers`                                | shipped | `discovery_join`, `hello_cluster`                                                    |
| `disco.cluster_map`     | Cluster map v1/v2          | `cluster_map`, `cluster_map_v2`             | shipped | `discovery_join`, `ha_client_failover`, `cluster_map_catalog`, `unified_client_tour` |
| `disco.catalog_query`   | Scoped catalog query       | `catalog_query`                             | shipped | `cluster_map_catalog`                                                                |
| `disco.catalog_watch`   | Delta watch topics         | `catalog_watch`                             | shipped | `discovery_watch_summary`                                                            |
| `disco.summary_query`   | Summary records            | `summary_query`                             | shipped | `discovery_watch_summary`, `discovery_rate_limit`                                    |
| `disco.summary_watch`   | Summary export topics      | `summary_watch`                             | shipped | `discovery_watch_summary`                                                            |
| `disco.access_audit`    | Discovery access audit     | `discovery_access_audit`                    | shipped | `discovery_resolver_audit`                                                           |
| `disco.resolver_stats`  | Resolver cache stats       | `resolver_cache_stats`                      | shipped | `discovery_resolver_audit`                                                           |
| `disco.resolver_resync` | Force catalog resync       | `resolver_resync`                           | shipped | `discovery_resolver_audit`                                                           |
| `disco.dns_register`    | DNS service register/unreg | `dns_register` / `dns_unregister` / CLI     | shipped | `plane_dns`                                                                          |
| `disco.dns_resolve`     | DNS gateway resolve        | `MPREGDnsClient`, `DnsGateway`              | shipped | `plane_dns`                                                                          |
| `disco.join`            | Live node join visibility  | peers + new resources                       | shipped | `discovery_join`                                                                     |
| `disco.signatures`      | Signed discovery summaries | `discovery_signatures`, `gossip_signatures` | shipped | `discovery_signatures_lab`                                                           |

---

## 8. Namespace policy (`ns.*`)

| ID            | Feature           | Primary APIs                | Depth   | Apps                    |
| ------------- | ----------------- | --------------------------- | ------- | ----------------------- |
| `ns.status`   | Namespace status  | `namespace_status`          | shipped | `namespace_policy_gate` |
| `ns.export`   | Export policy     | `namespace_policy_export`   | shipped | `namespace_policy_gate` |
| `ns.validate` | Validate rules    | `namespace_policy_validate` | shipped | `namespace_policy_gate` |
| `ns.apply`    | Apply rules       | `namespace_policy_apply`    | shipped | `namespace_policy_gate` |
| `ns.audit`    | Policy audit log  | `namespace_policy_audit`    | shipped | `namespace_policy_gate` |
| `ns.engine`   | In-process engine | `NamespacePolicyEngine`     | shipped | `ns_engine_direct`      |

---

## 9. Monitoring / observability (`mon.*`)

| ID                     | Feature                     | Primary APIs                                                                | Depth   | Apps                                       |
| ---------------------- | --------------------------- | --------------------------------------------------------------------------- | ------- | ------------------------------------------ |
| `mon.unified`          | Unified system monitor      | `create_unified_system_monitor`                                             | shipped | `hello_trace`, `plane_monitoring`          |
| `mon.events`           | Cross-system events         | `record_cross_system_event`                                                 | shipped | `hello_trace`, `plane_monitoring`          |
| `mon.timeline`         | Tracking timeline           | `get_tracking_timeline`                                                     | shipped | `hello_trace`, `global_edge_control_plane` |
| `mon.correlation`      | Correlation metrics         | `get_correlation_timeline`                                                  | shipped | `mon_logging_json`, `plane_monitoring`     |
| `mon.system_types`     | RPC/CACHE/QUEUE/… tags      | `SystemType`, `EventType`                                                   | shipped | monitoring apps                            |
| `mon.health`           | Aggregated health           | `get_unified_metrics`                                                       | shipped | `mon_logging_json`, `plane_monitoring`     |
| `mon.transport`        | Transport health attach     | `attach_transport_adapter`                                                  | shipped | `transport_health_attach`                  |
| `mon.slo`              | SLO helpers                 | `core.observability.slo`                                                    | shipped | `observability_slo_trace`, probe apps      |
| `mon.logging`          | Structured / JSON logs      | `configure_logging`, CLI `--json-logs`                                      | shipped | `mon_logging_json`                         |
| `mon.trace_bind`       | Trace context bind          | `bind_trace_context`, `trace_context`                                       | shipped | `client_trace_bind`                        |
| `mon.metrics_snapshot` | In-process metrics snapshot | `ServerMetricsTracker.snapshot` (samples/min/max/p50/p95/rps + fabric hops) | shipped | `rpc_microbench_lab`, probe apps           |
| `mon.server_tracker`   | Server metrics tracker      | `ServerMetricsTracker.record_rpc`                                           | shipped | `rpc_microbench_lab`                       |

---

## 10. Consensus / Raft (`cons.*`)

| ID                  | Feature                 | Primary APIs                      | Depth   | Apps                                            |
| ------------------- | ----------------------- | --------------------------------- | ------- | ----------------------------------------------- |
| `cons.raft`         | Production Raft         | `datastructures.production_raft*` | shipped | `partition_safe_counter` (teaching model)       |
| `cons.leader`       | Leader election helpers | `consensus`, `leader_election`    | shipped | `leader_election_lab`, `partition_safe_counter` |
| `cons.quorum_teach` | Majority vs minority    | teaching counter + partition      | shipped | `partition_safe_counter`                        |
| `cons.oracle`       | Raft oracle (tests)     | `testing.oracles.RaftOracle`      | shipped | `routing_oracle_lab` (`oracle.raft`)            |

---

## 11. Chaos / testing (`chaos.*`)

| ID                     | Feature                  | Primary APIs                         | Depth   | Apps                                       |
| ---------------------- | ------------------------ | ------------------------------------ | ------- | ------------------------------------------ |
| `chaos.partition`      | Network partition groups | `FaultInjector.partition`            | shipped | `partition_safe_counter`, `chaos_checkout` |
| `chaos.heal`           | Heal partitions          | `heal()`                             | shipped | `chaos_checkout`                           |
| `chaos.crash`          | Crash / recover node     | `crash` / `recover`                  | shipped | `chaos_crash_recover`                      |
| `chaos.clock_skew`     | Clock skew injection     | `set_clock_skew`                     | shipped | `chaos_transport`                          |
| `chaos.duplicate`      | Dup / reorder hooks      | `should_duplicate`, `should_reorder` | shipped | `chaos_transport`                          |
| `chaos.routing_oracle` | Expected next-hop oracle | `RoutingOracle`                      | shipped | `routing_oracle_lab` (`oracle.routing`)    |
| `chaos.no_loop`        | Loop assertion helper    | `assert_no_routing_loop`             | shipped | `correlation_routing_lab`                  |

---

## 12. Persistence (`pers.*`)

| ID                  | Feature                 | Primary APIs                           | Depth   | Apps                      |
| ------------------- | ----------------------- | -------------------------------------- | ------- | ------------------------- |
| `pers.sqlite_kv`    | SQLite KV backend       | `SQLiteKeyValueStore`                  | shipped | `config_reload_live`      |
| `pers.sqlite_queue` | SQLite queue store      | `SQLiteQueueStore`                     | shipped | `config_reload_live`      |
| `pers.memory`       | Memory backends         | `Memory*Store`                         | shipped | default demos             |
| `pers.cache_l2`     | Cache L2 store          | `CacheL2Store`                         | shipped | `cache_replication_geo`   |
| `pers.mode`         | Persistence mode config | `PersistenceConfig`, `PersistenceMode` | shipped | `cache_replication_geo`   |
| `pers.fabric_snap`  | Fabric snapshot files   | fabric persistence                     | shipped | `fabric_snapshot_restart` |
| `pers.restart`      | Survive process restart | dual-phase demo                        | shipped | `config_reload_live`      |

---

## 13. Transport (`tx.*`)

| ID                   | Feature                         | Primary APIs                                          | Depth   | Apps                                      |
| -------------------- | ------------------------------- | ----------------------------------------------------- | ------- | ----------------------------------------- |
| `tx.websocket`       | Default WS transport            | server/client default                                 | shipped | all live server apps                      |
| `tx.tcp`             | TCP transport                   | `tcp_transport`                                       | shipped | `transport_protocol_tour`                 |
| `tx.circuit_breaker` | Transport CB                    | `circuit_breaker`                                     | shipped | `tx_circuit_breaker_lab`                  |
| `tx.correlation`     | Correlation tracker             | `CorrelationTracker`                                  | shipped | `correlation_routing_lab`                 |
| `tx.security`        | TLS / certs config              | `SecurityConfig`, `TransportConfig`, `tls_*` settings | shipped | `client_auth_token` + `tls_dev_handshake` |
| `tx.multi_protocol`  | Enhanced multi-protocol adapter | `EnhancedMultiProtocolAdapter`                        | shipped | `transport_protocol_tour`                 |

---

## 14. Bootstrap / settings (`boot.*`)

| ID                | Feature                  | Primary APIs                                                                 | Depth   | Apps                                 |
| ----------------- | ------------------------ | ---------------------------------------------------------------------------- | ------- | ------------------------------------ |
| `boot.port_range` | Dynamic port allocation  | `port_range_context`                                                         | shipped | all server apps                      |
| `boot.auto_port`  | OS-assigned / auto ports | auto-port bootstrap patterns                                                 | shipped | `hello_ports`, `auto_port_bootstrap` |
| `boot.settings`   | Full settings surface    | `MPREGSettings` (peers, resources, cluster_id, federation_config, log_level) | shipped | all                                  |
| `boot.peers`      | Static peer join         | `peers=[url]`                                                                | shipped | cluster apps                         |
| `boot.resources`  | Resource advertisements  | `resources={…}`                                                              | shipped | RPC apps                             |
| `boot.profiles`   | TOML profiles            | `mpreg/profiles/*.toml`                                                      | shipped | `profile_settings_tour`              |

---

## 15. CLI / ops (`ops.*`)

| ID                              | Feature                 | Primary APIs                                                     | Depth   | Apps                                        |
| ------------------------------- | ----------------------- | ---------------------------------------------------------------- | ------- | ------------------------------------------- |
| `ops.cli_call`                  | `mpreg call`            | CLI                                                              | shipped | `ops_cli_tour`                              |
| `ops.cli_planes`                | queue/cache/publish CLI | `mpreg client-*` (incl. `cache-strong-retry-abort`)              | shipped | `ops_cli_tour`                              |
| `ops.cli_dns`                   | DNS CLI group           | `mpreg dns` / `mpreg client dns-*`                               | shipped | `ops_cli_tour`                              |
| `ops.cli_ns`                    | Namespace policy CLI    | `mpreg namespace-policy`                                         | shipped | `ops_cli_tour`                              |
| `ops.cli_discovery`             | peers / resolver        | `list-peers`, resolver cmds                                      | shipped | `ops_cli_tour`                              |
| `ops.example_runner`            | Curriculum runner       | `mpreg-example`, `mpreg examples`, `mpreg demo`                  | shipped | all                                         |
| `ops.doctor` / `ops.cli_doctor` | Doctor / admin          | `mpreg doctor`                                                   | shipped | `ops_cli_tour`                              |
| `ops.mgmt_drain`                | Node drain mutation     | `POST /mgmt/v1/nodes/drain`, `mpreg admin drain`                 | shipped | `live_partition_chaos`, `shared_audit_mesh` |
| `ops.mgmt_detach`               | Peer detach mutation    | `POST /mgmt/v1/peers/detach`, `mpreg admin detach`               | shipped | `live_partition_chaos`                      |
| `ops.mgmt_audit`                | Local audit read        | `GET /mgmt/v1/audit`, `mpreg admin audit`                        | shipped | `ops_cli_tour`, `live_partition_chaos`      |
| `ops.shared_audit`              | Cluster G-Set audit     | `mgmt_audit_shared_enabled`, `SharedAuditStore`, `scope=cluster` | shipped | `shared_audit_mesh`                         |

---

## 16. Product compositions (`prod.*`)

| ID                    | Composition                 | Apps                        |
| --------------------- | --------------------------- | --------------------------- |
| `prod.hello_vertical` | Minimal each plane          | L0 hellos                   |
| `prod.order`          | RPC+cache+pubsub+queue      | `order_intake`              |
| `prod.media`          | Multi-stage RPC ETL         | `media_pipeline`            |
| `prod.flags`          | L4 flag mesh                | `feature_flag_mesh`         |
| `prod.webhooks`       | Events → durable egress     | `webhook_dispatcher`        |
| `prod.ml`             | Specialized inference mesh  | `ml_inference_mesh`         |
| `prod.shop`           | Multi-region checkout path  | `multi_region_shop`         |
| `prod.edge`           | Global hub + edges          | `global_edge_control_plane` |
| `prod.tier3`          | Full multi-system expansion | `tier3_expansion`           |

---

## Coverage matrix (features × levels)

Legend: ● primary teach · ○ supporting · · absent

| Feature family                 | L0  | L1  | L2  | L3  | L4  |
| ------------------------------ | --- | --- | --- | --- | --- |
| RPC core (call/dag/locs)       | ●   | ●   | ●   | ●   | ●   |
| Client HA + policy M1/M2       | ·   | ●   | ○   | ●   | ○   |
| PubSub wildcards               | ●   | ●   | ●   | ○   | ·   |
| Queue ALO/quorum               | ·   | ●   | ●   | ○   | ·   |
| Cache L1–L4                    | ●   | ●   | ●   | ○   | ·   |
| Fabric multi-cluster           | ·   | ●   | ●   | ●   | ●   |
| Route security                 | ·   | ·   | ·   | ●   | ·   |
| Discovery join/map             | ○   | ○   | ·   | ●   | ○   |
| Monitoring timeline            | ●   | ●   | ·   | ○   | ●   |
| Chaos / partition              | ·   | ·   | ·   | ●   | ·   |
| Persistence restart            | ·   | ·   | ●   | ●   | ·   |
| DNS / namespace / atomic cache | ·   | ●   | ·   | ·   | ·   |

---

## Gap backlog (platform has it; curriculum thin or missing)

**Closed in Phase E (2026-08-05):**

| #   | Gap                                                     | App                       |
| --- | ------------------------------------------------------- | ------------------------- |
| 1   | Graph / resilience                                      | `fabric_graph_resilience` |
| 2   | Discovery watches + summary                             | `discovery_watch_summary` |
| 3   | Cache→pubsub events                                     | `cache_event_bus`         |
| 4   | Queue DLQ                                               | `job_queue_dlq`           |
| 5   | Auth token (monitoring + client wiring; mTLS non-claim) | `client_auth_token`       |
| 6   | Versioned / function_id RPC                             | `rpc_versioned_topic`     |
| 7   | Chaos extras (skew/dup/reorder/drop)                    | `chaos_transport`         |
| 8   | Ops CLI tour                                            | `ops_cli_tour`            |

Also E1–E5: DNS plane, namespace policy, atomic cache, unified client, publish-with-reply.

**Closed in Phase J (2026-08-05):** F10 live drain/detach (`live_partition_chaos`);
F11 `rpc_auth_token` (`client_auth_token`); F12 `generate_dev_tls_material` +
`tls_dev_handshake`; residual teach apps for disco resolver/audit, queue_fed,
mon.transport, tx.tcp/multi_protocol, blockchain message types.

**Closed in Phase K (2026-08-05):** depth non-claims productized —
`mtls_mesh_handshake` (CERT_REQUIRED), `packet_loss_chaos`,
`blockchain_hub_settlement`, `discovery_signatures_lab`; catalog partials
rpc.describe/report, client.trace, mon.trace_bind, tx.correlation, chaos.no_loop.

**Closed in Phase L (2026-08-05):** FEATURE `partial` promotion batch —
`queue_ack_receive_lab`, `pubsub_client_backlog`, `cache_replication_geo`,
`fabric_policy_modes`, `rpc_concurrency_lab`, `cluster_map_catalog`,
`mon_logging_json`, `chaos_crash_recover`, `tx_circuit_breaker_lab`,
`ns_engine_direct` (+ boot.profiles / cons.raft/leader tags). **95** apps.

**Closed in Phase O (2026-08-05):** integrity polish — thin `client_trace_bind`
deepened; `fabric.gossip` joined to APP_FEATURES + GossipMessage hop/TTL scenario
in `discovery_signatures_lab`; stale planned-operator language scrubbed.
Thin apps → **0**; uncovered feature constants → **0**.

**Closed in Phase N (2026-08-05):** Low DX F22/F23 — `MessageHeaders.coerce`
(dict headers on publish); catalog `entry_type` defaults to `functions` with
clearer errors. Taught in `pubsub_client_backlog` + `cluster_map_catalog`.
API_FRICTION open curriculum rows → **0**.

**Closed in Phase M (2026-08-05):** residual FEATURE partials —
`ops_cli_tour` deepened (planes/ns/discovery CLI); `pubsub_fabric_forward_lab`.
**99** apps (through Phase Y: +`shared_audit_mesh`, +`cache_strong_quorum`). FEATURE_CATALOG teachable `partial` rows → **0**.

**Still open / residual (operator topology — not curriculum blockers):**

- Multi-continent SLA / production multi-hub DAO treasury ops
- Kernel-level TCP byte-splice packet loss (platform teaches delivery model + admission)

**Closed in Phase I (2026-08-05):** false `gap` rows flipped to `shipped` for
apps that already teach them; FQN features `rpc.fqn` /
`rpc.namespace_deny` / `rpc.bound_namespace` via `rpc_fqn_namespace`; residual
Info friction F13/F15/F16/F19 closed or documented.

**Closed in Phase Q–S (2026-08-05):** single sequential multi-axis continuation —
client.summary join; config-check `--explain`; fabric hop snapshot; mgmt audit
teach; full MPREGClient façade parity; W3C converters; format_server_snapshot
fabric print. **97** apps.

Usability findings from building these apps: [API_FRICTION.md](./API_FRICTION.md).

---

## App → feature map (canonical)

| App                         | Level | Feature IDs (primary)                                                                               |
| --------------------------- | ----- | --------------------------------------------------------------------------------------------------- |
| `hello_rpc`                 | L0    | `rpc.register`, `rpc.dag`, `rpc.call`, `boot.port_range`                                            |
| `hello_cluster`             | L0    | `rpc.dag`, `rpc.locs`, `boot.peers`, `disco.list_peers`                                             |
| `hello_trace`               | L0    | `mon.unified`, `mon.events`, `mon.timeline`                                                         |
| `hello_pubsub`              | L0    | `pubsub.exchange`, `pubsub.wildcard_star`, `pubsub.fanout`                                          |
| `hello_cache`               | L0    | `cache.put_get`, `cache.l1`, `cache.ttl`                                                            |
| `hello_ports`               | L0    | `boot.port_range`, `boot.auto_port`, `rpc.call`                                                     |
| `ha_client_failover`        | L1    | `client.cluster`, `client.policy.m1`, `client.default_ha`, `disco.cluster_map`                      |
| `job_queue_worker`          | L1    | `queue.alo`, `queue.quorum`, `queue.subscribe`                                                      |
| `url_shortener_rpc`         | L1    | `rpc.call`, `cache.put_get`                                                                         |
| `sensor_ingest_pubsub`      | L1    | `pubsub.wildcard_*`, `pubsub.fanout`                                                                |
| `session_cache`             | L1    | `cache.ttl`, `cache.invalidate`                                                                     |
| `auto_port_bootstrap`       | L1    | `boot.auto_port`, `boot.peers`, `rpc.locs`                                                          |
| `plane_rpc`                 | L1    | `rpc.*` tour incl. list/describe/policy                                                             |
| `plane_pubsub`              | L1    | `pubsub.*` tour                                                                                     |
| `plane_queue`               | L1    | `queue.*` tour                                                                                      |
| `plane_cache`               | L1    | `cache.l1–l4`, `cache.fabric_protocol`                                                              |
| `plane_fabric`              | L1    | `fabric.permissive`, `fabric.cross_rpc`                                                             |
| `plane_monitoring`          | L1    | `mon.*` tour                                                                                        |
| `order_intake`              | L2    | `prod.order`, multi-plane                                                                           |
| `media_pipeline`            | L2    | `rpc.dag`, `rpc.locs`, multi-stage                                                                  |
| `feature_flag_mesh`         | L2    | `cache.l4`, `cache.fabric_protocol`                                                                 |
| `webhook_dispatcher`        | L2    | `pubsub` + `queue.topic_route`                                                                      |
| `config_reload_live`        | L2    | `pers.restart`, `pers.sqlite_*`                                                                     |
| `rpc_plus_cache`            | L2    | `rpc` + `cache` integration                                                                         |
| `pubsub_plus_queue`         | L2    | `pubsub` + `queue` bridge                                                                           |
| `cache_plus_federation`     | L2    | `cache.l3/l4` federation                                                                            |
| `ml_inference_mesh`         | L2    | `rpc.locs` specialized workers                                                                      |
| `multi_region_shop`         | L3    | `fabric.cross_rpc`, `rpc.target_cluster`                                                            |
| `signed_route_border`       | L3    | `fabric.route_security`, `fabric.route_keys`                                                        |
| `partition_safe_counter`    | L3    | `chaos.partition`, `cons.quorum_teach`                                                              |
| `discovery_join`            | L3    | `disco.join`, `disco.list_peers`, `disco.cluster_map`                                               |
| `chaos_checkout`            | L3    | `chaos.*`, `client.policy.m2`, `rpc.deadline`                                                       |
| `fabric_snapshot_restart`   | L3    | `pers.fabric_snap`, `fabric.catalog`                                                                |
| `tier3_expansion`           | L3    | multi-plane expansion                                                                               |
| `global_edge_control_plane` | L4    | `fabric.hubs`, `mon.timeline`, multi-cluster                                                        |
| `cache_atomic_ops`          | L1    | `cache.atomic`, `cache.structures`, `cache.namespace_ops`                                           |
| `namespace_policy_gate`     | L1    | `ns.validate`, `ns.apply`, `ns.status`, `ns.export`, `ns.audit`                                     |
| `plane_dns`                 | L1    | `disco.dns_register`, `disco.dns_resolve`, `client.dns` (+ unregister)                              |
| `unified_client_tour`       | L1    | `client.unified`, `cache.rpc_surface`, `cache.invalidate`, `queue.rpc_surface`, `disco.cluster_map` |
| `pubsub_request_reply`      | L1    | `pubsub.publish_reply`, `pubsub.client_wire`                                                        |
| `job_queue_dlq`             | L1    | `queue.dlq`, `queue.alo`                                                                            |
| `rpc_versioned_topic`       | L1    | `rpc.function_id`, `rpc.version_constraint`, `rpc.fqn` (bare+opaque id)                             |
| `client_auth_token`         | L1    | `client.auth`, `tx.security`, `mon.health`                                                          |
| `hello_queue`               | L0    | `queue.send`, `queue.subscribe`                                                                     |
| `hello_dns`                 | L0    | `disco.dns_register`, `disco.dns_resolve`                                                           |
| `discovery_watch_summary`   | L3    | `disco.catalog_watch`, `disco.summary_*`                                                            |
| `fabric_graph_resilience`   | L3    | `fabric.graph`, `fabric.resilience`                                                                 |
| `cache_event_bus`           | L2    | `cache.pubsub_events`                                                                               |
| `ops_cli_tour`              | L2    | `ops.cli_*`                                                                                         |
| `chaos_transport`           | L3    | `chaos.clock_skew`, `chaos.duplicate`, `chaos.reorder`, `chaos.drop`                                |
| `tls_dev_handshake`         | L1    | `tx.tls`, `tx.security`, `rpc.call`                                                                 |
| `discovery_resolver_audit`  | L1    | `disco.access_audit`, `disco.resolver_stats`, `disco.resolver_resync`                               |
| `transport_health_attach`   | L1    | `mon.transport`, `mon.health`                                                                       |
| `transport_protocol_tour`   | L1    | `tx.tcp`, `tx.multi_protocol`                                                                       |
| `queue_federation_lab`      | L2    | `fabric.queue_fed`                                                                                  |
| `blockchain_message_lab`    | L2    | `fabric.blockchain_msg`                                                                             |
| `live_partition_chaos`      | L3    | `chaos.live_drain`, `ops.mgmt_drain`, `ops.mgmt_audit`, `ops.mgmt_detach`, `mon.health`             |
| `discovery_signatures_lab`  | L1    | `disco.signatures`                                                                                  |
| `rpc_inventory_tour`        | L1    | `rpc.describe`, `rpc.report`                                                                        |
| `client_trace_bind`         | L1    | `client.trace`, `mon.trace_bind`                                                                    |
| `correlation_routing_lab`   | L1    | `tx.correlation`, `chaos.no_loop`                                                                   |
| `mtls_mesh_handshake`       | L2    | `tx.tls` CERT_REQUIRED                                                                              |
| `packet_loss_chaos`         | L3    | `chaos.drop`, `chaos.live_drain`                                                                    |
| `blockchain_hub_settlement` | L2    | `fabric.blockchain_msg`, `fabric.hubs`                                                              |
| `rpc_deadline_budget`       | L3    | `rpc.deadline`, `client.policy.m1/m2/m3`                                                            |
| `notification_fanout`       | L2    | `prod.notify`, `pubsub.*`                                                                           |
| `billing_ledger`            | L2    | `prod.billing`, rpc+cache+queue                                                                     |
| `inventory_reserve`         | L2    | `prod.inventory`, rpc+cache                                                                         |
| `topic_queue_bridge`        | L2    | `queue.topic_route`, pubsub→queue                                                                   |
| `multi_region_dns_policy`   | L3    | DNS namespaces + regional RPC                                                                       |
| `rpc_fqn_namespace`         | L1    | `rpc.fqn`, `rpc.namespace_deny`, `rpc.bound_namespace`                                              |
| `topic_taxonomy_tour`       | L1    | `topic.*`                                                                                           |
| `persistence_kv`            | L1    | `pers.memory_kv`, `pers.sqlite_kv`                                                                  |
| `profile_settings_tour`     | L1    | `boot.profile`                                                                                      |
| `discovery_rate_limit`      | L1    | `disco.rate_limit`                                                                                  |
| `observability_slo_trace`   | L1    | `mon.slo`, `mon.trace_context`                                                                      |
| `topic_queue_router_lab`    | L2    | `queue.topic_route`                                                                                 |
| `topic_dependency_lab`      | L2    | `rpc.dependency`, `rpc.topic_aware`                                                                 |
| `shipping_fulfillment`      | L2    | `prod.shipping`                                                                                     |
| `rpc_intermediate_results`  | L2    | `rpc.intermediate`                                                                                  |
| `shared_audit_mesh`         | L2    | `ops.shared_audit`, `ops.mgmt_drain`, `ops.mgmt_audit`                                              |
| `cache_strong_quorum`       | L2    | `cache.strong`                                                                                      |
| `routing_oracle_lab`        | L3    | `oracle.routing`, `oracle.raft`                                                                     |
| `deadline_hop_budget`       | L3    | `fabric.deadline_hop`                                                                               |
| `fabric_hub_hierarchy`      | L3    | `fabric.hubs`, `fabric.graph`                                                                       |
| `leader_election_lab`       | L3    | `cons.leader_election`                                                                              |
| `multi_pop_edge_mesh`       | L4    | second world tour                                                                                   |

---

## Depth contract (every app)

Each shipped app must:

1. **Catalog-tag** its feature IDs (registry `features` + README).
2. Run **≥2 scenarios** (happy path + one API edge: miss, replay, second guarantee, peer list, policy mode, …).
3. Use **`ensure()`** for every taught invariant (target ≥5 for L1+, ≥3 for L0).
4. Prefer **public APIs** (`call` / `request` / `call_dag`, not private `_client` except when demonstrating the wire request path explicitly).
5. Document **API drill-down** (signatures + what was asserted).
6. State **non-claims** for nearby unproven features.
7. Point to **production exit ramp** (next app, profile, HA, monitoring).

---

## Related docs

- [APP_CATALOG.md](./APP_CATALOG.md) — app matrix and bundles
- [APP_CONVENTIONS.md](./APP_CONVENTIONS.md) — code/README rules
- [STAGES.md](./STAGES.md) — learning path
- [OPERATE.md](./OPERATE.md) — ops entrypoints
- [TRACKER.md](./TRACKER.md) — delivery status
