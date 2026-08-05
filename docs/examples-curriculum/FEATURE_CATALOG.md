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

| Family | ID prefix | Primary packages |
|--------|-----------|------------------|
| RPC | `rpc.*` | `client`, `core.model`, `server` |
| Client / HA | `client.*` | `client_api`, `cluster_client`, `call_policy`, `unified_client` |
| Pub/Sub | `pubsub.*` | `topic_exchange`, `pubsub_client` |
| Queue | `queue.*` | `message_queue`, `message_queue_manager` |
| Cache | `cache.*` | `global_cache`, `advanced_cache_ops`, `fabric.cache_*` |
| Fabric / federation | `fabric.*` | `fabric/*`, `federation_config` |
| Discovery / DNS | `disco.*` | `cluster_map`, `dns_*`, `discovery_*` |
| Namespace policy | `ns.*` | `namespace_policy` |
| Monitoring / trace | `mon.*` | `unified_monitoring`, trace context |
| Consensus / Raft | `cons.*` | `consensus`, `datastructures/production_raft*` |
| Chaos / testing | `chaos.*` | `testing.faults`, oracles |
| Persistence | `pers.*` | `core.persistence/*` |
| Transport | `tx.*` | `core.transport/*` |
| Ports / bootstrap | `boot.*` | `port_allocator`, `MPREGSettings` |
| CLI / profiles | `ops.*` | `cli/*`, `profiles/*` |
| Multi-plane product | `prod.*` | composed apps |

---

## 1. RPC plane (`rpc.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `rpc.register` | Register command + resource tags | `MPREGServer.register_command(name, fn, resources)` | shipped | `hello_rpc`, `plane_rpc`, all product RPC apps |
| `rpc.call` | Single-function RPC | `MPREGClientAPI.call`, `MPREGClient.call` | shipped | `hello_rpc`, `url_shortener_rpc`, … |
| `rpc.dag` | Multi-command dependency DAG | `MPREGClientAPI.request` / `call_dag`, `RPCCommand(name, fun, args, locs)` | shipped | `hello_rpc`, `hello_cluster`, `media_pipeline`, `plane_rpc` |
| `rpc.locs` | Resource-location routing | `RPCCommand.locs` / `call(..., locs=frozenset(...))` | shipped | `hello_cluster`, `ml_inference_mesh`, `plane_rpc` |
| `rpc.concurrency` | Concurrent independent calls | multiple `call` / DAG branches | partial | `plane_rpc`, `media_pipeline` |
| `rpc.target_cluster` | Federated cluster target | `call(..., target_cluster=)` | shipped | `multi_region_shop`, `global_edge_control_plane` |
| `rpc.routing_topic` | Policy routing topic | `call(..., routing_topic=)` | partial | `signed_route_border` |
| `rpc.function_id` / version | Versioned function identity | `function_id`, `version_constraint` | gap | — |
| `rpc.list` | Capability inventory | `MPREGClientAPI.rpc_list` | shipped | `plane_rpc`, `discovery_join` |
| `rpc.describe` | Spec detail (local/catalog/scatter) | `rpc_describe` | partial | `plane_rpc` |
| `rpc.report` | Aggregated inventory metrics | `rpc_report` | partial | `plane_rpc` |
| `rpc.topic_aware` | Topic-aware RPC progress | `core.enhanced_rpc` | gap | — |
| `rpc.deadline` | Per-call timeout | `call(..., timeout=)` | shipped | `chaos_checkout`, `ha_client_failover` |

---

## 2. Client surfaces (`client.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `client.api` | RPC-focused client | `MPREGClientAPI` | shipped | most apps |
| `client.unified` | Four-plane façade | `MPREGClient` (call/publish/queue_*/cache_*) | shipped | `unified_client_tour`, `order_intake` |
| `client.cluster` | Multi-seed HA client | `MPREGClusterClient(seed_urls=…)` | shipped | `ha_client_failover` |
| `client.cluster_map` | Live cluster map refresh | `cluster_map`, `refresh_cluster_map` | partial | `ha_client_failover`, `discovery_join` |
| `client.summary` | Discovery summary routing | `summary_query`, `call_with_summary` | gap | — |
| `client.policy.m1` | Async retry policy | `ClientCallPolicy.for_mode(M1_ASYNC)` | shipped | `ha_client_failover`, `plane_rpc` |
| `client.policy.m2` | Soft-RT shared deadline | `for_mode(M2_SOFT_RT, deadline_seconds=…)` | shipped | `chaos_checkout` |
| `client.policy.m3` | Streaming modality defaults | `for_mode(M3_STREAMING)` | partial | `plane_rpc` |
| `client.default_ha` | HA retry defaults | `default_ha_policy()` | shipped | `ha_client_failover` |
| `client.pubsub` | Dedicated pubsub client | `MPREGPubSubClient`, `MPREGPubSubExtendedClient` | partial | `sensor_ingest_pubsub` |
| `client.dns` | DNS resolve client | `MPREGDnsClient.resolve` | shipped | `plane_dns` |
| `client.trace` | Last W3C trace context | `last_trace_context()` | partial | `hello_trace`, `global_edge_control_plane` |
| `client.auth` | Token / API key on wire | `auth_token`, `api_key`, `SecurityConfig` | gap | — |

---

## 3. Pub/Sub (`pubsub.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `pubsub.exchange` | In-process topic exchange | `TopicExchange` | shipped | `hello_pubsub`, `plane_pubsub` |
| `pubsub.wildcard_star` | Single-segment `*` | `TopicPattern("user.*.login")` | shipped | `hello_pubsub`, `sensor_ingest_pubsub` |
| `pubsub.wildcard_hash` | Multi-segment `#` | `TopicPattern("order.#")` | shipped | `plane_pubsub`, `webhook_dispatcher` |
| `pubsub.fanout` | Multi-subscriber match | multiple `PubSubSubscription` | shipped | `hello_pubsub`, `plane_pubsub` |
| `pubsub.backlog` | Subscribe with backlog | `subscribe(..., get_backlog=True)` | partial | `plane_pubsub` (client path) |
| `pubsub.publish_reply` | Request/reply over topics | `publish_with_reply` | shipped | `pubsub_request_reply` |
| `pubsub.client_wire` | Wire pubsub via client | `MPREGClient.publish/subscribe` | shipped | `pubsub_request_reply`, `sensor_ingest_pubsub` |
| `pubsub.headers` | Message headers | `PubSubMessage.headers` | partial | `webhook_dispatcher` |
| `pubsub.fabric_forward` | Cross-cluster topic forward | `fabric.pubsub_forwarding` | partial | `tier3_expansion` |

---

## 4. Queue (`queue.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `queue.create` | Named queue | `MessageQueueManager.create_queue` | shipped | `job_queue_worker`, `plane_queue` |
| `queue.send` | Enqueue with guarantee | `send_message(..., DeliveryGuarantee)` | shipped | `job_queue_worker`, `order_intake` |
| `queue.alo` | At-least-once | `DeliveryGuarantee.AT_LEAST_ONCE` | shipped | `job_queue_worker`, `plane_queue` |
| `queue.quorum` | Quorum acks | `QUORUM` + `required_acknowledgments` | shipped | `job_queue_worker`, `plane_queue` |
| `queue.broadcast` | Broadcast delivery | `DeliveryGuarantee.BROADCAST` | partial | `plane_queue` |
| `queue.fnf` | Fire-and-forget | `FIRE_AND_FORGET` | partial | `plane_queue` |
| `queue.subscribe` | Worker callback | `subscribe_to_queue` | shipped | all queue apps |
| `queue.ack` | Explicit ack | `acknowledge_message` / `queue_ack` RPC | partial | `plane_queue` |
| `queue.receive` | Poll receive | `receive_message` / client `queue_receive` | partial | `plane_queue` |
| `queue.dlq` | Dead-letter path | queue timeout → DLQ | gap | — |
| `queue.topic_route` | Topic → queue bridge | `route_topic_to_queue` | shipped | `pubsub_plus_queue`, `webhook_dispatcher` |
| `queue.rpc_surface` | Queue via unified client RPC | `MPREGClient.queue_send/receive/ack` | shipped | `unified_client_tour` |
| `queue.factories` | Standard / HT / reliable mgr | `create_*_queue_manager` | shipped | `plane_queue` |

---

## 5. Cache (`cache.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `cache.put_get` | Basic put/get | `GlobalCacheManager.put/get`, `GlobalCacheKey` | shipped | `hello_cache`, `session_cache` |
| `cache.ttl` | TTL / expiry metadata | `CacheMetadata(ttl_seconds=…)` | shipped | `session_cache`, `hello_cache` |
| `cache.l1` | Process-local L1 | default level | shipped | `hello_cache` |
| `cache.l2` | Persistent L2 | `enable_l2_persistent`, `CacheL2Store` | partial | `config_reload_live` |
| `cache.l3` | Distributed L3 | `enable_l3_distributed`, `CacheOptions(L3)` | shipped | `plane_cache`, `cache_plus_federation` |
| `cache.l4` | Federated L4 | `enable_l4_federation`, `CacheOptions(L4)` | shipped | `plane_cache`, `feature_flag_mesh` |
| `cache.fabric_protocol` | Fabric cache gossip | `FabricCacheProtocol`, `InProcessCacheTransport` | shipped | `plane_cache`, `feature_flag_mesh` |
| `cache.sync` | Explicit peer sync | `sync_cache_state(peer)` | shipped | `plane_cache`, `cache_plus_federation` |
| `cache.geo_hints` | Geographic placement hints | `CacheMetadata.geographic_hints` | partial | `plane_cache` |
| `cache.replication` | Replication strategy | `ReplicationStrategy`, `CacheReplicationPolicy` | partial | `feature_flag_mesh` |
| `cache.invalidate` | Pattern invalidate | `invalidate` / client `cache_invalidate` | partial | `session_cache` |
| `cache.atomic` | CAS / incr / append | `AdvancedCacheOperations.atomic_operation` | shipped | `cache_atomic_ops` |
| `cache.structures` | Set/list/map/counter ops | `data_structure_operation` | shipped | `cache_atomic_ops` |
| `cache.namespace_ops` | Clear/list/scan namespace | `namespace_operation` | shipped | `cache_atomic_ops` |
| `cache.pubsub_events` | Cache→pubsub integration | `CachePubSubIntegration` | gap | — |
| `cache.rpc_surface` | Cache via unified client | `MPREGClient.cache_get/put` | shipped | `unified_client_tour` |

---

## 6. Fabric / federation (`fabric.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `fabric.permissive` | Permissive bridging | `create_permissive_bridging_config` | shipped | `plane_fabric`, `multi_region_shop` |
| `fabric.strict` | Strict isolation | `create_strict_isolation_config` | partial | `signed_route_border` |
| `fabric.explicit` | Explicit bridge allowlist | `create_explicit_bridging_config` | partial | `plane_fabric` |
| `fabric.cluster_id` | Cluster identity on settings | `MPREGSettings.cluster_id` | shipped | fabric apps |
| `fabric.cross_rpc` | Cross-cluster RPC DAG | multi `cluster_id` + `locs` | shipped | `plane_fabric`, `multi_region_shop` |
| `fabric.catalog` | Routing catalog types | `fabric.catalog` Function/Topic/Queue/Cache catalogs | partial | `fabric_snapshot_restart` |
| `fabric.gossip` | Membership / gossip | `fabric.gossip*`, peer directory | partial | `discovery_join`, `tier3_expansion` |
| `fabric.link_state` | Link-state routing | `fabric.link_state` | partial | `global_edge_control_plane` |
| `fabric.route_security` | Signed route announcements | `RouteAnnouncementSigner`, `RouteSecurityConfig` | shipped | `signed_route_border` |
| `fabric.route_policy` | Neighbor / policy tags | route policy directory | shipped | `signed_route_border` |
| `fabric.route_keys` | Route key rotation | route keys + gossip | shipped | `signed_route_border` |
| `fabric.snapshot` | Catalog/key persistence | fabric persistence snapshot | shipped | `fabric_snapshot_restart` |
| `fabric.hubs` | Hub hierarchy / edges | hubs, hub_registry | shipped | `global_edge_control_plane` |
| `fabric.graph` | Graph / Dijkstra routers | `FederationGraph`, routers | gap | — |
| `fabric.resilience` | Circuit breakers / recovery | `FederationHealthMonitor` | gap | — |
| `fabric.queue_fed` | Queue federation | `queue_federation` | gap | — |
| `fabric.blockchain_msg` | Blockchain message federation | `blockchain_message_federation` | gap | — |

---

## 7. Discovery / DNS (`disco.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `disco.list_peers` | Peer snapshots | `list_peers` | shipped | `discovery_join`, `hello_cluster` |
| `disco.cluster_map` | Cluster map v1/v2 | `cluster_map`, `cluster_map_v2` | shipped | `discovery_join`, `ha_client_failover` |
| `disco.catalog_query` | Scoped catalog query | `catalog_query` | partial | `discovery_join` |
| `disco.catalog_watch` | Delta watch topics | `catalog_watch` | gap | — |
| `disco.summary_query` | Summary records | `summary_query` | gap | — |
| `disco.summary_watch` | Summary export topics | `summary_watch` | gap | — |
| `disco.access_audit` | Discovery access audit | `discovery_access_audit` | gap | — |
| `disco.resolver_stats` | Resolver cache stats | `resolver_cache_stats` | gap | — |
| `disco.resolver_resync` | Force catalog resync | `resolver_resync` | gap | — |
| `disco.dns_register` | DNS service register | `dns_register` / CLI | shipped | `plane_dns` |
| `disco.dns_resolve` | DNS gateway resolve | `MPREGDnsClient`, `DnsGateway` | shipped | `plane_dns` |
| `disco.join` | Live node join visibility | peers + new resources | shipped | `discovery_join` |
| `disco.signatures` | Signed discovery summaries | `discovery_signatures` | gap | — |

---

## 8. Namespace policy (`ns.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `ns.status` | Namespace status | `namespace_status` | shipped | `namespace_policy_gate` |
| `ns.export` | Export policy | `namespace_policy_export` | shipped | `namespace_policy_gate` |
| `ns.validate` | Validate rules | `namespace_policy_validate` | shipped | `namespace_policy_gate` |
| `ns.apply` | Apply rules | `namespace_policy_apply` | shipped | `namespace_policy_gate` |
| `ns.audit` | Policy audit log | `namespace_policy_audit` | shipped | `namespace_policy_gate` |
| `ns.engine` | In-process engine | `NamespacePolicyEngine` | partial | `namespace_policy_gate` (via apply) |

---

## 9. Monitoring / observability (`mon.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `mon.unified` | Unified system monitor | `create_unified_system_monitor` | shipped | `hello_trace`, `plane_monitoring` |
| `mon.events` | Cross-system events | `record_cross_system_event` | shipped | `hello_trace`, `plane_monitoring` |
| `mon.timeline` | Tracking timeline | `get_tracking_timeline` | shipped | `hello_trace`, `global_edge_control_plane` |
| `mon.correlation` | Correlation metrics | `get_correlation_timeline` | partial | `plane_monitoring` |
| `mon.system_types` | RPC/CACHE/QUEUE/… tags | `SystemType`, `EventType` | shipped | monitoring apps |
| `mon.health` | Aggregated health | `get_unified_metrics` | partial | `plane_monitoring` |
| `mon.transport` | Transport health attach | `attach_transport_adapter` | gap | — |
| `mon.slo` | SLO helpers | `core.observability.slo` | gap | — |
| `mon.logging` | Structured / JSON logs | `configure_logging`, CLI `--json-logs` | partial | ops docs |
| `mon.trace_bind` | Trace context bind | `bind_trace_context` | partial | `hello_trace` |

---

## 10. Consensus / Raft (`cons.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `cons.raft` | Production Raft | `datastructures.production_raft*` | partial | `partition_safe_counter` (teaching model) |
| `cons.leader` | Leader election helpers | `consensus`, `leader_election` | partial | `partition_safe_counter` |
| `cons.quorum_teach` | Majority vs minority | teaching counter + partition | shipped | `partition_safe_counter` |
| `cons.oracle` | Raft oracle (tests) | `testing.oracles.RaftOracle` | gap | test-only |

---

## 11. Chaos / testing (`chaos.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `chaos.partition` | Network partition groups | `FaultInjector.partition` | shipped | `partition_safe_counter`, `chaos_checkout` |
| `chaos.heal` | Heal partitions | `heal()` | shipped | `chaos_checkout` |
| `chaos.crash` | Crash / recover node | `crash` / `recover` | partial | `chaos_checkout` |
| `chaos.clock_skew` | Clock skew injection | `set_clock_skew` | gap | — |
| `chaos.duplicate` | Dup / reorder hooks | `should_duplicate`, `should_reorder` | gap | — |
| `chaos.routing_oracle` | Expected next-hop oracle | `RoutingOracle` | gap | test-only |
| `chaos.no_loop` | Loop assertion helper | `assert_no_routing_loop` | gap | — |

---

## 12. Persistence (`pers.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `pers.sqlite_kv` | SQLite KV backend | `SQLiteKeyValueStore` | shipped | `config_reload_live` |
| `pers.sqlite_queue` | SQLite queue store | `SQLiteQueueStore` | shipped | `config_reload_live` |
| `pers.memory` | Memory backends | `Memory*Store` | shipped | default demos |
| `pers.cache_l2` | Cache L2 store | `CacheL2Store` | partial | `config_reload_live` |
| `pers.mode` | Persistence mode config | `PersistenceConfig`, `PersistenceMode` | partial | `config_reload_live` |
| `pers.fabric_snap` | Fabric snapshot files | fabric persistence | shipped | `fabric_snapshot_restart` |
| `pers.restart` | Survive process restart | dual-phase demo | shipped | `config_reload_live` |

---

## 13. Transport (`tx.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `tx.websocket` | Default WS transport | server/client default | shipped | all live server apps |
| `tx.tcp` | TCP transport | `tcp_transport` | gap | — |
| `tx.circuit_breaker` | Transport CB | `circuit_breaker` | gap | — |
| `tx.correlation` | Correlation tracker | `CorrelationTracker` | partial | monitoring |
| `tx.security` | TLS / certs config | `SecurityConfig`, `TransportConfig` | gap | — |
| `tx.multi_protocol` | Enhanced multi-protocol adapter | `EnhancedMultiProtocolAdapter` | gap | — |

---

## 14. Bootstrap / settings (`boot.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `boot.port_range` | Dynamic port allocation | `port_range_context` | shipped | all server apps |
| `boot.auto_port` | OS-assigned / auto ports | auto-port bootstrap patterns | shipped | `hello_ports`, `auto_port_bootstrap` |
| `boot.settings` | Full settings surface | `MPREGSettings` (peers, resources, cluster_id, federation_config, log_level) | shipped | all |
| `boot.peers` | Static peer join | `peers=[url]` | shipped | cluster apps |
| `boot.resources` | Resource advertisements | `resources={…}` | shipped | RPC apps |
| `boot.profiles` | TOML profiles | `mpreg/profiles/*.toml` | partial | OPERATE.md |

---

## 15. CLI / ops (`ops.*`)

| ID | Feature | Primary APIs | Depth | Apps |
|----|---------|--------------|-------|------|
| `ops.cli_call` | `mpreg call` | CLI | partial | OPERATE |
| `ops.cli_planes` | queue/cache/publish CLI | `mpreg client-*` | partial | OPERATE |
| `ops.cli_dns` | DNS CLI group | `mpreg dns-*` | gap | — |
| `ops.cli_ns` | Namespace policy CLI | `mpreg namespace-policy` | gap | — |
| `ops.cli_discovery` | peers / resolver | `list-peers`, resolver cmds | partial | OPERATE |
| `ops.example_runner` | Curriculum runner | `mpreg-example`, `mpreg examples`, `mpreg demo` | shipped | all |
| `ops.doctor` | Doctor / admin (if present) | CLI | gap | — |

---

## 16. Product compositions (`prod.*`)

| ID | Composition | Apps |
|----|-------------|------|
| `prod.hello_vertical` | Minimal each plane | L0 hellos |
| `prod.order` | RPC+cache+pubsub+queue | `order_intake` |
| `prod.media` | Multi-stage RPC ETL | `media_pipeline` |
| `prod.flags` | L4 flag mesh | `feature_flag_mesh` |
| `prod.webhooks` | Events → durable egress | `webhook_dispatcher` |
| `prod.ml` | Specialized inference mesh | `ml_inference_mesh` |
| `prod.shop` | Multi-region checkout path | `multi_region_shop` |
| `prod.edge` | Global hub + edges | `global_edge_control_plane` |
| `prod.tier3` | Full multi-system expansion | `tier3_expansion` |

---

## Coverage matrix (features × levels)

Legend: ● primary teach · ○ supporting · · absent

| Feature family | L0 | L1 | L2 | L3 | L4 |
|----------------|----|----|----|----|-----|
| RPC core (call/dag/locs) | ● | ● | ● | ● | ● |
| Client HA + policy M1/M2 | · | ● | ○ | ● | ○ |
| PubSub wildcards | ● | ● | ● | ○ | · |
| Queue ALO/quorum | · | ● | ● | ○ | · |
| Cache L1–L4 | ● | ● | ● | ○ | · |
| Fabric multi-cluster | · | ● | ● | ● | ● |
| Route security | · | · | · | ● | · |
| Discovery join/map | ○ | ○ | · | ● | ○ |
| Monitoring timeline | ● | ● | · | ○ | ● |
| Chaos / partition | · | · | · | ● | · |
| Persistence restart | · | · | ● | ● | · |
| DNS / namespace / atomic cache | · | ● | · | · | · |

---

## Gap backlog (platform has it; curriculum thin or missing)

**Closed in Phase E waves E1–E5 (2026-08-05):** DNS plane, namespace policy,
advanced cache ops, unified client surfaces, publish-with-reply.

Still open (prioritized):

1. **Graph router / resilience** fabric drills (`fabric.graph`, `fabric.resilience`)
2. **Discovery watches + summary query** (`disco.catalog_watch`, `disco.summary_*`)
3. **Cache→pubsub events** (`cache.pubsub_events`)
4. **Queue DLQ path** (`queue.dlq`)
5. **TLS / auth_token** client security path (`client.auth`, `tx.security`)
6. **Topic-aware / versioned RPC** (`rpc.topic_aware`, `rpc.function_id`)
7. **Chaos extras** (clock skew, duplicate/reorder)
8. **Ops CLI teaching app** (`ops.cli_*`)

Until those ship, READMEs must **non-claim** them.

---

## App → feature map (canonical)

| App | Level | Feature IDs (primary) |
|-----|-------|----------------------|
| `hello_rpc` | L0 | `rpc.register`, `rpc.dag`, `rpc.call`, `boot.port_range` |
| `hello_cluster` | L0 | `rpc.dag`, `rpc.locs`, `boot.peers`, `disco.list_peers` |
| `hello_trace` | L0 | `mon.unified`, `mon.events`, `mon.timeline` |
| `hello_pubsub` | L0 | `pubsub.exchange`, `pubsub.wildcard_star`, `pubsub.fanout` |
| `hello_cache` | L0 | `cache.put_get`, `cache.l1`, `cache.ttl` |
| `hello_ports` | L0 | `boot.port_range`, `boot.auto_port`, `rpc.call` |
| `ha_client_failover` | L1 | `client.cluster`, `client.policy.m1`, `client.default_ha`, `disco.cluster_map` |
| `job_queue_worker` | L1 | `queue.alo`, `queue.quorum`, `queue.subscribe` |
| `url_shortener_rpc` | L1 | `rpc.call`, `cache.put_get` |
| `sensor_ingest_pubsub` | L1 | `pubsub.wildcard_*`, `pubsub.fanout` |
| `session_cache` | L1 | `cache.ttl`, `cache.invalidate` |
| `auto_port_bootstrap` | L1 | `boot.auto_port`, `boot.peers`, `rpc.locs` |
| `plane_rpc` | L1 | `rpc.*` tour incl. list/describe/policy |
| `plane_pubsub` | L1 | `pubsub.*` tour |
| `plane_queue` | L1 | `queue.*` tour |
| `plane_cache` | L1 | `cache.l1–l4`, `cache.fabric_protocol` |
| `plane_fabric` | L1 | `fabric.permissive`, `fabric.cross_rpc` |
| `plane_monitoring` | L1 | `mon.*` tour |
| `order_intake` | L2 | `prod.order`, multi-plane |
| `media_pipeline` | L2 | `rpc.dag`, `rpc.locs`, multi-stage |
| `feature_flag_mesh` | L2 | `cache.l4`, `cache.fabric_protocol` |
| `webhook_dispatcher` | L2 | `pubsub` + `queue.topic_route` |
| `config_reload_live` | L2 | `pers.restart`, `pers.sqlite_*` |
| `rpc_plus_cache` | L2 | `rpc` + `cache` integration |
| `pubsub_plus_queue` | L2 | `pubsub` + `queue` bridge |
| `cache_plus_federation` | L2 | `cache.l3/l4` federation |
| `ml_inference_mesh` | L2 | `rpc.locs` specialized workers |
| `multi_region_shop` | L3 | `fabric.cross_rpc`, `rpc.target_cluster` |
| `signed_route_border` | L3 | `fabric.route_security`, `fabric.route_keys` |
| `partition_safe_counter` | L3 | `chaos.partition`, `cons.quorum_teach` |
| `discovery_join` | L3 | `disco.join`, `disco.list_peers`, `disco.cluster_map` |
| `chaos_checkout` | L3 | `chaos.*`, `client.policy.m2`, `rpc.deadline` |
| `fabric_snapshot_restart` | L3 | `pers.fabric_snap`, `fabric.catalog` |
| `tier3_expansion` | L3 | multi-plane expansion |
| `global_edge_control_plane` | L4 | `fabric.hubs`, `mon.timeline`, multi-cluster |
| `cache_atomic_ops` | L1 | `cache.atomic`, `cache.structures`, `cache.namespace_ops` |
| `namespace_policy_gate` | L1 | `ns.validate`, `ns.apply`, `ns.status`, `ns.export`, `ns.audit` |
| `plane_dns` | L1 | `disco.dns_register`, `disco.dns_resolve`, `client.dns` |
| `unified_client_tour` | L1 | `client.unified`, `cache.rpc_surface`, `queue.rpc_surface` |
| `pubsub_request_reply` | L1 | `pubsub.publish_reply`, `pubsub.client_wire` |

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
