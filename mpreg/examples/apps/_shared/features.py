"""Feature IDs taught by curriculum apps (mirrors docs FEATURE_CATALOG).

Keep IDs stable — they are the join key between FEATURE_CATALOG.md, registry
metadata, README drill-downs, and pytest coverage checks.
"""

from __future__ import annotations

from typing import Final

# ── Canonical feature ID constants (subset used in registry; full list in docs)

RPC_REGISTER: Final = "rpc.register"
RPC_CALL: Final = "rpc.call"
RPC_DAG: Final = "rpc.dag"
RPC_LOCS: Final = "rpc.locs"
RPC_TARGET_CLUSTER: Final = "rpc.target_cluster"
RPC_LIST: Final = "rpc.list"
RPC_DESCRIBE: Final = "rpc.describe"
RPC_REPORT: Final = "rpc.report"
RPC_DEADLINE: Final = "rpc.deadline"
RPC_FUNCTION_ID: Final = "rpc.function_id"
RPC_VERSION_CONSTRAINT: Final = "rpc.version_constraint"
RPC_TOPIC_AWARE: Final = "rpc.topic_aware"
RPC_FQN: Final = "rpc.fqn"
RPC_NAMESPACE_DENY: Final = "rpc.namespace_deny"
RPC_BOUND_NAMESPACE: Final = "rpc.bound_namespace"
RPC_CONCURRENCY: Final = "rpc.concurrency"
RPC_ROUTING_TOPIC: Final = "rpc.routing_topic"

CLIENT_API: Final = "client.api"
CLIENT_PUBSUB: Final = "client.pubsub"
CLIENT_UNIFIED: Final = "client.unified"
CLIENT_CLUSTER: Final = "client.cluster"
CLIENT_CLUSTER_MAP: Final = "client.cluster_map"
CLIENT_POLICY_M1: Final = "client.policy.m1"
CLIENT_POLICY_M2: Final = "client.policy.m2"
CLIENT_POLICY_M3: Final = "client.policy.m3"
CLIENT_DEFAULT_HA: Final = "client.default_ha"
CLIENT_TRACE: Final = "client.trace"
CLIENT_SUMMARY: Final = "client.summary"
CLIENT_AUTH: Final = "client.auth"
TX_SECURITY: Final = "tx.security"

PUBSUB_EXCHANGE: Final = "pubsub.exchange"
PUBSUB_WILDCARD_STAR: Final = "pubsub.wildcard_star"
PUBSUB_WILDCARD_HASH: Final = "pubsub.wildcard_hash"
PUBSUB_FANOUT: Final = "pubsub.fanout"
PUBSUB_HEADERS: Final = "pubsub.headers"
PUBSUB_PUBLISH_REPLY: Final = "pubsub.publish_reply"
PUBSUB_CLIENT_WIRE: Final = "pubsub.client_wire"
PUBSUB_BACKLOG: Final = "pubsub.backlog"
PUBSUB_FABRIC_FORWARD: Final = "pubsub.fabric_forward"

QUEUE_CREATE: Final = "queue.create"
QUEUE_SEND: Final = "queue.send"
QUEUE_ALO: Final = "queue.alo"
QUEUE_QUORUM: Final = "queue.quorum"
QUEUE_BROADCAST: Final = "queue.broadcast"
QUEUE_FNF: Final = "queue.fnf"
QUEUE_SUBSCRIBE: Final = "queue.subscribe"
QUEUE_TOPIC_ROUTE: Final = "queue.topic_route"
QUEUE_FACTORIES: Final = "queue.factories"
QUEUE_RPC_SURFACE: Final = "queue.rpc_surface"
QUEUE_DLQ: Final = "queue.dlq"
QUEUE_ACK: Final = "queue.ack"
QUEUE_RECEIVE: Final = "queue.receive"

CACHE_PUT_GET: Final = "cache.put_get"
CACHE_TTL: Final = "cache.ttl"
CACHE_L1: Final = "cache.l1"
CACHE_L2: Final = "cache.l2"
CACHE_L3: Final = "cache.l3"
CACHE_L4: Final = "cache.l4"
CACHE_FABRIC: Final = "cache.fabric_protocol"
CACHE_SYNC: Final = "cache.sync"
CACHE_GEO: Final = "cache.geo_hints"
CACHE_INVALIDATE: Final = "cache.invalidate"
CACHE_REPLICATION: Final = "cache.replication"
CACHE_ATOMIC: Final = "cache.atomic"
CACHE_STRUCTURES: Final = "cache.structures"
CACHE_NAMESPACE_OPS: Final = "cache.namespace_ops"
CACHE_RPC_SURFACE: Final = "cache.rpc_surface"
CACHE_PUBSUB_EVENTS: Final = "cache.pubsub_events"

FABRIC_PERMISSIVE: Final = "fabric.permissive"
FABRIC_STRICT: Final = "fabric.strict"
FABRIC_EXPLICIT: Final = "fabric.explicit"
FABRIC_CLUSTER_ID: Final = "fabric.cluster_id"
FABRIC_CROSS_RPC: Final = "fabric.cross_rpc"
FABRIC_CATALOG: Final = "fabric.catalog"
FABRIC_ROUTE_SECURITY: Final = "fabric.route_security"
FABRIC_ROUTE_POLICY: Final = "fabric.route_policy"
FABRIC_ROUTE_KEYS: Final = "fabric.route_keys"
FABRIC_SNAPSHOT: Final = "fabric.snapshot"
FABRIC_HUBS: Final = "fabric.hubs"
FABRIC_GRAPH: Final = "fabric.graph"
FABRIC_RESILIENCE: Final = "fabric.resilience"
FABRIC_LINK_STATE: Final = "fabric.link_state"

DISCO_LIST_PEERS: Final = "disco.list_peers"
DISCO_CLUSTER_MAP: Final = "disco.cluster_map"
DISCO_CATALOG_QUERY: Final = "disco.catalog_query"
DISCO_JOIN: Final = "disco.join"
DISCO_DNS_REGISTER: Final = "disco.dns_register"
DISCO_DNS_RESOLVE: Final = "disco.dns_resolve"
DISCO_CATALOG_WATCH: Final = "disco.catalog_watch"
DISCO_SUMMARY_QUERY: Final = "disco.summary_query"
DISCO_SUMMARY_WATCH: Final = "disco.summary_watch"

NS_STATUS: Final = "ns.status"
NS_EXPORT: Final = "ns.export"
NS_VALIDATE: Final = "ns.validate"
NS_APPLY: Final = "ns.apply"
NS_AUDIT: Final = "ns.audit"
NS_ENGINE: Final = "ns.engine"

CLIENT_DNS: Final = "client.dns"

MON_UNIFIED: Final = "mon.unified"
MON_EVENTS: Final = "mon.events"
MON_TIMELINE: Final = "mon.timeline"
MON_CORRELATION: Final = "mon.correlation"
MON_SYSTEM_TYPES: Final = "mon.system_types"
MON_HEALTH: Final = "mon.health"
MON_LOGGING: Final = "mon.logging"

CONS_QUORUM_TEACH: Final = "cons.quorum_teach"
CONS_RAFT: Final = "cons.raft"
CONS_LEADER: Final = "cons.leader"
CHAOS_PARTITION: Final = "chaos.partition"
CHAOS_HEAL: Final = "chaos.heal"
CHAOS_CRASH: Final = "chaos.crash"
CHAOS_TRANSPORT: Final = "chaos.transport"
CHAOS_CLOCK_SKEW: Final = "chaos.clock_skew"
CHAOS_DUPLICATE: Final = "chaos.duplicate"
CHAOS_REORDER: Final = "chaos.reorder"
CHAOS_DROP: Final = "chaos.drop"

OPS_CLI_CALL: Final = "ops.cli_call"
OPS_CLI_DNS: Final = "ops.cli_dns"
OPS_CLI_DOCTOR: Final = "ops.cli_doctor"
OPS_CLI_CONFIG: Final = "ops.cli_config"
OPS_CLI_EXAMPLES: Final = "ops.cli_examples"
OPS_CLI_PLANES: Final = "ops.cli_planes"
OPS_CLI_NS: Final = "ops.cli_ns"
OPS_CLI_DISCOVERY: Final = "ops.cli_discovery"

PERS_SQLITE_KV: Final = "pers.sqlite_kv"
PERS_SQLITE_QUEUE: Final = "pers.sqlite_queue"
PERS_CACHE_L2: Final = "pers.cache_l2"
PERS_FABRIC_SNAP: Final = "pers.fabric_snap"
PERS_RESTART: Final = "pers.restart"
PERS_MODE: Final = "pers.mode"

BOOT_PORT_RANGE: Final = "boot.port_range"
BOOT_AUTO_PORT: Final = "boot.auto_port"
BOOT_SETTINGS: Final = "boot.settings"
BOOT_PEERS: Final = "boot.peers"
BOOT_RESOURCES: Final = "boot.resources"
BOOT_PROFILES: Final = "boot.profiles"
TX_CIRCUIT_BREAKER: Final = "tx.circuit_breaker"

PROD_ORDER: Final = "prod.order"
PROD_MEDIA: Final = "prod.media"
PROD_FLAGS: Final = "prod.flags"
PROD_WEBHOOKS: Final = "prod.webhooks"
PROD_ML: Final = "prod.ml"
PROD_SHOP: Final = "prod.shop"
PROD_EDGE: Final = "prod.edge"
PROD_TIER3: Final = "prod.tier3"
PROD_NOTIFY: Final = "prod.notify"
PROD_BILLING: Final = "prod.billing"
PROD_INVENTORY: Final = "prod.inventory"

TOPIC_TAXONOMY: Final = "topic.taxonomy"
TOPIC_VALIDATE: Final = "topic.validate"
TOPIC_GENERATE: Final = "topic.generate"
TOPIC_ACCESS: Final = "topic.access"

PERS_MEMORY_KV: Final = "pers.memory_kv"
BOOT_PROFILE: Final = "boot.profile"

RPC_INTERMEDIATE: Final = "rpc.intermediate"
ORACLE_ROUTING: Final = "oracle.routing"
ORACLE_RAFT: Final = "oracle.raft"
FABRIC_DEADLINE_HOP: Final = "fabric.deadline_hop"

DISCO_RATE_LIMIT: Final = "disco.rate_limit"
MON_SLO: Final = "mon.slo"
MON_GOLDEN_SIGNALS: Final = "mon.golden_signals"
MON_TRACE_CONTEXT: Final = "mon.trace_context"
RPC_DEPENDENCY: Final = "rpc.dependency"
CONS_LEADER_ELECTION: Final = "cons.leader_election"
PROD_SHIPPING: Final = "prod.shipping"

DISCO_ACCESS_AUDIT: Final = "disco.access_audit"
DISCO_RESOLVER_STATS: Final = "disco.resolver_stats"
DISCO_RESOLVER_RESYNC: Final = "disco.resolver_resync"
DISCO_SIGNATURES: Final = "disco.signatures"
FABRIC_QUEUE_FED: Final = "fabric.queue_fed"
FABRIC_BLOCKCHAIN_MSG: Final = "fabric.blockchain_msg"
MON_TRANSPORT: Final = "mon.transport"
TX_TLS: Final = "tx.tls"
TX_TCP: Final = "tx.tcp"
TX_MULTI_PROTOCOL: Final = "tx.multi_protocol"
CHAOS_LIVE_DRAIN: Final = "chaos.live_drain"
OPS_MGMT_DRAIN: Final = "ops.mgmt_drain"
OPS_MGMT_DETACH: Final = "ops.mgmt_detach"
OPS_MGMT_AUDIT: Final = "ops.mgmt_audit"
CHAOS_NO_LOOP: Final = "chaos.no_loop"
TX_CORRELATION: Final = "tx.correlation"
MON_TRACE_BIND: Final = "mon.trace_bind"
MON_METRICS_SNAPSHOT: Final = "mon.metrics_snapshot"
MON_SERVER_TRACKER: Final = "mon.server_tracker"
FABRIC_GOSSIP: Final = "fabric.gossip"

# App id → feature IDs (must stay aligned with FEATURE_CATALOG.md)
APP_FEATURES: dict[str, tuple[str, ...]] = {
    "hello_rpc": (
        RPC_REGISTER,
        RPC_CALL,
        RPC_DAG,
        CLIENT_API,
        BOOT_PORT_RANGE,
        BOOT_RESOURCES,
    ),
    "hello_cluster": (
        RPC_REGISTER,
        RPC_DAG,
        RPC_LOCS,
        CLIENT_API,
        BOOT_PEERS,
        BOOT_RESOURCES,
        DISCO_LIST_PEERS,
    ),
    "hello_trace": (MON_UNIFIED, MON_EVENTS, MON_TIMELINE, MON_SYSTEM_TYPES),
    "hello_pubsub": (
        PUBSUB_EXCHANGE,
        PUBSUB_WILDCARD_STAR,
        PUBSUB_WILDCARD_HASH,
        PUBSUB_FANOUT,
    ),
    "hello_cache": (CACHE_PUT_GET, CACHE_L1, CACHE_TTL, CACHE_FABRIC),
    "hello_ports": (
        BOOT_PORT_RANGE,
        BOOT_AUTO_PORT,
        RPC_CALL,
        RPC_REGISTER,
    ),
    "ha_client_failover": (
        CLIENT_CLUSTER,
        CLIENT_POLICY_M1,
        CLIENT_DEFAULT_HA,
        CLIENT_CLUSTER_MAP,
        RPC_CALL,
        DISCO_CLUSTER_MAP,
    ),
    "job_queue_worker": (
        QUEUE_CREATE,
        QUEUE_SEND,
        QUEUE_ALO,
        QUEUE_QUORUM,
        QUEUE_SUBSCRIBE,
        QUEUE_FACTORIES,
    ),
    "url_shortener_rpc": (RPC_CALL, RPC_REGISTER, CACHE_PUT_GET, CACHE_L1),
    "sensor_ingest_pubsub": (
        PUBSUB_EXCHANGE,
        PUBSUB_WILDCARD_STAR,
        PUBSUB_WILDCARD_HASH,
        PUBSUB_FANOUT,
    ),
    "session_cache": (CACHE_PUT_GET, CACHE_TTL, CACHE_INVALIDATE, CACHE_L1),
    "auto_port_bootstrap": (
        BOOT_AUTO_PORT,
        BOOT_PEERS,
        RPC_LOCS,
        RPC_CALL,
        DISCO_LIST_PEERS,
    ),
    "plane_rpc": (
        RPC_REGISTER,
        RPC_CALL,
        RPC_DAG,
        RPC_LOCS,
        RPC_LIST,
        RPC_DESCRIBE,
        RPC_REPORT,
        CLIENT_POLICY_M1,
        CLIENT_POLICY_M3,
    ),
    "plane_pubsub": (
        PUBSUB_EXCHANGE,
        PUBSUB_WILDCARD_STAR,
        PUBSUB_WILDCARD_HASH,
        PUBSUB_FANOUT,
        PUBSUB_HEADERS,
    ),
    "plane_queue": (
        QUEUE_CREATE,
        QUEUE_SEND,
        QUEUE_ALO,
        QUEUE_QUORUM,
        QUEUE_BROADCAST,
        QUEUE_FNF,
        QUEUE_SUBSCRIBE,
        QUEUE_FACTORIES,
    ),
    "plane_cache": (
        CACHE_PUT_GET,
        CACHE_L1,
        CACHE_L3,
        CACHE_L4,
        CACHE_FABRIC,
        CACHE_SYNC,
        CACHE_GEO,
        CACHE_TTL,
    ),
    "plane_fabric": (
        FABRIC_PERMISSIVE,
        FABRIC_CLUSTER_ID,
        FABRIC_CROSS_RPC,
        RPC_DAG,
        RPC_LOCS,
    ),
    "plane_monitoring": (
        MON_UNIFIED,
        MON_EVENTS,
        MON_TIMELINE,
        MON_CORRELATION,
        MON_SYSTEM_TYPES,
        MON_HEALTH,
    ),
    "order_intake": (
        PROD_ORDER,
        RPC_CALL,
        CACHE_PUT_GET,
        PUBSUB_EXCHANGE,
        QUEUE_ALO,
        QUEUE_SUBSCRIBE,
    ),
    "media_pipeline": (PROD_MEDIA, RPC_DAG, RPC_LOCS, RPC_REGISTER),
    "feature_flag_mesh": (
        PROD_FLAGS,
        CACHE_L4,
        CACHE_FABRIC,
        CACHE_SYNC,
        CACHE_REPLICATION,
    ),
    "webhook_dispatcher": (
        PROD_WEBHOOKS,
        PUBSUB_EXCHANGE,
        PUBSUB_WILDCARD_HASH,
        QUEUE_ALO,
        QUEUE_TOPIC_ROUTE,
    ),
    "config_reload_live": (
        PERS_RESTART,
        PERS_SQLITE_KV,
        PERS_SQLITE_QUEUE,
        CACHE_L2,
        QUEUE_ALO,
    ),
    "rpc_plus_cache": (RPC_CALL, CACHE_PUT_GET, CACHE_TTL),
    "pubsub_plus_queue": (PUBSUB_EXCHANGE, QUEUE_ALO, QUEUE_TOPIC_ROUTE),
    "cache_plus_federation": (CACHE_L3, CACHE_L4, CACHE_FABRIC, CACHE_SYNC),
    "ml_inference_mesh": (PROD_ML, RPC_CALL, RPC_LOCS, RPC_REGISTER),
    "multi_region_shop": (
        PROD_SHOP,
        FABRIC_PERMISSIVE,
        FABRIC_CROSS_RPC,
        FABRIC_CLUSTER_ID,
        RPC_TARGET_CLUSTER,
        RPC_DAG,
    ),
    "signed_route_border": (
        FABRIC_ROUTE_SECURITY,
        FABRIC_ROUTE_POLICY,
        FABRIC_ROUTE_KEYS,
        FABRIC_STRICT,
    ),
    "partition_safe_counter": (
        CONS_QUORUM_TEACH,
        CONS_RAFT,
        CONS_LEADER,
        CHAOS_PARTITION,
        CHAOS_HEAL,
    ),
    "discovery_join": (
        DISCO_JOIN,
        DISCO_LIST_PEERS,
        DISCO_CLUSTER_MAP,
        DISCO_CATALOG_QUERY,
        RPC_LIST,
        BOOT_PEERS,
    ),
    "chaos_checkout": (
        CHAOS_PARTITION,
        CHAOS_HEAL,
        CHAOS_CRASH,
        CLIENT_POLICY_M2,
        RPC_DEADLINE,
        RPC_CALL,
    ),
    "fabric_snapshot_restart": (PERS_FABRIC_SNAP, FABRIC_CATALOG, FABRIC_SNAPSHOT),
    "tier3_expansion": (
        PROD_TIER3,
        RPC_DAG,
        CACHE_L4,
        PUBSUB_EXCHANGE,
        QUEUE_ALO,
        FABRIC_CROSS_RPC,
        MON_TIMELINE,
    ),
    "global_edge_control_plane": (
        PROD_EDGE,
        FABRIC_HUBS,
        FABRIC_CROSS_RPC,
        FABRIC_PERMISSIVE,
        MON_TIMELINE,
        MON_EVENTS,
        RPC_TARGET_CLUSTER,
    ),
    # Phase E gap apps
    "cache_atomic_ops": (
        CACHE_ATOMIC,
        CACHE_STRUCTURES,
        CACHE_NAMESPACE_OPS,
        CACHE_PUT_GET,
    ),
    "namespace_policy_gate": (
        NS_VALIDATE,
        NS_APPLY,
        NS_STATUS,
        NS_EXPORT,
        NS_AUDIT,
        CLIENT_API,
    ),
    "plane_dns": (
        DISCO_DNS_REGISTER,
        DISCO_DNS_RESOLVE,
        CLIENT_DNS,
        CLIENT_API,
        BOOT_PORT_RANGE,
    ),
    "unified_client_tour": (
        CLIENT_UNIFIED,
        RPC_CALL,
        RPC_REGISTER,
        RPC_LIST,
        RPC_DESCRIBE,
        CACHE_RPC_SURFACE,
        CACHE_PUT_GET,
        CACHE_INVALIDATE,
        QUEUE_RPC_SURFACE,
        QUEUE_SEND,
        CLIENT_CLUSTER_MAP,
        DISCO_LIST_PEERS,
        DISCO_CATALOG_QUERY,
        DISCO_CLUSTER_MAP,
    ),
    "pubsub_request_reply": (
        PUBSUB_PUBLISH_REPLY,
        PUBSUB_EXCHANGE,
        PUBSUB_HEADERS,
        PUBSUB_CLIENT_WIRE,
        PUBSUB_FANOUT,
    ),
    "discovery_watch_summary": (
        DISCO_CATALOG_WATCH,
        DISCO_SUMMARY_QUERY,
        DISCO_SUMMARY_WATCH,
        DISCO_CATALOG_QUERY,
        PUBSUB_CLIENT_WIRE,
        CLIENT_API,
        CLIENT_SUMMARY,
    ),
    "fabric_graph_resilience": (
        FABRIC_GRAPH,
        FABRIC_RESILIENCE,
        TX_CIRCUIT_BREAKER,
    ),
    "cache_event_bus": (
        CACHE_PUBSUB_EVENTS,
        CACHE_PUT_GET,
        CACHE_INVALIDATE,
        PUBSUB_EXCHANGE,
    ),
    "job_queue_dlq": (
        QUEUE_DLQ,
        QUEUE_ALO,
        QUEUE_SEND,
        QUEUE_SUBSCRIBE,
        QUEUE_CREATE,
    ),
    # Phase E13+ gap / breadth apps
    "rpc_versioned_topic": (
        RPC_FUNCTION_ID,
        RPC_VERSION_CONSTRAINT,
        RPC_CALL,
        RPC_REGISTER,
        RPC_TOPIC_AWARE,
        RPC_FQN,
    ),
    "rpc_fqn_namespace": (
        RPC_FQN,
        RPC_NAMESPACE_DENY,
        RPC_BOUND_NAMESPACE,
        RPC_REGISTER,
        RPC_CALL,
        CLIENT_API,
    ),
    "client_auth_token": (
        CLIENT_AUTH,
        TX_SECURITY,
        CLIENT_API,
        RPC_CALL,
        MON_HEALTH,
        TX_TLS,
    ),
    "hello_queue": (
        QUEUE_CREATE,
        QUEUE_SEND,
        QUEUE_ALO,
        QUEUE_SUBSCRIBE,
    ),
    "hello_dns": (
        DISCO_DNS_REGISTER,
        DISCO_DNS_RESOLVE,
        CLIENT_DNS,
    ),
    "ops_cli_tour": (
        OPS_CLI_CALL,
        OPS_CLI_DNS,
        OPS_CLI_DOCTOR,
        OPS_CLI_CONFIG,
        OPS_CLI_EXAMPLES,
        OPS_CLI_PLANES,
        OPS_CLI_NS,
        OPS_CLI_DISCOVERY,
        OPS_MGMT_DRAIN,
        OPS_MGMT_AUDIT,
        RPC_CALL,
        DISCO_DNS_REGISTER,
        DISCO_LIST_PEERS,
        DISCO_RESOLVER_STATS,
        CACHE_RPC_SURFACE,
        QUEUE_RPC_SURFACE,
        PUBSUB_CLIENT_WIRE,
        NS_VALIDATE,
        MON_METRICS_SNAPSHOT,
    ),
    "chaos_transport": (
        CHAOS_TRANSPORT,
        CHAOS_CLOCK_SKEW,
        CHAOS_DUPLICATE,
        CHAOS_REORDER,
        CHAOS_DROP,
        CHAOS_PARTITION,
        CHAOS_CRASH,
    ),
    "rpc_deadline_budget": (
        RPC_DEADLINE,
        CLIENT_POLICY_M1,
        CLIENT_POLICY_M2,
        CLIENT_POLICY_M3,
        RPC_CALL,
    ),
    "notification_fanout": (
        PROD_NOTIFY,
        PUBSUB_EXCHANGE,
        PUBSUB_WILDCARD_STAR,
        PUBSUB_WILDCARD_HASH,
        PUBSUB_FANOUT,
        PUBSUB_HEADERS,
    ),
    "billing_ledger": (
        PROD_BILLING,
        RPC_CALL,
        RPC_REGISTER,
        CACHE_PUT_GET,
        CACHE_L1,
        QUEUE_SEND,
        QUEUE_ALO,
        QUEUE_SUBSCRIBE,
    ),
    "inventory_reserve": (
        PROD_INVENTORY,
        RPC_CALL,
        CACHE_PUT_GET,
    ),
    "topic_queue_bridge": (
        QUEUE_TOPIC_ROUTE,
        PUBSUB_EXCHANGE,
        PUBSUB_WILDCARD_HASH,
        PUBSUB_FANOUT,
        PUBSUB_HEADERS,
        QUEUE_CREATE,
        QUEUE_SEND,
        QUEUE_ALO,
        QUEUE_SUBSCRIBE,
    ),
    "multi_region_dns_policy": (
        DISCO_DNS_REGISTER,
        CLIENT_DNS,
        RPC_CALL,
        RPC_LOCS,
        FABRIC_CROSS_RPC,
        NS_VALIDATE,
        NS_STATUS,
    ),
    "topic_taxonomy_tour": (
        TOPIC_TAXONOMY,
        TOPIC_VALIDATE,
        TOPIC_GENERATE,
        TOPIC_ACCESS,
    ),
    "persistence_kv": (
        PERS_SQLITE_KV,
        PERS_MEMORY_KV,
        PERS_RESTART,
        PERS_MODE,
    ),
    "profile_settings_tour": (
        BOOT_SETTINGS,
        BOOT_PROFILE,
        BOOT_PROFILES,
        FABRIC_CLUSTER_ID,
        RPC_DEADLINE,
    ),
    "rpc_intermediate_results": (
        RPC_INTERMEDIATE,
        RPC_DAG,
        RPC_CALL,
        CLIENT_POLICY_M3,
    ),
    "routing_oracle_lab": (
        ORACLE_ROUTING,
        ORACLE_RAFT,
        FABRIC_GRAPH,
        CONS_QUORUM_TEACH,
    ),
    "deadline_hop_budget": (
        FABRIC_DEADLINE_HOP,
        RPC_DEADLINE,
    ),
    "discovery_rate_limit": (
        DISCO_RATE_LIMIT,
        DISCO_CATALOG_QUERY,
        DISCO_SUMMARY_QUERY,
    ),
    "observability_slo_trace": (
        MON_SLO,
        MON_GOLDEN_SIGNALS,
        MON_TRACE_CONTEXT,
        MON_UNIFIED,
    ),
    "topic_queue_router_lab": (
        QUEUE_TOPIC_ROUTE,
        QUEUE_FACTORIES,
        QUEUE_SEND,
        QUEUE_ALO,
    ),
    "topic_dependency_lab": (
        RPC_DEPENDENCY,
        RPC_TOPIC_AWARE,
        RPC_DAG,
    ),
    "shipping_fulfillment": (
        PROD_SHIPPING,
        RPC_CALL,
        RPC_REGISTER,
        CACHE_PUT_GET,
        CACHE_L1,
        QUEUE_SEND,
        QUEUE_ALO,
        QUEUE_SUBSCRIBE,
    ),
    "fabric_hub_hierarchy": (
        FABRIC_HUBS,
        FABRIC_GRAPH,
    ),
    "leader_election_lab": (
        CONS_LEADER_ELECTION,
        CONS_LEADER,
        CONS_QUORUM_TEACH,
    ),
    "multi_pop_edge_mesh": (
        PROD_EDGE,
        FABRIC_PERMISSIVE,
        FABRIC_CROSS_RPC,
        RPC_CALL,
        RPC_LOCS,
        MON_TIMELINE,
        MON_EVENTS,
        MON_UNIFIED,
        MON_TRACE_CONTEXT,
    ),
    "tls_dev_handshake": (
        TX_TLS,
        TX_SECURITY,
        CLIENT_API,
        RPC_CALL,
        RPC_REGISTER,
    ),
    "discovery_resolver_audit": (
        DISCO_ACCESS_AUDIT,
        DISCO_RESOLVER_STATS,
        DISCO_RESOLVER_RESYNC,
        DISCO_SIGNATURES,
        DISCO_CATALOG_QUERY,
        CLIENT_API,
    ),
    "queue_federation_lab": (FABRIC_QUEUE_FED,),
    "live_partition_chaos": (
        CHAOS_PARTITION,
        CHAOS_HEAL,
        CHAOS_TRANSPORT,
        CHAOS_LIVE_DRAIN,
        OPS_MGMT_DRAIN,
        OPS_MGMT_DETACH,
        OPS_MGMT_AUDIT,
        MON_HEALTH,
        RPC_CALL,
        DISCO_LIST_PEERS,
    ),
    "transport_health_attach": (
        MON_TRANSPORT,
        MON_HEALTH,
        MON_UNIFIED,
        TX_SECURITY,
    ),
    "transport_protocol_tour": (
        TX_TCP,
        TX_MULTI_PROTOCOL,
        TX_SECURITY,
        MON_TRANSPORT,
    ),
    "blockchain_message_lab": (
        FABRIC_BLOCKCHAIN_MSG,
        FABRIC_GRAPH,
        FABRIC_HUBS,
    ),
    "discovery_signatures_lab": (
        DISCO_SIGNATURES,
        FABRIC_GOSSIP,
        BOOT_SETTINGS,
    ),
    "rpc_inventory_tour": (
        RPC_DESCRIBE,
        RPC_REPORT,
        RPC_LIST,
        RPC_CALL,
        CLIENT_API,
    ),
    "client_trace_bind": (
        CLIENT_TRACE,
        MON_TRACE_BIND,
        MON_TRACE_CONTEXT,
        RPC_CALL,
        CLIENT_API,
    ),
    "rpc_microbench_lab": (
        RPC_CALL,
        CLIENT_API,
        MON_METRICS_SNAPSHOT,
        MON_SERVER_TRACKER,
    ),
    "correlation_routing_lab": (
        TX_CORRELATION,
        CHAOS_NO_LOOP,
        CHAOS_TRANSPORT,
        ORACLE_ROUTING,
        MON_CORRELATION,
    ),
    "mtls_mesh_handshake": (
        TX_TLS,
        TX_SECURITY,
        RPC_CALL,
        CLIENT_API,
    ),
    "packet_loss_chaos": (
        CHAOS_DROP,
        CHAOS_TRANSPORT,
        CHAOS_LIVE_DRAIN,
        OPS_MGMT_DRAIN,
        MON_HEALTH,
        RPC_CALL,
    ),
    "blockchain_hub_settlement": (
        FABRIC_BLOCKCHAIN_MSG,
        FABRIC_HUBS,
        FABRIC_GRAPH,
        CONS_QUORUM_TEACH,
    ),
    # Phase L — FEATURE partial → shipped depth batch
    "queue_ack_receive_lab": (
        QUEUE_ACK,
        QUEUE_RECEIVE,
        QUEUE_CREATE,
        QUEUE_SEND,
        QUEUE_ALO,
        QUEUE_BROADCAST,
        QUEUE_FNF,
        QUEUE_SUBSCRIBE,
    ),
    "pubsub_client_backlog": (
        CLIENT_PUBSUB,
        PUBSUB_BACKLOG,
        PUBSUB_HEADERS,
        PUBSUB_CLIENT_WIRE,
        PUBSUB_EXCHANGE,
    ),
    "cache_replication_geo": (
        CACHE_REPLICATION,
        CACHE_GEO,
        CACHE_L2,
        CACHE_INVALIDATE,
        CACHE_PUT_GET,
        PERS_CACHE_L2,
        PERS_MODE,
    ),
    "fabric_policy_modes": (
        FABRIC_STRICT,
        FABRIC_EXPLICIT,
        FABRIC_CATALOG,
        FABRIC_LINK_STATE,
    ),
    "rpc_concurrency_lab": (
        RPC_CONCURRENCY,
        RPC_ROUTING_TOPIC,
        RPC_CALL,
        CLIENT_POLICY_M3,
        CLIENT_API,
    ),
    "cluster_map_catalog": (
        CLIENT_CLUSTER_MAP,
        CLIENT_CLUSTER,
        DISCO_CLUSTER_MAP,
        DISCO_CATALOG_QUERY,
        DISCO_SUMMARY_QUERY,
        RPC_CALL,
    ),
    "mon_logging_json": (
        MON_LOGGING,
        MON_CORRELATION,
        MON_HEALTH,
        MON_UNIFIED,
        MON_TIMELINE,
    ),
    "chaos_crash_recover": (
        CHAOS_CRASH,
        CHAOS_PARTITION,
        CHAOS_HEAL,
        CHAOS_TRANSPORT,
    ),
    "tx_circuit_breaker_lab": (
        TX_CIRCUIT_BREAKER,
        FABRIC_RESILIENCE,
    ),
    "ns_engine_direct": (NS_ENGINE,),
    # Phase M — residual CLI ops + fabric_forward
    "pubsub_fabric_forward_lab": (
        PUBSUB_FABRIC_FORWARD,
        PUBSUB_HEADERS,
    ),
}

def features_for(app_id: str) -> tuple[str, ...]:
    return APP_FEATURES.get(app_id, ())

def all_feature_ids() -> frozenset[str]:
    out: set[str] = set()
    for feats in APP_FEATURES.values():
        out.update(feats)
    return frozenset(out)
