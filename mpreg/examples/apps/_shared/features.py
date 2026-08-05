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

CLIENT_API: Final = "client.api"
CLIENT_UNIFIED: Final = "client.unified"
CLIENT_CLUSTER: Final = "client.cluster"
CLIENT_CLUSTER_MAP: Final = "client.cluster_map"
CLIENT_POLICY_M1: Final = "client.policy.m1"
CLIENT_POLICY_M2: Final = "client.policy.m2"
CLIENT_POLICY_M3: Final = "client.policy.m3"
CLIENT_DEFAULT_HA: Final = "client.default_ha"
CLIENT_TRACE: Final = "client.trace"

PUBSUB_EXCHANGE: Final = "pubsub.exchange"
PUBSUB_WILDCARD_STAR: Final = "pubsub.wildcard_star"
PUBSUB_WILDCARD_HASH: Final = "pubsub.wildcard_hash"
PUBSUB_FANOUT: Final = "pubsub.fanout"
PUBSUB_HEADERS: Final = "pubsub.headers"

QUEUE_CREATE: Final = "queue.create"
QUEUE_SEND: Final = "queue.send"
QUEUE_ALO: Final = "queue.alo"
QUEUE_QUORUM: Final = "queue.quorum"
QUEUE_BROADCAST: Final = "queue.broadcast"
QUEUE_FNF: Final = "queue.fnf"
QUEUE_SUBSCRIBE: Final = "queue.subscribe"
QUEUE_TOPIC_ROUTE: Final = "queue.topic_route"
QUEUE_FACTORIES: Final = "queue.factories"

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

DISCO_LIST_PEERS: Final = "disco.list_peers"
DISCO_CLUSTER_MAP: Final = "disco.cluster_map"
DISCO_CATALOG_QUERY: Final = "disco.catalog_query"
DISCO_JOIN: Final = "disco.join"

MON_UNIFIED: Final = "mon.unified"
MON_EVENTS: Final = "mon.events"
MON_TIMELINE: Final = "mon.timeline"
MON_CORRELATION: Final = "mon.correlation"
MON_SYSTEM_TYPES: Final = "mon.system_types"
MON_HEALTH: Final = "mon.health"

CONS_QUORUM_TEACH: Final = "cons.quorum_teach"
CHAOS_PARTITION: Final = "chaos.partition"
CHAOS_HEAL: Final = "chaos.heal"
CHAOS_CRASH: Final = "chaos.crash"

PERS_SQLITE_KV: Final = "pers.sqlite_kv"
PERS_SQLITE_QUEUE: Final = "pers.sqlite_queue"
PERS_CACHE_L2: Final = "pers.cache_l2"
PERS_FABRIC_SNAP: Final = "pers.fabric_snap"
PERS_RESTART: Final = "pers.restart"

BOOT_PORT_RANGE: Final = "boot.port_range"
BOOT_AUTO_PORT: Final = "boot.auto_port"
BOOT_SETTINGS: Final = "boot.settings"
BOOT_PEERS: Final = "boot.peers"
BOOT_RESOURCES: Final = "boot.resources"

PROD_ORDER: Final = "prod.order"
PROD_MEDIA: Final = "prod.media"
PROD_FLAGS: Final = "prod.flags"
PROD_WEBHOOKS: Final = "prod.webhooks"
PROD_ML: Final = "prod.ml"
PROD_SHOP: Final = "prod.shop"
PROD_EDGE: Final = "prod.edge"
PROD_TIER3: Final = "prod.tier3"

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
    "hello_ports": (BOOT_PORT_RANGE, BOOT_AUTO_PORT, RPC_CALL, RPC_REGISTER),
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
}

def features_for(app_id: str) -> tuple[str, ...]:
    return APP_FEATURES.get(app_id, ())

def all_feature_ids() -> frozenset[str]:
    out: set[str] = set()
    for feats in APP_FEATURES.values():
        out.update(feats)
    return frozenset(out)
