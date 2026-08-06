"""Registry of curriculum example apps (product + capability planes + unified legacy)."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from importlib import import_module
from typing import Any, Awaitable

from mpreg.examples.apps._shared.features import features_for

class AppLevel(str, Enum):
    L0 = "L0"
    L1 = "L1"
    L2 = "L2"
    L3 = "L3"
    L4 = "L4"

@dataclass(frozen=True, slots=True)
class ExampleApp:
    """Metadata for one curriculum app."""

    id: str
    title: str
    level: AppLevel
    module: str
    summary: str
    systems: tuple[str, ...]
    smoke: bool = False
    suite: bool = True
    path: str = ""
    kind: str = "product"  # product | plane | integration | legacy
    features: tuple[str, ...] = ()

    def load_main(self) -> Callable[[], Awaitable[None]]:
        mod = import_module(self.module)
        main = getattr(mod, "main", None)
        if main is None or not callable(main):
            raise RuntimeError(
                f"App {self.id!r} module {self.module!r} has no async main()"
            )
        return main  # type: ignore[return-value]

_LEVEL_DIR = {
    AppLevel.L0: "00_getting_started",
    AppLevel.L1: "01_simple",
    AppLevel.L2: "02_moderate",
    AppLevel.L3: "03_complex",
    AppLevel.L4: "04_world",
}

def _app(
    id: str,
    title: str,
    level: AppLevel,
    summary: str,
    systems: Sequence[str],
    *,
    smoke: bool = False,
    suite: bool = True,
    kind: str = "product",
    features: Sequence[str] | None = None,
) -> ExampleApp:
    level_dir = _LEVEL_DIR[level]
    module = f"mpreg.examples.apps.{level_dir}.{id}.run"
    path = f"mpreg/examples/apps/{level_dir}/{id}/"
    feat = tuple(features) if features is not None else features_for(id)
    return ExampleApp(
        id=id,
        title=title,
        level=level,
        module=module,
        summary=summary,
        systems=tuple(systems),
        smoke=smoke,
        suite=suite,
        path=path,
        kind=kind,
        features=feat,
    )

# ── Full matrix ─────────────────────────────────────────────────────────────
APPS: tuple[ExampleApp, ...] = (
    # L0 — getting started
    _app(
        "hello_rpc",
        "Hello RPC",
        AppLevel.L0,
        "Register functions and run a dependency-style RPC chain.",
        ("rpc",),
        smoke=True,
    ),
    _app(
        "hello_cluster",
        "Hello Cluster",
        AppLevel.L0,
        "Two peers, resource locs, cross-node DAG.",
        ("rpc", "cluster"),
        smoke=True,
    ),
    _app(
        "hello_trace",
        "Hello Trace",
        AppLevel.L0,
        "In-process correlation timeline via unified monitoring.",
        ("monitoring",),
        smoke=True,
    ),
    _app(
        "hello_pubsub",
        "Hello PubSub",
        AppLevel.L0,
        "Topic wildcards and multi-subscriber fan-out.",
        ("pubsub",),
        smoke=True,
    ),
    _app(
        "hello_cache",
        "Hello Cache",
        AppLevel.L0,
        "GlobalCacheManager put/get happy path.",
        ("cache",),
        smoke=True,
    ),
    _app(
        "hello_ports",
        "Hello Ports",
        AppLevel.L0,
        "Dynamic port allocation without fixed numbers.",
        ("rpc", "ports"),
        smoke=True,
    ),
    _app(
        "hello_queue",
        "Hello Queue",
        AppLevel.L0,
        "Smallest durable queue send + subscribe hello.",
        ("queue",),
    ),
    _app(
        "hello_dns",
        "Hello DNS",
        AppLevel.L0,
        "Minimal DNS register + list + SRV resolve.",
        ("dns", "discovery"),
    ),
    # L1 — simple product + planes
    _app(
        "ha_client_failover",
        "HA Client Failover",
        AppLevel.L1,
        "Multi-seed MPREGClusterClient call path.",
        ("rpc", "ha-client"),
        smoke=True,
    ),
    _app(
        "job_queue_worker",
        "Job Queue Worker",
        AppLevel.L1,
        "At-least-once and quorum queue deliveries.",
        ("queue",),
        smoke=True,
    ),
    _app(
        "url_shortener_rpc",
        "URL Shortener RPC",
        AppLevel.L1,
        "Tiny CRUD-ish shorten/resolve with cache mirror.",
        ("rpc", "cache"),
    ),
    _app(
        "sensor_ingest_pubsub",
        "Sensor Ingest PubSub",
        AppLevel.L1,
        "Multi-pattern sensor topic fan-out.",
        ("pubsub",),
    ),
    _app(
        "session_cache",
        "Session Cache",
        AppLevel.L1,
        "TTL-aware session put/get/rotate.",
        ("cache",),
    ),
    _app(
        "auto_port_bootstrap",
        "Auto Port Bootstrap",
        AppLevel.L1,
        "OS-assigned ports + peer join without fixed numbers.",
        ("rpc", "cluster", "ports"),
        kind="legacy",
    ),
    _app(
        "plane_rpc",
        "Plane: RPC",
        AppLevel.L1,
        "Full RPC plane capability tour (legacy tier1 rpc).",
        ("rpc",),
        kind="plane",
        suite=True,
    ),
    _app(
        "plane_pubsub",
        "Plane: PubSub",
        AppLevel.L1,
        "Full pubsub plane capability tour (legacy tier1 pubsub).",
        ("pubsub",),
        kind="plane",
    ),
    _app(
        "plane_queue",
        "Plane: Queue",
        AppLevel.L1,
        "Full queue plane capability tour (legacy tier1 queue).",
        ("queue",),
        kind="plane",
    ),
    _app(
        "plane_cache",
        "Plane: Cache",
        AppLevel.L1,
        "Full cache plane capability tour (legacy tier1 cache).",
        ("cache",),
        kind="plane",
    ),
    _app(
        "plane_fabric",
        "Plane: Fabric",
        AppLevel.L1,
        "Full fabric plane capability tour (legacy tier1 fabric).",
        ("fabric",),
        kind="plane",
    ),
    _app(
        "plane_monitoring",
        "Plane: Monitoring",
        AppLevel.L1,
        "Full monitoring plane capability tour (legacy tier1 monitoring).",
        ("monitoring",),
        kind="plane",
    ),
    _app(
        "cache_atomic_ops",
        "Cache Atomic Ops",
        AppLevel.L1,
        "CAS, increment, structures, and namespace bulk cache ops.",
        ("cache",),
        kind="plane",
    ),
    _app(
        "namespace_policy_gate",
        "Namespace Policy Gate",
        AppLevel.L1,
        "Validate/apply/status/export/audit namespace policy via client API.",
        ("namespace", "discovery"),
        kind="plane",
    ),
    _app(
        "plane_dns",
        "Plane: DNS",
        AppLevel.L1,
        "DNS register/list/describe plus UDP/TCP resolve gateway tour.",
        ("dns", "discovery"),
        kind="plane",
    ),
    _app(
        "unified_client_tour",
        "Unified Client Tour",
        AppLevel.L1,
        "MPREGClient four-plane façade: RPC + cache + queue on one session.",
        ("rpc", "cache", "queue", "client"),
    ),
    _app(
        "pubsub_request_reply",
        "PubSub Request/Reply",
        AppLevel.L1,
        "publish_with_reply request/response over topics.",
        ("pubsub",),
    ),
    _app(
        "job_queue_dlq",
        "Job Queue DLQ",
        AppLevel.L1,
        "Poison message retries then dead-letter queue.",
        ("queue",),
    ),
    _app(
        "rpc_versioned_topic",
        "RPC Versioned Topic",
        AppLevel.L1,
        "function_id + version_constraint routing across nodes.",
        ("rpc",),
    ),
    _app(
        "rpc_fqn_namespace",
        "RPC FQN Namespace",
        AppLevel.L1,
        "Bare→FQN qualify, mpreg.* namespace deny, bound_rpc_namespace.",
        ("rpc",),
    ),
    _app(
        "client_auth_token",
        "Client Auth Token",
        AppLevel.L1,
        "Monitoring bearer + WS rpc_auth_token enforcement (F11).",
        ("client", "security", "monitoring"),
    ),
    _app(
        "tls_dev_handshake",
        "TLS Dev Handshake",
        AppLevel.L1,
        "generate_dev_tls_material + wss:// RPC (F12).",
        ("security", "transport", "rpc"),
    ),
    _app(
        "discovery_resolver_audit",
        "Discovery Resolver + Audit",
        AppLevel.L1,
        "resolver_cache_stats, resync, discovery_access_audit.",
        ("discovery",),
        kind="plane",
    ),
    _app(
        "transport_health_attach",
        "Transport Health Attach",
        AppLevel.L1,
        "TransportHealthAggregator + mon.attach_transport_adapter.",
        ("monitoring", "transport"),
        kind="plane",
    ),
    _app(
        "transport_protocol_tour",
        "Transport Protocol Tour",
        AppLevel.L1,
        "TCP framing constants + EnhancedMultiProtocolAdapter.",
        ("transport",),
        kind="plane",
    ),

    _app(
        "discovery_signatures_lab",
        "Discovery Signatures Lab",
        AppLevel.L1,
        "HMAC sign/verify for discovery summaries + gossip envelopes.",
        ("discovery", "security"),
        kind="plane",
    ),
    _app(
        "rpc_inventory_tour",
        "RPC Inventory Tour",
        AppLevel.L1,
        "rpc_describe + rpc_report inventory surfaces.",
        ("rpc",),
        kind="plane",
    ),
    _app(
        "client_trace_bind",
        "Client Trace Bind",
        AppLevel.L1,
        "last_trace_context + bind_trace_context / trace_context.",
        ("client", "monitoring"),
        kind="plane",
    ),
    _app(
        "correlation_routing_lab",
        "Correlation Routing Lab",
        AppLevel.L1,
        "CorrelationTracker + assert_no_routing_loop.",
        ("transport", "chaos"),
        kind="plane",
    ),

    _app(
        "topic_taxonomy_tour",
        "Topic Taxonomy Tour",
        AppLevel.L1,
        "TopicValidator + template engine + taxonomy constants.",
        ("pubsub", "taxonomy"),
        kind="plane",
    ),
    _app(
        "persistence_kv",
        "Persistence KV",
        AppLevel.L1,
        "Memory + SQLite key/value put/get/list/TTL.",
        ("persistence",),
        kind="plane",
    ),
    _app(
        "profile_settings_tour",
        "Profile Settings Tour",
        AppLevel.L1,
        "Load packaged TOML profiles via MPREGSettings.from_path.",
        ("boot",),
    ),
    # L2 — moderate composition
    _app(
        "order_intake",
        "Order Intake",
        AppLevel.L2,
        "Compose RPC + cache idempotency + pubsub + fulfill queue.",
        ("rpc", "cache", "pubsub", "queue"),
    ),
    _app(
        "media_pipeline",
        "Media Pipeline",
        AppLevel.L2,
        "Multi-stage ETL-style RPC across specialized nodes.",
        ("rpc", "cluster"),
    ),
    _app(
        "feature_flag_mesh",
        "Feature Flag Mesh",
        AppLevel.L2,
        "Flags in cache with federated L4 visibility.",
        ("cache", "fabric"),
    ),
    _app(
        "webhook_dispatcher",
        "Webhook Dispatcher",
        AppLevel.L2,
        "Pubsub events bridged to durable egress queue.",
        ("pubsub", "queue"),
    ),
    _app(
        "config_reload_live",
        "Config Reload Live",
        AppLevel.L2,
        "Cache+queue survive process restart via SQLite persistence.",
        ("cache", "queue", "persistence"),
        kind="legacy",
    ),
    _app(
        "rpc_plus_cache",
        "RPC + Cache",
        AppLevel.L2,
        "RPC output cached and reused (legacy tier2).",
        ("rpc", "cache"),
        kind="integration",
    ),
    _app(
        "pubsub_plus_queue",
        "PubSub + Queue",
        AppLevel.L2,
        "Topic fan-out feeds durable queue (legacy tier2).",
        ("pubsub", "queue"),
        kind="integration",
    ),
    _app(
        "cache_plus_federation",
        "Cache + Federation",
        AppLevel.L2,
        "Cache L4 federation between nodes (legacy tier2).",
        ("cache", "fabric"),
        kind="integration",
    ),
    _app(
        "ml_inference_mesh",
        "ML Inference Mesh",
        AppLevel.L2,
        "Router + vision/NLP specialized inference workers.",
        ("rpc", "cluster"),
    ),
    _app(
        "cache_event_bus",
        "Cache Event Bus",
        AppLevel.L2,
        "CachePubSubIntegration: cache ops fan out as topic events.",
        ("cache", "pubsub"),
        kind="integration",
    ),
    _app(
        "ops_cli_tour",
        "Ops CLI Tour",
        AppLevel.L2,
        "mpreg CLI: client call, dns-*, doctor friction, examples, config-check.",
        ("ops", "rpc", "dns"),
        kind="legacy",
    ),
    _app(
        "notification_fanout",
        "Notification Fanout",
        AppLevel.L2,
        "Multi-subscriber email/push/audit topic wildcards.",
        ("pubsub",),
    ),
    _app(
        "billing_ledger",
        "Billing Ledger",
        AppLevel.L2,
        "Idempotent charge RPC + balance cache + settlement queue.",
        ("rpc", "cache", "queue"),
    ),
    _app(
        "inventory_reserve",
        "Inventory Reserve",
        AppLevel.L2,
        "Stock reserve/release RPC with cache snapshot.",
        ("rpc", "cache"),
    ),
    _app(
        "topic_queue_bridge",
        "Topic→Queue Bridge",
        AppLevel.L2,
        "TopicExchange hits become durable queue work.",
        ("pubsub", "queue"),
        kind="integration",
    ),
    _app(
        "rpc_intermediate_results",
        "RPC Intermediate Results",
        AppLevel.L2,
        "Per-level IntermediateResultCollector + live DAG.",
        ("rpc",),
    ),
    _app(
        "discovery_rate_limit",
        "Discovery Rate Limit",
        AppLevel.L1,
        "DiscoveryRateLimiter sliding-window allow/deny per viewer/command.",
        ("discovery",),
        kind="plane",
    ),
    _app(
        "observability_slo_trace",
        "Observability SLO + Trace",
        AppLevel.L1,
        "Golden signals catalog + W3C traceparent inject/bind helpers.",
        ("monitoring", "observability"),
        kind="plane",
    ),
    _app(
        "topic_queue_router_lab",
        "Topic Queue Router Lab",
        AppLevel.L2,
        "TopicQueueRouter pattern fanout + routing strategies.",
        ("pubsub", "queue"),
        kind="integration",
    ),
    _app(
        "topic_dependency_lab",
        "Topic Dependency Lab",
        AppLevel.L2,
        "TopicDependencyResolver graph analysis + ready command set.",
        ("rpc", "pubsub"),
        kind="plane",
    ),
    _app(
        "shipping_fulfillment",
        "Shipping Fulfillment",
        AppLevel.L2,
        "Label RPC + tracking cache + carrier dispatch queue.",
        ("rpc", "cache", "queue"),
    ),

    _app(
        "queue_federation_lab",
        "Queue Federation Lab",
        AppLevel.L2,
        "Queue federation wire types + manager surface.",
        ("queue", "fabric"),
        kind="plane",
    ),
    _app(
        "blockchain_message_lab",
        "Blockchain Message Lab",
        AppLevel.L2,
        "BlockchainMessage + MessageRoute types.",
        ("fabric",),
        kind="plane",
    ),

    _app(
        "mtls_mesh_handshake",
        "mTLS Mesh Handshake",
        AppLevel.L2,
        "CERT_REQUIRED mTLS wss with client cert PEMs.",
        ("security", "transport", "rpc"),
    ),
    _app(
        "blockchain_hub_settlement",
        "Blockchain Hub Settlement",
        AppLevel.L2,
        "HubMessageQueue federation message + settlement routes.",
        ("fabric",),
        kind="plane",
    ),

    # L3 — complex mesh
    _app(
        "multi_region_shop",
        "Multi-Region Shop",
        AppLevel.L3,
        "Two-cluster fabric RPC with permissive bridging.",
        ("rpc", "fabric", "multi-cluster"),
    ),
    _app(
        "signed_route_border",
        "Signed Route Border",
        AppLevel.L3,
        "Signed routes, neighbor policy tags, key rotation.",
        ("fabric", "security"),
        kind="legacy",
    ),
    _app(
        "partition_safe_counter",
        "Partition-Safe Counter",
        AppLevel.L3,
        "FaultInjector majority vs minority quorum teaching.",
        ("consensus", "chaos"),
    ),
    _app(
        "discovery_join",
        "Discovery Join",
        AppLevel.L3,
        "Third node joins two-node cluster; peers + RPC visible.",
        ("rpc", "cluster", "discovery"),
    ),
    _app(
        "chaos_checkout",
        "Chaos Checkout",
        AppLevel.L3,
        "Checkout deadlines + FaultInjector fail-closed partition model.",
        ("rpc", "chaos"),
    ),
    _app(
        "fabric_snapshot_restart",
        "Fabric Snapshot Restart",
        AppLevel.L3,
        "Fabric catalog/route keys across restart (legacy demo).",
        ("fabric", "persistence"),
        kind="legacy",
    ),
    _app(
        "tier3_expansion",
        "Tier3 Expansion",
        AppLevel.L3,
        "Full multi-system expansion sketch (legacy tier3).",
        ("rpc", "cache", "pubsub", "queue", "fabric", "monitoring"),
        kind="legacy",
    ),
    _app(
        "discovery_watch_summary",
        "Discovery Watch + Summary",
        AppLevel.L3,
        "catalog_watch deltas plus summary_query/watch topic shapes.",
        ("discovery", "pubsub"),
    ),
    _app(
        "fabric_graph_resilience",
        "Fabric Graph + Resilience",
        AppLevel.L3,
        "FederationGraph Dijkstra paths and circuit-breaker recovery.",
        ("fabric",),
        kind="plane",
    ),
    _app(
        "chaos_transport",
        "Chaos Transport",
        AppLevel.L3,
        "FaultInjector clock skew, duplicate, reorder, plane drops.",
        ("chaos",),
        kind="plane",
    ),
    _app(
        "live_partition_chaos",
        "Live Partition Chaos",
        AppLevel.L3,
        "Live /mgmt drain+detach + /ready admission (F10).",
        ("chaos", "monitoring", "ops"),
        kind="plane",
    ),

    _app(
        "packet_loss_chaos",
        "Packet Loss Chaos",
        AppLevel.L3,
        "Plane drop rates + live drain admission compose.",
        ("chaos", "monitoring"),
        kind="plane",
    ),

    _app(
        "rpc_deadline_budget",
        "RPC Deadline Budget",
        AppLevel.L3,
        "M1 vs M2/M3 shared wall-clock deadline policies.",
        ("rpc", "client"),
    ),
    _app(
        "multi_region_dns_policy",
        "Multi-Region DNS + Policy",
        AppLevel.L3,
        "US/EU DNS namespaces + regional RPC; honest cross-cluster claims.",
        ("dns", "rpc", "fabric", "multi-cluster"),
    ),
    _app(
        "routing_oracle_lab",
        "Routing Oracle Lab",
        AppLevel.L3,
        "RoutingOracle BFS next hops + RaftOracle single-leader safety.",
        ("fabric", "consensus", "chaos"),
        kind="plane",
    ),
    _app(
        "deadline_hop_budget",
        "Deadline Hop Budget",
        AppLevel.L3,
        "DeadlineBudget header stamp + per-hop decrement.",
        ("rpc", "fabric"),
        kind="plane",
    ),
    _app(
        "fabric_hub_hierarchy",
        "Fabric Hub Hierarchy",
        AppLevel.L3,
        "Global/Regional/Local hub topology (library track).",
        ("fabric",),
        kind="plane",
    ),
    _app(
        "leader_election_lab",
        "Leader Election Lab",
        AppLevel.L3,
        "MetricBased + QuorumBased leader election fitness model.",
        ("consensus",),
        kind="plane",
    ),
    # L4 — world
    _app(
        "global_edge_control_plane",
        "Global Edge Control Plane",
        AppLevel.L4,
        "Hub + US/EU edge POPs with federated RPC and correlation timeline.",
        ("rpc", "fabric", "monitoring", "multi-cluster"),
    ),
    _app(
        "multi_pop_edge_mesh",
        "Multi-POP Edge Mesh",
        AppLevel.L4,
        "Hub + US/EU/AP edge POPs — second world tour with geo route plan.",
        ("rpc", "fabric", "monitoring", "multi-cluster"),
    ),
)

_BY_ID: dict[str, ExampleApp] = {a.id: a for a in APPS}

# Aliases so legacy CLI names resolve into the unified registry.
ALIASES: dict[str, str] = {
    # tier1 systems → plane_*
    "tier1_rpc": "plane_rpc",
    "tier1_pubsub": "plane_pubsub",
    "tier1_queue": "plane_queue",
    "tier1_cache": "plane_cache",
    "tier1_fabric": "plane_fabric",
    "tier1_federation": "plane_fabric",
    "tier1_monitoring": "plane_monitoring",
    # tier2
    "tier2": "rpc_plus_cache",  # first of bundle; use tier2_all for full
    "tier2_rpc_cache": "rpc_plus_cache",
    "tier2_pubsub_queue": "pubsub_plus_queue",
    "tier2_cache_federation": "cache_plus_federation",
    # tier3 / misc legacy filenames
    "tier3": "tier3_expansion",
    "quick_demo": "hello_rpc",
    "simple_working_demo": "plane_rpc",
    "auto_port_cluster_bootstrap": "auto_port_bootstrap",
    "persistence_restart_demo": "config_reload_live",
    "fabric_route_security_demo": "signed_route_border",
    "fabric_snapshot_restart_demo": "fabric_snapshot_restart",
}

def resolve_app_id(app_id: str) -> str:
    """Resolve aliases to canonical app ids."""
    return ALIASES.get(app_id, app_id)

def get_app(app_id: str) -> ExampleApp:
    canonical = resolve_app_id(app_id)
    try:
        return _BY_ID[canonical]
    except KeyError as exc:
        known = ", ".join(sorted(_BY_ID))
        raise KeyError(
            f"Unknown app {app_id!r} (resolved {canonical!r}). Known: {known}"
        ) from exc

def list_apps(
    *,
    level: AppLevel | str | None = None,
    smoke_only: bool = False,
    suite_only: bool = False,
    kind: str | None = None,
) -> list[ExampleApp]:
    items = list(APPS)
    if level is not None:
        lv = AppLevel(level) if not isinstance(level, AppLevel) else level
        items = [a for a in items if a.level == lv]
    if smoke_only:
        items = [a for a in items if a.smoke]
    if suite_only:
        items = [a for a in items if a.suite]
    if kind is not None:
        items = [a for a in items if a.kind == kind]
    return items

def app_to_dict(app: ExampleApp) -> dict[str, Any]:
    return {
        "id": app.id,
        "title": app.title,
        "level": app.level.value,
        "summary": app.summary,
        "systems": list(app.systems),
        "smoke": app.smoke,
        "suite": app.suite,
        "path": app.path,
        "module": app.module,
        "kind": app.kind,
        "features": list(app.features),
    }

def apps_covering(feature_id: str) -> list[ExampleApp]:
    """Return apps that list ``feature_id`` in their catalog tags."""
    return [a for a in APPS if feature_id in a.features]

# Bundles for unified demo CLI
DEMO_BUNDLES: dict[str, tuple[str, ...]] = {
    "tier1": (
        "plane_rpc",
        "plane_pubsub",
        "plane_queue",
        "plane_cache",
        "plane_fabric",
        "plane_monitoring",
    ),
    "tier2": ("rpc_plus_cache", "pubsub_plus_queue", "cache_plus_federation"),
    "tier3": ("tier3_expansion",),
    "quick": ("hello_rpc", "plane_rpc"),
    "all_planes": (
        "plane_rpc",
        "plane_pubsub",
        "plane_queue",
        "plane_cache",
        "plane_fabric",
        "plane_monitoring",
        "plane_dns",
        "cache_atomic_ops",
        "namespace_policy_gate",
    ),
    "product_vertical": (
        "hello_rpc",
        "hello_cluster",
        "hello_trace",
        "ha_client_failover",
        "job_queue_worker",
        "order_intake",
        "multi_region_shop",
        "global_edge_control_plane",
    ),
}
