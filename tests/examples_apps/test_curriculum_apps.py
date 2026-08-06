"""Live validation of curriculum example apps.

Each test runs the app's real async ``main()`` (same path as
``uv run mpreg-example run <id>``). These are platform proof under real
local workloads — not mock-only unit stubs.
"""

from __future__ import annotations

import pytest

from mpreg.examples.apps._shared.registry import (
    ALIASES,
    DEMO_BUNDLES,
    APPS,
    ExampleApp,
    get_app,
    list_apps,
    resolve_app_id,
)
from mpreg.examples.apps._shared.runtime import run_app_main

# Per-app ceilings (live servers + multi-plane composition).
_TIMEOUT_S: dict[str, float] = {
    "hello_rpc": 60.0,
    "hello_cluster": 90.0,
    "hello_trace": 60.0,
    "hello_pubsub": 30.0,
    "hello_cache": 30.0,
    "hello_ports": 60.0,
    "ha_client_failover": 90.0,
    "job_queue_worker": 90.0,
    "url_shortener_rpc": 90.0,
    "sensor_ingest_pubsub": 30.0,
    "session_cache": 30.0,
    "auto_port_bootstrap": 90.0,
    "plane_rpc": 90.0,
    "plane_pubsub": 30.0,
    "plane_queue": 30.0,
    "plane_cache": 60.0,
    "plane_fabric": 90.0,
    "plane_monitoring": 30.0,
    "cache_atomic_ops": 60.0,
    "namespace_policy_gate": 90.0,
    "plane_dns": 90.0,
    "unified_client_tour": 90.0,
    "pubsub_request_reply": 90.0,
    "job_queue_dlq": 90.0,
    "cache_event_bus": 60.0,
    "discovery_watch_summary": 120.0,
    "fabric_graph_resilience": 30.0,
    "rpc_versioned_topic": 90.0,
    "rpc_fqn_namespace": 90.0,
    "client_auth_token": 90.0,
    "tls_dev_handshake": 90.0,
    "live_partition_chaos": 120.0,
    "mtls_mesh_handshake": 90.0,
    "packet_loss_chaos": 120.0,
    "discovery_signatures_lab": 60.0,
    "rpc_inventory_tour": 90.0,
    "client_trace_bind": 90.0,
    "blockchain_hub_settlement": 90.0,

    "discovery_resolver_audit": 90.0,

    "client_auth_token": 90.0,
    "hello_queue": 30.0,
    "hello_dns": 90.0,
    "ops_cli_tour": 120.0,
    "chaos_transport": 30.0,
    "rpc_deadline_budget": 90.0,
    "notification_fanout": 30.0,
    "billing_ledger": 90.0,
    "inventory_reserve": 90.0,
    "topic_queue_bridge": 60.0,
    "multi_region_dns_policy": 120.0,
    "topic_taxonomy_tour": 30.0,
    "persistence_kv": 30.0,
    "profile_settings_tour": 30.0,
    "rpc_intermediate_results": 90.0,
    "routing_oracle_lab": 30.0,
    "deadline_hop_budget": 30.0,
    "discovery_rate_limit": 30.0,
    "observability_slo_trace": 30.0,
    "topic_queue_router_lab": 60.0,
    "topic_dependency_lab": 30.0,
    "shipping_fulfillment": 90.0,
    "fabric_hub_hierarchy": 60.0,
    "leader_election_lab": 30.0,
    "multi_pop_edge_mesh": 180.0,
    "order_intake": 120.0,
    "media_pipeline": 120.0,
    "feature_flag_mesh": 60.0,
    "webhook_dispatcher": 60.0,
    "config_reload_live": 120.0,
    "rpc_plus_cache": 90.0,
    "pubsub_plus_queue": 60.0,
    "cache_plus_federation": 60.0,
    "ml_inference_mesh": 120.0,
    "multi_region_shop": 180.0,
    "signed_route_border": 180.0,
    "partition_safe_counter": 30.0,
    "discovery_join": 120.0,
    "chaos_checkout": 120.0,
    "fabric_snapshot_restart": 180.0,
    "tier3_expansion": 180.0,
    "global_edge_control_plane": 180.0,
}

def _ids(apps: list[ExampleApp]) -> list[str]:
    return [a.id for a in apps]

SMOKE_IDS = _ids(list_apps(smoke_only=True))
SUITE_IDS = _ids(list_apps(suite_only=True))
ALL_IDS = [a.id for a in APPS]

@pytest.mark.example_apps
@pytest.mark.unit
def test_registry_unique_ids() -> None:
    assert len(ALL_IDS) == len(set(ALL_IDS))
    assert len(ALL_IDS) >= 30, f"expected full matrix, got {len(ALL_IDS)}"

@pytest.mark.example_apps
@pytest.mark.unit
def test_registry_smoke_subset_of_suite() -> None:
    smoke = set(SMOKE_IDS)
    suite = set(SUITE_IDS)
    assert smoke, "smoke bundle must not be empty"
    assert smoke <= suite, f"smoke apps missing from suite: {smoke - suite}"
    assert len(smoke) >= 6, f"smoke too small: {smoke}"

@pytest.mark.example_apps
@pytest.mark.unit
def test_aliases_resolve() -> None:
    assert resolve_app_id("tier1_rpc") == "plane_rpc"
    assert resolve_app_id("fabric_route_security_demo") == "signed_route_border"
    assert get_app("tier1_cache").id == "plane_cache"

@pytest.mark.example_apps
@pytest.mark.unit
def test_demo_bundles_resolve() -> None:
    for name, ids in DEMO_BUNDLES.items():
        assert ids, f"empty bundle {name}"
        for i in ids:
            get_app(i)  # must not raise

@pytest.mark.example_apps
@pytest.mark.unit
@pytest.mark.parametrize("app_id", ALL_IDS)
def test_app_module_exports_async_main(app_id: str) -> None:
    app = get_app(app_id)
    main = app.load_main()
    assert callable(main)
    assert app.path.endswith(f"{app_id}/")
    assert app.module.endswith(".run")

@pytest.mark.example_apps
@pytest.mark.example_smoke
@pytest.mark.integration
@pytest.mark.parametrize("app_id", SMOKE_IDS)
async def test_smoke_app_live(app_id: str) -> None:
    """CI-friendly smoke under real servers / in-process planes."""
    app = get_app(app_id)
    report = await run_app_main(
        app.id,
        app.load_main(),
        timeout_s=_TIMEOUT_S.get(app_id, 120.0),
    )
    assert report.ok, f"{app_id} failed ({report.duration_s:.2f}s): {report.error}"

@pytest.mark.example_apps
@pytest.mark.example_suite
@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.parametrize("app_id", SUITE_IDS)
async def test_suite_app_live(app_id: str) -> None:
    """Full curriculum suite — every shipped app's real main()."""
    app = get_app(app_id)
    report = await run_app_main(
        app.id,
        app.load_main(),
        timeout_s=_TIMEOUT_S.get(app_id, 180.0),
    )
    assert report.ok, f"{app_id} failed ({report.duration_s:.2f}s): {report.error}"

@pytest.mark.example_apps
@pytest.mark.unit
def test_every_app_has_feature_tags() -> None:
    """Feature catalog join: every shipped app lists at least one feature id."""
    from mpreg.examples.apps._shared.features import APP_FEATURES, all_feature_ids

    missing = [a.id for a in APPS if not a.features]
    assert not missing, f"apps missing features: {missing}"
    # Registry features should match APP_FEATURES map
    for app in APPS:
        mapped = APP_FEATURES.get(app.id, ())
        assert app.features == mapped, f"{app.id} features drift from APP_FEATURES"
    assert len(all_feature_ids()) >= 40, "feature catalog too thin"

@pytest.mark.example_apps
@pytest.mark.unit
def test_apps_covering_helper() -> None:
    from mpreg.examples.apps._shared.registry import apps_covering

    rpc_apps = apps_covering("rpc.call")
    assert any(a.id == "hello_rpc" for a in rpc_apps)
    assert apps_covering("this.feature.does.not.exist") == []
