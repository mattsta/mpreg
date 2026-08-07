"""T22 residual closeout: shared-audit capability honesty + doctor parity."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from mpreg.cli.main import (
    evaluate_shared_audit_doctor_payload,
    evaluate_strong_doctor_payload,
)
from mpreg.server_pkg.monitoring_metrics import build_shared_audit_metrics
from mpreg.server_pkg.openapi_surface import (
    build_monitoring_openapi,
    openapi_path_set,
    route_table_path_set,
)
from mpreg.testing.distlab.registry import SUITE_PRESETS, resolve_preset

def test_t22_shared_audit_metrics_capabilities_honesty() -> None:
    """build_shared_audit_metrics always advertises honest capability flags."""
    store = MagicMock()
    store.size.return_value = 3
    rep = MagicMock()
    rep.health.return_value = SimpleNamespace(
        to_dict=lambda: {"peers_known": 2}
    )
    server = SimpleNamespace(
        settings=SimpleNamespace(
            mgmt_audit_shared_enabled=True,
            mgmt_audit_shared_reconcile_interval_s=1.0,
            mgmt_audit_shared_gossip_targets=3,
        ),
        _shared_audit_store=store,
        _shared_audit_replicator=rep,
    )
    m = build_shared_audit_metrics(server)
    caps = m["capabilities"]
    assert caps["gset_epidemic"] is True
    assert caps["siem"] is False
    assert caps["bft"] is False
    assert caps["infinite_retention"] is False
    assert caps["linearizable_cluster_ops"] is False
    assert caps["multi_tenant_beyond_cluster_id"] is False
    assert m["status"] == "ok"
    assert m["store_size"] == 3

    off = build_shared_audit_metrics(
        SimpleNamespace(
            settings=SimpleNamespace(
                mgmt_audit_shared_enabled=False,
                mgmt_audit_shared_reconcile_interval_s=1.0,
                mgmt_audit_shared_gossip_targets=3,
            ),
            _shared_audit_store=None,
            _shared_audit_replicator=None,
        )
    )
    assert off["capabilities"]["gset_epidemic"] is False
    assert off["capabilities"]["siem"] is False
    assert off["status"] == "disabled"

def test_t22_evaluate_shared_audit_doctor_payload_honesty() -> None:
    ok, detail = evaluate_shared_audit_doctor_payload(
        {
            "shared_audit": {
                "status": "ok",
                "store_size": 4,
                "capabilities": {
                    "gset_epidemic": True,
                    "siem": False,
                    "bft": False,
                    "infinite_retention": False,
                    "linearizable_cluster_ops": False,
                    "multi_tenant_beyond_cluster_id": False,
                },
                "counters": {"deltas_recv": 2, "publish_dropped": 0},
            }
        }
    )
    assert ok is True
    assert "gset=True" in detail
    assert "siem=False" in detail
    assert "deltas_recv=2" in detail

    bad, bdetail = evaluate_shared_audit_doctor_payload(
        {
            "shared_audit": {
                "status": "ok",
                "capabilities": {"siem": True, "bft": False},
                "counters": {},
            }
        }
    )
    assert bad is False
    assert "dishonest" in bdetail
    assert "siem" in bdetail

    bad_bft, _ = evaluate_shared_audit_doctor_payload(
        {
            "shared_audit": {
                "status": "ok",
                "capabilities": {"bft": True},
                "counters": {},
            }
        }
    )
    assert bad_bft is False

    dis_ok, dis_d = evaluate_shared_audit_doctor_payload(
        {
            "shared_audit": {
                "status": "disabled",
                "capabilities": {"gset_epidemic": False, "siem": False},
                "counters": {},
                "store_size": 0,
            }
        }
    )
    assert dis_ok is True
    assert "disabled" in dis_d

    mis_ok, _ = evaluate_shared_audit_doctor_payload(
        {
            "shared_audit": {
                "status": "misconfigured",
                "capabilities": {},
                "counters": {},
            }
        }
    )
    assert mis_ok is False

    # Strong path still independent
    sok, _ = evaluate_strong_doctor_payload(
        {"strong": {"health": "ok", "capabilities": {"get_quorum": False}, "counters": {}}}
    )
    assert sok is True

def test_t22_openapi_shared_audit_schema_capabilities() -> None:
    doc = build_monitoring_openapi()
    schemas = (doc.get("components") or {}).get("schemas") or {}
    assert "SharedAuditMetricsResponse" in schemas
    audit = schemas["SharedAuditMetricsResponse"]
    body = (audit.get("properties") or {}).get("shared_audit") or {}
    caps = (body.get("properties") or {}).get("capabilities") or {}
    props = caps.get("properties") or {}
    for k in (
        "siem",
        "bft",
        "infinite_retention",
        "linearizable_cluster_ops",
        "multi_tenant_beyond_cluster_id",
    ):
        assert k in props, k
        assert props[k].get("enum") == [False], k
    assert "gset_epidemic" in props

    paths = doc.get("paths") or {}
    desc = paths["/metrics/shared-audit"]["get"]["description"]
    assert "SIEM" in desc or "siem" in desc.lower()
    assert "BFT" in desc or "bft" in desc.lower()

    tags = {t["name"]: t for t in (doc.get("tags") or []) if isinstance(t, dict)}
    assert "audit" in tags
    ad = tags["audit"]["description"].lower()
    assert "siem" in ad and "bft" in ad

def test_t22_openapi_still_matches_route_table() -> None:
    assert openapi_path_set() == route_table_path_set()

def test_t22_audit_core_preset_expanded() -> None:
    names = resolve_preset("audit-core")
    assert "audit.multi_origin" in names
    assert "audit.partition_heal" in names
    assert "audit.digest_repair" in names
    assert "audit.duplicate_idempotent" in names
    assert "audit.ineligible_local" in names
    assert len(SUITE_PRESETS["audit-core"]) >= 5
