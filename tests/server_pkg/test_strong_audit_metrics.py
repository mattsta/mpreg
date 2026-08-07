"""Unit tests for STRONG + shared-audit monitoring metric builders."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from mpreg.server_pkg.monitoring_metrics import (
    build_shared_audit_metrics,
    build_strong_metrics,
)


def test_build_strong_metrics_disabled() -> None:
    server = SimpleNamespace(
        settings=SimpleNamespace(
            cache_strong_enabled=False,
            cache_strong_replica_factor=3,
            cache_strong_min_replicas=3,
            cache_strong_prepare_timeout_s=1.0,
            cache_strong_commit_timeout_s=1.0,
            cache_strong_pending_ttl_s=30.0,
        ),
        _cache_manager=None,
        _strong_local_backend=None,
        _strong_pending_purge_task=None,
    )
    m = build_strong_metrics(server)
    assert m["enabled_flag"] is False
    assert m["health"] == "disabled"
    assert m["counters"] == {}


def test_build_strong_metrics_from_gcm_snapshot() -> None:
    cm = MagicMock()
    cm.strong_metrics_snapshot.return_value = {
        "enabled": True,
        "pending_count": 2,
        "counters": {
            "puts_ok": 5,
            "puts_fail": 1,
            "gets_refused": 2,
            "deletes_refused": 1,
        },
        "latency_ms": {"sample_count": 3, "p99_ms": 12.0, "p50_ms": 4.0},
        "coordinator": {"origin_id": "n0"},
    }
    cm.strong_status.return_value = {
        "enabled": True,
        "capabilities": {
            "put_majority_commit": True,
            "get_quorum": False,
            "delete_quorum": False,
            "local_ryw_after_put": True,
        },
    }
    server = SimpleNamespace(
        settings=SimpleNamespace(
            cache_strong_enabled=True,
            cache_strong_replica_factor=3,
            cache_strong_min_replicas=3,
            cache_strong_prepare_timeout_s=1.0,
            cache_strong_commit_timeout_s=1.0,
            cache_strong_pending_ttl_s=30.0,
        ),
        _cache_manager=cm,
        _strong_local_backend=None,
        _strong_pending_purge_task=object(),
    )
    m = build_strong_metrics(server)
    assert m["coordinator_bound"] is True
    assert m["pending_count"] == 2
    assert m["counters"]["puts_ok"] == 5
    assert m["counters"]["gets_refused"] == 2
    assert m["counters"]["deletes_refused"] == 1
    assert m["latency_ms"]["p99_ms"] == 12.0
    assert m["health"] == "ok"
    caps = m.get("capabilities") or {}
    assert caps.get("get_quorum") is False
    assert caps.get("delete_quorum") is False
    assert caps.get("put_majority_commit") is True
    # T98: always present (0 when no residual candidates)
    assert "abort_fail_peer_count" in m
    assert int(m.get("abort_fail_peer_count") or 0) == 0
    assert "residual_ops_hint" in m
    assert m.get("residual_ops_hint") == "" or isinstance(
        m.get("residual_ops_hint"), str
    )


def test_build_strong_metrics_abort_fail_peer_count() -> None:
    """T98: residual candidates surface count + non-empty residual_ops_hint."""
    cm = MagicMock()
    cm.strong_metrics_snapshot.return_value = {
        "enabled": True,
        "pending_count": 0,
        "visible_count": 1,
        "backups_count": 0,
        "backups_pruned_total": 0,
        "counters": {"puts_ok": 0, "aborts_peer_fail": 1},
        "latency_ms": {},
        "coordinator": {
            "last_abort_fail_peers": ["n1", "n2"],
            "last_abort_fail_op_id": "oid-m",
            "recent_abort_fails": [
                {"op_id": "oid-m", "key": "ns/k", "peers": ["n1", "n2"]}
            ],
        },
    }
    cm.strong_status.return_value = {
        "enabled": True,
        "capabilities": {
            "put_majority_commit": True,
            "get_quorum": False,
            "delete_quorum": False,
            "local_ryw_after_put": True,
            "cft_only": True,
            "abort_best_effort": True,
            "pending_ttl_clears_residual_l1": False,
            "retry_abort_ops_driven": True,
        },
        "last_abort_fail_peers": ["n1", "n2"],
        "last_abort_fail_op_id": "oid-m",
        "recent_abort_fails": [
            {"op_id": "oid-m", "key": "ns/k", "peers": ["n1", "n2"]}
        ],
        "abort_fail_peer_count": 2,
        "residual_ops_hint": "",
    }
    server = SimpleNamespace(
        settings=SimpleNamespace(
            cache_strong_enabled=True,
            cache_strong_replica_factor=3,
            cache_strong_min_replicas=3,
            cache_strong_prepare_timeout_s=1.0,
            cache_strong_commit_timeout_s=1.0,
            cache_strong_pending_ttl_s=30.0,
        ),
        _cache_manager=cm,
        _strong_local_backend=None,
        _strong_pending_purge_task=object(),
    )
    m = build_strong_metrics(server)
    assert int(m.get("abort_fail_peer_count") or 0) == 2
    assert "n1" in list(m.get("last_abort_fail_peers") or [])
    hint = m.get("residual_ops_hint") or ""
    assert "cache-strong-retry-abort" in hint
    assert "not auto-heal" in hint


def test_build_shared_audit_metrics_disabled() -> None:
    server = SimpleNamespace(
        settings=SimpleNamespace(
            mgmt_audit_shared_enabled=False,
            mgmt_audit_shared_reconcile_interval_s=1.0,
            mgmt_audit_shared_gossip_targets=3,
        ),
        _shared_audit_store=None,
        _shared_audit_replicator=None,
    )
    m = build_shared_audit_metrics(server)
    assert m["enabled_flag"] is False
    assert m["status"] == "disabled"
    caps = m.get("capabilities") or {}
    assert caps.get("gset_epidemic") is False
    assert caps.get("siem") is False
    assert caps.get("bft") is False
    assert caps.get("infinite_retention") is False
    assert caps.get("linearizable_cluster_ops") is False
    assert caps.get("multi_tenant_beyond_cluster_id") is False
