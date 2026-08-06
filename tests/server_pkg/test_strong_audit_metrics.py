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
        "counters": {"puts_ok": 5, "puts_fail": 1},
        "latency_ms": {"sample_count": 3, "p99_ms": 12.0, "p50_ms": 4.0},
        "coordinator": {"origin_id": "n0"},
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
    assert m["latency_ms"]["p99_ms"] == 12.0
    assert m["health"] == "ok"

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
