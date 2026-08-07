"""T53 residual closeout: residual_ops_hint on metrics / OpenAPI / status."""

from __future__ import annotations

from pathlib import Path

import pytest

from mpreg.core.cache_models import GlobalCacheKey
from mpreg.core.cache_strong import (
    InProcessStrongTransport,
    StrongLocalBackend,
    StrongPutCoordinator,
    format_residual_ops_hint,
)
from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager
from mpreg.server_pkg.monitoring_metrics import build_strong_metrics
from mpreg.server_pkg.openapi_surface import _strong_metrics_schema

def test_t53_format_residual_ops_hint_empty() -> None:
    assert format_residual_ops_hint([]) == ""
    assert format_residual_ops_hint(None) == ""

def test_t53_format_residual_ops_hint_content() -> None:
    h = format_residual_ops_hint(["n1", "n2"], "oid-1")
    assert "cache-strong-retry-abort" in h
    assert "--op-id oid-1" in h
    assert "--peer n1" in h
    assert "--peer n2" in h
    assert "not auto-heal" in h

@pytest.mark.asyncio
async def test_t53_gcm_status_and_metrics_hint() -> None:
    tr = InProcessStrongTransport()
    backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(5)}
    for be in backends.values():
        tr.register(be)
    tr.drop_commit |= {"n2", "n3", "n4"}
    tr.drop_abort |= {"n1"}
    coord = StrongPutCoordinator(
        origin_id="n0",
        local=backends["n0"],
        transport=tr,
        replica_factor=5,
        min_replicas=5,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.2,
        abort_attempts=2,
    )
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="t53-hint",
        )
    )
    gcm.attach_strong_coordinator(coord)
    try:
        key = GlobalCacheKey(namespace="t53", identifier="k", version="v1")
        res = await coord.strong_put(key, {"v": 1}, eligible_peers=list(backends))
        assert res.success is False
        st = gcm.strong_status()
        assert "n1" in list(st.get("last_abort_fail_peers") or [])
        hint = st.get("residual_ops_hint") or ""
        assert "cache-strong-retry-abort" in hint
        assert res.operation_id in hint or "--op-id" in hint

        class _Srv:
            settings = type(
                "S",
                (),
                {
                    "cache_strong_enabled": True,
                    "cache_strong_prepare_timeout_s": 0.3,
                    "cache_strong_commit_timeout_s": 0.2,
                    "cache_strong_pending_ttl_s": 30.0,
                },
            )()
            _cache_manager = gcm
            _strong_local_backend = backends["n0"]
            _strong_pending_purge_task = None

        payload = build_strong_metrics(_Srv())
        assert "residual_ops_hint" in payload
        assert "cache-strong-retry-abort" in (payload.get("residual_ops_hint") or "")
    finally:
        await gcm.shutdown()

def test_t53_openapi_residual_ops_hint() -> None:
    props = _strong_metrics_schema()["properties"]["strong"]["properties"]
    assert "residual_ops_hint" in props
    desc = (props["residual_ops_hint"].get("description") or "").lower()
    assert "not automatic" in desc or "not auto" in desc
    assert "cache-strong-retry-abort" in desc or "remediation" in desc

def test_t53_phase_41_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 41" in text
    assert "residual_ops_hint" in text

def test_t53_client_guide_hint() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "MPREG_CLIENT_GUIDE.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "residual_ops_hint" in text

def test_t53_claims_hint_json() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "invariants"
        / "claims.yaml"
    )
    text = path.read_text(encoding="utf-8")
    assert "residual_ops_hint" in text
