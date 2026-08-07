"""T49 residual closeout: GCM curriculum + DistLab self-target scenario."""

from __future__ import annotations

from pathlib import Path

import pytest

from mpreg.testing.distlab.builtins import ensure_builtins
from mpreg.testing.distlab.registry import get_registry, resolve_preset

@pytest.mark.asyncio
async def test_t49_distlab_self_target_scenario() -> None:
    ensure_builtins()
    r = await get_registry().run("strong.cft_retry_abort_self_target")
    assert r.ok, r
    meta = r.meta or {}
    assert meta.get("product_fix") is True
    assert meta.get("ops_driven") is True
    assert meta.get("not_automatic_heal") is True
    assert meta.get("track") == "T49"

def test_t49_preset_includes_self_target() -> None:
    ensure_builtins()
    assert "strong.cft_retry_abort_self_target" in resolve_preset("strong-core")
    assert "strong.cft_retry_abort_self_target" in resolve_preset("ci-core")
    # Prior clears-residual scenario retained
    assert "strong.cft_retry_abort_clears_residual" in resolve_preset("strong-core")

def test_t49_ops_surfaces_meta_on_clears_residual() -> None:
    ensure_builtins()
    from mpreg.testing.distlab.builtins import _strong_cft_retry_abort_clears_residual

    sc = _strong_cft_retry_abort_clears_residual()
    surfaces = list((sc.meta or {}).get("ops_surfaces") or [])
    blob = " ".join(surfaces)
    assert "StrongPutCoordinator.retry_abort" in blob
    assert "GlobalCacheManager.strong_retry_abort" in blob
    assert "mpreg.cache.strong_retry_abort" in blob
    assert "MPREGClient.cache_strong_retry_abort" in blob
    assert "cache-strong-retry-abort" in blob

def test_t49_curriculum_gcm_retry_path() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "examples"
        / "apps"
        / "02_moderate"
        / "cache_strong_quorum"
        / "run.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "strong_retry_abort" in text
    assert "GCM" in text or "GlobalCacheManager" in text
    assert "not auto-heal" in text or "not automatic" in text.lower()

def test_t49_phase_37_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 37" in text
    assert "self_target" in text or "self-target" in text
    assert "ops_surfaces" in text or "GCM" in text

def test_t49_claims_self_target_non_claim() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "invariants"
        / "claims.yaml"
    )
    text = path.read_text(encoding="utf-8")
    assert "self-target" in text or "self_target" in text or "peers=[self]" in text
