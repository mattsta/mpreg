"""T52 residual closeout: DistLab GCM.strong_retry_abort scenario."""

from __future__ import annotations

from pathlib import Path

import pytest

from mpreg.testing.distlab.builtins import ensure_builtins
from mpreg.testing.distlab.registry import get_registry, resolve_preset


@pytest.mark.asyncio
async def test_t52_distlab_gcm_retry_abort_scenario() -> None:
    ensure_builtins()
    r = await get_registry().run("strong.cft_gcm_retry_abort_clears_residual")
    assert r.ok, r
    meta = r.meta or {}
    assert meta.get("product_fix") is True
    assert meta.get("ops_driven") is True
    assert meta.get("not_automatic_heal") is True
    assert meta.get("track") == "T52"
    surfaces = " ".join(meta.get("ops_surfaces") or ())
    assert "GlobalCacheManager.strong_retry_abort" in surfaces


def test_t52_preset_includes_gcm_retry() -> None:
    ensure_builtins()
    assert "strong.cft_gcm_retry_abort_clears_residual" in resolve_preset("strong-core")
    assert "strong.cft_gcm_retry_abort_clears_residual" in resolve_preset("ci-core")


def test_t52_phase_40_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 40" in text
    assert "gcm_retry_abort" in text or "GCM.strong_retry_abort" in text


def test_t52_ledger_and_plan() -> None:
    root = Path(__file__).resolve().parents[2]
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T52" in ledger
    assert "gcm" in ledger.lower() or "GCM" in ledger
    plan = root / "docs" / "plans" / "DISTLAB_T52_DISTLAB_GCM_RETRY_PLAN.md"
    assert plan.is_file()


def test_t52_operate_gcm_scenario() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "examples-curriculum"
        / "OPERATE.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "gcm_retry_abort" in text or "cft_gcm_retry" in text
