"""T62 residual closeout: DistLab residual_ops_hint enrichment scenario."""

from __future__ import annotations

from pathlib import Path

import pytest

from mpreg.testing.distlab.builtins import ensure_builtins
from mpreg.testing.distlab.registry import get_registry, resolve_preset

@pytest.mark.asyncio
async def test_t62_distlab_hint_enriched_scenario() -> None:
    ensure_builtins()
    r = await get_registry().run("strong.cft_residual_ops_hint_enriched")
    assert r.ok, r
    meta = r.meta or {}
    assert meta.get("product_fix") is True
    assert meta.get("ops_driven") is True
    assert meta.get("not_automatic_heal") is True
    assert meta.get("track") == "T62"
    surfaces = " ".join(meta.get("ops_surfaces") or ())
    assert "residual_ops_hint" in surfaces

def test_t62_preset_includes_hint_enriched() -> None:
    ensure_builtins()
    assert "strong.cft_residual_ops_hint_enriched" in resolve_preset("strong-core")
    assert "strong.cft_residual_ops_hint_enriched" in resolve_preset("ci-core")

def test_t62_phase_50_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 50" in text
    assert "cft_residual_ops_hint_enriched" in text or "hint_enriched" in text

def test_t62_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T62_DISTLAB_HINT_ENRICHED_PLAN.md"
    ).is_file()
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T62" in ledger

def test_t62_operate_docs() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "examples-curriculum"
        / "OPERATE.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "ops_hint" in text or "residual_ops_hint" in text
