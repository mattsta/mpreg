"""T81 residual closeout: DistLab hint_enriched asserts abort_fail_peer_count."""

from __future__ import annotations

from pathlib import Path

import pytest

from mpreg.testing.distlab.builtins import ensure_builtins
from mpreg.testing.distlab.registry import get_registry

@pytest.mark.asyncio
async def test_t81_distlab_hint_enriched_peer_count() -> None:
    ensure_builtins()
    r = await get_registry().run("strong.cft_residual_ops_hint_enriched")
    assert r.ok, r

def test_t81_builtin_asserts_count() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "testing"
        / "distlab"
        / "builtins.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "abort_fail_peer_count" in text
    assert "cft_residual_ops_hint_enriched" in text

def test_t81_phase_69_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    assert "Phase 69" in path.read_text(encoding="utf-8")

def test_t81_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T81_DISTLAB_PEER_COUNT_PLAN.md"
    ).is_file()
    assert "T81" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
