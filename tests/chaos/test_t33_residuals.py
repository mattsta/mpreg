"""T33 residual closeout: curriculum CFT teaching + claims honesty."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from mpreg.cli.main import evaluate_strong_doctor_payload


def test_t33_doctor_detail_includes_ttl_gc_and_counts() -> None:
    ok, detail = evaluate_strong_doctor_payload(
        {
            "strong": {
                "health": "ok",
                "coordinator_bound": True,
                "visible_count": 3,
                "backups_count": 1,
                "backups_pruned_total": 5,
                "capabilities": {
                    "put_majority_commit": True,
                    "get_quorum": False,
                    "delete_quorum": False,
                    "local_ryw_after_put": True,
                    "cft_only": True,
                    "abort_best_effort": True,
                    "pending_ttl_clears_residual_l1": False,
                },
                "counters": {
                    "puts_ok": 2,
                    "aborts_peer_fail": 1,
                },
            }
        }
    )
    assert ok is True
    assert "ttl_gc=False" in detail
    assert "visible=3" in detail
    assert "backups=1" in detail
    assert "pruned=5" in detail
    assert "abort_fail=1" in detail


def test_t33_claims_yaml_cft_non_claims() -> None:
    path = Path(__file__).resolve().parents[1] / "invariants" / "claims.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    non = " ".join(data.get("non_claims") or []).lower()
    assert "lost abort" in non or "partial peer commit" in non
    assert "pending ttl" in non and "residual" in non
    assert "lww" in non or "reliable abort" in non
    # INV-CACHE-STRONG-01 lives under encapsulation
    items = data.get("encapsulation") or []
    strong = next(
        (
            i
            for i in items
            if isinstance(i, dict) and i.get("id") == "INV-CACHE-STRONG-01"
        ),
        None,
    )
    assert strong is not None
    claim = str(strong.get("claim") or "").lower()
    assert "cft" in claim or "abort" in claim
    assert "pending_ttl" in claim or "residual" in claim
    tests = strong.get("tests") or []
    assert "tests/chaos/test_t33_residuals.py" in tests
    assert "tests/examples_apps/test_curriculum_apps.py" in tests


def test_t33_curriculum_readme_mentions_cft_limit() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "examples"
        / "apps"
        / "02_moderate"
        / "cache_strong_quorum"
        / "README.md"
    )
    text = path.read_text(encoding="utf-8").lower()
    assert "cft" in text
    assert "lost abort" in text or "abort" in text
    assert "pending ttl" in text
    assert "not residual-free" in text or "not residual" in text


@pytest.mark.asyncio
async def test_t33_cache_strong_quorum_curriculum_cft_scenario() -> None:
    """Focused live run of curriculum including new CFT scenario."""
    from mpreg.examples.apps._shared.registry import get_app
    from mpreg.examples.apps._shared.runtime import run_app_main

    app = get_app("cache_strong_quorum")
    report = await run_app_main(app.id, app.load_main(), timeout_s=90.0)
    assert report.ok, f"cache_strong_quorum: {report.error}"
