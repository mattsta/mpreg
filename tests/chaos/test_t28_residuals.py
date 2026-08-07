"""T28 residual closeout: CFT ops live loop, LWW heal, presets, monitor."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from mpreg.cli.main import cli
from mpreg.testing.distlab.builtins import ensure_builtins
from mpreg.testing.distlab.registry import get_registry, resolve_preset


@pytest.mark.asyncio
async def test_t28_distlab_cft_residual_healed_by_lww() -> None:
    ensure_builtins()
    r = await get_registry().run("strong.cft_residual_healed_by_lww")
    assert r.ok, r
    assert (r.meta or {}).get("lww_heal") is True
    assert (r.meta or {}).get("not_reliable_abort") is True


def test_t28_strong_core_preset_includes_cft_scenarios() -> None:
    ensure_builtins()
    names = resolve_preset("strong-core")
    assert "strong.cft_partial_commit_lost_abort" in names
    assert "strong.cft_residual_healed_by_lww" in names
    ci = resolve_preset("ci-core")
    assert "strong.cft_partial_commit_lost_abort" in ci
    assert "strong.cft_residual_healed_by_lww" in ci


def test_t28_monitor_strong_table_includes_cft_fields() -> None:
    """Unit-level: table formatter path includes CFT keys in summary string.

    Full live path is covered by ops_cli_tour + live metrics e2e; here we
    exercise evaluate_strong_doctor_payload + a mocked table summary shape.
    """
    from mpreg.cli.main import evaluate_strong_doctor_payload

    ok, detail = evaluate_strong_doctor_payload(
        {
            "strong": {
                "health": "ok",
                "coordinator_bound": True,
                "capabilities": {
                    "put_majority_commit": True,
                    "get_quorum": False,
                    "delete_quorum": False,
                    "local_ryw_after_put": True,
                    "cft_only": True,
                    "abort_best_effort": True,
                },
                "counters": {
                    "puts_ok": 1,
                    "aborts_peer_fail": 3,
                },
            }
        }
    )
    assert ok is True
    assert "cft=True" in detail
    assert "abort_be=True" in detail
    assert "abort_fail=3" in detail


def test_t28_cli_monitor_strong_help_mentions_strong() -> None:
    runner = CliRunner()
    r = runner.invoke(cli, ["monitor", "strong", "--help"])
    assert r.exit_code == 0
    assert "STRONG" in r.output or "strong" in r.output.lower()


@pytest.mark.asyncio
async def test_t28_cft_and_heal_suite_slice() -> None:
    """Run both CFT honesty scenarios back-to-back via registry."""
    ensure_builtins()
    reg = get_registry()
    for name in (
        "strong.cft_partial_commit_lost_abort",
        "strong.cft_residual_healed_by_lww",
    ):
        r = await reg.run(name)
        assert r.ok, f"{name}: {r}"
