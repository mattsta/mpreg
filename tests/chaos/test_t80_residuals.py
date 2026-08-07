"""T80 residual closeout: count_abort_fail_peers + metrics field."""

from __future__ import annotations

from pathlib import Path

from mpreg.core.cache_strong import count_abort_fail_peers


def test_t80_helper_behavior() -> None:
    assert count_abort_fail_peers(["n1", "n1"]) == 1
    assert count_abort_fail_peers(body={"last_abort_fail_peers": ["a"]}) == 1


def test_t80_source_wires() -> None:
    root = Path(__file__).resolve().parents[2]
    assert "count_abort_fail_peers" in (
        root / "mpreg" / "core" / "cache_strong.py"
    ).read_text(encoding="utf-8")
    assert "abort_fail_peer_count" in (
        root / "mpreg" / "server_pkg" / "monitoring_metrics.py"
    ).read_text(encoding="utf-8")
    assert "abort_fail_peer_count" in (
        root / "mpreg" / "server_pkg" / "openapi_surface.py"
    ).read_text(encoding="utf-8")
    assert "abort_fail_peer_count" in (
        root / "mpreg" / "core" / "global_cache.py"
    ).read_text(encoding="utf-8")
    assert "count_abort_fail_peers" in (
        root / "mpreg" / "fabric" / "monitoring_endpoints.py"
    ).read_text(encoding="utf-8")


def test_t80_phase_68_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    assert "Phase 68" in path.read_text(encoding="utf-8")


def test_t80_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T80_ABORT_FAIL_PEER_COUNT_PLAN.md"
    ).is_file()
    assert "T80" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
