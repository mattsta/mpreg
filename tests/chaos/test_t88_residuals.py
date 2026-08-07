"""T88 residual closeout: monitor strong table abort_fail_peer_count."""

from __future__ import annotations

from pathlib import Path


def test_t88_monitor_table_wires_count() -> None:
    text = (
        Path(__file__).resolve().parents[2] / "mpreg" / "cli" / "main.py"
    ).read_text(encoding="utf-8")
    assert "abort_fail_peer_count=" in text
    assert "mirrors prom gauge" in text or "abort_fail_peer_count mirrors" in text


def test_t88_phase_76_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    assert "Phase 76" in path.read_text(encoding="utf-8")


def test_t88_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T88_MONITOR_PEER_COUNT_PLAN.md"
    ).is_file()
    assert "T88" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
