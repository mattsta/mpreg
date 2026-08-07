"""T73 residual closeout: Prometheus abort_fail_peers gauge."""

from __future__ import annotations

from pathlib import Path

def test_t73_prom_gauge_in_endpoints() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "fabric"
        / "monitoring_endpoints.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "mpreg_strong_abort_fail_peers" in text
    assert "last_abort_fail_peers" in text
    assert "not residual-free" in text.lower() or "not automatic heal" in text.lower()

def test_t73_mon_test_asserts_gauge() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "test_strong_audit_monitoring_endpoints.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "mpreg_strong_abort_fail_peers" in text

def test_t73_phase_61_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 61" in text

def test_t73_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T73_PROM_ABORT_FAIL_PEERS_PLAN.md"
    ).is_file()
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T73" in ledger
