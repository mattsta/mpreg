"""T75 residual closeout: Prometheus residual-candidate info alert."""

from __future__ import annotations

from pathlib import Path

from mpreg.core.observability import prometheus_alert_rules_yaml

def test_t75_slo_alert_present() -> None:
    yml = prometheus_alert_rules_yaml()
    assert "MPREGStrongAbortFailPeersPresent" in yml
    assert "mpreg_strong_abort_fail_peers" in yml
    assert "info" in yml
    assert "not automatic heal" in yml.lower() or "Not automatic heal" in yml

def test_t75_packaged_alerts_yml() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "ops"
        / "prometheus_alerts.yml"
    )
    text = path.read_text(encoding="utf-8")
    assert "MPREGStrongAbortFailPeersPresent" in text
    assert "mpreg_strong_abort_fail_peers" in text

def test_t75_phase_63_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 63" in text

def test_t75_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T75_PROM_RESIDUAL_ALERT_PLAN.md"
    ).is_file()
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T75" in ledger
