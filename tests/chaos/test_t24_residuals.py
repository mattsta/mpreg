"""T24 residual closeout: prom capability gauges + alert honesty rules."""

from __future__ import annotations

from importlib import resources
from pathlib import Path

from mpreg.server_pkg.openapi_surface import openapi_path_set, route_table_path_set

def _alerts_yml_text() -> str:
    path = Path(__file__).resolve().parents[2] / "mpreg" / "ops" / "prometheus_alerts.yml"
    if path.is_file():
        return path.read_text(encoding="utf-8")
    # Fallback via package resources
    return resources.files("mpreg.ops").joinpath("prometheus_alerts.yml").read_text(
        encoding="utf-8"
    )

def test_t24_prometheus_alerts_include_honesty_rules() -> None:
    """Packaged alert YAML includes STRONG/audit honesty fail-closed rules."""
    text = _alerts_yml_text()
    assert "mpreg_strong_shared_audit" in text
    assert "MPREGStrongCapGetQuorumClaimed" in text
    assert "MPREGStrongCapDeleteQuorumClaimed" in text
    assert "MPREGStrongCapCftOnlyMissing" in text
    assert "MPREGStrongCapAbortBestEffortMissing" in text
    assert "MPREGSharedAuditCapSiemClaimed" in text
    assert "MPREGSharedAuditCapBftClaimed" in text
    assert "MPREGStrongPendingElevated" in text
    assert "MPREGSharedAuditPublishDrops" in text
    assert "WAN" in text or "wan" in text.lower()
    assert "SIEM" in text or "siem" in text.lower()
    assert "mpreg_strong_cap_get_quorum" in text
    assert "mpreg_strong_cap_cft_only" in text
    assert "mpreg_strong_cap_abort_best_effort" in text
    assert "mpreg_shared_audit_cap_siem" in text

def test_t24_openapi_still_matches_route_table() -> None:
    assert openapi_path_set() == route_table_path_set()

def test_t24_slo_helper_includes_honesty_group() -> None:
    from mpreg.core.observability import prometheus_alert_rules_yaml

    yml = prometheus_alert_rules_yaml()
    assert "mpreg_strong_shared_audit" in yml
    assert "mpreg_strong_cap_get_quorum" in yml
    assert "mpreg_strong_cap_cft_only" in yml
    assert "mpreg_strong_cap_abort_best_effort" in yml
    assert "mpreg_shared_audit_cap_siem" in yml
