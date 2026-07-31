from mpreg.core.observability import GOLDEN_SIGNALS, prometheus_alert_rules_yaml

def test_golden_signals_and_rules() -> None:
    assert len(GOLDEN_SIGNALS) >= 4
    yaml = prometheus_alert_rules_yaml()
    assert "MPREGFederationHealthDegraded" in yaml
    assert "mpreg_monitoring_up" in yaml
