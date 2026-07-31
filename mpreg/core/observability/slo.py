"""Golden-signal / SLO helper constants for operators and docs generation."""

from __future__ import annotations

from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class GoldenSignal:
    name: str
    prometheus_metric: str
    description: str
    warning_threshold: str
    critical_threshold: str

GOLDEN_SIGNALS: tuple[GoldenSignal, ...] = (
    GoldenSignal(
        name="traffic",
        prometheus_metric="mpreg_unified_.*_request",
        description="RPC/request rate across unified metrics",
        warning_threshold="sustained drop >50% vs 1h baseline",
        critical_threshold="near-zero traffic with peers present",
    ),
    GoldenSignal(
        name="errors",
        prometheus_metric="mpreg_federation_health_score",
        description="Overall federation health score (0-1)",
        warning_threshold="< 0.85",
        critical_threshold="< 0.6",
    ),
    GoldenSignal(
        name="latency",
        prometheus_metric="mpreg_http_request_duration_ms",
        description="Monitoring handler latency proxy; prefer RPC p95 from unified",
        warning_threshold="p95 > 250ms",
        critical_threshold="p95 > 1000ms",
    ),
    GoldenSignal(
        name="saturation",
        prometheus_metric="mpreg_federation_active_connections",
        description="Active federation/peer connections",
        warning_threshold="unexpected growth vs peer target",
        critical_threshold="connection storms / thrash",
    ),
)

def prometheus_alert_rules_yaml() -> str:
    """Return example Prometheusrule YAML for golden signals."""
    return """groups:
  - name: mpreg_golden_signals
    rules:
      - alert: MPREGFederationHealthDegraded
        expr: mpreg_federation_health_score < 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "MPREG federation health degraded"
          description: "Health score {{ $value }} on {{ $labels.cluster_id }}"
      - alert: MPREGFederationHealthCritical
        expr: mpreg_federation_health_score < 0.6
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "MPREG federation health critical"
      - alert: MPREGMonitoringDown
        expr: mpreg_monitoring_up == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "MPREG monitoring scrape target down"
"""
