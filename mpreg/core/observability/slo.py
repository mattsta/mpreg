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
        prometheus_metric="mpreg_rpc_requests_total",
        description="RPC request counter (also scrape pubsub via mpreg_pubsub_* series)",
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
        prometheus_metric="mpreg_rpc_latency_ms",
        description="RPC handler latency histogram (p95 from mpreg_rpc_latency_ms)",
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
        expr: up{job=~".*mpreg.*"} == 0 or absent(mpreg_info)
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "MPREG monitoring scrape target down"
          description: "Prefer job-level up / absent(mpreg_info); mpreg_monitoring_up is always 1 while serving."
      - alert: MPREGRouteBlackholeSpike
        expr: increase(mpreg_route_blackhole_total[5m]) > 50
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Elevated fabric route blackhole decisions"
      - alert: MPREGRPCLatencyP95High
        expr: histogram_quantile(0.95, sum(rate(mpreg_rpc_latency_ms_bucket[5m])) by (le, cluster_id)) > 250
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "MPREG RPC p95 latency high"
      - alert: MPREGGossipPendingDrops
        expr: increase(mpreg_gossip_pending_drops_total[5m]) > 100
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Gossip pending queue dropping messages (storm loss)"

  # STRONG / shared-audit lab signals — process-local; NOT WAN multi-region SLOs.
  # Keep in sync with mpreg/ops/prometheus_alerts.yml
  - name: mpreg_strong_shared_audit
    rules:
      - alert: MPREGStrongPendingElevated
        expr: mpreg_strong_pending > 64
        for: 5m
        labels:
          severity: warning
          scope: lab_process_local
        annotations:
          summary: "STRONG pending prepares elevated (process-local)"
          description: "Lab/process signal only — not a WAN multi-region SLA."
      - alert: MPREGSharedAuditPublishDrops
        expr: increase(mpreg_shared_audit_publish_dropped_total[10m]) > 0
        for: 10m
        labels:
          severity: warning
          scope: lab_process_local
        annotations:
          summary: "Shared audit publish drops (bounded G-Set, not SIEM)"
      - alert: MPREGStrongCapGetQuorumClaimed
        expr: mpreg_strong_cap_get_quorum > 0
        for: 0m
        labels:
          severity: critical
          scope: honesty
        annotations:
          summary: "STRONG dishonestly claims get_quorum (must be 0 in v1)"
      - alert: MPREGStrongCapDeleteQuorumClaimed
        expr: mpreg_strong_cap_delete_quorum > 0
        for: 0m
        labels:
          severity: critical
          scope: honesty
        annotations:
          summary: "STRONG dishonestly claims delete_quorum (must be 0 in v1)"
      - alert: MPREGStrongCapCftOnlyMissing
        expr: mpreg_strong_cap_cft_only == 0
        for: 0m
        labels:
          severity: critical
          scope: honesty
        annotations:
          summary: "STRONG dishonestly omits cft_only (must be 1 in v1)"
      - alert: MPREGStrongCapAbortBestEffortMissing
        expr: mpreg_strong_cap_abort_best_effort == 0
        for: 0m
        labels:
          severity: critical
          scope: honesty
        annotations:
          summary: "STRONG dishonestly claims reliable ABORT (must stay best-effort)"
      - alert: MPREGStrongCapPendingTtlClearsResidualClaimed
        expr: mpreg_strong_cap_pending_ttl_clears_residual_l1 > 0
        for: 0m
        labels:
          severity: critical
          scope: honesty
        annotations:
          summary: "STRONG dishonestly claims pending TTL clears residual L1"
      - alert: MPREGSharedAuditCapSiemClaimed
        expr: mpreg_shared_audit_cap_siem > 0
        for: 0m
        labels:
          severity: critical
          scope: honesty
        annotations:
          summary: "Shared audit dishonestly claims SIEM capability"
      - alert: MPREGSharedAuditCapBftClaimed
        expr: mpreg_shared_audit_cap_bft > 0
        for: 0m
        labels:
          severity: critical
          scope: honesty
        annotations:
          summary: "Shared audit dishonestly claims BFT capability"
"""
