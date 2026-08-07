# MPREG Golden Signals and SLOs

## Scrape

```yaml
scrape_configs:
  - job_name: mpreg
    metrics_path: /metrics/prometheus
    static_configs:
      - targets: ["mpreg-host:<monitoring-port>"]
    authorization:
      type: Bearer
      credentials: "<monitoring_auth_token>"
```

## Golden signals

| Signal     | Metric / source                                                           | Warning           | Critical                 |
| ---------- | ------------------------------------------------------------------------- | ----------------- | ------------------------ |
| Traffic    | `mpreg_rpc_requests_total` (+ pubsub series)                              | −50% vs baseline  | near zero with peers     |
| Errors     | `mpreg_federation_health_score`, `mpreg_rpc_errors_total`                 | score < 0.85      | score < 0.6              |
| Latency    | `mpreg_rpc_latency_ms` histogram p95                                      | p95 > 250ms       | p95 > 1s                 |
| Saturation | `mpreg_federation_active_connections`, `mpreg_gossip_pending_drops_total` | unexpected growth | connection/gossip storms |

## Example alert rules

Generate from code:

```python
from mpreg.core.observability import prometheus_alert_rules_yaml

print(prometheus_alert_rules_yaml())
```

Packaged copy: `mpreg/ops/prometheus_alerts.yml`.

Also see `mpreg doctor` for a live multi-endpoint probe.

## Route decision audit

```bash
mpreg monitor decisions --url "$MPREG_MONITORING_URL" --limit 50
# or
curl -H "Authorization: Bearer $TOKEN" \
  "$MPREG_MONITORING_URL/routing/decisions?correlation_id=..."
```

## Trace context

Fabric hops carry W3C `traceparent` in `MessageHeaders.metadata`. Correlate with
`/routing/decisions` using `correlation_id` or `traceparent`.

## Troubleshooting map

| Symptom                | First checks                                                           |
| ---------------------- | ---------------------------------------------------------------------- |
| RPC p95 high           | `/metrics/unified`, `/routing/decisions`, peer dial pressure           |
| Cross-cluster failures | hop budget errors (code 1003), `/routing/trace`, federation path       |
| Discovery stale        | summary HMAC verify failures, resolver cache stats, catalog gossip lag |
| Scrape 401             | `monitoring_auth_token` / `MPREG_MONITORING_TOKEN`                     |
| HA client sticky fail  | non-retryable `MpregError` (do not spin endpoints); check code         |

## CLI probes

```bash
uv run mpreg doctor
uv run mpreg monitor health --summary
uv run mpreg monitor decisions --limit 50 --format table
uv run mpreg monitor prometheus
uv run mpreg monitor status
uv run mpreg monitor strong --format table   # capabilities + refuse counters
uv run mpreg monitor audit --format table    # capabilities honesty (not SIEM/BFT)
uv run mpreg doctor --url "$MPREG_MONITORING_URL" --strong --audit
```

## STRONG / shared audit (lab metrics — not WAN SLO)

| Signal | Series / endpoint | Note |
| --- | --- | --- |
| STRONG puts | `mpreg_strong_puts_ok_total` / `_fail_total` | Process-local |
| STRONG refuse | `mpreg_strong_*_refused_total` | 1012 paths (disabled/get/delete) |
| STRONG pending | `mpreg_strong_pending` | >64 → degraded_pending |
| STRONG latency | `mpreg_strong_put_latency_p99_ms` | **Lab ring only — not WAN SLA** |
| Audit store | `mpreg_shared_audit_store_size` | Bounded G-Set |
| Audit drops | `mpreg_shared_audit_publish_dropped_total` | degraded_drops |
| Audit caps | `/metrics/shared-audit` `capabilities.*` | siem/bft/… always false |
| Cap gauges | `mpreg_strong_cap_*` / `mpreg_shared_audit_cap_*` | 0/1 honesty ads |

Honesty fail-closed alerts (packaged `mpreg/ops/prometheus_alerts.yml` group
`mpreg_strong_shared_audit`): fire if `mpreg_strong_cap_get_quorum` or
`mpreg_shared_audit_cap_siem` (etc.) ever scrape **> 0**. Lab pending/drop
alerts are process-local — **not** WAN SLOs.

Do **not** page on STRONG p99 as a multi-region contract. See
`docs/ops/STRONG_AND_SHARED_AUDIT_RUNBOOK.md`.
