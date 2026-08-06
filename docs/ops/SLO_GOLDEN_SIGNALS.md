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
mpreg doctor
mpreg monitor health --summary
mpreg monitor decisions --limit 50 --format table
mpreg monitor prometheus
mpreg monitor status
```
