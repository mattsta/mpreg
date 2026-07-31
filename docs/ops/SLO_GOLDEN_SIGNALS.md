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

| Signal | Metric / source | Warning | Critical |
|--------|-----------------|---------|----------|
| Traffic | unified request counters (JSON `/metrics/unified` + prom flatten) | −50% vs baseline | near zero with peers |
| Errors | `mpreg_federation_health_score` | < 0.85 | < 0.6 |
| Latency | RPC p95 (unified) / `mpreg_http_request_duration_ms` | p95 > 250ms | p95 > 1s |
| Saturation | `mpreg_federation_active_connections`, queue depth | unexpected growth | connection storms |

## Example alert rules

Generate from code:

```python
from mpreg.core.observability import prometheus_alert_rules_yaml
print(prometheus_alert_rules_yaml())
```

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

| Symptom | First checks |
|---------|----------------|
| RPC p95 high | `/metrics/unified`, `/routing/decisions`, peer dial pressure |
| Cross-cluster failures | hop budget errors (code 1003), `/routing/trace`, federation path |
| Discovery stale | summary HMAC verify failures, resolver cache stats, catalog gossip lag |
| Scrape 401 | `monitoring_auth_token` / `MPREG_MONITORING_TOKEN` |
| HA client sticky fail | non-retryable `MpregError` (do not spin endpoints); check code |

## CLI probes

```bash
mpreg doctor
mpreg monitor health --summary
mpreg monitor decisions --limit 50 --format table
mpreg monitor prometheus
mpreg monitor status
```
