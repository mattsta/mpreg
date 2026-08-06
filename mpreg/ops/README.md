# Packaged operator artifacts

| File                    | Purpose                                                          |
| ----------------------- | ---------------------------------------------------------------- |
| `prometheus_alerts.yml` | Golden-signal alert rules (from `prometheus_alert_rules_yaml()`) |

Load into Prometheus / your rules controller. Scrape `/metrics/prometheus` with bearer auth when configured.
