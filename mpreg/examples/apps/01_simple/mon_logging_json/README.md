# mon_logging_json (L1 · plane)

Structured JSON logging config plus correlation timeline and unified health.

```bash
uv run mpreg-example run mon_logging_json
```

## Proves

- `mon.logging` — `configure_logging(..., json_logs=True)` (CLI `--json-logs`)
- `mon.correlation` / `mon.health` / `mon.unified` on unified system monitor

## Non-claims

- Production log shipper pipelines (CloudWatch/Loki agents)
