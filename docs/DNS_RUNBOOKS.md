# DNS Gateway Runbooks

This runbook covers operational tasks for the MPREG DNS interoperability
gateway.

## Start/Stop

- Enable with `dns_gateway_enabled = True`.
- The gateway starts alongside the MPREG server.
- UDP/TCP ports are auto-allocated if not specified.
- Set `dns_allow_external_names = True` to respond to queries without the zone
  suffix (useful when integrating with legacy search domains).

## Health & Metrics

The monitoring system exposes:

- `GET /dns/metrics` with query counts, NXDOMAIN rates, and latency.
- CLI: `mpreg monitor dns --url http://127.0.0.1:<port>`
- Watch mode: `mpreg monitor dns-watch --url http://127.0.0.1:<port> --interval 5`

Example response:

```
{
  "status": "ok",
  "dns_gateway": {
    "enabled": true,
    "status": "running",
    "udp_port": 5353,
    "tcp_port": 5353,
    "zones": ["mpreg"],
    "metrics": {
      "total_queries": 1200,
      "udp_queries": 1180,
      "tcp_queries": 20,
      "nxdomain_responses": 14,
      "error_responses": 0,
      "avg_latency_ms": 0.42,
      "max_latency_ms": 2.1
    }
  },
  "timestamp": 1712345678.0
}
```

## Troubleshooting

- **NXDOMAIN for known service**
  - Verify the service is registered (`dns_list`).
  - Check namespace policy for the namespace.
  - Ensure `dns_zones` includes the served suffix.
  - If querying without a zone suffix, enable `dns_allow_external_names`.

- **No SRV records**
  - Use the `_svc._tcp.<name>.<namespace>` form.
  - Confirm service targets are non-empty.

- **A/AAAA missing**
  - Targets must be IPs for direct A/AAAA responses.
  - For hostnames, query SRV and resolve the target separately.

- **Quick checks**
  - `mpreg client dns-resolve --host <gateway-host> --port <udp-port> --qname _svc._tcp.tradefeed.market.mpreg --qtype SRV`
  - `mpreg client dns-resolve --host <gateway-host> --port <udp-port> --qname tradefeed.market.mpreg --qtype A`

## Safe Disable

Set `dns_gateway_enabled = False` and restart the server. This does not affect
MPREG RPC, pubsub, queue, or cache operations.
