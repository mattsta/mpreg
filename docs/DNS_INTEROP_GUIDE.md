# DNS Interoperability Guide

MPREG can optionally expose service discovery via a DNS gateway. This gateway
derives records directly from the routing catalog (no separate authority) and
respects namespace policies and scopes.

## Enable the Gateway

Set the DNS configuration in `MPREGSettings` or environment:

- `dns_gateway_enabled = True`
- `dns_listen_host = "127.0.0.1"` (defaults to server host)
- `dns_udp_port` / `dns_tcp_port` (optional; auto-allocated if omitted)
- `dns_zones = ("mpreg",)` (zone suffixes served by the gateway)
- `dns_min_ttl_seconds`, `dns_max_ttl_seconds`
- `dns_allow_external_names` (respond to queries without the zone suffix)

## Naming Scheme

The gateway supports both service-specific SRV names and plain A/AAAA lookups:

- RPC: `_mpreg._tcp.<function>.<namespace>.mpreg`
- Queue: `_queue._tcp.<queue>.<namespace>.mpreg`
- Service: `_svc._tcp.<service>.<namespace>.mpreg`
- Node: `<node_label>.node.mpreg`
- Plain service name: `<service>.<namespace>.mpreg` (A/AAAA/TXT)

### Node Labels

Node IDs are typically URLs (for example `ws://host:port`), which are not DNS
label safe. Use the base32 label helper to generate a DNS-safe label:

```python
from mpreg.dns import encode_node_id

node_label = encode_node_id("ws://127.0.0.1:10000")
# Query: <node_label>.node.mpreg
```

If the node ID is already DNS-safe (letters, digits, `-`), it can be used
directly as `<node_label>`.

CLI helpers:

```
mpreg client dns-node-encode "ws://127.0.0.1:10000"
mpreg client dns-node-decode b32-...
```

## Record Mapping

| Query  | Response                                                      |
| ------ | ------------------------------------------------------------- |
| SRV    | Targets from transport endpoints or service targets           |
| A/AAAA | IP targets when provided (or node transport hosts if IPs)     |
| TXT    | Namespace, tags, capabilities, cluster/node info, spec digest |

### Node Lookup Example

```bash
node_label=$(mpreg client dns-node-encode "ws://127.0.0.1:10000")
mpreg client dns-resolve --host 127.0.0.1 --port <udp-port> \
  --qname "${node_label}.node.mpreg" --qtype A
```

Debug helper script:

```
uv run python tools/debug/dns_node_lookup_demo.py
```

## Register a Service

Example using the client API:

```
await client.dns_register(
    {
        "name": "tradefeed",
        "namespace": "market",
        "protocol": "tcp",
        "port": 9000,
        "targets": ["10.0.1.12", "10.0.1.13"],
        "tags": ["primary"],
        "capabilities": ["quotes"],
        "metadata": {"tier": "gold"},
        "priority": 10,
        "weight": 5,
    }
)
```

CLI equivalent:

```
mpreg client dns-register --url ws://ingress:<port> \
  --name tradefeed --namespace market --protocol tcp --port 9000 \
  --target 10.0.1.12 --target 10.0.1.13 --tag primary \
  --capability quotes --metadata tier=gold
```

## Query Examples

```
dig @<gateway-host> -p <udp-port> _svc._tcp.tradefeed.market.mpreg SRV
dig @<gateway-host> -p <udp-port> tradefeed.market.mpreg A
dig @<gateway-host> -p <udp-port> _mpreg._tcp.echo.mpreg SRV
```

CLI resolver helper:

```
mpreg client dns-resolve --host <gateway-host> --port <udp-port> \
  --qname _svc._tcp.tradefeed.market.mpreg --qtype SRV
```

Python resolver helper:

```
from mpreg.client.dns_client import MPREGDnsClient

dns = MPREGDnsClient(host="<gateway-host>", port=<udp-port>)
result = await dns.resolve("_svc._tcp.tradefeed.market.mpreg", qtype="SRV")
print(result.to_dict())
```

## Notes

- The DNS gateway is optional and does not change the MPREG data plane.
- Namespace policy and scope filters are enforced through catalog queries.
- DNS TTLs are clamped to configured min/max values.
- If `dns_allow_external_names` is enabled, the gateway will resolve names
  without a configured zone suffix (for example `_svc._tcp.tradefeed.market`).
