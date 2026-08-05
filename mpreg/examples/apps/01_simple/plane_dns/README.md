# plane_dns (L1 · plane)

## Story

Service discovery is not only peer lists — operators register endpoints into
the DNS interoperability plane and resolve them over UDP/TCP for external
clients.

## Lesson

`dns_register` / `dns_list` / `dns_describe` on `MPREGClientAPI` plus
`MPREGDnsClient.resolve` form a complete DNS plane tour.

## Run

```bash
uv run mpreg-example run plane_dns
```

## What it proves

- DNS gateway starts with `dns_gateway_enabled=True`
- Service registration returns a registration id
- List/describe see the registration under namespace `market`
- UDP and TCP SRV resolve for `_svc._tcp.tradefeed.market.mpreg`

## Architecture

- One `MPREGServer` with DNS gateway (dedicated UDP + TCP ports)
- Dynamic ports via `port_range_context`

## API drill-down

| Call | Feature ID |
|------|------------|
| `MPREGClientAPI.dns_register` | `disco.dns_register` |
| `dns_list` / `dns_describe` | `disco.dns_register` (list/describe path) |
| `MPREGDnsClient.resolve` | `disco.dns_resolve`, `client.dns` |

## Non-claims

- Not a public authoritative DNS deployment guide.
- Not DNSSEC.
- Does not cover every zone layout or multi-gateway HA.

## Production exit ramp

- `docs/DNS_INTEROP_GUIDE.md`, `docs/DNS_RUNBOOKS.md`
- CLI: `mpreg dns-*`
- Next: compose with `discovery_join` and namespace policy
