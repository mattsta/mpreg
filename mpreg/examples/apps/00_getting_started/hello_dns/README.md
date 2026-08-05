# hello_dns (L0 · product)

## Story

A tiny service name is registered in the DNS gateway and listed back — the
smallest honest DNS hello before the full `plane_dns` tour.

## Lesson

Enable `dns_gateway_enabled` + `dns_register` / `dns_list` / UDP resolve.

## Run

```bash
uv run mpreg-example run hello_dns
```

## What it proves

- Gateway starts with UDP/TCP ports
- Register + list for `hello.demo`
- Resolve call returns without error

## Non-claims

- Not full SRV/TXT matrix (see `plane_dns`).
- Not multi-zone federation DNS.

## Production exit ramp

- Next: `plane_dns`, `ops_cli_tour` dns subcommands
