# tls_dev_handshake (L1 · product)

## Story

Local TLS without a corporate CA: generate an ephemeral CA + server cert,
start MPREG with `tls_cert_file` / `tls_key_file`, and call over `wss://`.

## Lesson

`mpreg.core.dev_certs.generate_dev_tls_material` + `MPREGSettings.tls_*` +
`SecurityConfig(ca_file=…)` on the client.

## Run

```bash
uv run mpreg-example run tls_dev_handshake
```

## What it proves

- PEM chain written (ca, server, client)
- `wss://` RPC succeeds with client trust material
- Plain `ws://` fails against a TLS-only listener
- Client cert PEMs load into `SecurityConfig` for mTLS drills

## Non-claims

- Not a production PKI / rotation story.
- Hostname verification vs `127.0.0.1` IP URLs may require `verify_cert=False`
  even when the CA is trusted (SAN is DNS `localhost`).

## Production exit ramp

- Real certs from your CA; pin `verify_cert=True` + proper hostnames
- Next: `client_auth_token` (bearer), fabric route security
