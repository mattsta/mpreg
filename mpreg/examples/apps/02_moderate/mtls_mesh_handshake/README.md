# mtls_mesh_handshake (L2 · product)

Full **CERT_REQUIRED** mTLS: server `tls_ca_file` + client cert/key PEMs.

```bash
uv run mpreg-example run mtls_mesh_handshake
```

Proves mutual TLS handshake success and missing-client-cert fail-closed.
