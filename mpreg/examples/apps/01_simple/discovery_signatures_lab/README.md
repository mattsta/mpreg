# discovery_signatures_lab (L1 · plane)

HMAC authenticity for discovery summaries and fabric gossip envelopes.

```bash
uv run mpreg-example run discovery_signatures_lab
```

Proves: `sign_summary` / `verify_summary`, gossip HMAC, settings knobs.
Non-claim: not a PKI substitute — use route security for announcement authenticity.
