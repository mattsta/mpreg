# Support

## How to get help

1. **Docs first:** `docs/GETTING_STARTED.md`, `docs/PRODUCTION_DEPLOYMENT.md`,
 `docs/MPREG_CLIENT_GUIDE.md`, `docs/ops/RELEASE_CHECKLIST.md`.
2. **Config gate:** `uv run mpreg config-check <profile> --strict --format json`.
3. **Health:** `uv run mpreg doctor --url $MPREG_MONITORING_URL`.
4. **Claims / non-claims:** `tests/invariants/claims.yaml` and honesty banners in
 README / PRODUCTION docs.

## Security issues

Do **not** file public issues for vulnerabilities. See **`SECURITY.md`**.

## What we can help with

- Using profiles, `MPREGClient`, fabric routing, queues/pubsub/cache
- Flag-gated STRONG put and shared audit (ops visibility, not auto-heal)
- CI/release gate scripts and DistLab scenarios

## What this is not

| Not offered | Notes |
| --- | --- |
| Commercial SLA / 24×7 on-call | Community / maintainer best-effort |
| BFT or WAN linearizability guarantees | See `non_claims` |
| Guaranteed residual-free cache after lost ABORT | CFT best-effort; doctor/monitor only |
| Full pen-test certification | CI + SECURITY.md guidance only |
| OAuth2/OIDC product support | Roadmap — not shipped in 0.3.x |

## Reporting bugs

Prefer GitHub issues with:

- MPREG version (`python -c "import mpreg; print(mpreg.__version__)"`)
- Profile or minimal settings (redact secrets)
- Repro steps and logs (INFO level)

## Deployment posture

