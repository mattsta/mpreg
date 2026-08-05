# signed_route_border (L3)

## Story

Border cluster accepts only signed routes, filters neighbors by tags, survives key rotation.

## Lesson

Fabric route security + neighbor policy under live gossip.

## Run

```bash
uv run mpreg-example run signed_route_border
```

## What it proves

- Signed B route accepted
- Untagged C blocked
- Gold-tagged C accepted
- B key rotation keeps route

## Architecture

```text
A (require sigs + policy) ← B (signer) / C (signer+tags)
```

## Non-claims

- Not a full PKI story.
- Not multi-hop AS-path security.

## Production exit ramp

- Supersedes fabric_route_security_demo.py
- See FABRIC route security docs
