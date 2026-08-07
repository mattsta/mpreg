# Fabric Route Security (Optional)

## Overview

Fabric route announcements can be signed to prevent spoofed or unauthorized
route advertisements. This is optional and disabled by default. When enabled,
nodes validate signatures against a configured key registry and reject
unsigned or invalid announcements.

Use this when:

- You operate multiple clusters with explicit trust boundaries.
- You need auditability for who advertised routes.
- You want controlled key rotation without downtime.

Avoid this when:

- The cluster is ephemeral and you prioritize minimal overhead.
- You already enforce strict mTLS and have no risk of unauthorized peers.

## Configuration

```python
from mpreg.core.config import MPREGSettings
from mpreg.fabric.route_keys import RouteKeyRegistry
from mpreg.fabric.route_security import RouteAnnouncementSigner, RouteSecurityConfig

registry = RouteKeyRegistry()
signer = RouteAnnouncementSigner.create()
registry.register_key(cluster_id="cluster-a", public_key=signer.public_key)

settings = MPREGSettings(
    fabric_route_security_config=RouteSecurityConfig(
        require_signatures=True,
        allow_unsigned=False,
        signature_algorithm="ed25519",
    ),
    fabric_route_signer=signer,
    fabric_route_key_registry=registry,
)
```

## Key Rotation

Rotate keys with overlap to avoid dropped routes during rollout:

```python
new_signer = RouteAnnouncementSigner.create()
registry.rotate_key(
    cluster_id="cluster-a",
    public_key=new_signer.public_key,
    overlap_seconds=60.0,
)
```

During the overlap window, both old and new keys validate announcements.
Once the overlap expires, only the new key remains active.

## Key Distribution Automation

To integrate with PKI/mTLS/HSM workflows, provide a key provider that refreshes
the registry on a schedule:

```python
from mpreg.fabric.route_keys import RouteKeyRegistry, RouteKeyProvider


class PKIKeyProvider(RouteKeyProvider):
    def refresh(self, registry: RouteKeyRegistry) -> None:
        # Load keys from your PKI, secrets manager, or mTLS bundle.
        registry.register_key(cluster_id="cluster-a", public_key=b"...")
```

```python
settings = MPREGSettings(
    fabric_route_key_registry=registry,
    fabric_route_key_provider=PKIKeyProvider(),
    fabric_route_key_refresh_interval_seconds=60.0,
)
```

The server will periodically call the provider and keep routes validated
without restarts.

### Gossip-Based Key Distribution

When a route key registry is configured, the control plane will also gossip the
active key set for the local cluster to peers. This keeps registries synchronized
without external coordination.

```python
settings = MPREGSettings(
    fabric_route_key_registry=registry,
    fabric_route_key_ttl_seconds=120.0,
    fabric_route_key_announce_interval_seconds=60.0,
)
```

Peers validate that the announcement's cluster_id matches the sender cluster
before applying it to their registry.

## Operational Notes

- If a registry is present, verification prefers registry keys and ignores
  announcement `public_key` fields unless the registry has no entry.
- Keep overlap windows short but long enough for propagation (1–2 gossip
  intervals is usually sufficient).
