"""Optional HMAC authenticity for fabric gossip envelopes.

Enable with ``fabric_gossip_require_hmac`` + ``fabric_gossip_hmac_secret``.
This authenticates the gossip *envelope* payload; it is not a substitute for
route announcement signing (``RouteSecurityConfig``).
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping
from typing import Any

from mpreg.core.native_codec import canonical_dumps

SIGNATURE_KEY = "mpreg_gossip_hmac"
SIGNATURE_ALG = "hmac-sha256"


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    body = {k: v for k, v in payload.items() if k != SIGNATURE_KEY}
    return canonical_dumps(body)


def sign_gossip_payload(payload: Mapping[str, Any], secret: str) -> dict[str, Any]:
    """Return a copy of payload with HMAC field attached."""
    if not secret:
        return dict(payload)
    digest = hmac.new(
        secret.encode("utf-8"), _canonical_bytes(payload), hashlib.sha256
    ).hexdigest()
    out = dict(payload)
    out[SIGNATURE_KEY] = f"{SIGNATURE_ALG}:{digest}"
    return out


def verify_gossip_payload(payload: Mapping[str, Any], secret: str) -> bool:
    """Verify HMAC on payload.

    When ``secret`` is empty, verification is off (returns True).
    When a secret is configured, missing/invalid HMAC fails closed.
    """
    if not secret:
        return True
    provided = payload.get(SIGNATURE_KEY)
    if not isinstance(provided, str) or ":" not in provided:
        return False
    alg, _, digest = provided.partition(":")
    if alg != SIGNATURE_ALG or not digest:
        return False
    expected = hmac.new(
        secret.encode("utf-8"), _canonical_bytes(payload), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, digest)


def envelope_dict_with_hmac(
    payload: Mapping[str, Any],
    *,
    require_hmac: bool,
    secret: str | None,
) -> dict[str, Any]:
    """Build a fabric-gossip wire dict, signing when required."""
    body = dict(payload)
    if require_hmac and secret:
        body = sign_gossip_payload(body, secret)
    return {"role": "fabric-gossip", "payload": body}


def accept_gossip_payload(
    payload: Mapping[str, Any],
    *,
    require_hmac: bool,
    secret: str | None,
) -> bool:
    """Return True if the inbound gossip payload is acceptable under policy."""
    if not require_hmac:
        return True
    if not secret:
        # Misconfiguration: require without secret → refuse all
        return False
    return verify_gossip_payload(payload, secret)
