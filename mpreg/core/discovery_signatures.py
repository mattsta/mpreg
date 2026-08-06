"""Optional HMAC signatures for discovery summary exports.

Enable with a shared secret to detect tampering of summary payloads in
untrusted multi-tenant fabrics. Not a PKI substitute — use route security
for announcement authenticity.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping
from typing import Any

from mpreg.core.native_codec import canonical_dumps

SIGNATURE_KEY = "mpreg_summary_sig"
SIGNATURE_ALG = "hmac-sha256"

def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    body = {k: v for k, v in payload.items() if k != SIGNATURE_KEY}
    return canonical_dumps(body)

def sign_summary(payload: Mapping[str, Any], secret: str) -> dict[str, Any]:
    """Return a copy of payload with HMAC signature field attached."""
    if not secret:
        return dict(payload)
    digest = hmac.new(
        secret.encode("utf-8"), _canonical_bytes(payload), hashlib.sha256
    ).hexdigest()
    out = dict(payload)
    out[SIGNATURE_KEY] = f"{SIGNATURE_ALG}:{digest}"
    return out

def verify_summary(payload: Mapping[str, Any], secret: str) -> bool:
    """Verify HMAC on payload; True if valid or secret empty (verification off)."""
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
