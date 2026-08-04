"""W3C Trace Context helpers for fabric message metadata.

Full OpenTelemetry SDK export is optional; these helpers ensure multi-hop
fabric messages can carry ``traceparent`` / ``tracestate`` in
``MessageHeaders.metadata`` without requiring a hard OTel dependency.
"""

from __future__ import annotations

import secrets
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator, Mapping, MutableMapping

TRACEPARENT_KEY = "traceparent"
TRACESTATE_KEY = "tracestate"

# OBS-T10-05: request-scoped W3C parent so fabric hops continue RPC ingress.
_current_traceparent: ContextVar[str | None] = ContextVar(
    "mpreg_current_traceparent", default=None
)
_current_tracestate: ContextVar[str | None] = ContextVar(
    "mpreg_current_tracestate", default=None
)

def generate_trace_id() -> str:
    """Return a 16-byte trace id as 32 lowercase hex characters."""
    return secrets.token_hex(16)

def generate_span_id() -> str:
    """Return an 8-byte span id as 16 lowercase hex characters."""
    return secrets.token_hex(8)

def generate_traceparent(*, sampled: bool = True) -> str:
    """Build a W3C ``traceparent`` header value (version 00)."""
    flags = "01" if sampled else "00"
    return f"00-{generate_trace_id()}-{generate_span_id()}-{flags}"

def extract_traceparent(metadata: Mapping[str, Any] | None) -> str | None:
    if not metadata:
        return None
    value = metadata.get(TRACEPARENT_KEY)
    if value is None:
        return None
    text = str(value).strip()
    return text or None

def ensure_traceparent(metadata: MutableMapping[str, Any] | None = None) -> str:
    """Return existing traceparent or create one; mutate metadata when provided."""
    meta: MutableMapping[str, Any]
    if metadata is None:
        meta = {}
    else:
        meta = metadata
    existing = extract_traceparent(meta)
    if existing:
        return existing
    tp = generate_traceparent()
    meta[TRACEPARENT_KEY] = tp
    return tp

def get_current_traceparent() -> str | None:
    """Return the task-local ingress/outbound traceparent if bound."""
    return _current_traceparent.get()

def get_current_tracestate() -> str | None:
    return _current_tracestate.get()

@contextmanager
def bind_current_trace(
    traceparent: str | None = None,
    *,
    tracestate: str | None = None,
) -> Iterator[None]:
    """Bind W3C fields for the duration of an RPC/fabric request (OBS-T10-05)."""
    token_tp = _current_traceparent.set(traceparent)
    token_ts = _current_tracestate.set(tracestate)
    try:
        yield
    finally:
        _current_traceparent.reset(token_tp)
        _current_tracestate.reset(token_ts)

def inject_trace_metadata(
    metadata: MutableMapping[str, Any] | None = None,
    *,
    traceparent: str | None = None,
    tracestate: str | None = None,
) -> dict[str, Any]:
    """Return a metadata dict with W3C trace fields injected.

    Preference order: explicit arg → existing metadata → task-local bind
    (RPC ingress) → freshly generated parent.
    """
    result: dict[str, Any] = dict(metadata or {})
    tp = (
        traceparent
        or extract_traceparent(result)
        or _current_traceparent.get()
        or generate_traceparent()
    )
    result[TRACEPARENT_KEY] = tp
    ts = tracestate if tracestate is not None else _current_tracestate.get()
    if ts is not None:
        result[TRACESTATE_KEY] = ts
    elif TRACESTATE_KEY not in result:
        # Leave tracestate absent unless provided — valid per W3C.
        pass
    return result
