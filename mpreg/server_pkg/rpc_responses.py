"""Canonical builders for wire RPCResponse error payloads.

Keeps MPREGServer handlers free of ad-hoc RPCError construction. All codes
flow through ``mpreg.core.errors``.
"""

from __future__ import annotations

from typing import Any

from mpreg.core.errors import (
    MpregError,
    internal_error,
    invalid_argument,
    policy_denied,
    protocol_error,
    timeout_error,
    unavailable,
)
from mpreg.core.model import RPCResponse

def w3c_trace_fields(
    *,
    traceparent: str | None = None,
    tracestate: str | None = None,
    headers: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build kwargs that echo W3C trace onto an RPCResponse (Phase P / OBS).

    Preference: explicit args → task-local bind (RPC ingress) → headers bag.
    Empty dict when nothing is available (backward compatible omission).
    """
    from mpreg.core.observability.trace_context import (
        TRACEPARENT_KEY,
        TRACESTATE_KEY,
        extract_traceparent,
        get_current_traceparent,
        get_current_tracestate,
    )

    tp = traceparent
    ts = tracestate
    hdrs: dict[str, Any] = dict(headers or {})

    if not tp:
        tp = extract_traceparent(hdrs) or get_current_traceparent()
    if ts is None:
        if TRACESTATE_KEY in hdrs and hdrs[TRACESTATE_KEY] is not None:
            ts = str(hdrs[TRACESTATE_KEY])
        else:
            ts = get_current_tracestate()

    out: dict[str, Any] = {}
    if tp:
        out["traceparent"] = str(tp)
        hdrs.setdefault(TRACEPARENT_KEY, str(tp))
    if ts is not None:
        out["tracestate"] = str(ts)
        hdrs.setdefault(TRACESTATE_KEY, str(ts))
    if hdrs:
        out["headers"] = hdrs
    return out

def error_response(
    u: str,
    err: MpregError,
    *,
    traceparent: str | None = None,
    tracestate: str | None = None,
    headers: dict[str, Any] | None = None,
) -> RPCResponse:
    """Build an RPCResponse carrying a structured error (with W3C echo)."""
    return RPCResponse(
        r=None,
        error=err.rpc_error,
        u=u,
        **w3c_trace_fields(
            traceparent=traceparent, tracestate=tracestate, headers=headers
        ),
    )

def timeout_response(u: str, details: str, **ctx: object) -> RPCResponse:
    return error_response(u, timeout_error(details, **ctx))

def internal_response(u: str, details: str | None = None) -> RPCResponse:
    return error_response(u, internal_error(details))

def protocol_response(u: str, details: str) -> RPCResponse:
    return error_response(u, protocol_error(details))

def unavailable_response(u: str, details: str) -> RPCResponse:
    return error_response(u, unavailable(details))

def policy_response(u: str, details: str) -> RPCResponse:
    return error_response(u, policy_denied(details))

def invalid_arg_response(u: str, details: str) -> RPCResponse:
    return error_response(u, invalid_argument(details))

def from_exception(u: str, exc: BaseException) -> RPCResponse:
    from mpreg.core.errors import map_exception

    return error_response(u, map_exception(exc))
