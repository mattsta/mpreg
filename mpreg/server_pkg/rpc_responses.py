"""Canonical builders for wire RPCResponse error payloads.

Keeps MPREGServer handlers free of ad-hoc RPCError construction. All codes
flow through ``mpreg.core.errors``.
"""

from __future__ import annotations

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

def error_response(
    u: str,
    err: MpregError,
) -> RPCResponse:
    """Build an RPCResponse carrying a structured error."""
    return RPCResponse(r=None, error=err.rpc_error, u=u)

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
