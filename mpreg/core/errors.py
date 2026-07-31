"""Structured MPREG error codes for machine-readable client handling.

Numeric codes in the 1000+ range are the **only** stable public RPC error
namespace. Servers must emit via helpers in this module (never bare integers).
Clients should branch on ``rpc_error.code`` / ``MpregError.code``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Final

from mpreg.core.model import MPREGException, RPCError

class MpregErrorCode(IntEnum):
    """Stable public error codes (do not renumber existing members)."""

    UNKNOWN = -1
    # Reserved / protocol
    PROTOCOL = 1000
    COMMAND_NOT_FOUND = 1001
    VERSION_MISMATCH = 1002
    HOP_BUDGET_EXCEEDED = 1003
    POLICY_DENIED = 1004
    ROUTE_NOT_FOUND = 1005
    TIMEOUT = 1006
    UNAVAILABLE = 1007
    INVALID_ARGUMENT = 1008
    AUTH_REQUIRED = 1009
    AUTH_FAILED = 1010
    # Discovery / control plane (1100+)
    DISCOVERY_ACCESS_DENIED = 1101
    DISCOVERY_RATE_LIMITED = 1102
    # Catch-all
    INTERNAL = 1099

PUBLIC_ERROR_CODES: Final[frozenset[int]] = frozenset(
    int(c) for c in MpregErrorCode if int(c) >= 1000
)

_DEFAULT_MESSAGES: dict[MpregErrorCode, str] = {
    MpregErrorCode.UNKNOWN: "Unknown error",
    MpregErrorCode.PROTOCOL: "Protocol error",
    MpregErrorCode.COMMAND_NOT_FOUND: "Command not found",
    MpregErrorCode.VERSION_MISMATCH: "Function version constraint not satisfied",
    MpregErrorCode.HOP_BUDGET_EXCEEDED: "Fabric hop budget exceeded",
    MpregErrorCode.POLICY_DENIED: "Namespace or routing policy denied the request",
    MpregErrorCode.ROUTE_NOT_FOUND: "No route to target",
    MpregErrorCode.TIMEOUT: "Operation timed out",
    MpregErrorCode.UNAVAILABLE: "Service temporarily unavailable",
    MpregErrorCode.INVALID_ARGUMENT: "Invalid argument",
    MpregErrorCode.AUTH_REQUIRED: "Authentication required",
    MpregErrorCode.AUTH_FAILED: "Authentication failed",
    MpregErrorCode.DISCOVERY_ACCESS_DENIED: "Discovery access denied",
    MpregErrorCode.DISCOVERY_RATE_LIMITED: "Discovery rate limit exceeded",
    MpregErrorCode.INTERNAL: "Internal error",
}

_DEFAULT_RETRYABLE: frozenset[MpregErrorCode] = frozenset(
    {
        MpregErrorCode.TIMEOUT,
        MpregErrorCode.UNAVAILABLE,
        MpregErrorCode.ROUTE_NOT_FOUND,
        MpregErrorCode.DISCOVERY_RATE_LIMITED,
    }
)

# Historical bare integers that collided with the public namespace.
_LEGACY_WIRE_REMAP: dict[tuple[int, str], MpregErrorCode] = {
    # (code, message_substring_lower) → new code
}

def _coerce_enum(code: MpregErrorCode | int) -> MpregErrorCode:
    if isinstance(code, MpregErrorCode):
        return code
    try:
        return MpregErrorCode(int(code))
    except ValueError:
        return MpregErrorCode.UNKNOWN

@dataclass
class MpregError(MPREGException):
    """Structured application error with stable numeric code."""

    code: int = field(default=int(MpregErrorCode.UNKNOWN))
    message: str = field(default="Unknown error")
    details: str | None = field(default=None)
    retryable: bool = field(default=False)
    context: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.rpc_error = RPCError(
            code=int(self.code),
            message=self.message,
            details=self.details,
        )
        self.args = (self.details or self.message,)

    @classmethod
    def of(
        cls,
        code: MpregErrorCode | int,
        *,
        details: str | None = None,
        message: str | None = None,
        retryable: bool | None = None,
        **context: Any,
    ) -> MpregError:
        enum_code = _coerce_enum(code)
        default_retryable = enum_code in _DEFAULT_RETRYABLE
        return cls(
            code=int(enum_code),
            message=message or _DEFAULT_MESSAGES.get(enum_code, "Error"),
            details=details,
            retryable=default_retryable if retryable is None else retryable,
            context=dict(context),
        )

    def to_rpc_error(self) -> RPCError:
        """Canonical wire ``RPCError`` (prefer this over constructing RPCError)."""
        return self.rpc_error

def rpc_error(
    code: MpregErrorCode | int,
    *,
    message: str | None = None,
    details: str | None = None,
    retryable: bool | None = None,
    **context: Any,
) -> RPCError:
    """Build a wire ``RPCError`` from the public code namespace only."""
    return MpregError.of(
        code, message=message, details=details, retryable=retryable, **context
    ).to_rpc_error()

def as_exception(
    code: MpregErrorCode | int,
    *,
    message: str | None = None,
    details: str | None = None,
    retryable: bool | None = None,
    **context: Any,
) -> MpregError:
    """Raise-ready structured exception (alias of ``MpregError.of``)."""
    return MpregError.of(
        code, message=message, details=details, retryable=retryable, **context
    )

def command_not_found(name: str, **context: Any) -> MpregError:
    return MpregError.of(
        MpregErrorCode.COMMAND_NOT_FOUND,
        details=f"Command not found: {name}",
        command_name=name,
        **context,
    )

def version_mismatch(function_id: str, constraint: str, **context: Any) -> MpregError:
    return MpregError.of(
        MpregErrorCode.VERSION_MISMATCH,
        details=f"No endpoint for {function_id} matching {constraint}",
        function_id=function_id,
        constraint=constraint,
        **context,
    )

def hop_budget_exceeded(hop_budget: int, **context: Any) -> MpregError:
    return MpregError.of(
        MpregErrorCode.HOP_BUDGET_EXCEEDED,
        details=f"Hop budget {hop_budget} exceeded",
        hop_budget=hop_budget,
        **context,
    )

def policy_denied(reason: str, **context: Any) -> MpregError:
    return MpregError.of(
        MpregErrorCode.POLICY_DENIED,
        details=reason,
        **context,
    )

def route_not_found(target: str, **context: Any) -> MpregError:
    return MpregError.of(
        MpregErrorCode.ROUTE_NOT_FOUND,
        details=f"No route to {target}",
        target=target,
        **context,
    )

def timeout_error(details: str, **context: Any) -> MpregError:
    return MpregError.of(
        MpregErrorCode.TIMEOUT,
        details=details,
        retryable=True,
        **context,
    )

def unavailable(details: str, **context: Any) -> MpregError:
    return MpregError.of(
        MpregErrorCode.UNAVAILABLE,
        details=details,
        retryable=True,
        **context,
    )

def invalid_argument(details: str, **context: Any) -> MpregError:
    return MpregError.of(
        MpregErrorCode.INVALID_ARGUMENT,
        details=details,
        **context,
    )

def internal_error(details: str | None = None, **context: Any) -> MpregError:
    return MpregError.of(
        MpregErrorCode.INTERNAL,
        details=details,
        message="Internal server error",
        **context,
    )

def protocol_error(details: str, **context: Any) -> MpregError:
    return MpregError.of(
        MpregErrorCode.PROTOCOL,
        details=details,
        **context,
    )

def discovery_access_denied(details: str, **context: Any) -> MpregError:
    return MpregError.of(
        MpregErrorCode.DISCOVERY_ACCESS_DENIED,
        details=details,
        message="discovery_access_denied",
        **context,
    )

def discovery_rate_limited(details: str, **context: Any) -> MpregError:
    return MpregError.of(
        MpregErrorCode.DISCOVERY_RATE_LIMITED,
        details=details,
        message="discovery_rate_limited",
        retryable=True,
        **context,
    )

def _legacy_remap_code(code: int, message: str, details: str) -> int:
    """Map historical colliding wire codes onto the public namespace."""
    blob = f"{message} {details}".lower()
    if code == 1004 and ("timeout" in blob or "timed out" in blob):
        return int(MpregErrorCode.TIMEOUT)
    if code == 1004 and ("invalid" in blob or "role" in blob):
        return int(MpregErrorCode.INVALID_ARGUMENT)
    if code == 1002 and ("internal" in blob or "traceback" in details.lower()):
        return int(MpregErrorCode.INTERNAL)
    if code == 1003 and (
        "execution failed" in blob
        or "traceback" in details.lower()
        or "departed" in blob
    ):
        if "departed" in blob:
            return int(MpregErrorCode.UNAVAILABLE)
        return int(MpregErrorCode.INTERNAL)
    if code == 1000:
        return int(MpregErrorCode.PROTOCOL)
    if code == 403 or (code == 1004 and "denied" in blob):
        if "discovery" in blob or "access" in blob:
            return int(MpregErrorCode.DISCOVERY_ACCESS_DENIED)
    if code == 429:
        return int(MpregErrorCode.DISCOVERY_RATE_LIMITED)
    # Known public codes pass through.
    if code in PUBLIC_ERROR_CODES or code == int(MpregErrorCode.UNKNOWN):
        return code
    # HTTP-ish leftovers
    if code == 401:
        return int(MpregErrorCode.AUTH_REQUIRED)
    if code == 500:
        return int(MpregErrorCode.INTERNAL)
    return code

def map_exception(exc: BaseException) -> MpregError:
    """Map any exception to a structured ``MpregError`` (never returns None)."""
    if isinstance(exc, MpregError):
        return exc
    if isinstance(exc, MPREGException):
        raw_code = getattr(exc.rpc_error, "code", -1)
        message = str(getattr(exc.rpc_error, "message", exc) or exc)
        details = str(getattr(exc.rpc_error, "details", None) or "")
        code = _legacy_remap_code(
            int(raw_code) if raw_code is not None else -1, message, details
        )
        enum = _coerce_enum(code)
        retryable = enum in _DEFAULT_RETRYABLE
        return MpregError(
            code=int(enum) if enum is not MpregErrorCode.UNKNOWN else code,
            message=message
            if message
            else _DEFAULT_MESSAGES.get(enum, "Error"),
            details=details or None,
            retryable=retryable,
        )
    name = type(exc).__name__
    text = str(exc)
    if name == "CommandNotFoundException" or "Command not found" in text:
        return command_not_found(getattr(exc, "command_name", text))
    if isinstance(exc, TimeoutError) or name in {
        "TimeoutError",
        "asyncio.TimeoutError",
    }:
        return timeout_error(text)
    if isinstance(exc, ConnectionError) or name in {
        "ConnectionError",
        "ConnectionResetError",
        "BrokenPipeError",
    }:
        return unavailable(text)
    if "Command not found" in text or "command not found" in text.lower():
        return command_not_found(text)
    if "hop budget" in text.lower():
        return hop_budget_exceeded(0, details=text)
    if "policy" in text.lower() and "denied" in text.lower():
        return policy_denied(text)
    if "no route" in text.lower():
        return route_not_found(text)
    if "rate limit" in text.lower() or "rate_limited" in text.lower():
        return discovery_rate_limited(text)
    return internal_error(text, exception_type=name)

def error_code_catalog() -> list[dict[str, Any]]:
    """Language-neutral catalog of public error codes."""
    rows: list[dict[str, Any]] = []
    for code in sorted(MpregErrorCode, key=lambda c: int(c)):
        if int(code) < 1000 and code is not MpregErrorCode.UNKNOWN:
            continue
        rows.append(
            {
                "code": int(code),
                "name": code.name,
                "message": _DEFAULT_MESSAGES.get(code, ""),
                "retryable": code in _DEFAULT_RETRYABLE,
            }
        )
    return rows
