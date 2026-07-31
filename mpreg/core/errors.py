"""Structured MPREG error codes for machine-readable client handling.

Numeric codes extend the historical CommandNotFound (1001) range.
Clients should branch on ``rpc_error.code`` / ``MpregError.code``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

from mpreg.core.model import MPREGException, RPCError

class MpregErrorCode(IntEnum):
    """Stable public error codes (do not renumber)."""

    UNKNOWN = -1
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
    INTERNAL = 1099

_DEFAULT_MESSAGES: dict[MpregErrorCode, str] = {
    MpregErrorCode.UNKNOWN: "Unknown error",
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
    MpregErrorCode.INTERNAL: "Internal error",
}

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
        enum_code = (
            code if isinstance(code, MpregErrorCode) else MpregErrorCode(int(code))
        )
        default_retryable = enum_code in {
            MpregErrorCode.TIMEOUT,
            MpregErrorCode.UNAVAILABLE,
            MpregErrorCode.ROUTE_NOT_FOUND,
        }
        return cls(
            code=int(enum_code),
            message=message or _DEFAULT_MESSAGES.get(enum_code, "Error"),
            details=details,
            retryable=default_retryable if retryable is None else retryable,
            context=dict(context),
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

def map_exception(exc: BaseException) -> MpregError | None:
    """Best-effort map of known exceptions to structured errors."""
    if isinstance(exc, MpregError):
        return exc
    if isinstance(exc, MPREGException):
        code = getattr(exc.rpc_error, "code", -1)
        return MpregError(
            code=int(code) if code is not None else -1,
            message=str(getattr(exc.rpc_error, "message", exc)),
            details=str(getattr(exc.rpc_error, "details", None) or exc),
        )
    name = type(exc).__name__
    text = str(exc)
    if name == "CommandNotFoundException" or "Command not found" in text:
        return command_not_found(getattr(exc, "command_name", text))
    if isinstance(exc, TimeoutError) or name in {"TimeoutError", "asyncio.TimeoutError"}:
        return MpregError.of(MpregErrorCode.TIMEOUT, details=text, retryable=True)
    if isinstance(exc, ConnectionError) or name in {
        "ConnectionError",
        "ConnectionResetError",
        "BrokenPipeError",
    }:
        return MpregError.of(MpregErrorCode.UNAVAILABLE, details=text, retryable=True)
    if "Command not found" in text or "command not found" in text.lower():
        return command_not_found(text)
    if "hop budget" in text.lower():
        return hop_budget_exceeded(0, details=text)
    if "policy" in text.lower() and "denied" in text.lower():
        return policy_denied(text)
    if "no route" in text.lower():
        return route_not_found(text)
    return None
