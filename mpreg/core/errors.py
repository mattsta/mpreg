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

# ---------------------------------------------------------------------------
# Exception categories + operator-correct logging
# ---------------------------------------------------------------------------
# BLE001 forbids bare ``except Exception``. Do **not** paste a 7-type tuple at
# every call site. Use the shared groups below — and log with the matching
# severity so operators can tell "expected failure mode" from "bug".
#
# Categories
# ~~~~~~~~~~
# EXPECTED_EXCEPTIONS
#     True operational conditions: I/O, network, timeouts. These are *error
#     conditions expressed as exceptions*, not unknown faults. Log with
#     ``logger.error`` / ``logger.warning`` (message only — no stack dump).
#
# CONDITION_EXCEPTIONS
#     Explicit validation / control-flow failures (bad args, missing key,
#     RuntimeError used as a typed failure). Usually message-only logging.
#     If a site treats these as programmer bugs, pass ``expected=False`` to
#     :func:`log_caught_exception`.
#
# OPERATIONAL_EXCEPTIONS
#     Catch-group for boundary handlers that degrade/continue:
#     ``EXPECTED_EXCEPTIONS + CONDITION_EXCEPTIONS``. Prefer this over bare
#     ``Exception``. After catching, call :func:`log_caught_exception` (or
#     dual-catch EXPECTED then ``Exception`` with ``logger.exception``).
#
# Unknown / unexpected
#     Anything else that a supervisor still absorbs **must** use
#     ``logger.exception(...)`` (or :func:`log_caught_exception` with
#     ``expected=False``) so the stack reaches operators. Prefer re-raising
#     when the process cannot safely continue.
#
# Public RPC surfaces still use :class:`MpregError` + :func:`map_exception`.
# Never put CancelledError / KeyboardInterrupt / SystemExit in these tuples.
# ---------------------------------------------------------------------------

EXPECTED_EXCEPTIONS: tuple[type[BaseException], ...] = (
    OSError,  # includes many socket/file failures; ConnectionError is separate below
    TimeoutError,
    ConnectionError,
)

CONDITION_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ValueError,
    TypeError,
    KeyError,
    RuntimeError,
)

OPERATIONAL_EXCEPTIONS: tuple[type[BaseException], ...] = (
    *EXPECTED_EXCEPTIONS,
    *CONDITION_EXCEPTIONS,
)


def with_operational(
    *extra: type[BaseException],
) -> tuple[type[BaseException], ...]:
    """Compose :data:`OPERATIONAL_EXCEPTIONS` with call-site-specific types.

    Use when a boundary must also catch domain errors without re-listing the
    operational set::

        except with_operational(TransportError, MpregError) as exc:
            log_caught_exception(logger, "boundary failed", exc)
    """
    if not extra:
        return OPERATIONAL_EXCEPTIONS
    seen: set[type[BaseException]] = set()
    out: list[type[BaseException]] = []
    for cls in (*OPERATIONAL_EXCEPTIONS, *extra):
        if cls in seen:
            continue
        seen.add(cls)
        out.append(cls)
    return tuple(out)


def is_expected_failure(exc: BaseException) -> bool:
    """True when *exc* is an ordinary operational/condition failure (no bug dump)."""
    if isinstance(exc, (*EXPECTED_EXCEPTIONS, *CONDITION_EXCEPTIONS)):
        return True
    # Structured app errors are intentional wire/control outcomes.
    return type(exc).__name__ == "MpregError" or isinstance(exc, MPREGException)


def log_caught_exception(
    log: Any,
    message: str,
    exc: BaseException | None = None,
    *,
    expected: bool | None = None,
    level: str = "error",
) -> None:
    """Log a caught exception with operator-correct verbosity.

    * **expected** (default: auto via :func:`is_expected_failure`):
      message-only ``error`` / ``warning`` — no stack trace.
    * **unexpected** (``expected=False``, or auto-classify miss):
      ``log.exception(...)`` — **includes stack trace** (loguru/stdlib).

    Call this from ``except`` handlers instead of ad-hoc ``log.error(f"...{e}")``
    so unknown faults never silently lose their traceback.
    """
    import sys

    active: BaseException | None = exc if exc is not None else sys.exc_info()[1]
    if expected is None:
        expected = bool(active is not None and is_expected_failure(active))

    # Never let logging itself take down a supervisor.
    try:
        if expected:
            text = f"{message}: {active}" if active is not None else message
            if level == "warning" and hasattr(log, "warning"):
                log.warning(text)
            elif level == "debug" and hasattr(log, "debug"):
                log.debug(text)
            elif level == "info" and hasattr(log, "info"):
                log.info(text)
            else:
                log.error(text)
            return

        # Unexpected path — prefer stack-bearing exception() API.
        if hasattr(log, "opt") and callable(log.opt) and active is not None:
            # loguru: attach exception even if we were re-bound.
            log.opt(exception=active).error(message)
        elif hasattr(log, "exception") and callable(log.exception):
            log.exception(message)
        else:
            log.error("%s: %r", message, active)
    except OPERATIONAL_EXCEPTIONS:
        pass


def dual_catch_log(
    log: Any,
    message: str,
    exc: BaseException,
    *,
    level: str = "error",
) -> None:
    """Log *exc* with the dual-catch severity policy (operator template).

    Equivalent to calling :func:`log_caught_exception` with auto-classification.
    Prefer the explicit dual-arm pattern at RPC boundaries::

        except OPERATIONAL_EXCEPTIONS as exc:
            log_caught_exception(log, message, exc)
        except Exception as exc:
            log_caught_exception(log, message, exc, expected=False)

    Use this helper when a single ``except Exception`` arm must still classify.
    """
    log_caught_exception(log, message, exc, level=level)


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
    # Modality refuse (ERG-T10-06) — not available as production semantics
    UNSUPPORTED_DELIVERY = 1011  # EXACTLY_ONCE etc. refused
    UNSUPPORTED_CONSISTENCY = 1012  # STRONG etc. residual-free refused
    # Fabric routing (fail-closed hop advancement)
    ROUTE_LOOP = 1013
    # 1014 reserved / unused
    # ConsistencyLevel.STRONG operational (majority-commit barrier)
    INSUFFICIENT_QUORUM = 1015
    QUORUM_TIMEOUT = 1016
    STRONG_CONFLICT = 1017
    STRONG_PENDING_FULL = 1018
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
    MpregErrorCode.UNSUPPORTED_DELIVERY: (
        "Delivery guarantee not supported (EXACTLY_ONCE is refused)"
    ),
    MpregErrorCode.UNSUPPORTED_CONSISTENCY: (
        "Consistency level not supported (STRONG disabled or not implemented)"
    ),
    MpregErrorCode.ROUTE_LOOP: "Fabric routing path loop detected",
    MpregErrorCode.INSUFFICIENT_QUORUM: (
        "STRONG put could not form a majority replica set"
    ),
    MpregErrorCode.QUORUM_TIMEOUT: (
        "STRONG put timed out waiting for prepare/commit quorum"
    ),
    MpregErrorCode.STRONG_CONFLICT: "STRONG put lost last-writer-wins conflict",
    MpregErrorCode.STRONG_PENDING_FULL: "STRONG pending prepare slots exhausted",
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
        MpregErrorCode.QUORUM_TIMEOUT,
        MpregErrorCode.INSUFFICIENT_QUORUM,
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
            retryable=bool(self.retryable),
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


def route_loop_detected(**context: Any) -> MpregError:
    """Fail-closed when a node already appears on the fabric routing path."""
    return MpregError.of(
        MpregErrorCode.ROUTE_LOOP,
        details=context.pop("details", "Routing path loop detected"),
        **context,
    )


def policy_denied(reason: str, **context: Any) -> MpregError:
    return MpregError.of(
        MpregErrorCode.POLICY_DENIED,
        details=reason,
        **context,
    )


def route_not_found(target: str, **context: Any) -> MpregError:
    """No fabric/cluster route to *target*.

    Phase I F13: peers alone do not create a cross-cluster fabric bridge.
    The details string names the missing route and points operators at
    fabric/cluster configuration rather than a bare "no route".
    """
    command_name = context.get("command_name")
    cmd_bit = f" for command '{command_name}'" if command_name else ""
    return MpregError.of(
        MpregErrorCode.ROUTE_NOT_FOUND,
        details=(
            f"No fabric route to cluster '{target}'{cmd_bit}. "
            "Peer gossip alone does not bridge clusters — configure fabric "
            "bridging / target_cluster routing (see multi_region_shop, "
            "plane_fabric)."
        ),
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
        wire_retryable = getattr(exc.rpc_error, "retryable", None)
        code = _legacy_remap_code(
            int(raw_code) if raw_code is not None else -1, message, details
        )
        enum = _coerce_enum(code)
        retryable = (
            bool(wire_retryable)
            if wire_retryable is not None
            else enum in _DEFAULT_RETRYABLE
        )
        return MpregError(
            code=int(enum) if enum is not MpregErrorCode.UNKNOWN else code,
            message=message or _DEFAULT_MESSAGES.get(enum, "Error"),
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
    if name == "UnsupportedDeliveryGuaranteeError" or "exactly_once" in text.lower():
        if "exactly" in text.lower() or name == "UnsupportedDeliveryGuaranteeError":
            return MpregError(
                code=int(MpregErrorCode.UNSUPPORTED_DELIVERY),
                message=_DEFAULT_MESSAGES[MpregErrorCode.UNSUPPORTED_DELIVERY],
                details=text or None,
                retryable=False,
            )
    if "consistencylevel.strong" in text.lower() or (
        "strong" in text.lower() and "not implemented" in text.lower()
    ):
        return MpregError(
            code=int(MpregErrorCode.UNSUPPORTED_CONSISTENCY),
            message=_DEFAULT_MESSAGES[MpregErrorCode.UNSUPPORTED_CONSISTENCY],
            details=text or None,
            retryable=False,
        )
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
