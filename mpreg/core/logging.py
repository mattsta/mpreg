"""Central logging configuration helpers for MPREG."""

from __future__ import annotations

import sys
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any, TextIO

from loguru import logger

from mpreg.core.native_codec import dumps_text

DEFAULT_LOG_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | "
    "{name}:{function}:{line} - {message}"
)

def _json_sink(message: Any) -> None:
    """Emit a single JSON line for structured logging sinks."""
    record = message.record
    payload: dict[str, Any] = {
        "ts": record["time"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "level": record["level"].name,
        "logger": record["name"],
        "function": record["function"],
        "line": record["line"],
        "message": record["message"],
    }
    extra = record.get("extra") or {}
    cleaned = {
        k: v
        for k, v in extra.items()
        if not str(k).startswith("_") and k not in {"serialized"}
    }
    if cleaned:
        payload["extra"] = cleaned
    if record["exception"] is not None:
        payload["exception"] = str(record["exception"])
    sys.stderr.write(dumps_text(payload) + "\n")
    sys.stderr.flush()

def configure_logging(
    level: str,
    *,
    debug_scopes: Iterable[str] = (),
    colorize: bool = False,
    json_logs: bool = False,
    sink: TextIO | None = None,
) -> tuple[int, ...]:
    """Configure loguru with module-based debug filtering.

    When ``json_logs`` is True, each line is a JSON object suitable for
    aggregation (CloudWatch, Loki, etc.). Human format remains the default.
    """
    logger.remove()
    target = sink if sink is not None else sys.stderr
    handler_ids: list[int] = []

    if json_logs and sink is None:
        handler_ids.append(
            logger.add(
                _json_sink,
                level=level,
                colorize=False,
            )
        )
    elif json_logs:
        handler_ids.append(
            logger.add(
                target,
                level=level,
                colorize=False,
                serialize=True,
            )
        )
    else:
        handler_ids.append(
            logger.add(
                target,
                level=level,
                format=DEFAULT_LOG_FORMAT,
                colorize=colorize,
            )
        )

    level_upper = level.upper()
    scopes = tuple(scope.strip() for scope in debug_scopes if scope.strip())
    if scopes and level_upper != "DEBUG":

        def _debug_filter(record: object) -> bool:
            if not isinstance(record, Mapping):
                return False
            level_obj = record.get("level")
            if getattr(level_obj, "name", None) != "DEBUG":
                return False
            record_name = record.get("name", "")

            for scope in scopes:
                if record_name.startswith(scope):
                    return True
                if not scope.startswith("mpreg.") and record_name.startswith(
                    f"mpreg.{scope}"
                ):
                    return True
            return False

        if json_logs and sink is None:
            handler_ids.append(
                logger.add(
                    _json_sink,
                    level="DEBUG",
                    colorize=False,
                    filter=_debug_filter,
                )
            )
        elif json_logs:
            handler_ids.append(
                logger.add(
                    target,
                    level="DEBUG",
                    colorize=False,
                    serialize=True,
                    filter=_debug_filter,
                )
            )
        else:
            handler_ids.append(
                logger.add(
                    target,
                    level="DEBUG",
                    format=DEFAULT_LOG_FORMAT,
                    colorize=colorize,
                    filter=_debug_filter,
                )
            )

    return tuple(handler_ids)

def bind_trace_context(
    *,
    traceparent: str | None = None,
    correlation_id: str | None = None,
    request_u: str | None = None,
) -> object:
    """Return a loguru logger bound with correlation fields for JSON sinks.

    Prefer :func:`trace_context` as a context manager so nested ``logger``
    calls on the request path pick up the same extras via contextualize.
    """
    extra: dict[str, str] = {}
    if traceparent:
        extra["traceparent"] = str(traceparent)
    if correlation_id:
        extra["correlation_id"] = str(correlation_id)
    if request_u:
        extra["request_u"] = str(request_u)
    return logger.bind(**extra) if extra else logger

@contextmanager
def trace_context(
    *,
    traceparent: str | None = None,
    correlation_id: str | None = None,
    request_u: str | None = None,
) -> Iterator[None]:
    """Context manager that contextualizes loguru with correlation fields."""
    extra: dict[str, str] = {}
    if traceparent:
        extra["traceparent"] = str(traceparent)
    if correlation_id:
        extra["correlation_id"] = str(correlation_id)
    if request_u:
        extra["request_u"] = str(request_u)
    if not extra:
        yield
        return
    with logger.contextualize(**extra):
        yield

