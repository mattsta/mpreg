"""Central logging configuration helpers for MPREG."""

from __future__ import annotations

import sys
from collections.abc import Iterable, Mapping

from loguru import logger

DEFAULT_LOG_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | "
    "{name}:{function}:{line} - {message}"
)


def configure_logging(
    level: str,
    *,
    debug_scopes: Iterable[str] = (),
    colorize: bool = False,
) -> tuple[int, ...]:
    """Configure loguru with module-based debug filtering."""
    logger.remove()

    handler_ids: list[int] = [
        logger.add(
            sys.stderr,
            level=level,
            format=DEFAULT_LOG_FORMAT,
            colorize=colorize,
        )
    ]

    level_upper = level.upper()
    scopes = tuple(scope.strip() for scope in debug_scopes if scope.strip())
    if scopes and level_upper != "DEBUG":

        def _debug_filter(record: object) -> bool:
            if not isinstance(record, Mapping):
                return False
            level = record.get("level")
            if getattr(level, "name", None) != "DEBUG":
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

        handler_ids.append(
            logger.add(
                sys.stderr,
                level="DEBUG",
                format=DEFAULT_LOG_FORMAT,
                colorize=colorize,
                filter=_debug_filter,
            )
        )

    return tuple(handler_ids)
