"""Structured JSON logging configuration."""

from __future__ import annotations

import io
import json

from loguru import logger

from mpreg.core.logging import configure_logging


def test_configure_logging_json_emits_parseable_lines() -> None:
    # Capture via a custom path: configure_logging with json uses stderr sink.
    # We re-bind by temporarily swapping; call logger after configure.
    configure_logging("INFO", json_logs=True)
    # loguru custom sink writes to real stderr; exercise serialize path with TextIO
    buf = io.StringIO()
    configure_logging("INFO", json_logs=True, sink=buf)
    logger.info("hello structured {}", "world")
    logger.bind(request_id="r1").info("with extra")
    text = buf.getvalue()
    lines = [ln for ln in text.strip().splitlines() if ln.strip()]
    assert lines, "expected JSON log lines"
    # loguru serialize=True wraps record; accept either our shape or loguru's
    parsed_any = False
    for ln in lines:
        obj = json.loads(ln)
        assert isinstance(obj, dict)
        parsed_any = True
        # serialize=True uses nested "record" or top-level fields
        if "record" in obj:
            assert "message" in obj["record"] or "msg" in obj["record"]
        else:
            assert "message" in obj or "text" in obj or "msg" in obj
    assert parsed_any


def test_configure_logging_human_default() -> None:
    buf = io.StringIO()
    configure_logging("INFO", json_logs=False, sink=buf, colorize=False)
    logger.info("plain message")
    out = buf.getvalue()
    assert "plain message" in out
    assert not out.strip().startswith("{")
