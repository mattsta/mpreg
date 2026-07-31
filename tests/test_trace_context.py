"""W3C trace context helpers and fabric header integration."""

from __future__ import annotations

from mpreg.core.observability.trace_context import (
    TRACEPARENT_KEY,
    ensure_traceparent,
    generate_traceparent,
    inject_trace_metadata,
)
from mpreg.fabric.message import MessageHeaders
from mpreg.fabric.message_codec import (
    message_headers_from_dict,
    message_headers_to_dict,
)

def test_generate_traceparent_shape() -> None:
    tp = generate_traceparent()
    parts = tp.split("-")
    assert parts[0] == "00"
    assert len(parts[1]) == 32
    assert len(parts[2]) == 16
    assert parts[3] in {"00", "01"}

def test_inject_and_ensure() -> None:
    meta: dict = {}
    tp = ensure_traceparent(meta)
    assert meta[TRACEPARENT_KEY] == tp
    assert ensure_traceparent(meta) == tp

def test_message_headers_with_trace_context() -> None:
    headers = MessageHeaders(correlation_id="c1")
    traced = headers.with_trace_context()
    assert traced.traceparent is not None
    assert TRACEPARENT_KEY in traced.metadata

def test_codec_accepts_top_level_traceparent() -> None:
    headers = message_headers_from_dict(
        {
            "correlation_id": "abc",
            "traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01",
        }
    )
    assert headers.traceparent is not None
    encoded = message_headers_to_dict(headers)
    assert "traceparent" in encoded["metadata"]

def test_inject_trace_metadata_preserves_existing() -> None:
    existing = generate_traceparent()
    out = inject_trace_metadata({"traceparent": existing, "x": 1})
    assert out["traceparent"] == existing
    assert out["x"] == 1
