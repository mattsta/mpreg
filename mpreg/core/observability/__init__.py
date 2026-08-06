"""Observability helpers (trace context, SLO constants, future OTel bridges)."""

from __future__ import annotations

from .slo import GOLDEN_SIGNALS, GoldenSignal, prometheus_alert_rules_yaml
from .trace_context import (
    TRACEPARENT_KEY,
    TRACESTATE_KEY,
    bind_current_trace,
    ensure_traceparent,
    extract_traceparent,
    generate_traceparent,
    get_current_traceparent,
    inject_trace_metadata,
)

__all__ = [
    "GOLDEN_SIGNALS",
    "TRACEPARENT_KEY",
    "TRACESTATE_KEY",
    "GoldenSignal",
    "bind_current_trace",
    "ensure_traceparent",
    "extract_traceparent",
    "generate_traceparent",
    "get_current_traceparent",
    "inject_trace_metadata",
    "prometheus_alert_rules_yaml",
]
