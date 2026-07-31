"""Observability helpers (trace context, SLO constants, future OTel bridges)."""

from __future__ import annotations

from .slo import GOLDEN_SIGNALS, GoldenSignal, prometheus_alert_rules_yaml
from .trace_context import (
    TRACEPARENT_KEY,
    TRACESTATE_KEY,
    ensure_traceparent,
    extract_traceparent,
    generate_traceparent,
    inject_trace_metadata,
)

__all__ = [
    "TRACEPARENT_KEY",
    "TRACESTATE_KEY",
    "ensure_traceparent",
    "extract_traceparent",
    "generate_traceparent",
    "inject_trace_metadata",
    "GOLDEN_SIGNALS",
    "GoldenSignal",
    "prometheus_alert_rules_yaml",
]
