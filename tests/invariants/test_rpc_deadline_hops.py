"""C1: Deadline remaining decrements across hops and fails closed."""

from __future__ import annotations

import pytest

from mpreg.core.errors import MpregError, MpregErrorCode
from mpreg.core.rpc_deadline import (
    DeadlineBudget,
    decrement_deadline_headers,
    headers_with_deadline_seconds,
)
from mpreg.fabric.message import MessageHeaders
from mpreg.server_pkg.rpc_handlers import RpcPlane


def _headers() -> MessageHeaders:
    return MessageHeaders(correlation_id="corr-1")


def test_stamp_and_decrement() -> None:
    h = headers_with_deadline_seconds(_headers(), 1.0)
    assert h.deadline_remaining_ms == pytest.approx(1000.0)
    h2 = decrement_deadline_headers(h, hop_latency_ms=250.0)
    assert h2.deadline_remaining_ms == pytest.approx(750.0)
    h3 = decrement_deadline_headers(h2, hop_latency_ms=800.0)
    assert h3.deadline_remaining_ms == pytest.approx(0.0)
    assert h3.deadline_exhausted()


def test_budget_raise_if_exhausted() -> None:
    budget = DeadlineBudget(remaining_ms=0.0, started_mono=0.0)
    with pytest.raises(MpregError) as ei:
        budget.raise_if_exhausted()
    assert ei.value.code == int(MpregErrorCode.TIMEOUT)


def test_rpc_plane_timeout_response() -> None:
    h = headers_with_deadline_seconds(_headers(), 0.0)
    resp = RpcPlane.timeout_if_exhausted("u1", h)
    assert resp is not None
    assert resp.error is not None
    assert resp.error.code == int(MpregErrorCode.TIMEOUT)


def test_apply_to_headers_preserves_correlation() -> None:
    h = headers_with_deadline_seconds(_headers(), 2.0)
    budget = DeadlineBudget.from_headers(h)
    assert budget is not None
    out = budget.apply_to_headers(h)
    assert out.correlation_id == "corr-1"
    assert out.deadline_remaining_ms is not None
    assert out.deadline_remaining_ms <= 2000.0
