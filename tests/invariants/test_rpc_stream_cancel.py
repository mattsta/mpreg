"""C3: Stream cancellation and slow-consumer buffer limits (oracle + collector)."""

from __future__ import annotations

from collections import deque

import pytest

from mpreg.core.intermediate_results import IntermediateResultCollector
from mpreg.testing.oracles import RpcOracle, RpcStreamEvent


class BoundedStreamBuffer:
    """Minimal backpressure buffer for progressive RPC events."""

    def __init__(self, maxsize: int = 8) -> None:
        self.maxsize = maxsize
        self._q: deque[RpcStreamEvent] = deque()
        self.dropped = 0
        self.cancelled = False

    def push(self, event: RpcStreamEvent) -> bool:
        if self.cancelled:
            return False
        if len(self._q) >= self.maxsize:
            self.dropped += 1
            return False
        self._q.append(event)
        return True

    def cancel(self) -> None:
        self.cancelled = True
        self._q.clear()

    def drain(self) -> list[RpcStreamEvent]:
        out = list(self._q)
        self._q.clear()
        return out


def test_cancel_stops_delivery() -> None:
    buf = BoundedStreamBuffer()
    o = RpcOracle()
    assert buf.push(RpcStreamEvent(kind="intermediate", level=0))
    buf.cancel()
    assert buf.push(RpcStreamEvent(kind="intermediate", level=1)) is False
    for ev in buf.drain():
        o.observe(ev)


def test_slow_consumer_drops_with_backpressure() -> None:
    buf = BoundedStreamBuffer(maxsize=2)
    assert buf.push(RpcStreamEvent(kind="partial"))
    assert buf.push(RpcStreamEvent(kind="intermediate", level=0))
    assert buf.push(RpcStreamEvent(kind="intermediate", level=1)) is False
    assert buf.dropped == 1


def test_collector_monotonic_levels() -> None:
    c = IntermediateResultCollector(request_id="r", total_levels=3)
    o = RpcOracle()
    for i in range(3):
        c.start_level(i)
        r = c.complete_level(i, {f"c{i}": i}, {f"c{j}": j for j in range(i + 1)})
        o.observe(RpcStreamEvent(kind="intermediate", level=r.level_index))
    o.observe(RpcStreamEvent(kind="final"))
    with pytest.raises(AssertionError):
        o.observe(RpcStreamEvent(kind="partial"))
