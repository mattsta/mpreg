"""In-memory ring buffer of recent fabric routing decisions for ops audit."""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any

@dataclass(frozen=True, slots=True)
class RouteDecisionRecord:
    timestamp: float
    message_id: str
    correlation_id: str
    topic: str
    message_type: str
    reason: str
    cached: bool
    targets: tuple[str, ...]
    routing_path: tuple[str, ...]
    hops_required: int
    traceparent: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

class RouteDecisionLog:
    """Thread-safe fixed-size log of route decisions."""

    def __init__(self, maxlen: int = 256) -> None:
        self._maxlen = max(1, maxlen)
        self._items: deque[RouteDecisionRecord] = deque(maxlen=self._maxlen)
        self._lock = threading.Lock()

    def record(self, entry: RouteDecisionRecord) -> None:
        with self._lock:
            self._items.append(entry)

    def recent(
        self,
        *,
        limit: int = 50,
        message_id: str | None = None,
        correlation_id: str | None = None,
    ) -> list[RouteDecisionRecord]:
        with self._lock:
            items = list(self._items)
        if message_id:
            items = [i for i in items if i.message_id == message_id]
        if correlation_id:
            items = [i for i in items if i.correlation_id == correlation_id]
        if limit > 0:
            items = items[-limit:]
        return list(reversed(items))

    def clear(self) -> None:
        with self._lock:
            self._items.clear()

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {"size": len(self._items), "maxlen": self._maxlen}

# Process-wide default log used by FabricRouter instances that share it.
_DEFAULT_LOG = RouteDecisionLog(maxlen=512)

def get_default_route_decision_log() -> RouteDecisionLog:
    return _DEFAULT_LOG

def make_record_from_route(
    *,
    message_id: str,
    correlation_id: str,
    topic: str,
    message_type: str,
    reason: str,
    cached: bool,
    targets: list[str],
    routing_path: list[str] | tuple[str, ...],
    hops_required: int,
    traceparent: str | None = None,
) -> RouteDecisionRecord:
    return RouteDecisionRecord(
        timestamp=time.time(),
        message_id=message_id,
        correlation_id=correlation_id or "",
        topic=topic,
        message_type=message_type,
        reason=reason,
        cached=cached,
        targets=tuple(targets),
        routing_path=tuple(routing_path),
        hops_required=int(hops_required),
        traceparent=traceparent,
    )
