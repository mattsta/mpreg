"""Append-only history log for DistLab client operations."""

from __future__ import annotations

import threading
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any

from mpreg.testing.distlab.models import (
    HistoryEvent,
    OpKind,
    OpStatus,
    wall_now,
)

@dataclass(slots=True)
class History:
    """Thread-safe append-only event log.

    Processes (logical clients) emit INVOKE then OK/FAIL/INFO pairs. Checkers
    consume the frozen list after a scenario completes.
    """

    _events: list[HistoryEvent] = field(default_factory=list)
    _lock: threading.RLock = field(default_factory=threading.RLock)
    _next_index: int = 0

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)

    def __iter__(self) -> Iterator[HistoryEvent]:
        return iter(self.snapshot())

    def snapshot(self) -> list[HistoryEvent]:
        with self._lock:
            return list(self._events)

    def clear(self) -> None:
        with self._lock:
            self._events.clear()
            self._next_index = 0

    def append(
        self,
        *,
        process: str,
        kind: OpKind,
        status: OpStatus,
        key: str | None = None,
        value: Any = None,
        op_id: str | None = None,
        error_code: int | None = None,
        error_message: str | None = None,
        meta: dict[str, Any] | None = None,
        wall_time: float | None = None,
    ) -> HistoryEvent:
        with self._lock:
            ev = HistoryEvent(
                index=self._next_index,
                process=process,
                kind=kind,
                status=status,
                wall_time=wall_now() if wall_time is None else wall_time,
                key=key,
                value=value,
                op_id=op_id,
                error_code=error_code,
                error_message=error_message,
                meta=dict(meta or {}),
            )
            self._next_index += 1
            self._events.append(ev)
            return ev

    def invoke(
        self,
        process: str,
        kind: OpKind,
        *,
        key: str | None = None,
        value: Any = None,
        op_id: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> HistoryEvent:
        return self.append(
            process=process,
            kind=kind,
            status=OpStatus.INVOKE,
            key=key,
            value=value,
            op_id=op_id,
            meta=meta,
        )

    def ok(
        self,
        process: str,
        kind: OpKind,
        *,
        key: str | None = None,
        value: Any = None,
        op_id: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> HistoryEvent:
        return self.append(
            process=process,
            kind=kind,
            status=OpStatus.OK,
            key=key,
            value=value,
            op_id=op_id,
            meta=meta,
        )

    def fail(
        self,
        process: str,
        kind: OpKind,
        *,
        key: str | None = None,
        value: Any = None,
        op_id: str | None = None,
        error_code: int | None = None,
        error_message: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> HistoryEvent:
        return self.append(
            process=process,
            kind=kind,
            status=OpStatus.FAIL,
            key=key,
            value=value,
            op_id=op_id,
            error_code=error_code,
            error_message=error_message,
            meta=meta,
        )

    def info(
        self,
        process: str,
        kind: OpKind,
        *,
        key: str | None = None,
        value: Any = None,
        op_id: str | None = None,
        error_message: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> HistoryEvent:
        return self.append(
            process=process,
            kind=kind,
            status=OpStatus.INFO,
            key=key,
            value=value,
            op_id=op_id,
            error_message=error_message,
            meta=meta,
        )

    def pairs(self) -> list[tuple[HistoryEvent, HistoryEvent | None]]:
        """Match INVOKE → terminal (OK/FAIL/INFO) per process FIFO."""
        pending: dict[str, list[HistoryEvent]] = {}
        out: list[tuple[HistoryEvent, HistoryEvent | None]] = []
        for ev in self.snapshot():
            if ev.status is OpStatus.INVOKE:
                pending.setdefault(ev.process, []).append(ev)
                continue
            q = pending.get(ev.process) or []
            if not q:
                continue
            inv = q.pop(0)
            out.append((inv, ev))
        for proc, q in pending.items():
            for inv in q:
                out.append((inv, None))
        return out

    def by_key(self, key: str) -> list[HistoryEvent]:
        return [e for e in self.snapshot() if e.key == key]

    def successful_puts(self, key: str | None = None) -> list[HistoryEvent]:
        evs = self.snapshot()
        out = [
            e
            for e in evs
            if e.kind is OpKind.PUT and e.status is OpStatus.OK
        ]
        if key is not None:
            out = [e for e in out if e.key == key]
        return out

    def failed_puts(self, key: str | None = None) -> list[HistoryEvent]:
        evs = self.snapshot()
        out = [
            e
            for e in evs
            if e.kind is OpKind.PUT and e.status is OpStatus.FAIL
        ]
        if key is not None:
            out = [e for e in out if e.key == key]
        return out

    def extend(self, events: Iterable[HistoryEvent]) -> None:
        with self._lock:
            for e in events:
                # re-index
                ne = HistoryEvent(
                    index=self._next_index,
                    process=e.process,
                    kind=e.kind,
                    status=e.status,
                    wall_time=e.wall_time,
                    key=e.key,
                    value=e.value,
                    op_id=e.op_id,
                    error_code=e.error_code,
                    error_message=e.error_message,
                    meta=dict(e.meta),
                )
                self._next_index += 1
                self._events.append(ne)
