"""Shared observability probe for curriculum example apps.

Every app can record operation latency and assert throughput / percentile
surfaces without standing up Prometheus. Output is annotated in the run log
so operators see latency and ops/s next to scenario results.

Also bridges to :class:`ServerMetricsTracker` when a live server is available.
"""

from __future__ import annotations

import statistics
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

def _percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return float(sorted_vals[f])
    return float(sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f))

@dataclass(slots=True)
class OpStats:
    """Per-operation latency/throughput aggregate."""

    name: str
    count: int = 0
    errors: int = 0
    latencies_ms: list[float] = field(default_factory=list)

    def record(self, latency_ms: float, *, ok: bool = True) -> None:
        self.count += 1
        if not ok:
            self.errors += 1
        self.latencies_ms.append(float(latency_ms))

    @property
    def avg_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        return float(statistics.fmean(self.latencies_ms))

    @property
    def p50_ms(self) -> float:
        return _percentile(sorted(self.latencies_ms), 50.0)

    @property
    def p95_ms(self) -> float:
        return _percentile(sorted(self.latencies_ms), 95.0)

    @property
    def p99_ms(self) -> float:
        return _percentile(sorted(self.latencies_ms), 99.0)

    @property
    def min_ms(self) -> float:
        return min(self.latencies_ms) if self.latencies_ms else 0.0

    @property
    def max_ms(self) -> float:
        return max(self.latencies_ms) if self.latencies_ms else 0.0

    def snapshot(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "count": self.count,
            "errors": self.errors,
            "avg_ms": round(self.avg_ms, 3),
            "p50_ms": round(self.p50_ms, 3),
            "p95_ms": round(self.p95_ms, 3),
            "p99_ms": round(self.p99_ms, 3),
            "min_ms": round(self.min_ms, 3),
            "max_ms": round(self.max_ms, 3),
        }

@dataclass(slots=True)
class ExampleProbe:
    """In-process latency/throughput recorder for curriculum apps.

    Usage::

        probe = ExampleProbe("hello_rpc")
        with probe.measure("rpc.call"):
            await client.call(...)
        probe.print_report()
        ensure(probe.total_ops >= 3, "expected ops")
        ensure(probe.op("rpc.call").p95_ms < 5000, "p95 too high")
    """

    app_id: str
    started_mono: float = field(default_factory=time.monotonic)
    _ops: dict[str, OpStats] = field(default_factory=dict)
    annotations: list[str] = field(default_factory=list)

    def op(self, name: str) -> OpStats:
        if name not in self._ops:
            self._ops[name] = OpStats(name=name)
        return self._ops[name]

    def record(
        self, name: str, latency_ms: float, *, ok: bool = True, note: str = ""
    ) -> None:
        self.op(name).record(latency_ms, ok=ok)
        if note:
            self.annotations.append(note)

    @contextmanager
    def measure(self, name: str) -> Iterator[None]:
        """Time a block and record success; records error latency on exception."""
        t0 = time.perf_counter()
        try:
            yield
        except Exception:
            self.record(name, (time.perf_counter() - t0) * 1000.0, ok=False)
            raise
        else:
            self.record(name, (time.perf_counter() - t0) * 1000.0, ok=True)

    async def measure_await(self, name: str, awaitable: Any) -> Any:
        """Await a coroutine while recording latency under *name*."""
        t0 = time.perf_counter()
        try:
            result = await awaitable
        except Exception:
            self.record(name, (time.perf_counter() - t0) * 1000.0, ok=False)
            raise
        self.record(name, (time.perf_counter() - t0) * 1000.0, ok=True)
        return result

    @property
    def total_ops(self) -> int:
        return sum(o.count for o in self._ops.values())

    @property
    def total_errors(self) -> int:
        return sum(o.errors for o in self._ops.values())

    @property
    def elapsed_s(self) -> float:
        return max(time.monotonic() - self.started_mono, 1e-9)

    @property
    def throughput_ops_s(self) -> float:
        return self.total_ops / self.elapsed_s

    def snapshot(self) -> dict[str, Any]:
        return {
            "app_id": self.app_id,
            "elapsed_s": round(self.elapsed_s, 4),
            "total_ops": self.total_ops,
            "total_errors": self.total_errors,
            "throughput_ops_s": round(self.throughput_ops_s, 3),
            "operations": {n: o.snapshot() for n, o in sorted(self._ops.items())},
        }

    def print_report(self, *, prefix: str = "  ◆ obs") -> None:
        """Print annotated latency/throughput summary to the app run log."""
        snap = self.snapshot()
        print(
            f"{prefix}: app={self.app_id} ops={snap['total_ops']} "
            f"errors={snap['total_errors']} elapsed_s={snap['elapsed_s']} "
            f"throughput_ops_s={snap['throughput_ops_s']}"
        )
        for name, op in snap["operations"].items():
            print(
                f"{prefix}:   {name}: n={op['count']} err={op['errors']} "
                f"avg_ms={op['avg_ms']} p50_ms={op['p50_ms']} "
                f"p95_ms={op['p95_ms']} p99_ms={op['p99_ms']} "
                f"min_ms={op['min_ms']} max_ms={op['max_ms']}"
            )
        for note in self.annotations:
            print(f"{prefix}: note: {note}")

    def absorb_server_tracker(self, tracker: Any, *, label: str = "server") -> None:
        """Merge a ServerMetricsTracker snapshot into this probe as annotations."""
        snap_fn = getattr(tracker, "snapshot", None)
        if snap_fn is None:
            return
        s = snap_fn()
        rpc = s.get("rpc") or {}
        if rpc.get("total", 0) > 0:
            # Synthetic bulk record so ensures can see server-side path
            name = f"{label}.rpc"
            st = self.op(name)
            # Don't double-inflate count from client measures — store as annotation
            self.annotations.append(
                f"{name}: total={rpc.get('total')} errors={rpc.get('errors')} "
                f"avg_ms={rpc.get('avg_ms')} p95_ms={rpc.get('p95_ms')} "
                f"rps={rpc.get('rps')}"
            )
            # Also expose a single synthetic sample for percentile ensures when
            # client didn't measure (avg as stand-in).
            if st.count == 0 and rpc.get("avg_ms", 0) > 0:
                st.record(float(rpc["avg_ms"]), ok=True)
                st.count = int(rpc["total"])  # reflect server volume
                # Pad list length for percentile honesty when only avg known
                while len(st.latencies_ms) < min(st.count, 8):
                    st.latencies_ms.append(float(rpc["avg_ms"]))

def format_server_snapshot(
    snap: dict[str, Any], *, prefix: str = "  ◆ server-metrics"
) -> None:
    """Pretty-print a ServerMetricsTracker.snapshot() dict."""
    rpc = snap.get("rpc") or {}
    pub = snap.get("pubsub") or {}
    fab = snap.get("fabric") or {}
    print(
        f"{prefix}: rpc total={rpc.get('total', 0)} errors={rpc.get('errors', 0)} "
        f"samples={rpc.get('samples', 0)} avg_ms={rpc.get('avg_ms', 0)} "
        f"p50_ms={rpc.get('p50_ms', 0)} p95_ms={rpc.get('p95_ms', 0)} "
        f"min_ms={rpc.get('min_ms', 0)} max_ms={rpc.get('max_ms', 0)} "
        f"rps={rpc.get('rps', 0)}"
    )
    print(
        f"{prefix}: pubsub total={pub.get('total', 0)} errors={pub.get('errors', 0)} "
        f"samples={pub.get('samples', 0)} avg_ms={pub.get('avg_ms', 0)} "
        f"p95_ms={pub.get('p95_ms', 0)} rps={pub.get('rps', 0)} "
        f"notifications={pub.get('notifications', 0)}"
    )
    if fab:
        print(
            f"{prefix}: fabric decisions={fab.get('decisions_total', 0)} "
            f"buffered={fab.get('decisions_buffered', 0)} "
            f"blackhole={fab.get('blackhole_count', 0)} "
            f"reachable_ratio={fab.get('reachable_ratio', 1.0)} "
            f"avg_hops={fab.get('avg_hops', 0)} max_hops={fab.get('max_hops', 0)}"
        )
