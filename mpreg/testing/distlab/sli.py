"""DistLab performance SLI helpers (lab bounds — not production WAN SLA).

Honest scope:
* Wall-clock timers around scenario / put batches on the same host.
* Percentile helpers over samples collected in-process.
* Soft bounds for soak scenarios (assert in tests, not production alerts).

Non-claims: these numbers are **not** multi-region latency SLAs, disk fsync
guarantees, or operator-facing SLO contracts for WAN deployments.
"""

from __future__ import annotations

import math
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

def percentile(samples: Sequence[float], p: float) -> float:
    """Nearest-rank percentile for p in [0, 100]. Empty → 0.0."""
    if not samples:
        return 0.0
    if p <= 0:
        return float(min(samples))
    if p >= 100:
        return float(max(samples))
    ordered = sorted(float(x) for x in samples)
    # nearest-rank (same style as GCM strong_metrics_snapshot)
    idx = max(0, int(math.ceil(len(ordered) * (p / 100.0))) - 1)
    return float(ordered[min(idx, len(ordered) - 1)])

def summarize_latencies_ms(samples: Sequence[float]) -> dict[str, float | int]:
    """Compact latency summary for scenario meta / assertions."""
    xs = [float(x) for x in samples]
    if not xs:
        return {
            "sample_count": 0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "max_ms": 0.0,
            "avg_ms": 0.0,
        }
    return {
        "sample_count": len(xs),
        "p50_ms": percentile(xs, 50),
        "p95_ms": percentile(xs, 95),
        "p99_ms": percentile(xs, 99),
        "max_ms": float(max(xs)),
        "avg_ms": float(sum(xs) / len(xs)),
    }

@dataclass
class WallTimer:
    """Simple wall-clock stopwatch (perf_counter)."""

    name: str = "timer"
    samples_ms: list[float] = field(default_factory=list)
    _t0: float | None = field(default=None, repr=False)

    def start(self) -> None:
        self._t0 = time.perf_counter()

    def stop(self) -> float:
        if self._t0 is None:
            return 0.0
        elapsed_ms = (time.perf_counter() - self._t0) * 1000.0
        self._t0 = None
        self.samples_ms.append(elapsed_ms)
        return elapsed_ms

    @contextmanager
    def measure(self) -> Iterator[None]:
        self.start()
        try:
            yield
        finally:
            self.stop()

    def summary(self) -> dict[str, float | int]:
        out = summarize_latencies_ms(self.samples_ms)
        out["name"] = self.name  # type: ignore[assignment]
        return out

@dataclass
class SliBudget:
    """Soft in-process SLI budget for DistLab soaks (not WAN SLA)."""

    max_p99_ms: float = 5_000.0
    max_avg_ms: float = 2_000.0
    max_duration_s: float = 120.0
    min_success_rate: float = 1.0

    def check(
        self,
        *,
        latency_ms: Sequence[float] | None = None,
        duration_s: float | None = None,
        success: int = 0,
        total: int = 0,
    ) -> dict[str, Any]:
        """Return {ok, violations, summary} for test assertions."""
        violations: list[str] = []
        summary: dict[str, Any] = {}
        if latency_ms is not None:
            lat = summarize_latencies_ms(latency_ms)
            summary["latency_ms"] = lat
            if lat["sample_count"] and float(lat["p99_ms"]) > self.max_p99_ms:
                violations.append(
                    f"p99_ms={lat['p99_ms']:.1f} > max_p99_ms={self.max_p99_ms}"
                )
            if lat["sample_count"] and float(lat["avg_ms"]) > self.max_avg_ms:
                violations.append(
                    f"avg_ms={lat['avg_ms']:.1f} > max_avg_ms={self.max_avg_ms}"
                )
        if duration_s is not None:
            summary["duration_s"] = duration_s
            if duration_s > self.max_duration_s:
                violations.append(
                    f"duration_s={duration_s:.2f} > max_duration_s={self.max_duration_s}"
                )
        if total > 0:
            rate = success / total
            summary["success_rate"] = rate
            summary["success"] = success
            summary["total"] = total
            if rate + 1e-12 < self.min_success_rate:
                violations.append(
                    f"success_rate={rate:.3f} < min_success_rate={self.min_success_rate}"
                )
        return {"ok": not violations, "violations": violations, "summary": summary}

# Default lab soak budget for strong.soak_20 (same-host in-process).
DEFAULT_STRONG_SOAK_BUDGET = SliBudget(
    max_p99_ms=2_000.0,
    max_avg_ms=500.0,
    max_duration_s=60.0,
    min_success_rate=1.0,
)
