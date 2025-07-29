"""
Advanced Metrics Collection Datastructures for MPREG Federation.

This module provides well-encapsulated, type-safe datastructures for efficient
metrics collection, aggregation, and analysis. All datastructures are designed
for O(1) operations with bounded memory usage and thread-safe access patterns.

Key Features:
- O(1) moving averages with sliding windows
- Bounded percentile tracking with configurable accuracy
- Efficient time-series storage with automatic cleanup
- Thread-safe concurrent access patterns
- Memory-bounded datastructures preventing OOM
- Type-safe interfaces with comprehensive validation
"""

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from threading import RLock
from typing import Any, TypeVar

from .type_aliases import Timestamp

T = TypeVar("T")


class MetricType(Enum):
    """Types of metrics for collection and aggregation."""

    COUNTER = "counter"  # Monotonically increasing values
    GAUGE = "gauge"  # Point-in-time values that can go up/down
    HISTOGRAM = "histogram"  # Distribution of values with buckets
    SUMMARY = "summary"  # Statistical summaries (quantiles)
    TIMER = "timer"  # Duration measurements with percentiles


class AlertSeverity(Enum):
    """Alert severity levels with escalation."""

    INFO = "info"  # Informational alerts
    WARNING = "warning"  # Warning conditions
    CRITICAL = "critical"  # Critical conditions requiring attention
    EMERGENCY = "emergency"  # Emergency conditions requiring immediate action


@dataclass(frozen=True, slots=True)
class MetricValue:
    """Immutable metric value with metadata."""

    value: float
    timestamp: Timestamp
    metric_type: MetricType
    labels: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate metric value constraints."""
        if self.metric_type == MetricType.COUNTER and self.value < 0:
            raise ValueError("Counter metrics must be non-negative")

        if not isinstance(self.timestamp, int | float) or self.timestamp <= 0:
            raise ValueError("Timestamp must be a positive number")


@dataclass(slots=True)
class BoundedMovingAverage:
    """
    O(1) moving average with configurable window size and memory bounds.

    This datastructure maintains a sliding window of values and provides
    constant-time operations for adding values and computing averages.
    Memory usage is bounded by the window size.
    """

    window_size: int
    _values: deque[float] = field(default_factory=deque)
    _sum: float = field(default=0.0, init=False)
    _count: int = field(default=0, init=False)
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """Validate window size constraints."""
        if self.window_size <= 0:
            raise ValueError("Window size must be positive")
        if self.window_size > 10000:
            raise ValueError("Window size too large (max 10000)")

    def add_value(self, value: float) -> None:
        """Add a new value to the moving average (O(1))."""
        if not isinstance(value, int | float):
            raise TypeError("Value must be numeric")

        with self._lock:
            if len(self._values) >= self.window_size:
                # Remove oldest value
                old_value = self._values.popleft()
                self._sum -= old_value
            else:
                self._count += 1

            self._values.append(float(value))
            self._sum += float(value)

    @property
    def average(self) -> float:
        """Get current average (O(1))."""
        with self._lock:
            if not self._values:
                return 0.0
            return self._sum / len(self._values)

    @property
    def count(self) -> int:
        """Get number of values in current window (O(1))."""
        with self._lock:
            return len(self._values)

    @property
    def sum(self) -> float:
        """Get sum of values in current window (O(1))."""
        with self._lock:
            return self._sum

    def reset(self) -> None:
        """Reset the moving average (O(1))."""
        with self._lock:
            self._values.clear()
            self._sum = 0.0
            self._count = 0

    def get_snapshot(self) -> dict[str, float]:
        """Get thread-safe snapshot of current state (O(1))."""
        with self._lock:
            return {
                "average": self.average,
                "sum": self._sum,
                "count": len(self._values),
                "window_size": self.window_size,
            }


@dataclass(slots=True)
class BoundedPercentileTracker:
    """
    Efficient percentile tracking with bounded memory and configurable accuracy.

    Uses reservoir sampling to maintain a representative sample of values
    while keeping memory usage bounded. Percentile calculation is O(n log n)
    where n is the sample size (bounded), but with lazy evaluation.
    """

    max_samples: int = 1000
    percentiles: list[float] = field(default_factory=lambda: [50.0, 95.0, 99.0])
    _samples: list[float] = field(default_factory=list)
    _sorted: bool = field(default=True, init=False)
    _total_samples: int = field(default=0, init=False)
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """Validate percentile tracker constraints."""
        if self.max_samples <= 0:
            raise ValueError("Max samples must be positive")
        if self.max_samples > 100000:
            raise ValueError("Max samples too large (max 100000)")

        for p in self.percentiles:
            if not 0 <= p <= 100:
                raise ValueError(f"Percentile {p} must be between 0 and 100")

    def add_sample(self, value: float) -> None:
        """Add a sample using reservoir sampling (O(1) amortized)."""
        if not isinstance(value, int | float):
            raise TypeError("Sample value must be numeric")

        with self._lock:
            self._total_samples += 1

            if len(self._samples) < self.max_samples:
                # Still building initial sample
                self._samples.append(float(value))
                self._sorted = False
            else:
                # Reservoir sampling: replace random sample
                import random

                replace_index = random.randint(0, self._total_samples - 1)
                if replace_index < self.max_samples:
                    self._samples[replace_index] = float(value)
                    self._sorted = False

    def get_percentiles(self) -> dict[float, float]:
        """Get configured percentiles (O(n log n) when dirty, O(1) when clean)."""
        with self._lock:
            if not self._samples:
                return {p: 0.0 for p in self.percentiles}

            if not self._sorted:
                self._samples.sort()
                self._sorted = True

            result = {}
            n = len(self._samples)

            for p in self.percentiles:
                if p <= 0:
                    result[p] = self._samples[0]
                elif p >= 100:
                    result[p] = self._samples[-1]
                else:
                    # Linear interpolation for percentile
                    index = (p / 100.0) * (n - 1)
                    lower = int(index)
                    upper = min(lower + 1, n - 1)
                    weight = index - lower

                    result[p] = (1 - weight) * self._samples[
                        lower
                    ] + weight * self._samples[upper]

            return result

    def get_statistics(self) -> dict[str, Any]:
        """Get comprehensive statistics (O(n log n) when dirty)."""
        with self._lock:
            if not self._samples:
                return {
                    "count": 0,
                    "total_samples": self._total_samples,
                    "percentiles": {p: 0.0 for p in self.percentiles},
                    "min": 0.0,
                    "max": 0.0,
                    "mean": 0.0,
                }

            percentiles = self.get_percentiles()

            return {
                "count": len(self._samples),
                "total_samples": self._total_samples,
                "percentiles": percentiles,
                "min": min(self._samples),
                "max": max(self._samples),
                "mean": sum(self._samples) / len(self._samples),
            }

    def reset(self) -> None:
        """Reset all samples and statistics (O(1))."""
        with self._lock:
            self._samples.clear()
            self._sorted = True
            self._total_samples = 0


@dataclass(slots=True)
class TimeSeriesBuffer:
    """
    Memory-bounded time series storage with automatic cleanup.

    Maintains time-ordered samples with automatic cleanup of old data.
    Provides efficient access patterns for recent data analysis.
    """

    max_age_seconds: float = 3600.0  # 1 hour default
    max_samples: int = 10000
    _samples: deque[tuple[Timestamp, float]] = field(default_factory=deque)
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """Validate time series buffer constraints."""
        if self.max_age_seconds <= 0:
            raise ValueError("Max age must be positive")
        if self.max_samples <= 0:
            raise ValueError("Max samples must be positive")

    def add_sample(self, timestamp: Timestamp, value: float) -> None:
        """Add a timestamped sample (O(1) amortized)."""
        if not isinstance(timestamp, int | float) or timestamp <= 0:
            raise ValueError("Timestamp must be a positive number")
        if not isinstance(value, int | float):
            raise TypeError("Value must be numeric")

        with self._lock:
            self._samples.append((float(timestamp), float(value)))

            # Remove old samples by age and count
            current_time = time.time()
            cutoff_time = current_time - self.max_age_seconds

            # Remove by age
            while self._samples and self._samples[0][0] < cutoff_time:
                self._samples.popleft()

            # Remove by count
            while len(self._samples) > self.max_samples:
                self._samples.popleft()

    def get_recent_samples(
        self, duration_seconds: float
    ) -> list[tuple[Timestamp, float]]:
        """Get samples from the last N seconds (O(k) where k is result size)."""
        if duration_seconds <= 0:
            raise ValueError("Duration must be positive")

        with self._lock:
            cutoff_time = time.time() - duration_seconds
            return [(ts, val) for ts, val in self._samples if ts >= cutoff_time]

    def get_sample_count(self) -> int:
        """Get current number of samples (O(1))."""
        with self._lock:
            return len(self._samples)

    def get_time_range(self) -> tuple[Timestamp, Timestamp] | None:
        """Get the time range of stored samples (O(1))."""
        with self._lock:
            if not self._samples:
                return None
            return (self._samples[0][0], self._samples[-1][0])

    def cleanup_old_samples(self) -> int:
        """Manually cleanup old samples and return count removed (O(k))."""
        with self._lock:
            initial_count = len(self._samples)
            current_time = time.time()
            cutoff_time = current_time - self.max_age_seconds

            while self._samples and self._samples[0][0] < cutoff_time:
                self._samples.popleft()

            return initial_count - len(self._samples)


class HealthStatus(Enum):
    """Health status enumeration with clear semantics."""

    HEALTHY = "healthy"  # System operating normally
    DEGRADED = "degraded"  # System operating with reduced performance
    CRITICAL = "critical"  # System operating with significant issues
    UNAVAILABLE = "unavailable"  # System not responding or offline


@dataclass(frozen=True, slots=True)
class HealthScore:
    """Immutable health score with components and metadata."""

    overall_score: float  # 0.0 to 1.0
    latency_score: float  # 0.0 to 1.0
    error_score: float  # 0.0 to 1.0
    throughput_score: float  # 0.0 to 1.0
    timestamp: Timestamp

    def __post_init__(self) -> None:
        """Validate health score constraints."""
        for score_name, score_value in [
            ("overall_score", self.overall_score),
            ("latency_score", self.latency_score),
            ("error_score", self.error_score),
            ("throughput_score", self.throughput_score),
        ]:
            if not 0.0 <= score_value <= 1.0:
                raise ValueError(f"{score_name} must be between 0.0 and 1.0")

    @property
    def status(self) -> HealthStatus:
        """Derive health status from overall score."""
        if self.overall_score >= 0.8:
            return HealthStatus.HEALTHY
        elif self.overall_score >= 0.6:
            return HealthStatus.DEGRADED
        elif self.overall_score >= 0.3:
            return HealthStatus.CRITICAL
        else:
            return HealthStatus.UNAVAILABLE

    @property
    def is_healthy(self) -> bool:
        """Check if status is healthy."""
        return self.status == HealthStatus.HEALTHY

    @property
    def is_degraded(self) -> bool:
        """Check if status is degraded."""
        return self.status == HealthStatus.DEGRADED

    @property
    def needs_attention(self) -> bool:
        """Check if health needs attention."""
        return self.status in (HealthStatus.CRITICAL, HealthStatus.UNAVAILABLE)


@dataclass(slots=True)
class MetricsAggregator:
    """
    Efficient metrics aggregation with multiple aggregation functions.

    Provides O(1) aggregation operations for common statistical functions
    while maintaining bounded memory usage.
    """

    name: str
    moving_average: BoundedMovingAverage = field(
        default_factory=lambda: BoundedMovingAverage(window_size=100)
    )
    percentile_tracker: BoundedPercentileTracker = field(
        default_factory=BoundedPercentileTracker
    )
    time_series: TimeSeriesBuffer = field(default_factory=TimeSeriesBuffer)

    # O(1) statistics
    _min_value: float = field(default=float("inf"), init=False)
    _max_value: float = field(default=float("-inf"), init=False)
    _total_samples: int = field(default=0, init=False)
    _last_update: Timestamp = field(default=0.0, init=False)
    _lock: RLock = field(default_factory=RLock)

    def add_measurement(self, value: float, timestamp: Timestamp | None = None) -> None:
        """Add a measurement to all aggregators (O(1) amortized)."""
        if timestamp is None:
            timestamp = time.time()

        if not isinstance(value, int | float):
            raise TypeError("Value must be numeric")

        with self._lock:
            # Update all datastructures
            self.moving_average.add_value(value)
            self.percentile_tracker.add_sample(value)
            self.time_series.add_sample(timestamp, value)

            # Update O(1) statistics
            self._min_value = min(self._min_value, value)
            self._max_value = max(self._max_value, value)
            self._total_samples += 1
            self._last_update = timestamp

    def get_comprehensive_stats(self) -> dict[str, Any]:
        """Get comprehensive statistics from all aggregators."""
        with self._lock:
            return {
                "name": self.name,
                "moving_average": self.moving_average.get_snapshot(),
                "percentiles": self.percentile_tracker.get_percentiles(),
                "time_series_count": self.time_series.get_sample_count(),
                "min_value": self._min_value if self._total_samples > 0 else 0.0,
                "max_value": self._max_value if self._total_samples > 0 else 0.0,
                "total_samples": self._total_samples,
                "last_update": self._last_update,
            }

    def reset(self) -> None:
        """Reset all aggregators and statistics (O(1))."""
        with self._lock:
            self.moving_average.reset()
            self.percentile_tracker.reset()
            self._min_value = float("inf")
            self._max_value = float("-inf")
            self._total_samples = 0
            self._last_update = 0.0


# Factory functions for common metric patterns


def create_latency_aggregator(window_size: int = 100) -> MetricsAggregator:
    """Create a metrics aggregator optimized for latency measurements."""
    return MetricsAggregator(
        name="latency",
        moving_average=BoundedMovingAverage(window_size=window_size),
        percentile_tracker=BoundedPercentileTracker(
            max_samples=1000, percentiles=[50.0, 90.0, 95.0, 99.0, 99.9]
        ),
        time_series=TimeSeriesBuffer(max_age_seconds=3600.0, max_samples=10000),
    )


def create_throughput_aggregator(window_size: int = 60) -> MetricsAggregator:
    """Create a metrics aggregator optimized for throughput measurements."""
    return MetricsAggregator(
        name="throughput",
        moving_average=BoundedMovingAverage(window_size=window_size),
        percentile_tracker=BoundedPercentileTracker(
            max_samples=500, percentiles=[50.0, 95.0, 99.0]
        ),
        time_series=TimeSeriesBuffer(max_age_seconds=7200.0, max_samples=5000),
    )


def create_error_rate_aggregator(window_size: int = 50) -> MetricsAggregator:
    """Create a metrics aggregator optimized for error rate measurements."""
    return MetricsAggregator(
        name="error_rate",
        moving_average=BoundedMovingAverage(window_size=window_size),
        percentile_tracker=BoundedPercentileTracker(
            max_samples=200, percentiles=[50.0, 90.0, 95.0, 99.0]
        ),
        time_series=TimeSeriesBuffer(max_age_seconds=1800.0, max_samples=2000),
    )
