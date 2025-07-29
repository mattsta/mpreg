"""
Property-Based Tests for MPREG Metrics Datastructures.

This module provides comprehensive property-based testing for the metrics
collection datastructures to validate O(1) algorithms, bounded memory usage,
and thread-safe operations under all conditions.

Key Test Areas:
- BoundedMovingAverage: O(1) operations and sliding window behavior
- BoundedPercentileTracker: Reservoir sampling and percentile accuracy
- TimeSeriesBuffer: Memory bounds and automatic cleanup
- MetricsAggregator: Comprehensive aggregation correctness
- Thread safety under concurrent access
"""

import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from mpreg.datastructures.metrics_structures import (
    BoundedMovingAverage,
    BoundedPercentileTracker,
    HealthScore,
    HealthStatus,
    MetricsAggregator,
    MetricType,
    MetricValue,
    TimeSeriesBuffer,
    create_error_rate_aggregator,
    create_latency_aggregator,
    create_throughput_aggregator,
)

# Test Constants and Fixtures

FIXED_TIME_BASE = 1640995200.0  # Fixed timestamp for consistent testing
FIXED_TIME_RANGE = 86400.0  # 24 hours


@pytest.fixture
def mock_time():
    """Mock time.time() to return consistent values for testing."""
    with patch("time.time") as mock:
        mock.return_value = FIXED_TIME_BASE
        yield mock


# Test Strategies


@st.composite
def bounded_moving_average_config(draw):
    """Generate valid BoundedMovingAverage configurations."""
    window_size = draw(st.integers(min_value=1, max_value=1000))
    return {"window_size": window_size}


@st.composite
def percentile_tracker_config(draw):
    """Generate valid BoundedPercentileTracker configurations."""
    max_samples = draw(st.integers(min_value=10, max_value=1000))
    percentiles = draw(
        st.lists(
            st.floats(min_value=0.0, max_value=100.0),
            min_size=1,
            max_size=10,
            unique=True,
        )
    )
    return {"max_samples": max_samples, "percentiles": sorted(percentiles)}


@st.composite
def time_series_config(draw):
    """Generate valid TimeSeriesBuffer configurations."""
    max_age_seconds = draw(st.floats(min_value=1.0, max_value=3600.0))
    max_samples = draw(st.integers(min_value=10, max_value=1000))
    return {"max_age_seconds": max_age_seconds, "max_samples": max_samples}


@st.composite
def metric_value_strategy(draw):
    """Generate valid MetricValue instances."""
    value = draw(
        st.floats(
            min_value=0.0, max_value=10000.0, allow_nan=False, allow_infinity=False
        )
    )
    # Use fixed timestamp range to avoid flaky generation
    timestamp = draw(
        st.floats(
            min_value=FIXED_TIME_BASE, max_value=FIXED_TIME_BASE + FIXED_TIME_RANGE
        )
    )
    metric_type = draw(st.sampled_from(list(MetricType)))
    labels = draw(
        st.dictionaries(
            st.text(
                min_size=1,
                max_size=10,
                alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
            ),
            st.text(
                min_size=1,
                max_size=20,
                alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
            ),
            max_size=3,  # Reduce complexity
        )
    )

    # Ensure counter values are non-negative
    if metric_type == MetricType.COUNTER and value < 0:
        value = abs(value)

    return MetricValue(
        value=value, timestamp=timestamp, metric_type=metric_type, labels=labels
    )


@st.composite
def health_score_strategy(draw):
    """Generate valid HealthScore instances."""
    overall = draw(
        st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)
    )
    latency = draw(
        st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)
    )
    error = draw(
        st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)
    )
    throughput = draw(
        st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)
    )
    # Use fixed timestamp range
    timestamp = draw(
        st.floats(
            min_value=FIXED_TIME_BASE, max_value=FIXED_TIME_BASE + FIXED_TIME_RANGE
        )
    )

    return HealthScore(
        overall_score=overall,
        latency_score=latency,
        error_score=error,
        throughput_score=throughput,
        timestamp=timestamp,
    )


# Property Tests for BoundedMovingAverage


class TestBoundedMovingAverageProperties:
    """Property tests for BoundedMovingAverage datastructure."""

    @given(config=bounded_moving_average_config())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_moving_average_initialization_properties(self, config):
        """Test that BoundedMovingAverage initializes with correct properties."""
        avg = BoundedMovingAverage(**config)

        # Initial state properties
        assert avg.window_size == config["window_size"]
        assert avg.average == 0.0
        assert avg.count == 0
        assert avg.sum == 0.0

        # Snapshot consistency
        snapshot = avg.get_snapshot()
        assert snapshot["window_size"] == config["window_size"]
        assert snapshot["average"] == 0.0
        assert snapshot["count"] == 0
        assert snapshot["sum"] == 0.0

    @given(
        config=bounded_moving_average_config(),
        values=st.lists(
            st.floats(
                min_value=-1000.0,
                max_value=1000.0,
                allow_nan=False,
                allow_infinity=False,
            ),
            min_size=1,
            max_size=100,
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_moving_average_correctness_properties(self, config, values):
        """Test that moving average calculations are mathematically correct."""
        avg = BoundedMovingAverage(**config)
        window_size = config["window_size"]

        for i, value in enumerate(values):
            avg.add_value(value)

            # Count should never exceed window size
            assert avg.count <= window_size

            # Calculate expected average manually
            start_idx = max(0, i + 1 - window_size)
            expected_values = values[start_idx : i + 1]
            expected_average = sum(expected_values) / len(expected_values)
            expected_sum = sum(expected_values)

            # Allow small floating point errors
            assert abs(avg.average - expected_average) < 1e-10
            assert abs(avg.sum - expected_sum) < 1e-10
            assert avg.count == len(expected_values)

    @given(
        config=bounded_moving_average_config(),
        values=st.lists(
            st.floats(
                min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False
            ),
            min_size=1,
            max_size=50,
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_moving_average_window_sliding_properties(self, config, values):
        """Test that sliding window behavior is correct."""
        avg = BoundedMovingAverage(**config)
        window_size = config["window_size"]

        # Fill beyond window size
        all_values = values * (window_size // len(values) + 2)

        for i, value in enumerate(all_values):
            old_count = avg.count
            avg.add_value(value)

            if old_count < window_size:
                # Still filling window
                assert avg.count == old_count + 1
            else:
                # Window is full, should maintain size
                assert avg.count == window_size

            # Average should always be reasonable
            assert not math.isnan(avg.average)
            assert not math.isinf(avg.average)

    @given(
        config=bounded_moving_average_config(),
        values=st.lists(
            st.floats(
                min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
            ),
            min_size=10,
            max_size=50,
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_moving_average_thread_safety_properties(self, config, values):
        """Test that BoundedMovingAverage is thread-safe."""
        avg = BoundedMovingAverage(**config)

        def worker_thread(thread_values):
            for value in thread_values:
                avg.add_value(value)
                # Reading should always work
                _ = avg.average
                _ = avg.count
                _ = avg.sum

        # Split values across multiple threads
        num_threads = min(4, len(values))
        chunk_size = len(values) // num_threads
        threads = []

        for i in range(num_threads):
            start = i * chunk_size
            end = start + chunk_size if i < num_threads - 1 else len(values)
            thread_values = values[start:end]
            thread = threading.Thread(target=worker_thread, args=(thread_values,))
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Final state should be consistent
        snapshot = avg.get_snapshot()
        assert snapshot["count"] <= config["window_size"]
        assert not math.isnan(snapshot["average"])
        assert not math.isinf(snapshot["sum"])


# Property Tests for BoundedPercentileTracker


class TestBoundedPercentileTrackerProperties:
    """Property tests for BoundedPercentileTracker datastructure."""

    @given(config=percentile_tracker_config())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_percentile_tracker_initialization_properties(self, config):
        """Test that BoundedPercentileTracker initializes correctly."""
        tracker = BoundedPercentileTracker(**config)

        assert tracker.max_samples == config["max_samples"]
        assert tracker.percentiles == config["percentiles"]

        # Empty tracker should return zeros
        percentiles = tracker.get_percentiles()
        for p in config["percentiles"]:
            assert percentiles[p] == 0.0

        stats = tracker.get_statistics()
        assert stats["count"] == 0
        assert stats["total_samples"] == 0

    @given(
        config=percentile_tracker_config(),
        values=st.lists(
            st.floats(
                min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False
            ),
            min_size=1,
            max_size=100,
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_percentile_tracker_accuracy_properties(self, config, values):
        """Test that percentile calculations are approximately correct."""
        tracker = BoundedPercentileTracker(**config)

        for value in values:
            tracker.add_sample(value)

        percentiles = tracker.get_percentiles()
        stats = tracker.get_statistics()

        # Total samples should be tracked correctly
        assert stats["total_samples"] == len(values)

        # Sample count should be bounded
        assert stats["count"] <= config["max_samples"]

        if len(values) <= config["max_samples"] and len(values) > 2:
            # If we haven't exceeded max samples, percentiles should be reasonable
            sorted_values = sorted(values)
            for p in config["percentiles"]:
                if 0 <= p <= 100:
                    # Calculate expected percentile manually
                    if p == 0:
                        expected = sorted_values[0]
                    elif p == 100:
                        expected = sorted_values[-1]
                    else:
                        # Linear interpolation for percentile
                        index = (p / 100.0) * (len(sorted_values) - 1)
                        lower_idx = int(index)
                        upper_idx = min(lower_idx + 1, len(sorted_values) - 1)
                        weight = index - lower_idx
                        expected = (1 - weight) * sorted_values[
                            lower_idx
                        ] + weight * sorted_values[upper_idx]

                    # Allow reasonable tolerance for floating point arithmetic and reservoir sampling
                    tolerance = max(
                        0.1, abs(expected) * 0.1
                    )  # 10% tolerance or 0.1, whichever is larger
                    assert abs(percentiles[p] - expected) <= tolerance

    @given(
        config=percentile_tracker_config(),
        values=st.lists(
            st.floats(
                min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
            ),
            min_size=10,
            max_size=2000,  # Test beyond max_samples
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_percentile_tracker_memory_bounds_properties(self, config, values):
        """Test that BoundedPercentileTracker respects memory bounds."""
        tracker = BoundedPercentileTracker(**config)
        max_samples = config["max_samples"]

        for i, value in enumerate(values):
            tracker.add_sample(value)

            stats = tracker.get_statistics()

            # Memory bounds must be respected
            assert stats["count"] <= max_samples

            # Total samples should always be accurate
            assert stats["total_samples"] == i + 1

            # Percentiles should always be computable
            percentiles = tracker.get_percentiles()
            for p in config["percentiles"]:
                assert not math.isnan(percentiles[p])
                assert not math.isinf(percentiles[p])

    @given(
        config=percentile_tracker_config(),
        values=st.lists(
            st.floats(
                min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
            ),
            min_size=20,
            max_size=100,
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_percentile_tracker_thread_safety_properties(self, config, values):
        """Test that BoundedPercentileTracker is thread-safe."""
        tracker = BoundedPercentileTracker(**config)

        def worker_thread(thread_values):
            for value in thread_values:
                tracker.add_sample(value)
                # Reading should always work
                _ = tracker.get_percentiles()
                _ = tracker.get_statistics()

        # Split values across threads
        num_threads = min(4, len(values))
        chunk_size = len(values) // num_threads
        threads = []

        for i in range(num_threads):
            start = i * chunk_size
            end = start + chunk_size if i < num_threads - 1 else len(values)
            thread_values = values[start:end]
            thread = threading.Thread(target=worker_thread, args=(thread_values,))
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Final state should be consistent
        stats = tracker.get_statistics()
        assert stats["count"] <= config["max_samples"]
        assert stats["total_samples"] == len(values)


# Property Tests for TimeSeriesBuffer


class TestTimeSeriesBufferProperties:
    """Property tests for TimeSeriesBuffer datastructure."""

    @given(config=time_series_config())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_time_series_buffer_initialization_properties(self, config):
        """Test that TimeSeriesBuffer initializes correctly."""
        buffer = TimeSeriesBuffer(**config)

        assert buffer.max_age_seconds == config["max_age_seconds"]
        assert buffer.max_samples == config["max_samples"]

        # Empty buffer properties
        assert buffer.get_sample_count() == 0
        assert buffer.get_time_range() is None
        assert buffer.get_recent_samples(60.0) == []

    @given(
        config=time_series_config(),
        samples=st.lists(
            st.tuples(
                st.floats(
                    min_value=FIXED_TIME_BASE, max_value=FIXED_TIME_BASE + 3660.0
                ),  # Fixed range
                st.floats(
                    min_value=0.0,
                    max_value=1000.0,
                    allow_nan=False,
                    allow_infinity=False,
                ),
            ),
            min_size=1,
            max_size=100,
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_time_series_buffer_storage_properties(self, config, samples, mock_time):
        """Test that TimeSeriesBuffer stores samples correctly."""
        buffer = TimeSeriesBuffer(**config)
        current_time = FIXED_TIME_BASE  # Fixed current time

        # Add samples with valid timestamps
        valid_samples = []
        for timestamp, value in samples:
            # Ensure timestamp is positive and reasonable
            if timestamp > 0:
                buffer.add_sample(timestamp, value)
                valid_samples.append((timestamp, value))

        if not valid_samples:
            return  # Skip if no valid samples

        # Sample count should be bounded
        assert buffer.get_sample_count() <= config["max_samples"]

        # Time range should make sense if we have samples
        time_range = buffer.get_time_range()
        if time_range and valid_samples:
            start_time, end_time = time_range
            # NOTE: TimeSeriesBuffer assumes chronological insertion order
            # so start_time/end_time are first/last inserted, not min/max timestamps
            assert start_time > 0
            assert end_time > 0

            # Both timestamps should be from our valid samples
            valid_timestamps = [ts for ts, _ in valid_samples]
            assert (
                start_time in valid_timestamps
                or len(valid_samples) != buffer.get_sample_count()
            )  # Account for cleanup
            assert (
                end_time in valid_timestamps
                or len(valid_samples) != buffer.get_sample_count()
            )  # Account for cleanup

    @given(
        config=time_series_config(),
        base_time=st.floats(
            min_value=FIXED_TIME_BASE, max_value=FIXED_TIME_BASE + 1800.0
        ),  # Fixed range
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_time_series_buffer_age_cleanup_properties(
        self, config, base_time, mock_time
    ):
        """Test that TimeSeriesBuffer cleans up old samples correctly."""
        buffer = TimeSeriesBuffer(**config)
        max_age = config["max_age_seconds"]

        # Add samples at different times
        old_time = base_time - max_age - 100  # Definitely old
        recent_time = base_time  # Recent
        future_time = base_time + 60  # Future

        buffer.add_sample(old_time, 100.0)
        buffer.add_sample(recent_time, 200.0)
        buffer.add_sample(future_time, 300.0)

        # Get recent samples within age limit
        recent = buffer.get_recent_samples(max_age + 200)

        # Should contain recent and future samples, possibly not old ones
        assert len(recent) >= 1  # At least recent sample should be there

        # All returned samples should have reasonable timestamps
        for timestamp, value in recent:
            assert timestamp > 0
            assert not math.isnan(value)
            assert not math.isinf(value)

    @given(
        config=time_series_config(),
        num_samples=st.integers(min_value=50, max_value=200),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_time_series_buffer_count_bounds_properties(
        self, config, num_samples, mock_time
    ):
        """Test that TimeSeriesBuffer respects count bounds."""
        buffer = TimeSeriesBuffer(**config)
        max_samples = config["max_samples"]
        current_time = FIXED_TIME_BASE  # Fixed current time

        # Add many samples
        for i in range(num_samples):
            timestamp = current_time + i
            buffer.add_sample(timestamp, float(i))

        # Count should never exceed max_samples
        assert buffer.get_sample_count() <= max_samples

        # Cleanup should work
        cleaned = buffer.cleanup_old_samples()
        assert cleaned >= 0
        assert buffer.get_sample_count() <= max_samples


# Property Tests for MetricsAggregator


class TestMetricsAggregatorProperties:
    """Property tests for MetricsAggregator comprehensive functionality."""

    @given(
        name=st.text(min_size=1, max_size=50),
        measurements=st.lists(
            st.floats(
                min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False
            ),
            min_size=1,
            max_size=100,
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_metrics_aggregator_comprehensive_properties(
        self, name, measurements, mock_time
    ):
        """Test that MetricsAggregator aggregates all metrics correctly."""
        aggregator = MetricsAggregator(name=name)

        for measurement in measurements:
            aggregator.add_measurement(measurement)

        stats = aggregator.get_comprehensive_stats()

        # Basic properties
        assert stats["name"] == name
        assert stats["total_samples"] == len(measurements)
        assert stats["min_value"] == min(measurements)
        assert stats["max_value"] == max(measurements)
        assert stats["last_update"] > 0

        # Moving average should be reasonable
        ma_stats = stats["moving_average"]
        assert not math.isnan(ma_stats["average"])
        assert ma_stats["count"] <= 100  # Default window size

        # Percentiles should be reasonable
        percentiles = stats["percentiles"]
        for p, value in percentiles.items():
            assert not math.isnan(value)
            assert not math.isinf(value)
            assert 0 <= p <= 100

    @given(
        measurements=st.lists(
            st.floats(
                min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
            ),
            min_size=10,
            max_size=50,
        )
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_metrics_aggregator_factory_functions_properties(
        self, measurements, mock_time
    ):
        """Test that factory functions create valid aggregators."""
        latency_agg = create_latency_aggregator()
        throughput_agg = create_throughput_aggregator()
        error_agg = create_error_rate_aggregator()

        aggregators = [latency_agg, throughput_agg, error_agg]

        for agg in aggregators:
            # Add measurements to each
            for measurement in measurements:
                agg.add_measurement(measurement)

            stats = agg.get_comprehensive_stats()

            # All aggregators should have consistent behavior
            assert stats["total_samples"] == len(measurements)
            assert not math.isnan(stats["moving_average"]["average"])
            assert len(stats["percentiles"]) > 0

            # Reset should work
            agg.reset()
            reset_stats = agg.get_comprehensive_stats()
            assert reset_stats["total_samples"] == 0


# Property Tests for HealthScore and Status


class TestHealthScoreProperties:
    """Property tests for HealthScore datastructure."""

    @given(health_score=health_score_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_health_score_status_derivation_properties(self, health_score):
        """Test that health status is correctly derived from scores."""
        overall = health_score.overall_score

        # Status derivation should be consistent
        if overall >= 0.8:
            assert health_score.status == HealthStatus.HEALTHY
            assert health_score.is_healthy
            assert not health_score.is_degraded
            assert not health_score.needs_attention
        elif overall >= 0.6:
            assert health_score.status == HealthStatus.DEGRADED
            assert not health_score.is_healthy
            assert health_score.is_degraded
            assert not health_score.needs_attention
        elif overall >= 0.3:
            assert health_score.status == HealthStatus.CRITICAL
            assert not health_score.is_healthy
            assert not health_score.is_degraded
            assert health_score.needs_attention
        else:
            assert health_score.status == HealthStatus.UNAVAILABLE
            assert not health_score.is_healthy
            assert not health_score.is_degraded
            assert health_score.needs_attention

    @given(health_score=health_score_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_health_score_immutability_properties(self, health_score):
        """Test that HealthScore instances are properly immutable."""
        # Should not be able to modify fields
        with pytest.raises(AttributeError):
            health_score.overall_score = 0.5  # type: ignore

        # Properties should be stable
        original_status = health_score.status
        original_healthy = health_score.is_healthy

        # Multiple calls should return same results
        assert health_score.status == original_status
        assert health_score.is_healthy == original_healthy


# Property Tests for MetricValue


class TestMetricValueProperties:
    """Property tests for MetricValue datastructure."""

    @given(metric_value=metric_value_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_metric_value_validation_properties(self, metric_value):
        """Test that MetricValue validation works correctly."""
        # Should be properly constructed
        assert metric_value.value >= 0 or metric_value.metric_type != MetricType.COUNTER
        assert metric_value.timestamp > 0
        assert isinstance(metric_value.labels, dict)

        # Should be immutable
        with pytest.raises(AttributeError):
            metric_value.value = 999.0  # type: ignore

    def test_metric_value_counter_validation_properties(self):
        """Test that counter metrics enforce non-negative values."""
        # Negative counter should raise error
        with pytest.raises(ValueError, match="Counter metrics must be non-negative"):
            MetricValue(
                value=-1.0, timestamp=time.time(), metric_type=MetricType.COUNTER
            )

        # Positive counter should work
        counter = MetricValue(
            value=10.0, timestamp=time.time(), metric_type=MetricType.COUNTER
        )
        assert counter.value == 10.0

    def test_metric_value_timestamp_validation_properties(self):
        """Test that timestamp validation works correctly."""
        # Invalid timestamps should raise errors
        with pytest.raises(ValueError, match="Timestamp must be a positive number"):
            MetricValue(value=1.0, timestamp=0.0, metric_type=MetricType.GAUGE)

        with pytest.raises(ValueError, match="Timestamp must be a positive number"):
            MetricValue(value=1.0, timestamp=-1.0, metric_type=MetricType.GAUGE)


# Comprehensive Integration Properties


class TestMetricsStructuresIntegrationProperties:
    """Integration property tests across all metrics datastructures."""

    @given(
        window_size=st.integers(min_value=10, max_value=100),
        max_samples=st.integers(min_value=50, max_value=200),
        measurements=st.lists(
            st.floats(
                min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False
            ),
            min_size=20,
            max_size=300,
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_all_structures_memory_bounds_properties(
        self, window_size, max_samples, measurements, mock_time
    ):
        """Test that all datastructures respect memory bounds under load."""
        # Create all structures
        moving_avg = BoundedMovingAverage(window_size=window_size)
        percentile_tracker = BoundedPercentileTracker(max_samples=max_samples)
        time_series = TimeSeriesBuffer(max_samples=max_samples)
        aggregator = MetricsAggregator(
            name="test",
            moving_average=moving_avg,
            percentile_tracker=percentile_tracker,
            time_series=time_series,
        )

        current_time = FIXED_TIME_BASE

        # Add many measurements
        for i, measurement in enumerate(measurements):
            timestamp = current_time + i

            # Add to individual structures
            moving_avg.add_value(measurement)
            percentile_tracker.add_sample(measurement)
            time_series.add_sample(timestamp, measurement)
            aggregator.add_measurement(measurement, timestamp)

        # All structures should respect memory bounds
        assert moving_avg.count <= window_size
        assert percentile_tracker.get_statistics()["count"] <= max_samples
        assert time_series.get_sample_count() <= max_samples

        # Aggregator should have consistent state
        stats = aggregator.get_comprehensive_stats()
        assert stats["total_samples"] == len(measurements)
        assert not math.isnan(stats["moving_average"]["average"])

    @given(
        num_threads=st.integers(min_value=2, max_value=8),
        measurements_per_thread=st.integers(min_value=10, max_value=50),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_concurrent_access_properties(
        self, num_threads, measurements_per_thread, mock_time
    ):
        """Test that all structures handle concurrent access correctly."""
        aggregator = MetricsAggregator(name="concurrent_test")
        results = []

        def worker_thread(thread_id):
            thread_results = []
            for i in range(measurements_per_thread):
                value = float(thread_id * 100 + i)
                aggregator.add_measurement(value)
                thread_results.append(value)
            return thread_results

        # Run concurrent threads
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(worker_thread, thread_id)
                for thread_id in range(num_threads)
            ]

            for future in as_completed(futures):
                results.extend(future.result())

        # Final state should be consistent
        stats = aggregator.get_comprehensive_stats()
        expected_total = num_threads * measurements_per_thread

        assert stats["total_samples"] == expected_total
        assert stats["min_value"] == min(results)
        assert stats["max_value"] == max(results)
        assert not math.isnan(stats["moving_average"]["average"])


if __name__ == "__main__":
    pytest.main([__file__])
