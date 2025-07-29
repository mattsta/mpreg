"""
Comprehensive tests for transport enhancement modules.

Tests the circuit breaker, correlation tracking, and enhanced health monitoring
functionality to ensure they work correctly and can be integrated with
existing MPREG transport infrastructure.
"""

import asyncio
import time
from unittest.mock import AsyncMock

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.core.transport.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
    create_circuit_breaker,
)
from mpreg.core.transport.correlation import (
    create_correlation_tracker,
)
from mpreg.core.transport.enhanced_health import (
    HealthTrend,
    create_connection_health_monitor,
    create_transport_health_aggregator,
)


class TestCircuitBreaker:
    """Test circuit breaker functionality."""

    def test_circuit_breaker_creation(self):
        """Test circuit breaker creation and initial state."""
        breaker = create_circuit_breaker("ws://test:8080")

        assert breaker.endpoint == "ws://test:8080"
        assert breaker.state == CircuitBreakerState.CLOSED
        assert breaker.failure_count == 0
        assert breaker.success_count == 0
        assert breaker.can_execute() is True

    def test_circuit_breaker_failure_threshold(self):
        """Test circuit breaker opens after failure threshold."""
        breaker = create_circuit_breaker("ws://test:8080", failure_threshold=3)

        # Should remain closed for failures under threshold
        for i in range(2):
            breaker.record_failure()
            assert breaker.state == CircuitBreakerState.CLOSED
            assert breaker.can_execute() is True

        # Should open at threshold
        breaker.record_failure()
        assert breaker.state == CircuitBreakerState.OPEN
        assert breaker.can_execute() is False

    def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery process."""
        breaker = create_circuit_breaker(
            "ws://test:8080",
            failure_threshold=2,
            recovery_timeout_ms=500.0,  # 500ms for stable testing
            success_threshold=2,
        )

        # Trigger open state
        breaker.record_failure()
        breaker.record_failure()
        assert breaker.state == CircuitBreakerState.OPEN

        # Wait for recovery timeout
        time.sleep(0.6)  # 600ms > 500ms timeout

        # Should transition to half-open
        assert breaker.can_execute() is True
        assert breaker.state == CircuitBreakerState.HALF_OPEN

        # Record successful operations
        breaker.record_success()
        assert breaker.state == CircuitBreakerState.HALF_OPEN

        breaker.record_success()
        assert breaker.state == CircuitBreakerState.CLOSED

    def test_circuit_breaker_config(self):
        """Test circuit breaker configuration."""
        config = CircuitBreakerConfig(
            failure_threshold=10, recovery_timeout_ms=30000.0, success_threshold=5
        )

        breaker = CircuitBreaker("ws://test:8080", config)

        assert breaker.config.failure_threshold == 10
        assert breaker.config.recovery_timeout_ms == 30000.0
        assert breaker.config.success_threshold == 5

    @given(
        failure_threshold=st.integers(1, 10),
        recovery_timeout_ms=st.floats(100.0, 10000.0),
        success_threshold=st.integers(1, 5),
    )
    @settings(max_examples=20, deadline=2000)
    def test_circuit_breaker_properties(
        self, failure_threshold, recovery_timeout_ms, success_threshold
    ):
        """Property-based test for circuit breaker behavior."""
        breaker = create_circuit_breaker(
            "ws://test:8080",
            failure_threshold=failure_threshold,
            recovery_timeout_ms=recovery_timeout_ms,
            success_threshold=success_threshold,
        )

        # Circuit breaker should start closed
        assert breaker.state == CircuitBreakerState.CLOSED
        assert breaker.can_execute() is True

        # Should open after failure threshold
        for _ in range(failure_threshold):
            breaker.record_failure()
        assert breaker.state == CircuitBreakerState.OPEN


class TestCorrelationTracker:
    """Test correlation tracking functionality."""

    def test_correlation_tracker_creation(self):
        """Test correlation tracker creation."""
        tracker = create_correlation_tracker()

        assert len(tracker.active_correlations) == 0
        assert len(tracker.correlation_history) == 0
        assert tracker.config.correlation_timeout_ms == 30000.0

    def test_correlation_id_generation(self):
        """Test correlation ID generation."""
        tracker = create_correlation_tracker()

        corr_id1 = tracker.generate_correlation_id()
        corr_id2 = tracker.generate_correlation_id()

        assert corr_id1 != corr_id2
        assert len(corr_id1) > 0
        assert len(corr_id2) > 0

    def test_correlation_lifecycle(self):
        """Test complete correlation lifecycle."""
        tracker = create_correlation_tracker()

        # Start correlation
        corr_id = tracker.start_correlation()
        assert corr_id in tracker.active_correlations

        # Complete correlation
        result = tracker.complete_correlation(
            corr_id,
            response_data=b"test response",
            endpoint="ws://test:8080",
            connection_id="conn_123",
            success=True,
        )

        assert corr_id not in tracker.active_correlations
        assert result.correlation_id == corr_id
        assert result.response_data == b"test response"
        assert result.success is True
        assert result.latency_ms >= 0.0
        assert len(tracker.correlation_history) == 1

    def test_correlation_cleanup(self):
        """Test correlation cleanup of expired entries."""
        tracker = create_correlation_tracker(
            correlation_timeout_ms=1000.0
        )  # 1000ms timeout for stable testing

        # Start some correlations
        corr_id1 = tracker.start_correlation()
        time.sleep(0.3)  # 300ms for stable testing
        corr_id2 = tracker.start_correlation()
        time.sleep(0.8)  # Additional 800ms for stable testing

        # Clean up expired correlations
        expired_count = tracker.cleanup_expired_correlations()

        # First correlation should be expired (1100ms > 1000ms)
        # Second correlation should still be active (80ms < 100ms)
        assert expired_count == 1
        assert corr_id1 not in tracker.active_correlations
        assert corr_id2 in tracker.active_correlations

    def test_correlation_statistics(self):
        """Test correlation statistics collection."""
        tracker = create_correlation_tracker()

        # Complete some correlations
        for i in range(5):
            corr_id = tracker.start_correlation()
            success = i < 4  # 4 successes, 1 failure
            tracker.complete_correlation(
                corr_id,
                response_data=b"test",
                endpoint="ws://test:8080",
                connection_id=f"conn_{i}",
                success=success,
            )

        stats = tracker.get_correlation_statistics()

        assert stats["total_correlation_history"] == 5
        assert stats["success_rate_percent"] == 80.0  # 4/5 = 80%
        assert stats["active_correlations"] == 0
        assert stats["recent_operations"] == 5

    @given(num_correlations=st.integers(1, 20), success_rate=st.floats(0.0, 1.0))
    @settings(max_examples=10, deadline=3000)
    def test_correlation_properties(self, num_correlations, success_rate):
        """Property-based test for correlation tracking."""
        tracker = create_correlation_tracker()

        for i in range(num_correlations):
            corr_id = tracker.start_correlation()
            success = i < int(num_correlations * success_rate)
            tracker.complete_correlation(
                corr_id,
                response_data=b"test",
                endpoint="ws://test:8080",
                connection_id=f"conn_{i}",
                success=success,
            )

        stats = tracker.get_correlation_statistics()
        assert stats["total_correlation_history"] == num_correlations
        assert 0.0 <= stats["success_rate_percent"] <= 100.0


class TestConnectionHealthMonitor:
    """Test connection health monitoring functionality."""

    def test_health_monitor_creation(self):
        """Test health monitor creation."""
        monitor = create_connection_health_monitor("conn_123", "ws://test:8080")

        assert monitor.connection_id == "conn_123"
        assert monitor.endpoint == "ws://test:8080"
        assert monitor.total_operations == 0
        assert (
            monitor.calculate_health_score() == 1.0
        )  # Perfect health when no operations

    def test_health_score_calculation(self):
        """Test health score calculation with operations."""
        monitor = create_connection_health_monitor("conn_123", "ws://test:8080")

        # Record successful operations
        for _ in range(8):
            monitor.record_operation(latency_ms=10.0, success=True)

        # Record some failures
        for _ in range(2):
            monitor.record_operation(latency_ms=100.0, success=False)

        health_score = monitor.calculate_health_score()

        # Should be between 0 and 1, and less than perfect due to failures
        assert 0.0 <= health_score <= 1.0
        assert health_score < 1.0  # Not perfect due to failures
        assert health_score > 0.5  # Still reasonably healthy (80% success rate)

    def test_health_trends(self):
        """Test health trend analysis."""
        monitor = create_connection_health_monitor("conn_123", "ws://test:8080")

        # All successful operations should be stable/improving
        for _ in range(10):
            monitor.record_operation(latency_ms=10.0, success=True)

        trend = monitor.get_health_trend()
        assert trend in [HealthTrend.STABLE, HealthTrend.IMPROVING]

        # Recent failures should show degrading trend
        for _ in range(3):
            monitor.record_operation(latency_ms=200.0, success=False)

        trend = monitor.get_health_trend()
        assert trend in [HealthTrend.DEGRADING, HealthTrend.CRITICAL]

    def test_health_metrics(self):
        """Test comprehensive health metrics."""
        monitor = create_connection_health_monitor("conn_123", "ws://test:8080")

        # Record mixed operations
        for i in range(10):
            latency = 10.0 if i < 8 else 100.0
            success = i < 8
            monitor.record_operation(latency_ms=latency, success=success)

        metrics = monitor.get_health_metrics()

        assert metrics.connection_id == "conn_123"
        assert metrics.endpoint == "ws://test:8080"
        assert metrics.total_operations == 10
        assert metrics.success_rate_percent == 80.0
        assert 0.0 <= metrics.health_score <= 1.0
        assert metrics.recent_failures == 2

    @given(
        num_operations=st.integers(10, 100),
        success_rate=st.floats(0.1, 1.0),
        avg_latency=st.floats(1.0, 1000.0),
    )
    @settings(max_examples=10, deadline=3000)
    def test_health_monitor_properties(self, num_operations, success_rate, avg_latency):
        """Property-based test for health monitoring."""
        monitor = create_connection_health_monitor("conn_123", "ws://test:8080")

        for i in range(num_operations):
            success = i < int(num_operations * success_rate)
            monitor.record_operation(latency_ms=avg_latency, success=success)

        metrics = monitor.get_health_metrics()

        # Invariants
        assert metrics.total_operations == num_operations
        assert 0.0 <= metrics.health_score <= 1.0
        assert 0.0 <= metrics.success_rate_percent <= 100.0
        assert metrics.average_latency_ms >= 0.0


class TestTransportHealthAggregator:
    """Test transport health aggregation functionality."""

    def test_aggregator_creation(self):
        """Test health aggregator creation."""
        aggregator = create_transport_health_aggregator("ws://test:8080")

        assert aggregator.endpoint == "ws://test:8080"
        assert len(aggregator.connection_monitors) == 0

    def test_aggregator_with_monitors(self):
        """Test aggregator with multiple connection monitors."""
        aggregator = create_transport_health_aggregator("ws://test:8080")

        # Add connection monitors
        for i in range(3):
            monitor = create_connection_health_monitor(f"conn_{i}", "ws://test:8080")

            # Record different performance for each connection
            for j in range(10):
                success = j < (8 - i)  # Different success rates
                monitor.record_operation(latency_ms=10.0 * (i + 1), success=success)

            aggregator.add_connection_monitor(monitor)

        snapshot = aggregator.get_transport_health_snapshot()

        assert snapshot.endpoint == "ws://test:8080"
        assert snapshot.active_connections == 3
        assert snapshot.total_connections == 3
        assert 0.0 <= snapshot.overall_health_score <= 1.0
        assert snapshot.average_response_time_ms > 0.0

    def test_aggregator_empty_state(self):
        """Test aggregator with no connections."""
        aggregator = create_transport_health_aggregator("ws://test:8080")

        snapshot = aggregator.get_transport_health_snapshot()

        assert snapshot.endpoint == "ws://test:8080"
        assert snapshot.active_connections == 0
        assert snapshot.overall_health_score == 1.0
        assert snapshot.average_response_time_ms == 0.0
        assert snapshot.error_rate_percent == 0.0

    def test_monitor_removal(self):
        """Test removing connection monitors."""
        aggregator = create_transport_health_aggregator("ws://test:8080")

        monitor = create_connection_health_monitor("conn_123", "ws://test:8080")
        aggregator.add_connection_monitor(monitor)

        assert len(aggregator.connection_monitors) == 1

        aggregator.remove_connection_monitor("conn_123")

        assert len(aggregator.connection_monitors) == 0


class TestTransportEnhancementIntegration:
    """Test integration scenarios with existing transport infrastructure."""

    async def test_circuit_breaker_with_mock_transport(self):
        """Test circuit breaker integration with mock transport."""
        # Mock transport operation
        mock_transport = AsyncMock()
        mock_transport.send = AsyncMock()
        mock_transport.receive = AsyncMock(return_value=b"response")

        # Create circuit breaker
        breaker = create_circuit_breaker("ws://test:8080", failure_threshold=2)

        # Simulate operations with circuit breaker protection
        for i in range(5):
            if breaker.can_execute():
                try:
                    if i < 3:  # First 3 succeed
                        await mock_transport.send(b"data")
                        response = await mock_transport.receive()
                        breaker.record_success()
                    else:  # Last 2 fail
                        mock_transport.send.side_effect = Exception("Connection failed")
                        await mock_transport.send(b"data")
                        breaker.record_failure()
                except Exception:
                    breaker.record_failure()
            else:
                # Circuit is open, skip operation
                pass

        # Circuit should be open after failures
        assert breaker.state == CircuitBreakerState.OPEN
        assert not breaker.can_execute()

    async def test_correlation_with_health_monitoring(self):
        """Test correlation tracking combined with health monitoring."""
        tracker = create_correlation_tracker()
        monitor = create_connection_health_monitor("conn_123", "ws://test:8080")

        # Simulate correlated operations with health tracking
        for i in range(10):
            corr_id = tracker.start_correlation()

            # Simulate operation latency and success
            await asyncio.sleep(0.001)  # 1ms operation
            latency_ms = 5.0 + (i * 2.0)  # Increasing latency
            success = i < 8  # Most operations succeed

            # Record in both systems
            monitor.record_operation(latency_ms=latency_ms, success=success)
            tracker.complete_correlation(
                corr_id,
                response_data=b"response",
                endpoint="ws://test:8080",
                connection_id="conn_123",
                success=success,
            )

        # Verify both systems tracked the operations
        health_metrics = monitor.get_health_metrics()
        correlation_stats = tracker.get_correlation_statistics()

        assert health_metrics.total_operations == 10
        assert correlation_stats["total_correlation_history"] == 10
        assert (
            health_metrics.success_rate_percent
            == correlation_stats["success_rate_percent"]
        )

    def test_all_enhancements_together(self):
        """Test all transport enhancements working together."""
        # Create all enhancement components
        breaker = create_circuit_breaker("ws://test:8080")
        tracker = create_correlation_tracker()
        monitor = create_connection_health_monitor("conn_123", "ws://test:8080")
        aggregator = create_transport_health_aggregator("ws://test:8080")

        aggregator.add_connection_monitor(monitor)

        # Simulate operations using all enhancements
        for i in range(10):
            if breaker.can_execute():
                corr_id = tracker.start_correlation()

                # Simulate operation
                latency_ms = 10.0
                success = i < 8  # 80% success rate

                # Record in all systems
                monitor.record_operation(latency_ms=latency_ms, success=success)
                tracker.complete_correlation(
                    corr_id,
                    response_data=b"response",
                    endpoint="ws://test:8080",
                    connection_id="conn_123",
                    success=success,
                )

                if success:
                    breaker.record_success()
                else:
                    breaker.record_failure()

        # Verify all systems have consistent data
        health_snapshot = aggregator.get_transport_health_snapshot()
        correlation_stats = tracker.get_correlation_statistics()

        assert health_snapshot.active_connections == 1
        assert correlation_stats["total_correlation_history"] == 10
        assert (
            breaker.state == CircuitBreakerState.CLOSED
        )  # Should remain closed with 80% success


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
