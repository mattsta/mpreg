"""
Tests for unified monitoring and observability system.

Tests the comprehensive monitoring capabilities across all MPREG systems
including ULID-based stable tracking, cross-system correlation, and
performance analytics.
"""

import time

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.core.monitoring.unified_monitoring import (
    EventType,
    HealthStatus,
    MonitoringConfig,
    SystemPerformanceMetrics,
    SystemType,
    UnifiedSystemMonitor,
    create_unified_system_monitor,
)
from mpreg.core.transport.enhanced_health import HealthScore


class MockSystemMonitor:
    """Mock system monitor for testing."""

    def __init__(
        self,
        system_type: SystemType,
        health_score: HealthScore = 0.95,
        health_status: HealthStatus = HealthStatus.HEALTHY,
        requests_per_second: float = 100.0,
        average_latency_ms: float = 50.0,
        error_rate_percent: float = 1.0,
    ):
        self.system_type = system_type
        self.health_score = health_score
        self.health_status = health_status
        self.requests_per_second = requests_per_second
        self.average_latency_ms = average_latency_ms
        self.error_rate_percent = error_rate_percent

    async def get_system_metrics(self) -> SystemPerformanceMetrics:
        """Get mock system performance metrics."""
        return SystemPerformanceMetrics(
            system_type=self.system_type,
            system_name=f"mock_{self.system_type.value}",
            requests_per_second=self.requests_per_second,
            average_latency_ms=self.average_latency_ms,
            p95_latency_ms=self.average_latency_ms * 1.5,
            p99_latency_ms=self.average_latency_ms * 2.0,
            error_rate_percent=self.error_rate_percent,
            active_connections=10,
            total_operations_last_hour=3600,
            last_updated=time.time(),
        )

    async def get_health_status(self) -> tuple[HealthScore, HealthStatus]:
        """Get mock health status."""
        return self.health_score, self.health_status


class TestMonitoringConfig:
    """Test monitoring configuration."""

    def test_monitoring_config_creation(self):
        """Test monitoring configuration creation with defaults."""
        config = MonitoringConfig()

        assert config.metrics_collection_interval_ms == 5000.0
        assert config.correlation_cleanup_interval_ms == 60000.0
        assert config.health_check_interval_ms == 10000.0
        assert config.max_event_history == 50000
        assert config.max_correlation_history == 100000
        assert config.enable_transport_monitoring is True
        assert config.enable_federation_monitoring is True
        assert config.enable_cache_monitoring is True
        assert config.enable_cross_system_correlation is True

    def test_monitoring_config_custom_values(self):
        """Test monitoring configuration with custom values."""
        config = MonitoringConfig(
            metrics_collection_interval_ms=1000.0,
            correlation_cleanup_interval_ms=30000.0,
            max_event_history=10000,
            enable_transport_monitoring=False,
        )

        assert config.metrics_collection_interval_ms == 1000.0
        assert config.correlation_cleanup_interval_ms == 30000.0
        assert config.max_event_history == 10000
        assert config.enable_transport_monitoring is False


class TestTrackingIdGeneration:
    """Test ULID-based tracking ID generation."""

    def test_tracking_id_generation_with_existing_id(self):
        """Test tracking ID generation with existing object ID."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        existing_id = "existing_request_123"
        tracking_id = monitor.generate_tracking_id(existing_id)

        assert tracking_id == existing_id

    def test_tracking_id_generation_new_ulid(self):
        """Test tracking ID generation with new ULID."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        tracking_id = monitor.generate_tracking_id()

        assert tracking_id.startswith("mon-")
        assert len(tracking_id) == 30  # "mon-" + 26-char ULID

    def test_tracking_id_generation_custom_namespace(self):
        """Test tracking ID generation with custom namespace."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        tracking_id = monitor.generate_tracking_id(namespace_prefix="custom")

        assert tracking_id.startswith("custom-")
        assert len(tracking_id) == 33  # "custom-" + 26-char ULID

    def test_system_tracking_id_generation(self):
        """Test system-specific tracking ID generation."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        # Test all system types
        system_prefixes = {
            SystemType.RPC: "rpc-",
            SystemType.PUBSUB: "ps-",
            SystemType.QUEUE: "q-",
            SystemType.CACHE: "cache-",
            SystemType.FEDERATION: "fed-",
            SystemType.TRANSPORT: "tx-",
        }

        for system_type, expected_prefix in system_prefixes.items():
            tracking_id = monitor.generate_system_tracking_id(system_type)
            assert tracking_id.startswith(expected_prefix)
            assert len(tracking_id) >= len(expected_prefix) + 26  # prefix + ULID

    def test_tracking_id_uniqueness(self):
        """Test that generated tracking IDs are unique."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        tracking_ids = set()
        for _ in range(100):
            tracking_id = monitor.generate_tracking_id()
            assert tracking_id not in tracking_ids
            tracking_ids.add(tracking_id)


class TestCrossSystemEventRecording:
    """Test cross-system event recording and tracking."""

    @pytest.mark.asyncio
    async def test_event_recording_with_ulid_tracking(self):
        """Test event recording with ULID-based tracking."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        correlation_id = "test_correlation_123"
        existing_object_id = "rpc_request_456"

        tracking_id = await monitor.record_cross_system_event(
            correlation_id=correlation_id,
            event_type=EventType.REQUEST_START,
            source_system=SystemType.RPC,
            target_system=SystemType.PUBSUB,
            latency_ms=25.5,
            success=True,
            existing_object_id=existing_object_id,
        )

        # Should use existing object ID as tracking ID
        assert tracking_id == existing_object_id

        # Event should be in all tracking structures
        assert len(monitor.event_history) == 1
        assert correlation_id in monitor.correlation_chains
        assert len(monitor.correlation_chains[correlation_id]) == 1
        assert tracking_id in monitor.tracking_id_to_events
        assert len(monitor.tracking_id_to_events[tracking_id]) == 1
        assert correlation_id in monitor.active_correlations
        assert tracking_id in monitor.active_tracking_ids

    @pytest.mark.asyncio
    async def test_event_recording_without_existing_id(self):
        """Test event recording without existing object ID (generates new ULID)."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        correlation_id = "test_correlation_789"

        tracking_id = await monitor.record_cross_system_event(
            correlation_id=correlation_id,
            event_type=EventType.REQUEST_START,
            source_system=SystemType.QUEUE,
            success=True,
        )

        # Should generate new monitoring ULID
        assert tracking_id.startswith("mon-")
        assert len(tracking_id) == 30

        # Verify event tracking
        assert len(monitor.event_history) == 1
        event = monitor.event_history[0]
        assert event.tracking_id == tracking_id
        assert event.correlation_id == correlation_id
        assert event.source_system == SystemType.QUEUE

    @pytest.mark.asyncio
    async def test_request_lifecycle_tracking(self):
        """Test complete request lifecycle tracking."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        correlation_id = "lifecycle_test"
        object_id = "request_lifecycle_123"

        # Start request
        tracking_id = await monitor.record_cross_system_event(
            correlation_id=correlation_id,
            event_type=EventType.REQUEST_START,
            source_system=SystemType.RPC,
            existing_object_id=object_id,
        )

        assert tracking_id in monitor.active_tracking_ids
        assert correlation_id in monitor.active_correlations

        # Complete request
        await monitor.record_cross_system_event(
            correlation_id=correlation_id,
            event_type=EventType.REQUEST_COMPLETE,
            source_system=SystemType.RPC,
            tracking_id=tracking_id,
            latency_ms=150.0,
        )

        # Should be removed from active tracking
        assert tracking_id not in monitor.active_tracking_ids
        assert correlation_id not in monitor.active_correlations

        # But should remain in history
        timeline = monitor.get_tracking_timeline(tracking_id)
        assert len(timeline) == 2
        assert timeline[0].event_type == EventType.REQUEST_START
        assert timeline[1].event_type == EventType.REQUEST_COMPLETE

    @pytest.mark.asyncio
    async def test_cross_system_performance_tracking(self):
        """Test cross-system performance metrics tracking."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        # Record several cross-system interactions
        latencies = [10.0, 15.0, 20.0, 25.0, 30.0]

        for i, latency in enumerate(latencies):
            await monitor.record_cross_system_event(
                correlation_id=f"perf_test_{i}",
                event_type=EventType.REQUEST_START,
                source_system=SystemType.RPC,
                target_system=SystemType.PUBSUB,
                latency_ms=latency,
            )

        # Check performance tracking
        performance_summary = monitor.get_cross_system_performance_summary()
        rpc_to_pubsub_key = (SystemType.RPC, SystemType.PUBSUB)

        assert rpc_to_pubsub_key in performance_summary
        perf_data = performance_summary[rpc_to_pubsub_key]

        assert perf_data["total_interactions"] == 5
        assert perf_data["average_latency_ms"] == 20.0  # Average of latencies
        assert perf_data["p50_latency_ms"] == 20.0
        assert (
            perf_data["p95_latency_ms"] == 30.0
        )  # Should be close to max for small dataset


class TestUnifiedSystemMonitor:
    """Test unified system monitor functionality."""

    @pytest.mark.asyncio
    async def test_monitor_creation_and_lifecycle(self):
        """Test monitor creation and lifecycle management."""
        config = MonitoringConfig(
            metrics_collection_interval_ms=100.0,  # Fast for testing
            correlation_cleanup_interval_ms=200.0,
            health_check_interval_ms=150.0,
        )
        monitor = UnifiedSystemMonitor(config=config)

        # Test start
        await monitor.start()
        assert monitor._running is True
        assert len(monitor._monitoring_tasks) == 3  # Three background tasks

        # Test stop
        await monitor.stop()
        assert monitor._running is False
        assert len(monitor._monitoring_tasks) == 0

    @pytest.mark.asyncio
    async def test_monitor_with_system_monitors(self):
        """Test monitor with mock system monitors attached."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        # Add mock system monitors
        monitor.rpc_monitor = MockSystemMonitor(SystemType.RPC, health_score=0.95)
        monitor.pubsub_monitor = MockSystemMonitor(SystemType.PUBSUB, health_score=0.88)
        monitor.queue_monitor = MockSystemMonitor(SystemType.QUEUE, health_score=0.92)
        monitor.cache_monitor = MockSystemMonitor(SystemType.CACHE, health_score=0.90)

        # Get unified metrics
        metrics = await monitor.get_unified_metrics()

        assert metrics.rpc_metrics is not None
        assert metrics.pubsub_metrics is not None
        assert metrics.queue_metrics is not None
        assert metrics.cache_metrics is not None
        assert metrics.federation_metrics is None  # Not attached

        # Check overall health calculation
        assert 0.0 <= metrics.overall_health_score <= 1.0
        assert metrics.overall_health_status in [
            HealthStatus.HEALTHY,
            HealthStatus.DEGRADED,
            HealthStatus.CRITICAL,
        ]

        # Check system counts (should count all 6 possible systems)
        total_systems = (
            metrics.systems_healthy
            + metrics.systems_degraded
            + metrics.systems_critical
            + metrics.systems_unavailable
        )
        assert (
            total_systems == 6
        )  # Six possible systems (federation_monitor and transport_monitor are None -> unavailable)

    @pytest.mark.asyncio
    async def test_timeline_retrieval(self):
        """Test correlation and tracking timeline retrieval."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        correlation_id = "timeline_test"
        object_id = "timeline_object_123"

        # Record multiple events with same correlation and tracking IDs
        tracking_id = await monitor.record_cross_system_event(
            correlation_id=correlation_id,
            event_type=EventType.REQUEST_START,
            source_system=SystemType.RPC,
            existing_object_id=object_id,
        )

        await monitor.record_cross_system_event(
            correlation_id=correlation_id,
            event_type=EventType.DEPENDENCY_RESOLVED,
            source_system=SystemType.RPC,
            target_system=SystemType.PUBSUB,
            tracking_id=tracking_id,
        )

        await monitor.record_cross_system_event(
            correlation_id=correlation_id,
            event_type=EventType.REQUEST_COMPLETE,
            source_system=SystemType.RPC,
            tracking_id=tracking_id,
        )

        # Test correlation timeline
        correlation_timeline = monitor.get_correlation_timeline(correlation_id)
        assert len(correlation_timeline) == 3

        # Test tracking timeline
        tracking_timeline = monitor.get_tracking_timeline(tracking_id)
        assert len(tracking_timeline) == 3

        # Should be the same events
        assert correlation_timeline == tracking_timeline

        # Verify event order and types
        assert correlation_timeline[0].event_type == EventType.REQUEST_START
        assert correlation_timeline[1].event_type == EventType.DEPENDENCY_RESOLVED
        assert correlation_timeline[2].event_type == EventType.REQUEST_COMPLETE

    @pytest.mark.asyncio
    async def test_event_filtering(self):
        """Test event filtering by system type and event type."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        # Record events from different systems
        await monitor.record_cross_system_event(
            correlation_id="filter_test_1",
            event_type=EventType.REQUEST_START,
            source_system=SystemType.RPC,
        )

        await monitor.record_cross_system_event(
            correlation_id="filter_test_2",
            event_type=EventType.REQUEST_START,
            source_system=SystemType.PUBSUB,
        )

        await monitor.record_cross_system_event(
            correlation_id="filter_test_3",
            event_type=EventType.HEALTH_CHECK,
            source_system=SystemType.RPC,
        )

        # Test system type filtering
        rpc_events = monitor.get_recent_events(system_type=SystemType.RPC)
        assert len(rpc_events) == 2
        assert all(
            event.source_system == SystemType.RPC
            or event.target_system == SystemType.RPC
            for event in rpc_events
        )

        # Test event type filtering
        start_events = monitor.get_recent_events(event_type=EventType.REQUEST_START)
        assert len(start_events) == 2
        assert all(
            event.event_type == EventType.REQUEST_START for event in start_events
        )

        # Test combined filtering
        rpc_start_events = monitor.get_recent_events(
            system_type=SystemType.RPC, event_type=EventType.REQUEST_START
        )
        assert len(rpc_start_events) == 1


class TestMonitoringAsyncContextManager:
    """Test monitoring system as async context manager."""

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test unified system monitor as async context manager."""
        config = MonitoringConfig(
            metrics_collection_interval_ms=100.0,
            correlation_cleanup_interval_ms=200.0,
            health_check_interval_ms=150.0,
        )
        monitor = UnifiedSystemMonitor(config=config)

        async with monitor as active_monitor:
            assert active_monitor is monitor
            assert monitor._running is True

        # Should be stopped after context exit
        assert monitor._running is False


class TestFactoryFunction:
    """Test factory function for creating monitors."""

    def test_factory_function_defaults(self):
        """Test factory function with default parameters."""
        monitor = create_unified_system_monitor()

        assert monitor.config.metrics_collection_interval_ms == 5000.0
        assert monitor.config.correlation_cleanup_interval_ms == 60000.0
        assert monitor.config.health_check_interval_ms == 10000.0
        assert monitor.config.enable_transport_monitoring is True

    def test_factory_function_custom_parameters(self):
        """Test factory function with custom parameters."""
        monitor = create_unified_system_monitor(
            metrics_collection_interval_ms=1000.0,
            max_event_history=25000,
            enable_transport_monitoring=False,
            enable_cache_monitoring=False,
        )

        assert monitor.config.metrics_collection_interval_ms == 1000.0
        assert monitor.config.max_event_history == 25000
        assert monitor.config.enable_transport_monitoring is False
        assert monitor.config.enable_cache_monitoring is False


class TestMonitoringPropertyBasedTests:
    """Property-based tests for monitoring system."""

    @given(
        num_events=st.integers(1, 50),
        system_types=st.lists(
            st.sampled_from(
                [SystemType.RPC, SystemType.PUBSUB, SystemType.QUEUE, SystemType.CACHE]
            ),
            min_size=1,
            max_size=4,
        ),
    )
    @settings(max_examples=20, deadline=3000)
    @pytest.mark.asyncio
    async def test_event_recording_properties(self, num_events, system_types):
        """Property-based test for event recording correctness."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        recorded_tracking_ids = []

        for i in range(num_events):
            source_system = system_types[i % len(system_types)]

            tracking_id = await monitor.record_cross_system_event(
                correlation_id=f"prop_test_{i}",
                event_type=EventType.REQUEST_START,
                source_system=source_system,
            )

            recorded_tracking_ids.append(tracking_id)

        # Properties to verify
        assert len(monitor.event_history) == num_events
        assert len(monitor.correlation_chains) == num_events
        assert len(monitor.tracking_id_to_events) == num_events
        assert len(monitor.active_tracking_ids) == num_events

        # All tracking IDs should be unique
        assert len(set(recorded_tracking_ids)) == num_events

        # All tracking IDs should have events
        for tracking_id in recorded_tracking_ids:
            timeline = monitor.get_tracking_timeline(tracking_id)
            assert len(timeline) >= 1

    @given(
        health_scores=st.lists(
            st.floats(min_value=0.0, max_value=1.0), min_size=1, max_size=6
        ),
    )
    @settings(max_examples=15, deadline=2000)
    @pytest.mark.asyncio
    async def test_health_calculation_properties(self, health_scores):
        """Property-based test for health score calculation."""
        config = MonitoringConfig()
        monitor = UnifiedSystemMonitor(config=config)

        # Create mock monitors with different health scores
        system_types = [
            SystemType.RPC,
            SystemType.PUBSUB,
            SystemType.QUEUE,
            SystemType.CACHE,
            SystemType.FEDERATION,
            SystemType.TRANSPORT,
        ]

        for i, health_score in enumerate(health_scores):
            if i < len(system_types):
                system_type = system_types[i]
                mock_monitor = MockSystemMonitor(
                    system_type=system_type,
                    health_score=health_score,
                    error_rate_percent=max(
                        0.0, (1.0 - health_score) * 10.0
                    ),  # Higher error rate for lower health
                )

                # Attach to monitor
                if system_type == SystemType.RPC:
                    monitor.rpc_monitor = mock_monitor
                elif system_type == SystemType.PUBSUB:
                    monitor.pubsub_monitor = mock_monitor
                elif system_type == SystemType.QUEUE:
                    monitor.queue_monitor = mock_monitor
                elif system_type == SystemType.CACHE:
                    monitor.cache_monitor = mock_monitor
                elif system_type == SystemType.FEDERATION:
                    monitor.federation_monitor = mock_monitor
                elif system_type == SystemType.TRANSPORT:
                    monitor.transport_monitor = mock_monitor

        metrics = await monitor.get_unified_metrics()

        # Properties to verify
        assert 0.0 <= metrics.overall_health_score <= 1.0
        assert metrics.overall_health_status in [
            HealthStatus.HEALTHY,
            HealthStatus.DEGRADED,
            HealthStatus.CRITICAL,
            HealthStatus.UNAVAILABLE,
        ]

        # Total system count should be number of attached monitors + remaining unavailable ones
        total_systems = (
            metrics.systems_healthy
            + metrics.systems_degraded
            + metrics.systems_critical
            + metrics.systems_unavailable
        )
        # We have 6 possible system types, but only attach len(health_scores) of them
        # The rest should be counted as unavailable
        assert total_systems == 6  # Always 6 system types total


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
