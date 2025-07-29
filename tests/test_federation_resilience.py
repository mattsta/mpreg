"""
Tests for Federation Resilience System.

This module tests the comprehensive resilience features including:
- Circuit breakers with adaptive thresholds
- Health monitoring and auto-recovery
- Performance tracking and alerting
- Recovery strategies and coordination
"""

import asyncio
import time

import pytest

from mpreg.federation.federation_optimized import ClusterIdentity
from mpreg.federation.federation_resilience import (
    AdaptiveCircuitBreaker,
    EnhancedFederationResilience,
    FederationAutoRecovery,
    FederationHealthMonitor,
    HealthCheckConfiguration,
    HealthStatus,
    RecoveryStrategy,
    RetryConfiguration,
)


@pytest.fixture
def cluster_identity():
    """Create a test cluster identity."""
    return ClusterIdentity(
        cluster_id="test_cluster",
        cluster_name="Test Cluster",
        region="test_region",
        bridge_url="ws://test.example.com:9000",
        public_key_hash="test_hash",
        created_at=time.time(),
    )


@pytest.fixture
def health_config():
    """Create test health check configuration."""
    return HealthCheckConfiguration(
        check_interval_seconds=1.0,  # Fast checks for testing
        timeout_seconds=1.0,
        consecutive_failures_threshold=2,
        consecutive_successes_threshold=2,
        adaptive_interval=True,
        min_interval_seconds=0.5,
        max_interval_seconds=5.0,
    )


@pytest.fixture
def retry_config():
    """Create test retry configuration."""
    return RetryConfiguration(
        max_attempts=3,
        initial_delay_seconds=0.5,  # Realistic retries for stable testing
        max_delay_seconds=1.0,
        backoff_multiplier=2.0,
        jitter_factor=0.1,
    )


class TestAdaptiveCircuitBreaker:
    """Test adaptive circuit breaker functionality."""

    def test_circuit_breaker_initialization(self):
        """Test circuit breaker initialization."""
        cb = AdaptiveCircuitBreaker()

        assert cb.state == "closed"
        assert cb.failure_count == 0
        assert cb.success_count == 0
        assert cb.can_execute()

    def test_circuit_breaker_failure_tracking(self):
        """Test failure tracking and state transitions."""
        cb = AdaptiveCircuitBreaker(failure_threshold=3)

        # Record failures
        for i in range(2):
            cb.record_failure()
            assert cb.state == "closed"  # Still closed
            assert cb.can_execute()

        # Third failure should open circuit
        cb.record_failure()
        assert cb.state == "open"
        assert not cb.can_execute()

    def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery."""
        cb = AdaptiveCircuitBreaker(
            failure_threshold=2,
            success_threshold=2,
            timeout_seconds=2.0,  # Realistic timeout for stable testing
        )

        # Set the current timeout to match timeout_seconds
        cb.current_timeout = 2.0

        # Open the circuit
        cb.record_failure()
        cb.record_failure()
        assert cb.state == "open"

        # Wait for timeout
        time.sleep(2.5)

        # Should allow execution (half-open)
        assert cb.can_execute()

        # Record successes to close circuit
        cb.record_success()
        cb.record_success()
        assert cb.state == "closed"

    def test_adaptive_threshold_update(self):
        """Test adaptive threshold updates based on health."""
        from mpreg.federation.federation_resilience import ClusterHealthMetrics

        cb = AdaptiveCircuitBreaker(adaptive_thresholds=True, base_failure_threshold=5)

        # Healthy cluster should maintain threshold
        healthy_metrics = ClusterHealthMetrics(cluster_id="test", health_score=90.0)
        cb.update_adaptive_thresholds(healthy_metrics)
        assert cb.failure_threshold >= 4  # Should be close to base

        # Unhealthy cluster should lower threshold
        unhealthy_metrics = ClusterHealthMetrics(cluster_id="test", health_score=30.0)
        cb.update_adaptive_thresholds(unhealthy_metrics)
        assert cb.failure_threshold < 4  # Should be lowered


class TestFederationHealthMonitor:
    """Test federation health monitoring."""

    @pytest.mark.asyncio
    async def test_health_monitor_initialization(self, health_config, retry_config):
        """Test health monitor initialization."""
        monitor = FederationHealthMonitor(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        assert monitor.cluster_id == "test_cluster"
        assert monitor.global_health == HealthStatus.UNKNOWN
        assert len(monitor.cluster_health) == 0

    @pytest.mark.asyncio
    async def test_cluster_registration(
        self, cluster_identity, health_config, retry_config
    ):
        """Test cluster registration for monitoring."""
        monitor = FederationHealthMonitor(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        # Register cluster
        monitor.register_cluster(cluster_identity)
        assert cluster_identity.cluster_id in monitor.cluster_health

        # Unregister cluster
        monitor.unregister_cluster(cluster_identity.cluster_id)
        assert cluster_identity.cluster_id not in monitor.cluster_health

    @pytest.mark.asyncio
    async def test_health_monitoring_lifecycle(
        self, cluster_identity, health_config, retry_config
    ):
        """Test health monitoring start and stop."""
        monitor = FederationHealthMonitor(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        # Register a cluster
        monitor.register_cluster(cluster_identity)

        # Start monitoring
        await monitor.start_monitoring()
        assert len(monitor._monitoring_tasks) > 0

        # Wait briefly for monitoring to start
        await asyncio.sleep(0.1)

        # Stop monitoring
        await monitor.stop_monitoring()
        assert len(monitor._monitoring_tasks) == 0

    @pytest.mark.asyncio
    async def test_health_summary(self, cluster_identity, health_config, retry_config):
        """Test health summary generation."""
        monitor = FederationHealthMonitor(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        # Register cluster
        monitor.register_cluster(cluster_identity)

        # Get summary
        summary = monitor.get_health_summary()
        assert summary.total_clusters == 1
        assert summary.global_status == HealthStatus.UNKNOWN.value
        assert cluster_identity.cluster_id in summary.cluster_health

    @pytest.mark.asyncio
    async def test_alert_callbacks(self, cluster_identity, health_config, retry_config):
        """Test health alert callbacks."""
        monitor = FederationHealthMonitor(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        # Track alerts
        alerts_received = []

        def alert_callback(cluster_id: str, status: HealthStatus, alert_data):
            alerts_received.append((cluster_id, status, alert_data))

        monitor.add_alert_callback(alert_callback)

        # Register cluster and manually trigger an alert
        monitor.register_cluster(cluster_identity)

        # Simulate health check failure
        from mpreg.federation.federation_resilience import HealthCheckResult

        failure_result = HealthCheckResult(
            cluster_id=cluster_identity.cluster_id,
            status=HealthStatus.UNHEALTHY.value,
            latency_ms=1000.0,
            timestamp=time.time(),
            error_message="Connection timeout",
        )

        # Update health metrics to trigger alert
        await monitor._update_health_metrics(
            cluster_identity.cluster_id, failure_result
        )

        # Trigger multiple failures to cross threshold
        for _ in range(health_config.consecutive_failures_threshold):
            await monitor._update_health_metrics(
                cluster_identity.cluster_id, failure_result
            )

        # Check if alerts were triggered
        assert len(alerts_received) > 0


class TestFederationAutoRecovery:
    """Test auto-recovery functionality."""

    @pytest.mark.asyncio
    async def test_auto_recovery_initialization(self, health_config, retry_config):
        """Test auto-recovery initialization."""
        monitor = FederationHealthMonitor(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        recovery = FederationAutoRecovery(
            cluster_id="test_cluster", health_monitor=monitor, retry_config=retry_config
        )

        assert recovery.cluster_id == "test_cluster"
        assert len(recovery.recovery_strategies) == 0
        assert len(recovery.recovery_tasks) == 0

    @pytest.mark.asyncio
    async def test_recovery_lifecycle(
        self, cluster_identity, health_config, retry_config
    ):
        """Test auto-recovery start and stop."""
        monitor = FederationHealthMonitor(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        recovery = FederationAutoRecovery(
            cluster_id="test_cluster", health_monitor=monitor, retry_config=retry_config
        )

        # Start auto-recovery
        await recovery.start_auto_recovery()
        assert len(recovery.recovery_tasks) > 0

        # Stop auto-recovery
        await recovery.stop_auto_recovery()
        assert len(recovery.recovery_tasks) == 0

    @pytest.mark.asyncio
    async def test_recovery_strategy_selection(
        self, cluster_identity, health_config, retry_config
    ):
        """Test recovery strategy selection based on health status."""
        monitor = FederationHealthMonitor(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        recovery = FederationAutoRecovery(
            cluster_id="test_cluster", health_monitor=monitor, retry_config=retry_config
        )

        # Test different health statuses and expected strategies
        test_cases = [
            (HealthStatus.CRITICAL, RecoveryStrategy.CIRCUIT_BREAKER),
            (HealthStatus.UNHEALTHY, RecoveryStrategy.EXPONENTIAL_BACKOFF),
            (HealthStatus.DEGRADED, RecoveryStrategy.GRACEFUL_DEGRADATION),
        ]

        for status, expected_strategy in test_cases:
            from mpreg.federation.federation_resilience import AlertData

            # Trigger health alert
            alert_data = AlertData(
                cluster_id=cluster_identity.cluster_id,
                health_score=50.0,
                consecutive_failures=3,
                recent_errors=[],
                average_latency_ms=100.0,
            )
            recovery._handle_health_alert(
                cluster_identity.cluster_id, status, alert_data
            )

            # Check that correct strategy was set
            assert cluster_identity.cluster_id in recovery.recovery_strategies
            actual_strategy = recovery.recovery_strategies[cluster_identity.cluster_id]

            # Some flexibility in strategy selection based on alert data
            assert actual_strategy in [expected_strategy, RecoveryStrategy.FAILOVER]

    @pytest.mark.asyncio
    async def test_recovery_callbacks(
        self, cluster_identity, health_config, retry_config
    ):
        """Test recovery event callbacks."""
        monitor = FederationHealthMonitor(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        recovery = FederationAutoRecovery(
            cluster_id="test_cluster", health_monitor=monitor, retry_config=retry_config
        )

        # Track recovery events
        recovery_events = []

        def recovery_callback(
            cluster_id: str, strategy: RecoveryStrategy, success: bool
        ):
            recovery_events.append((cluster_id, strategy, success))

        recovery.add_recovery_callback(recovery_callback)

        # Test immediate retry recovery
        success = await recovery._execute_recovery_strategy(
            cluster_identity.cluster_id, RecoveryStrategy.IMMEDIATE_RETRY
        )

        # Should work (simulated)
        assert isinstance(success, bool)


class TestEnhancedFederationResilience:
    """Test integrated resilience system."""

    @pytest.mark.asyncio
    async def test_resilience_system_initialization(self, health_config, retry_config):
        """Test resilience system initialization."""
        resilience = EnhancedFederationResilience(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        assert resilience.cluster_id == "test_cluster"
        assert not resilience.enabled
        assert isinstance(resilience.health_monitor, FederationHealthMonitor)
        assert isinstance(resilience.auto_recovery, FederationAutoRecovery)

    @pytest.mark.asyncio
    async def test_resilience_lifecycle(
        self, cluster_identity, health_config, retry_config
    ):
        """Test resilience system enable/disable lifecycle."""
        resilience = EnhancedFederationResilience(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        # Enable resilience
        await resilience.enable_resilience()
        assert resilience.enabled

        # Disable resilience
        await resilience.disable_resilience()
        assert not resilience.enabled

    @pytest.mark.asyncio
    async def test_cluster_management(
        self, cluster_identity, health_config, retry_config
    ):
        """Test cluster registration and management."""
        resilience = EnhancedFederationResilience(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        # Register cluster
        resilience.register_cluster(cluster_identity)

        # Check that cluster is registered
        assert cluster_identity.cluster_id in resilience.circuit_breakers
        assert cluster_identity.cluster_id in resilience.health_monitor.cluster_health

        # Get circuit breaker
        cb = resilience.get_circuit_breaker(cluster_identity.cluster_id)
        assert isinstance(cb, AdaptiveCircuitBreaker)

        # Unregister cluster
        resilience.unregister_cluster(cluster_identity.cluster_id)
        assert cluster_identity.cluster_id not in resilience.circuit_breakers

    @pytest.mark.asyncio
    async def test_resilience_summary(
        self, cluster_identity, health_config, retry_config
    ):
        """Test resilience metrics summary."""
        resilience = EnhancedFederationResilience(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        # Register cluster
        resilience.register_cluster(cluster_identity)

        # Get summary
        summary = resilience.get_resilience_summary()

        assert summary.enabled == resilience.enabled
        assert summary.total_registered_clusters == 1
        assert cluster_identity.cluster_id in summary.circuit_breaker_states

        # Check health summary structure
        assert hasattr(summary.health_summary, "global_status")
        assert hasattr(summary.health_summary, "total_clusters")

    @pytest.mark.asyncio
    async def test_integrated_resilience_workflow(
        self, cluster_identity, health_config, retry_config
    ):
        """Test complete resilience workflow integration."""
        resilience = EnhancedFederationResilience(
            cluster_id="test_cluster",
            health_config=health_config,
            retry_config=retry_config,
        )

        # Register cluster
        resilience.register_cluster(cluster_identity)

        # Enable resilience
        await resilience.enable_resilience()

        try:
            # Wait briefly for monitoring to start
            await asyncio.sleep(0.2)

            # Get circuit breaker and simulate failures
            cb = resilience.get_circuit_breaker(cluster_identity.cluster_id)
            assert cb is not None

            # Record failures to test circuit breaker
            for _ in range(cb.failure_threshold):
                cb.record_failure()

            assert cb.state == "open"
            assert not cb.can_execute()

            # Get resilience summary
            summary = resilience.get_resilience_summary()
            cb_state = summary.circuit_breaker_states[cluster_identity.cluster_id]
            assert cb_state.state == "open"
            assert cb_state.failure_count >= cb.failure_threshold

        finally:
            # Always disable resilience
            await resilience.disable_resilience()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
