"""
Tests for Federation Performance Metrics System.

This module provides comprehensive tests for the performance metrics collection,
alerting, and monitoring capabilities of the federated MPREG system.
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from mpreg.core.statistics import (
    AlertsSummary,
    ClusterPerformanceSummary,
    CollectionStatus,
    PerformanceSummary,
)
from mpreg.federation.performance_metrics import (
    AlertSeverity,
    ClusterMetrics,
    PerformanceAlert,
    PerformanceMetricsService,
    PerformanceThresholds,
    create_development_thresholds,
    create_performance_metrics_service,
    create_production_thresholds,
)


class TestPerformanceThresholds:
    """Test performance threshold configurations."""

    def test_default_thresholds(self):
        """Test default threshold values."""
        thresholds = PerformanceThresholds()

        assert thresholds.latency_warning == 100.0
        assert thresholds.latency_error == 500.0
        assert thresholds.latency_critical == 1000.0

        assert thresholds.throughput_warning == 10.0
        assert thresholds.throughput_error == 5.0
        assert thresholds.throughput_critical == 1.0

        assert thresholds.error_rate_warning == 1.0
        assert thresholds.error_rate_error == 5.0
        assert thresholds.error_rate_critical == 10.0

        assert thresholds.health_score_warning == 80.0
        assert thresholds.health_score_error == 60.0
        assert thresholds.health_score_critical == 40.0

    def test_production_thresholds(self):
        """Test production threshold configuration."""
        thresholds = create_production_thresholds()

        # Production should have stricter thresholds
        assert thresholds.latency_warning == 50.0
        assert thresholds.latency_error == 200.0
        assert thresholds.latency_critical == 500.0

        assert thresholds.throughput_warning == 50.0
        assert thresholds.error_rate_warning == 0.5
        assert thresholds.health_score_warning == 85.0

    def test_development_thresholds(self):
        """Test development threshold configuration."""
        thresholds = create_development_thresholds()

        # Development should have more relaxed thresholds
        assert thresholds.latency_warning == 200.0
        assert thresholds.latency_critical == 2000.0
        assert thresholds.error_rate_warning == 5.0
        assert thresholds.health_score_warning == 70.0


class TestClusterMetrics:
    """Test cluster metrics data structures."""

    def test_cluster_metrics_creation(self):
        """Test creating cluster metrics."""
        metrics = ClusterMetrics(
            cluster_id="test-cluster",
            cluster_name="Test Cluster",
            region="us-west-2",
            avg_latency_ms=50.0,
            throughput_rps=100.0,
            error_rate_percent=1.5,
            health_score=85.0,
            cpu_usage_percent=45.0,
            memory_usage_percent=60.0,
            active_connections=25,
            messages_sent=1000,
            messages_received=950,
            messages_failed=50,
        )

        assert metrics.cluster_id == "test-cluster"
        assert metrics.cluster_name == "Test Cluster"
        assert metrics.region == "us-west-2"
        assert metrics.avg_latency_ms == 50.0
        assert metrics.throughput_rps == 100.0
        assert metrics.error_rate_percent == 1.5
        assert metrics.health_score == 85.0
        assert metrics.cpu_usage_percent == 45.0
        assert metrics.memory_usage_percent == 60.0
        assert metrics.active_connections == 25
        assert metrics.messages_sent == 1000
        assert metrics.messages_received == 950
        assert metrics.messages_failed == 50

    def test_cluster_metrics_defaults(self):
        """Test cluster metrics with default values."""
        metrics = ClusterMetrics(
            cluster_id="minimal-cluster",
            cluster_name="Minimal Cluster",
            region="local",
        )

        assert metrics.avg_latency_ms == 0.0
        assert metrics.throughput_rps == 0.0
        assert metrics.error_rate_percent == 0.0
        assert metrics.health_score == 100.0
        assert metrics.cpu_usage_percent == 0.0
        assert metrics.active_connections == 0
        assert metrics.messages_sent == 0


class TestPerformanceAlert:
    """Test performance alert data structures."""

    def test_alert_creation(self):
        """Test creating performance alerts."""
        alert = PerformanceAlert(
            alert_id="test-cluster_latency_warning",
            severity=AlertSeverity.WARNING,
            metric_name="avg_latency_ms",
            current_value=150.0,
            threshold_value=100.0,
            cluster_id="test-cluster",
            node_id=None,
            timestamp=time.time(),
            message="High latency in test-cluster: 150.0ms",
        )

        assert alert.alert_id == "test-cluster_latency_warning"
        assert alert.cluster_id == "test-cluster"
        assert alert.severity == AlertSeverity.WARNING
        assert alert.metric_name == "avg_latency_ms"
        assert alert.current_value == 150.0
        assert alert.threshold_value == 100.0
        assert "High latency" in alert.message
        assert alert.cluster_id == "test-cluster"
        assert alert.resolved is False

    def test_alert_severity_levels(self):
        """Test different alert severity levels."""
        severities = [
            AlertSeverity.INFO,
            AlertSeverity.WARNING,
            AlertSeverity.CRITICAL,
            AlertSeverity.CRITICAL,
        ]

        for severity in severities:
            alert = PerformanceAlert(
                alert_id=f"test_{severity.value}",
                severity=severity,
                metric_name="test_metric",
                current_value=100.0,
                threshold_value=50.0,
                cluster_id="test-cluster",
                node_id=None,
                timestamp=time.time(),
                message=f"Test {severity.value} alert",
            )
            assert alert.severity == severity


class TestPerformanceMetricsService:
    """Test the main performance metrics service."""

    def test_service_creation(self):
        """Test creating a performance metrics service."""
        service = PerformanceMetricsService(
            collection_interval=30.0,
            retention_hours=12,
        )

        assert service.collection_interval == 30.0
        assert service.retention_hours == 12
        assert service.collecting is False
        assert len(service.collectors) == 0
        assert len(service.cluster_metrics) == 0
        assert len(service.alert_callbacks) == 0

    def test_service_creation_with_factory(self):
        """Test creating service with factory function."""
        thresholds = create_production_thresholds()
        service = create_performance_metrics_service(
            collection_interval=15.0,
            retention_hours=48,
            custom_thresholds=thresholds,
        )

        assert service.collection_interval == 15.0
        assert service.retention_hours == 48
        assert service.thresholds == thresholds

    def test_add_collector(self):
        """Test adding metrics collectors."""
        service = PerformanceMetricsService()

        # Mock collector
        collector = MagicMock()
        collector.collect_metrics = AsyncMock()
        collector.health_check = AsyncMock(return_value=True)

        service.add_collector(collector)

        assert len(service.collectors) == 1
        assert service.collectors[0] == collector

    def test_add_alert_callback(self):
        """Test adding alert callbacks."""
        service = PerformanceMetricsService()

        callback = MagicMock()
        service.add_alert_callback(callback)

        assert len(service.alert_callbacks) == 1
        assert service.alert_callbacks[0] == callback

    def test_record_measurements(self):
        """Test recording performance measurements."""
        service = PerformanceMetricsService()

        # Record some measurements
        service.record_latency("test-cluster", 45.0)
        service.record_latency("test-cluster", 55.0)
        service.record_throughput("test-cluster", 100.0)
        service.record_error("test-cluster")
        service.record_error("test-cluster")

        # Check measurements were recorded
        assert len(service.latency_measurements["test-cluster"]) == 2
        assert list(service.latency_measurements["test-cluster"]) == [45.0, 55.0]
        assert len(service.throughput_measurements["test-cluster"]) == 1
        assert list(service.throughput_measurements["test-cluster"]) == [100.0]
        assert service.error_counts["test-cluster"] == 2

    @pytest.mark.asyncio
    async def test_cluster_metrics_update(self):
        """Test updating cluster metrics."""
        service = PerformanceMetricsService()

        metrics = ClusterMetrics(
            cluster_id="test-cluster",
            cluster_name="Test Cluster",
            region="us-west-2",
            avg_latency_ms=75.0,
            throughput_rps=150.0,
            health_score=90.0,
        )

        await service._update_cluster_metrics(metrics)

        # Check metrics were stored
        stored_metrics = service.get_cluster_metrics("test-cluster")
        assert stored_metrics is not None
        assert stored_metrics.cluster_id == "test-cluster"
        assert stored_metrics.avg_latency_ms == 75.0
        assert stored_metrics.throughput_rps == 150.0
        assert stored_metrics.health_score == 90.0

        # Check history was updated
        history = service.get_cluster_history("test-cluster")
        assert len(history) == 1
        assert history[0] == metrics

    @pytest.mark.asyncio
    async def test_federation_metrics_generation(self):
        """Test generating federation-wide metrics."""
        service = PerformanceMetricsService()

        # Add multiple cluster metrics
        clusters = [
            ClusterMetrics(
                cluster_id="cluster-1",
                cluster_name="Cluster 1",
                region="us-west-2",
                avg_latency_ms=50.0,
                throughput_rps=100.0,
                health_score=90.0,
                messages_sent=1000,
                messages_received=950,
            ),
            ClusterMetrics(
                cluster_id="cluster-2",
                cluster_name="Cluster 2",
                region="eu-central-1",
                avg_latency_ms=75.0,
                throughput_rps=80.0,
                health_score=85.0,
                messages_sent=800,
                messages_received=760,
            ),
            ClusterMetrics(
                cluster_id="cluster-3",
                cluster_name="Cluster 3",
                region="ap-southeast-1",
                avg_latency_ms=100.0,
                throughput_rps=60.0,
                health_score=70.0,
                messages_sent=600,
                messages_received=570,
            ),
        ]

        for metrics in clusters:
            await service._update_cluster_metrics(metrics)

        # Generate federation metrics
        await service._generate_federation_metrics()

        # Check federation metrics
        fed_metrics = service.get_federation_metrics()
        assert fed_metrics is not None
        assert fed_metrics.total_clusters == 3
        assert fed_metrics.healthy_clusters == 2  # clusters with health_score >= 80
        assert (
            fed_metrics.degraded_clusters == 1
        )  # clusters with 60 <= health_score < 80
        assert fed_metrics.unhealthy_clusters == 0  # clusters with health_score < 60

        # Check aggregated values
        assert fed_metrics.federation_avg_latency_ms == 75.0  # (50 + 75 + 100) / 3
        assert fed_metrics.federation_total_throughput_rps == 240.0  # 100 + 80 + 60
        assert (
            abs(fed_metrics.federation_health_score - 81.67) < 0.01
        )  # (90 + 85 + 70) / 3 ≈ 81.67
        assert fed_metrics.total_messages_sent == 2400  # 1000 + 800 + 600
        assert fed_metrics.total_messages_received == 2280  # 950 + 760 + 570

    @pytest.mark.asyncio
    async def test_threshold_checking_and_alerting(self):
        """Test threshold checking and alert generation."""
        # Use stricter thresholds for testing
        thresholds = PerformanceThresholds(
            latency_warning=50.0,
            latency_error=100.0,
            latency_critical=200.0,
            throughput_warning=100.0,
            throughput_error=50.0,
            throughput_critical=10.0,
            health_score_warning=80.0,
            health_score_error=60.0,
            health_score_critical=40.0,
        )

        service = PerformanceMetricsService(thresholds=thresholds)
        alert_callback = MagicMock()
        service.add_alert_callback(alert_callback)

        # Add cluster with problematic metrics
        problematic_metrics = ClusterMetrics(
            cluster_id="problem-cluster",
            cluster_name="Problem Cluster",
            region="us-west-2",
            avg_latency_ms=150.0,  # Above error threshold (100.0)
            throughput_rps=25.0,  # Below warning threshold (100.0)
            health_score=70.0,  # Below warning threshold (80.0)
            error_rate_percent=3.0,  # Above warning threshold but not checked in defaults
        )

        await service._update_cluster_metrics(problematic_metrics)
        await service._check_thresholds()

        # Check that alerts were generated
        active_alerts = service.get_active_alerts()
        assert len(active_alerts) > 0

        # Check for specific alert types
        alert_types = {alert.metric_name for alert in active_alerts}
        assert "avg_latency_ms" in alert_types
        assert "health_score" in alert_types

        # Check that callbacks were called
        assert alert_callback.call_count > 0

    @pytest.mark.asyncio
    async def test_alert_resolution(self):
        """Test alert resolution."""
        service = PerformanceMetricsService()

        # Create and trigger an alert
        alert = PerformanceAlert(
            alert_id="test-alert",
            severity=AlertSeverity.WARNING,
            metric_name="test_metric",
            current_value=100.0,
            threshold_value=50.0,
            cluster_id="test-cluster",
            node_id=None,
            timestamp=time.time(),
            message="Test alert",
        )

        await service._trigger_alert(alert)

        # Check alert is active
        active_alerts = service.get_active_alerts()
        assert len(active_alerts) == 1
        assert active_alerts[0].alert_id == "test-alert"

        # Resolve the alert
        resolved = service.resolve_alert("test-alert")
        assert resolved is True

        # Check alert is no longer active
        active_alerts = service.get_active_alerts()
        assert len(active_alerts) == 0

        # Try to resolve non-existent alert
        resolved = service.resolve_alert("non-existent")
        assert resolved is False

    @pytest.mark.asyncio
    async def test_collection_lifecycle(self):
        """Test starting and stopping metrics collection."""
        service = PerformanceMetricsService(
            collection_interval=0.1
        )  # Very fast for testing

        # Start collection
        await service.start_collection()
        assert service.collecting is True
        assert service.collection_task is not None

        # Let it run briefly
        await asyncio.sleep(0.2)

        # Stop collection
        await service.stop_collection()
        assert service.collecting is False

    def test_performance_summary(self):
        """Test generating performance summary."""
        service = PerformanceMetricsService()

        # Add some test data
        metrics1 = ClusterMetrics(
            cluster_id="cluster-1",
            cluster_name="Cluster 1",
            region="us-west-2",
            avg_latency_ms=50.0,
            throughput_rps=100.0,
            health_score=90.0,
            error_rate_percent=1.0,
        )

        metrics2 = ClusterMetrics(
            cluster_id="cluster-2",
            cluster_name="Cluster 2",
            region="eu-central-1",
            avg_latency_ms=75.0,
            throughput_rps=80.0,
            health_score=85.0,
            error_rate_percent=2.0,
        )

        service.cluster_metrics["cluster-1"] = metrics1
        service.cluster_metrics["cluster-2"] = metrics2

        # Add some alerts
        alert1 = PerformanceAlert(
            alert_id="alert-1",
            severity=AlertSeverity.WARNING,
            metric_name="latency",
            current_value=50.0,
            threshold_value=40.0,
            cluster_id="cluster-1",
            node_id=None,
            timestamp=time.time(),
            message="Test warning",
        )

        alert2 = PerformanceAlert(
            alert_id="alert-2",
            severity=AlertSeverity.ERROR,
            metric_name="throughput",
            current_value=80.0,
            threshold_value=100.0,
            cluster_id="cluster-2",
            node_id=None,
            timestamp=time.time(),
            message="Test error",
        )

        service.active_alerts["alert-1"] = alert1
        service.active_alerts["alert-2"] = alert2

        # Generate summary
        summary = service.get_performance_summary()

        # Verify summary structure and content
        assert isinstance(summary, PerformanceSummary)
        assert summary.cluster_count == 2
        assert len(summary.clusters) == 2
        assert summary.active_alerts == 2

        # Check cluster summaries
        assert "cluster-1" in summary.clusters
        assert "cluster-2" in summary.clusters

        cluster1_summary = summary.clusters["cluster-1"]
        assert isinstance(cluster1_summary, ClusterPerformanceSummary)
        assert cluster1_summary.cluster_id == "cluster-1"
        assert cluster1_summary.avg_latency_ms == 50.0
        assert cluster1_summary.throughput_rps == 100.0
        assert cluster1_summary.health_score == 90.0
        assert cluster1_summary.error_rate_percent == 1.0

        # Check alerts summary
        assert isinstance(summary.alerts_by_severity, AlertsSummary)
        assert summary.alerts_by_severity.warning == 1
        assert summary.alerts_by_severity.error == 1
        assert summary.alerts_by_severity.critical == 0
        assert summary.alerts_by_severity.total == 2

        # Check collection status
        assert isinstance(summary.collection_status, CollectionStatus)
        assert summary.collection_status.collecting is False
        assert summary.collection_status.collectors == 0
        assert summary.collection_status.collection_interval == 30.0

    def test_historical_data_retrieval(self):
        """Test retrieving historical metrics data."""
        service = PerformanceMetricsService()

        # Add historical data with different timestamps
        current_time = time.time()

        # Recent metrics (within 1 hour)
        recent_metrics = ClusterMetrics(
            cluster_id="test-cluster",
            cluster_name="Test Cluster",
            region="us-west-2",
            avg_latency_ms=50.0,
            last_seen=current_time - 1800,  # 30 minutes ago
        )

        # Old metrics (beyond 1 hour)
        old_metrics = ClusterMetrics(
            cluster_id="test-cluster",
            cluster_name="Test Cluster",
            region="us-west-2",
            avg_latency_ms=75.0,
            last_seen=current_time - 7200,  # 2 hours ago
        )

        service.cluster_metrics_history["test-cluster"].append(old_metrics)
        service.cluster_metrics_history["test-cluster"].append(recent_metrics)

        # Get recent history (1 hour)
        recent_history = service.get_cluster_history("test-cluster", hours=1)
        assert len(recent_history) == 1
        assert recent_history[0].avg_latency_ms == 50.0

        # Get extended history (3 hours)
        extended_history = service.get_cluster_history("test-cluster", hours=3)
        assert len(extended_history) == 2

        # Get history for non-existent cluster
        no_history = service.get_cluster_history("non-existent", hours=1)
        assert len(no_history) == 0


class MockMetricsCollector:
    """Mock metrics collector for testing."""

    def __init__(self, cluster_data: dict[str, ClusterMetrics]):
        self.cluster_data = cluster_data

    async def collect_metrics(self, cluster_id: str) -> ClusterMetrics:
        """Return mock metrics for the specified cluster."""
        if cluster_id in self.cluster_data:
            return self.cluster_data[cluster_id]
        raise ValueError(f"No data for cluster: {cluster_id}")

    async def health_check(self) -> bool:
        """Mock health check."""
        return True


class TestMetricsCollectorIntegration:
    """Test integration with metrics collectors."""

    @pytest.mark.asyncio
    async def test_collector_integration(self):
        """Test integration with mock collector."""
        service = PerformanceMetricsService()

        # Create mock collector with test data
        test_metrics = {
            "test-cluster": ClusterMetrics(
                cluster_id="test-cluster",
                cluster_name="Test Cluster",
                region="us-west-2",
                avg_latency_ms=65.0,
                throughput_rps=120.0,
                health_score=88.0,
            )
        }

        collector = MockMetricsCollector(test_metrics)
        service.add_collector(collector)

        # Add the cluster to the service first
        service.cluster_metrics["test-cluster"] = test_metrics["test-cluster"]

        # Collect metrics
        await service._collect_all_metrics()

        # Verify metrics were collected and stored
        stored_metrics = service.get_cluster_metrics("test-cluster")
        assert stored_metrics is not None
        assert stored_metrics.avg_latency_ms == 65.0
        assert stored_metrics.throughput_rps == 120.0
        assert stored_metrics.health_score == 88.0


# Integration test that demonstrates the complete workflow
@pytest.mark.asyncio
async def test_complete_metrics_workflow():
    """Test the complete metrics collection and alerting workflow."""
    # Create service with custom thresholds
    thresholds = PerformanceThresholds(
        latency_warning=30.0,
        latency_error=60.0,
        health_score_warning=85.0,
    )

    service = create_performance_metrics_service(
        collection_interval=0.1,  # Fast for testing
        custom_thresholds=thresholds,
    )

    # Set up alert tracking
    triggered_alerts = []

    def alert_handler(alert: PerformanceAlert):
        triggered_alerts.append(alert)

    service.add_alert_callback(alert_handler)

    # Create test clusters with various health states
    healthy_cluster = ClusterMetrics(
        cluster_id="healthy-cluster",
        cluster_name="Healthy Cluster",
        region="us-west-2",
        avg_latency_ms=25.0,  # Below warning threshold
        throughput_rps=150.0,
        health_score=95.0,  # Above warning threshold
        error_rate_percent=0.5,
    )

    degraded_cluster = ClusterMetrics(
        cluster_id="degraded-cluster",
        cluster_name="Degraded Cluster",
        region="eu-central-1",
        avg_latency_ms=45.0,  # Above warning, below error
        throughput_rps=80.0,
        health_score=75.0,  # Below warning threshold
        error_rate_percent=2.0,
    )

    critical_cluster = ClusterMetrics(
        cluster_id="critical-cluster",
        cluster_name="Critical Cluster",
        region="ap-southeast-1",
        avg_latency_ms=80.0,  # Above error threshold
        throughput_rps=20.0,
        health_score=70.0,  # Below warning threshold
        error_rate_percent=8.0,
    )

    # Update metrics
    await service._update_cluster_metrics(healthy_cluster)
    await service._update_cluster_metrics(degraded_cluster)
    await service._update_cluster_metrics(critical_cluster)

    # Generate federation metrics
    await service._generate_federation_metrics()

    # Check thresholds and trigger alerts
    await service._check_thresholds()

    # Verify federation metrics
    fed_metrics = service.get_federation_metrics()
    assert fed_metrics is not None
    assert fed_metrics.total_clusters == 3
    assert (
        fed_metrics.healthy_clusters == 1
    )  # Only healthy-cluster has health_score >= 85
    assert (
        fed_metrics.degraded_clusters == 2
    )  # Both degraded and critical have health_score < 85

    # Check that we have calculated averages correctly
    expected_avg_latency = (25.0 + 45.0 + 80.0) / 3  # 50.0
    assert abs(fed_metrics.federation_avg_latency_ms - expected_avg_latency) < 0.1

    expected_total_throughput = 150.0 + 80.0 + 20.0  # 250.0
    assert fed_metrics.federation_total_throughput_rps == expected_total_throughput

    # Verify alerts were triggered
    assert len(triggered_alerts) > 0

    # Check for specific alert conditions
    alert_metrics = {alert.metric_name for alert in triggered_alerts}
    assert "avg_latency_ms" in alert_metrics  # Should have latency alerts
    assert "health_score" in alert_metrics  # Should have health alerts

    # Verify alert severities
    alert_severities = {alert.severity for alert in triggered_alerts}
    assert AlertSeverity.WARNING in alert_severities
    assert AlertSeverity.CRITICAL in alert_severities

    # Get comprehensive summary
    summary = service.get_performance_summary()
    assert isinstance(summary, PerformanceSummary)
    assert summary.cluster_count == 3
    assert summary.active_alerts > 0
    assert summary.alerts_by_severity.total > 0

    # Verify cluster summaries
    assert len(summary.clusters) == 3
    for cluster_id in ["healthy-cluster", "degraded-cluster", "critical-cluster"]:
        assert cluster_id in summary.clusters
        cluster_summary = summary.clusters[cluster_id]
        assert isinstance(cluster_summary, ClusterPerformanceSummary)
        assert cluster_summary.cluster_id == cluster_id
